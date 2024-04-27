use anyhow::Result;
use electrs_rocksdb as rocksdb;
use itertools::Itertools;
use rlp;
use rocksdb::WriteBatchWithTransaction;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use wasmtime::{Caller, Linker, Store, StoreLimits, StoreLimitsBuilder};

type SerBlock = Vec<u8>;
pub trait BatchLike {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V);
}
pub trait KeyValueStoreLike {
    type Error: std::fmt::Debug;
    type Batch: BatchLike;
    fn write(&self, batch: Self::Batch) -> Result<(), Self::Error>;
    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Self::Error>;
    fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Self::Error>;
    fn put<K, V>(&self, key: K, value: V) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;
}

pub struct RocksDBBatch(pub WriteBatchWithTransaction<false>);

impl BatchLike for RocksDBBatch {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        self.0.put(key, value);
    }
}

//const TIP_KEY: &[u8] = b"T";
//const HEADERS_CF: &str = "headers";

pub struct State {
    limits: StoreLimits,
}

pub struct MetashrewRuntimeContext<T: KeyValueStoreLike> {
    pub db: T,
    pub height: u32,
    pub block: SerBlock,
}

impl<T: KeyValueStoreLike> MetashrewRuntimeContext<T> {
    fn new(db: T, height: u32, block: SerBlock) -> Self {
        return Self {
            db: db,
            height: height,
            block: block,
        };
    }
}

pub struct MetashrewRuntime<T: KeyValueStoreLike + 'static> {
    pub context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
    pub engine: wasmtime::Engine,
    pub wasmstore: wasmtime::Store<State>,
    pub module: wasmtime::Module,
    pub linker: wasmtime::Linker<State>,
    pub instance: wasmtime::Instance,
}

impl State {
    pub fn new() -> Self {
        State {
            limits: StoreLimitsBuilder::new()
                .memories(usize::MAX)
                .tables(usize::MAX)
                .instances(usize::MAX)
                .build(),
        }
    }
}

pub fn db_make_list_key(v: &Vec<u8>, index: u32) -> Vec<u8> {
    let mut entry = v.clone();
    let index_bits: Vec<u8> = index.to_le_bytes().try_into().unwrap();
    entry.extend(index_bits);
    return entry;
}

pub fn db_make_length_key(key: &Vec<u8>) -> Vec<u8> {
    return db_make_list_key(key, u32::MAX);
}

pub fn db_make_updated_key(key: &Vec<u8>) -> Vec<u8> {
    return key.clone();
}

pub fn u32_to_vec(v: u32) -> Vec<u8> {
    return v.to_le_bytes().try_into().unwrap();
}

pub fn read_arraybuffer_as_vec(data: &[u8], data_start: i32) -> Vec<u8> {
    let len = u32::from_le_bytes(
        (data[((data_start - 4) as usize)..(data_start as usize)])
            .try_into()
            .unwrap(),
    );
    return Vec::<u8>::from(&data[(data_start as usize)..(((data_start as u32) + len) as usize)]);
}

pub fn db_annotate_value(v: &Vec<u8>, block_height: u32) -> Vec<u8> {
    let mut entry: Vec<u8> = v.clone();
    let height: Vec<u8> = block_height.to_le_bytes().try_into().unwrap();
    entry.extend(height);
    return entry;
}

impl<T: KeyValueStoreLike> MetashrewRuntime<T>
where
    T: KeyValueStoreLike<Batch = RocksDBBatch>,
    T: Sync + Send,
{
    pub fn load(indexer: PathBuf, store: T) -> Result<Self> {
        let engine = wasmtime::Engine::default();
        let module = wasmtime::Module::from_file(&engine, indexer.into_os_string()).unwrap();
        let mut linker = Linker::<State>::new(&engine);
        let mut wasmstore = Store::<State>::new(&engine, State::new());
        let context = Arc::<Mutex<MetashrewRuntimeContext<T>>>::new(Mutex::<
            MetashrewRuntimeContext<T>,
        >::new(
            MetashrewRuntimeContext::<T>::new(store, 0, vec![]),
        ));
        {
            wasmstore.limiter(|state| &mut state.limits)
        }
        {
            Self::setup_linker(context.clone(), &mut linker);
            Self::setup_linker_indexer(context.clone(), &mut linker);
        }
        let instance = linker.instantiate(&mut wasmstore, &module).unwrap();
        return Ok(MetashrewRuntime {
            wasmstore: wasmstore,
            engine: engine,
            module: module,
            linker: linker,
            context: context,
            instance: instance,
        });
    }
    pub fn db_create_empty_update_list(batch: &mut T::Batch, height: u32) {
        let height_vec: Vec<u8> = height.to_le_bytes().try_into().unwrap();
        let key: Vec<u8> = db_make_length_key(&db_make_updated_key(&height_vec));
        let value_vec: Vec<u8> = (0 as u32).to_le_bytes().try_into().unwrap();
        batch.put(&key, &value_vec);
    }
    pub fn run(&mut self) -> Result<(), anyhow::Error> {
        let start = self
            .instance
            .get_typed_func::<(), ()>(&mut self.wasmstore, "_start")
            .unwrap();
        Self::handle_reorg(self.context.clone());
        start.call(&mut self.wasmstore, ())
    }

    pub fn check_latest_block_for_reorg(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        height: u32,
    ) -> u32 {
        match context
            .lock()
            .unwrap()
            .db
            .get(db_make_length_key(&db_make_updated_key(&u32_to_vec(
                height as u32,
            ))))
            .unwrap()
        {
            Some(_v) => Self::check_latest_block_for_reorg(context.clone(), height + 1),
            None => return height,
        }
    }

    pub fn db_length_at_key(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        length_key: &Vec<u8>,
    ) -> u32 {
        return match context.lock().unwrap().db.get(length_key).unwrap() {
            Some(v) => u32::from_le_bytes(v.try_into().unwrap()),
            None => 0,
        };
    }

    pub fn db_updated_keys_for_block(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        height: u32,
    ) -> HashSet<Vec<u8>> {
        let key: Vec<u8> = db_make_length_key(&db_make_updated_key(&u32_to_vec(height)));
        let length: i32 = Self::db_length_at_key(context.clone(), &key) as i32;
        let mut i: i32 = 0;
        let mut set: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
        while i < length {
            set.insert(
                context
                    .lock()
                    .unwrap()
                    .db
                    .get(&db_make_list_key(&key, i as u32))
                    .unwrap()
                    .unwrap(),
            );
            i = i + 1;
        }
        return set;
    }

    pub fn db_updated_keys_for_block_range(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        from: u32,
        to: u32,
    ) -> HashSet<Vec<u8>> {
        let mut i = from;
        let mut result: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
        while to >= i {
            result.extend(Self::db_updated_keys_for_block(context.clone(), i));
            i = i + 1;
        }
        return result;
    }

    pub fn db_rollback_key(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        key: &Vec<u8>,
        to_block: u32,
    ) {
        let length: i32 = Self::db_length_at_key(context.clone(), &key)
            .try_into()
            .unwrap();
        let mut index: i32 = length - 1;
        let mut end_length: i32 = length;
        while index >= 0 {
            let list_key = db_make_list_key(key, index.try_into().unwrap());
            let _ = match context.lock().unwrap().db.get(&list_key).unwrap() {
                Some(value) => {
                    let value_height: u32 = u32::from_le_bytes(
                        value.as_slice()[(value.len() - 4)..].try_into().unwrap(),
                    );
                    if to_block <= value_height.try_into().unwrap() {
                        context.lock().unwrap().db.delete(&list_key).unwrap();
                        end_length = end_length - 1;
                    } else {
                        break;
                    }
                }
                None => {
                    break;
                }
            };
            index -= 1;
        }
        if end_length != length {
            Self::db_set_length(context.clone(), key, end_length as u32);
        }
    }

    pub fn db_set_length(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        key: &Vec<u8>,
        length: u32,
    ) {
        let length_key = db_make_length_key(key);
        if length == 0 {
            context.lock().unwrap().db.delete(&length_key).unwrap();
            return;
        }
        let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
        context
            .lock()
            .unwrap()
            .db
            .put(&length_key, &new_length_bits)
            .unwrap();
    }

    pub fn handle_reorg(context: Arc<Mutex<MetashrewRuntimeContext<T>>>) {
        let height = { context.lock().unwrap().height };
        let latest: u32 = Self::check_latest_block_for_reorg(context.clone(), height);
        let set: HashSet<Vec<u8>> =
            Self::db_updated_keys_for_block_range(context.clone(), height, latest);
        for key in set.iter() {
            Self::db_rollback_key(context.clone(), &key, height);
        }
    }

    pub fn setup_linker(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        linker: &mut Linker<State>,
    ) {
        let context_ref_len = context.clone();
        let context_ref_input = context.clone();
        linker
            .func_wrap(
                "env",
                "__host_len",
                move |mut _caller: Caller<'_, State>| -> i32 {
                    return context_ref_len.lock().unwrap().block.len() as i32 + 4;
                },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "__load_input",
                move |mut caller: Caller<'_, State>, data_start: i32| {
                    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                    let (input, height) = {
                        let ref_copy = context_ref_input.clone();
                        let ctx = ref_copy.lock().unwrap();
                        (ctx.block.clone(), ctx.height)
                    };
                    let mut input_clone: Vec<u8> =
                        <Vec<u8> as TryFrom<[u8; 4]>>::try_from(height.to_le_bytes()).unwrap();
                    input_clone.extend(input.clone());
                    let _ = mem.write(
                        &mut caller,
                        data_start.try_into().unwrap(),
                        input_clone.as_slice(),
                    );
                },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "__log",
                |mut caller: Caller<'_, State>, data_start: i32| {
                    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                    let data = mem.data(&caller);
                    let bytes = read_arraybuffer_as_vec(data, data_start);
                    println!("{}", std::str::from_utf8(bytes.as_slice()).unwrap());
                },
            )
            .unwrap();
        linker
            .func_wrap("env", "abort", |_: i32, _: i32, _: i32, _: i32| {
                panic!("abort!");
            })
            .unwrap();
    }
    pub fn db_append_annotated(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        batch: &mut rocksdb::WriteBatch,
        key: &Vec<u8>,
        value: &Vec<u8>,
        block_height: u32,
    ) {
        let length_key = db_make_length_key(key);
        let length: u32 = Self::db_length_at_key(context.clone(), &length_key);
        let entry = db_annotate_value(value, block_height);

        let entry_key: Vec<u8> = db_make_list_key(key, length);
        batch.put(&entry_key, &entry);
        let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
        batch.put(&length_key, &new_length_bits);
    }
    pub fn db_append(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        batch: &mut WriteBatchWithTransaction<false>,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) {
        let length_key = db_make_length_key(key);
        let length: u32 = Self::db_length_at_key(context.clone(), &length_key);
        let entry_key: Vec<u8> = db_make_list_key(key, length);
        batch.put(&entry_key, &value);
        let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
        batch.put(&length_key, &new_length_bits);
    }

    pub fn setup_linker_indexer(
        context: Arc<Mutex<MetashrewRuntimeContext<T>>>,
        linker: &mut Linker<State>,
    ) {
        let context_ref = context.clone();
        let context_get = context.clone();
        let context_get_len = context.clone();
        linker
            .func_wrap(
                "env",
                "__flush",
                move |mut caller: Caller<'_, State>, encoded: i32| {
                    let height = {
                        let val = context_ref.clone().lock().unwrap().height;
                        val
                    };
                    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                    let data = mem.data(&caller);
                    let encoded_vec = read_arraybuffer_as_vec(data, encoded);
                    let mut batch = RocksDBBatch(rocksdb::WriteBatch::default());
                    let _ = Self::db_create_empty_update_list(&mut batch, height as u32);
                    let decoded: Vec<Vec<u8>> = rlp::decode_list(&encoded_vec);

                    for (k, v) in decoded.iter().tuples() {
                        let k_owned = <Vec<u8> as Clone>::clone(k);
                        let v_owned = <Vec<u8> as Clone>::clone(v);
                        Self::db_append_annotated(
                            context_ref.clone(),
                            &mut batch.0,
                            &k_owned,
                            &v_owned,
                            height as u32,
                        );
                        let update_key: Vec<u8> =
                            <Vec<u8> as TryFrom<[u8; 4]>>::try_from((height as u32).to_le_bytes())
                                .unwrap();
                        Self::db_append(context_ref.clone(), &mut batch.0, &update_key, &k_owned);
                    }
                    debug!(
                        "saving {:?} k/v pairs for block {:?}",
                        decoded.len() / 2,
                        height
                    );
                    context_ref.clone().lock().unwrap().db.write(batch).unwrap();
                },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "__get",
                move |mut caller: Caller<'_, State>, key: i32, value: i32| {
                    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                    let data = mem.data(&caller);
                    let key_vec = read_arraybuffer_as_vec(data, key);
                    let length =
                        Self::db_length_at_key(context_get.clone(), &db_make_length_key(&key_vec));
                    if length != 0 {
                        let indexed_key = db_make_list_key(&key_vec, length - 1);
                        let mut value_vec = (context_get.clone().lock().unwrap().db)
                            .get(&indexed_key)
                            .unwrap()
                            .unwrap();
                        value_vec.truncate(value_vec.len().saturating_sub(4));
                        let _ =
                            mem.write(&mut caller, value.try_into().unwrap(), value_vec.as_slice());
                    }
                },
            )
            .unwrap();
        linker
            .func_wrap(
                "env",
                "__get_len",
                move |mut caller: Caller<'_, State>, key: i32| -> i32 {
                    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                    let data = mem.data(&caller);
                    let key_vec = read_arraybuffer_as_vec(data, key);
                    let length = Self::db_length_at_key(
                        context_get_len.clone(),
                        &db_make_length_key(&key_vec),
                    );
                    if length != 0 {
                        let indexed_key = db_make_list_key(&key_vec, length - 1);
                        let value_vec = context_get_len
                            .clone()
                            .lock()
                            .unwrap()
                            .db
                            .get(&indexed_key)
                            .unwrap()
                            .unwrap();
                        return (value_vec.len() - 4).try_into().unwrap();
                    } else {
                        return 0;
                    }
                },
            )
            .unwrap();
    }
}

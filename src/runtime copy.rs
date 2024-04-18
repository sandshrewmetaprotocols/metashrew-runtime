use anyhow::{Context, Result};
use bitcoin::consensus::{deserialize, serialize};
use bitcoin::{BlockHash, OutPoint, Txid};
use electrs_rocksdb as rocksdb;
use itertools::Itertools;
use rlp;
use rocksdb::{WriteBatch, WriteBatchWithTransaction, DB};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use wasmtime::{Caller, Linker, Store, StoreLimits, StoreLimitsBuilder};

pub(crate) type SerBlock = Vec<u8>;
pub trait KeyValueStoreLike {
    type Error;
    type Batch;
    fn write(&self, batch: Self::Batch) -> Result<(), Self::Error>;
    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Self::Error>;
}

pub struct DBStore(pub DB);

impl KeyValueStoreLike for DBStore {
    type Error = rocksdb::Error;
    type Batch = WriteBatchWithTransaction<false>;
    fn write(&self, batch: Self::Batch) -> Result<(), Self::Error> {
        self.0.write(batch)
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Self::Error> {
        self.0.get(key)
    }
}

const TIP_KEY: &[u8] = b"T";
const HEADERS_CF: &str = "headers";

impl DBStore {
    pub fn get_tip(&self) -> Option<Vec<u8>> {
        self.0
            .get_cf(self.headers_cf(), TIP_KEY)
            .expect("get_tip failed")
    }
    fn headers_cf(&self) -> &rocksdb::ColumnFamily {
        self.0.cf_handle(HEADERS_CF).expect("missing HEADERS_CF")
    }
}

pub struct State {
    limits: StoreLimits,
}

pub struct MetashrewRuntime<T: KeyValueStoreLike + 'static> {
    pub store: &'static T,
    batch_size: usize,
    lookup_limit: Option<usize>,
    // chain: Chain,
    // stats: Stats,
    is_ready: bool,
    flush_needed: bool,
    engine: wasmtime::Engine,
    module: wasmtime::Module,
    wasmstore: Arc<Mutex<wasmtime::Store<State>>>,
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

impl<T: KeyValueStoreLike + 'static> MetashrewRuntime<T> {
    pub(crate) fn load(
        indexer: PathBuf,
        store: &'static T,
        // mut chain: Chain,
        // metrics: &Metrics,
        batch_size: usize,
        lookup_limit: Option<usize>,
        reindex_last_blocks: usize,
    ) -> Result<Self> {
        // let stats = Stats::new(metrics);
        // stats.observe_chain(&chain);
        // stats.observe_db(store);
        let engine = wasmtime::Engine::default();
        let module = wasmtime::Module::from_file(&engine, indexer.into_os_string()).unwrap();
        let wasmstore = Arc::new(Mutex::new(Store::<State>::new(&engine, State::new())));
        {
            (*wasmstore.lock().unwrap()).limiter(|state| &mut state.limits)
        }
        Ok(MetashrewRuntime {
            store,
            batch_size,
            lookup_limit,
            // chain,
            // stats,
            is_ready: false,
            flush_needed: false,
            engine,
            module,
            wasmstore,
        })
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

pub fn check_latest_block_for_reorg(dbstore: &'static DBStore, height: u32) -> u32 {
    match dbstore
        .0
        .get(db_make_length_key(&db_make_updated_key(&u32_to_vec(
            height as u32,
        ))))
        .unwrap()
    {
        Some(_v) => check_latest_block_for_reorg(dbstore, height + 1),
        None => return height,
    }
}

pub fn db_length_at_key(dbstore: &'static DBStore, length_key: &Vec<u8>) -> u32 {
    return match dbstore.0.get(length_key).unwrap() {
        Some(v) => u32::from_le_bytes(v.try_into().unwrap()),
        None => 0,
    };
}

pub fn db_updated_keys_for_block(dbstore: &'static DBStore, height: u32) -> HashSet<Vec<u8>> {
    let key: Vec<u8> = db_make_length_key(&db_make_updated_key(&u32_to_vec(height)));
    let length: i32 = db_length_at_key(dbstore, &key) as i32;
    let mut i: i32 = 0;
    let mut set: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
    while i < length {
        set.insert(
            dbstore
                .0
                .get(&db_make_list_key(&key, i as u32))
                .unwrap()
                .unwrap(),
        );
        i = i + 1;
    }
    return set;
}

pub fn db_updated_keys_for_block_range(
    dbstore: &'static DBStore,
    from: u32,
    to: u32,
) -> HashSet<Vec<u8>> {
    let mut i = from;
    let mut result: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
    while to >= i {
        result.extend(db_updated_keys_for_block(dbstore, i));
        i = i + 1;
    }
    return result;
}

pub fn db_rollback_key(dbstore: &'static DBStore, key: &Vec<u8>, to_block: u32) {
    let length: i32 = db_length_at_key(dbstore, &key).try_into().unwrap();
    let mut index: i32 = length - 1;
    let mut end_length: i32 = length;
    while index >= 0 {
        let list_key = db_make_list_key(key, index.try_into().unwrap());
        let _ = match dbstore.0.get(&list_key).unwrap() {
            Some(value) => {
                let value_height: u32 =
                    u32::from_le_bytes(value.as_slice()[(value.len() - 4)..].try_into().unwrap());
                if to_block <= value_height.try_into().unwrap() {
                    dbstore.0.delete(&list_key).unwrap();
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
        db_set_length(dbstore, key, end_length as u32);
    }
}

pub fn db_set_length(dbstore: &'static DBStore, key: &Vec<u8>, length: u32) {
    let length_key = db_make_length_key(key);
    if length == 0 {
        dbstore.0.delete(&length_key).unwrap();
        return;
    }
    let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
    dbstore.0.put(&length_key, &new_length_bits).unwrap();
}

pub fn handle_reorg(dbstore: &'static DBStore, from: u32) {
    let latest: u32 = check_latest_block_for_reorg(dbstore, from);
    let set: HashSet<Vec<u8>> = db_updated_keys_for_block_range(dbstore, from, latest);
    for key in set.iter() {
        db_rollback_key(dbstore, &key, from);
    }
}

pub fn read_arraybuffer_as_vec(data: &[u8], data_start: i32) -> Vec<u8> {
    let len = u32::from_le_bytes(
        (data[((data_start - 4) as usize)..(data_start as usize)])
            .try_into()
            .unwrap(),
    );
    return Vec::<u8>::from(&data[(data_start as usize)..(((data_start as u32) + len) as usize)]);
}

pub fn setup_linker(linker: &mut Linker<State>, input: &Vec<u8>, height: u32) {
    let mut input_clone: Vec<u8> =
        <Vec<u8> as TryFrom<[u8; 4]>>::try_from(height.to_le_bytes()).unwrap();
    input_clone.extend(input.clone());
    let __host_len = input_clone.len();
    linker
        .func_wrap(
            "env",
            "__host_len",
            move |mut _caller: Caller<'_, State>| -> i32 {
                return __host_len.try_into().unwrap();
            },
        )
        .unwrap();
    linker
        .func_wrap(
            "env",
            "__load_input",
            move |mut caller: Caller<'_, State>, data_start: i32| {
                let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
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
pub fn db_create_empty_update_list(batch: &mut rocksdb::WriteBatch, height: u32) {
    let height_vec: Vec<u8> = height.to_le_bytes().try_into().unwrap();
    let key: Vec<u8> = db_make_length_key(&db_make_updated_key(&height_vec));
    let value_vec: Vec<u8> = (0 as u32).to_le_bytes().try_into().unwrap();
    batch.put(&key, &value_vec);
}
pub fn db_annotate_value(v: &Vec<u8>, block_height: u32) -> Vec<u8> {
    let mut entry: Vec<u8> = v.clone();
    let height: Vec<u8> = block_height.to_le_bytes().try_into().unwrap();
    entry.extend(height);
    return entry;
}
pub fn db_append_annotated(
    dbstore: &'static DBStore,
    batch: &mut rocksdb::WriteBatch,
    key: &Vec<u8>,
    value: &Vec<u8>,
    block_height: u32,
) {
    let length_key = db_make_length_key(key);
    let length: u32 = db_length_at_key(dbstore, &length_key);
    let entry = db_annotate_value(value, block_height);

    let entry_key: Vec<u8> = db_make_list_key(key, length);
    batch.put(&entry_key, &entry);
    let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
    batch.put(&length_key, &new_length_bits);
}
pub fn db_append(
    dbstore: &'static DBStore,
    batch: &mut rocksdb::WriteBatch,
    key: &Vec<u8>,
    value: &Vec<u8>,
) {
    let length_key = db_make_length_key(key);
    let length: u32 = db_length_at_key(dbstore, &length_key);
    let entry_key: Vec<u8> = db_make_list_key(key, length);
    batch.put(&entry_key, &value);
    let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
    batch.put(&length_key, &new_length_bits);
}

pub fn setup_linker_indexer(linker: &mut Linker<State>, dbstore: &'static DBStore, height: usize) {
    linker
        .func_wrap(
            "env",
            "__flush",
            move |mut caller: Caller<'_, State>, encoded: i32| {
                let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                let data = mem.data(&caller);
                let encoded_vec = read_arraybuffer_as_vec(data, encoded);
                let mut batch = rocksdb::WriteBatch::default();
                let _ = db_create_empty_update_list(&mut batch, height as u32);
                let decoded: Vec<Vec<u8>> = rlp::decode_list(&encoded_vec);

                for (k, v) in decoded.iter().tuples() {
                    let k_owned = <Vec<u8> as Clone>::clone(k);
                    let v_owned = <Vec<u8> as Clone>::clone(v);
                    db_append_annotated(dbstore, &mut batch, &k_owned, &v_owned, height as u32);
                    let update_key: Vec<u8> =
                        <Vec<u8> as TryFrom<[u8; 4]>>::try_from((height as u32).to_le_bytes())
                            .unwrap();
                    db_append(dbstore, &mut batch, &update_key, &k_owned);
                }
                debug!(
                    "saving {:?} k/v pairs for block {:?}",
                    decoded.len() / 2,
                    height
                );
                (dbstore.0).write(batch).unwrap();
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
                let length = db_length_at_key(dbstore, &db_make_length_key(&key_vec));
                if length != 0 {
                    let indexed_key = db_make_list_key(&key_vec, length - 1);
                    let mut value_vec = (dbstore.0).get(&indexed_key).unwrap().unwrap();
                    value_vec.truncate(value_vec.len().saturating_sub(4));
                    let _ = mem.write(&mut caller, value.try_into().unwrap(), value_vec.as_slice());
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
                let length = db_length_at_key(dbstore, &db_make_length_key(&key_vec));
                if length != 0 {
                    let indexed_key = db_make_list_key(&key_vec, length - 1);
                    let value_vec = (dbstore.0).get(&indexed_key).unwrap().unwrap();
                    return (value_vec.len() - 4).try_into().unwrap();
                } else {
                    return 0;
                }
            },
        )
        .unwrap();
}

pub fn index_single_block(
    dbstore: &'static DBStore,
    engine: Arc<&wasmtime::Engine>,
    module: Arc<&wasmtime::Module>,
    _store: Arc<Mutex<wasmtime::Store<State>>>,
    block_hash: BlockHash,
    block: Arc<&SerBlock>,
    height: usize,
    batch: &mut WriteBatch,
) {
    let mut store = _store.lock().unwrap();
    let mut linker = Linker::<State>::new(*engine);
    setup_linker(&mut linker, *block, height as u32);
    setup_linker_indexer(&mut linker, dbstore, height);
    let instance = linker.instantiate(&mut *store, &module).unwrap();
    let start = instance
        .get_typed_func::<(), ()>(&mut *store, "_start")
        .unwrap();
    handle_reorg(dbstore, height as u32);
    /*
        instance
            .get_memory(&mut store, "memory")
            .unwrap()
            .grow(&mut store, 32767)
            .unwrap();
    */

    start.call(&mut *store, ()).unwrap();
    // batch.tip_row = serialize(&block_hash).into_boxed_slice();
}

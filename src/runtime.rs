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

impl BatchLike for WriteBatchWithTransaction<false> {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        self.put(key, value);
    }
}

const TIP_KEY: &[u8] = b"T";
const HEADERS_CF: &str = "headers";

pub struct State {
    limits: StoreLimits,
}

pub struct MetashrewRuntime<T: KeyValueStoreLike> {
    pub db: Arc<T>,
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

impl<T: KeyValueStoreLike> MetashrewRuntime<T>
where
    T: KeyValueStoreLike<Batch = WriteBatchWithTransaction<false>>,
    T: Sync + Send,
{
    pub fn load(indexer: PathBuf, store: Arc<T>) -> Result<Self> {
        let engine = wasmtime::Engine::default();
        let module = wasmtime::Module::from_file(&engine, indexer.into_os_string()).unwrap();
        let wasmstore = Arc::new(Mutex::new(Store::<State>::new(&engine, State::new())));
        {
            (*wasmstore.lock().unwrap()).limiter(|state| &mut state.limits)
        }
        Ok(MetashrewRuntime {
            db: store,
            engine,
            module,
            wasmstore,
        })
    }

    pub fn instantiate(&self) -> Result<wasmtime::Instance> {
        let mut store = self.wasmstore.lock().unwrap();
        let linker = Linker::new(&self.engine);
        linker.instantiate(&mut *store, &self.module)
    }

    pub fn db_make_list_key(v: &Vec<u8>, index: u32) -> Vec<u8> {
        let mut entry = v.clone();
        let index_bits: Vec<u8> = index.to_le_bytes().try_into().unwrap();
        entry.extend(index_bits);
        return entry;
    }

    pub fn db_make_length_key(key: &Vec<u8>) -> Vec<u8> {
        return Self::db_make_list_key(key, u32::MAX);
    }

    pub fn db_make_updated_key(key: &Vec<u8>) -> Vec<u8> {
        return key.clone();
    }

    pub fn u32_to_vec(v: u32) -> Vec<u8> {
        return v.to_le_bytes().try_into().unwrap();
    }

    pub fn check_latest_block_for_reorg(&self, height: u32) -> u32 {
        match self
            .db
            .get(Self::db_make_length_key(&Self::db_make_updated_key(
                &Self::u32_to_vec(height as u32),
            )))
            .unwrap()
        {
            Some(_v) => self.check_latest_block_for_reorg(height + 1),
            None => return height,
        }
    }

    pub fn db_length_at_key(&self, length_key: &Vec<u8>) -> u32 {
        return match self.db.get(length_key).unwrap() {
            Some(v) => u32::from_le_bytes(v.try_into().unwrap()),
            None => 0,
        };
    }

    pub fn db_updated_keys_for_block(&self, height: u32) -> HashSet<Vec<u8>> {
        let key: Vec<u8> =
            Self::db_make_length_key(&Self::db_make_updated_key(&Self::u32_to_vec(height)));
        let length: i32 = Self::db_length_at_key(self, &key) as i32;
        let mut i: i32 = 0;
        let mut set: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
        while i < length {
            set.insert(
                self.db
                    .get(&Self::db_make_list_key(&key, i as u32))
                    .unwrap()
                    .unwrap(),
            );
            i = i + 1;
        }
        return set;
    }

    pub fn db_updated_keys_for_block_range(&self, from: u32, to: u32) -> HashSet<Vec<u8>> {
        let mut i = from;
        let mut result: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
        while to >= i {
            result.extend(Self::db_updated_keys_for_block(self, i));
            i = i + 1;
        }
        return result;
    }

    pub fn db_rollback_key(&self, key: &Vec<u8>, to_block: u32) {
        let length: i32 = Self::db_length_at_key(self, &key).try_into().unwrap();
        let mut index: i32 = length - 1;
        let mut end_length: i32 = length;
        while index >= 0 {
            let list_key = Self::db_make_list_key(key, index.try_into().unwrap());
            let _ = match self.db.get(&list_key).unwrap() {
                Some(value) => {
                    let value_height: u32 = u32::from_le_bytes(
                        value.as_slice()[(value.len() - 4)..].try_into().unwrap(),
                    );
                    if to_block <= value_height.try_into().unwrap() {
                        self.db.delete(&list_key).unwrap();
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
            Self::db_set_length(self, key, end_length as u32);
        }
    }

    pub fn db_set_length(&self, key: &Vec<u8>, length: u32) {
        let length_key = Self::db_make_length_key(key);
        if length == 0 {
            self.db.delete(&length_key).unwrap();
            return;
        }
        let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
        self.db.put(&length_key, &new_length_bits).unwrap();
    }

    pub fn handle_reorg(&self, from: u32) {
        let latest: u32 = Self::check_latest_block_for_reorg(self, from);
        let set: HashSet<Vec<u8>> = Self::db_updated_keys_for_block_range(self, from, latest);
        for key in set.iter() {
            Self::db_rollback_key(self, &key, from);
        }
    }

    pub fn read_arraybuffer_as_vec(data: &[u8], data_start: i32) -> Vec<u8> {
        let len = u32::from_le_bytes(
            (data[((data_start - 4) as usize)..(data_start as usize)])
                .try_into()
                .unwrap(),
        );
        return Vec::<u8>::from(
            &data[(data_start as usize)..(((data_start as u32) + len) as usize)],
        );
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
                    let bytes = Self::read_arraybuffer_as_vec(data, data_start);
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
        let key: Vec<u8> = Self::db_make_length_key(&Self::db_make_updated_key(&height_vec));
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
        &self,
        batch: &mut rocksdb::WriteBatch,
        key: &Vec<u8>,
        value: &Vec<u8>,
        block_height: u32,
    ) {
        let length_key = Self::db_make_length_key(key);
        let length: u32 = Self::db_length_at_key(self, &length_key);
        let entry = Self::db_annotate_value(value, block_height);

        let entry_key: Vec<u8> = Self::db_make_list_key(key, length);
        batch.put(&entry_key, &entry);
        let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
        batch.put(&length_key, &new_length_bits);
    }
    pub fn db_append(&self, batch: &mut rocksdb::WriteBatch, key: &Vec<u8>, value: &Vec<u8>) {
        let length_key = Self::db_make_length_key(key);
        let length: u32 = Self::db_length_at_key(self, &length_key);
        let entry_key: Vec<u8> = Self::db_make_list_key(key, length);
        batch.put(&entry_key, &value);
        let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
        batch.put(&length_key, &new_length_bits);
    }

    pub fn setup_linker_indexer(&self, linker: &mut Linker<State>, height: usize) {
        linker
            .func_wrap(
                "env",
                "__flush",
                move |mut caller: Caller<'_, State>, encoded: i32| {
                    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
                    let data = mem.data(&caller);
                    let encoded_vec = Self::read_arraybuffer_as_vec(data, encoded);
                    let mut batch = rocksdb::WriteBatch::default();
                    let _ = Self::db_create_empty_update_list(&mut batch, height as u32);
                    let decoded: Vec<Vec<u8>> = rlp::decode_list(&encoded_vec);

                    for (k, v) in decoded.iter().tuples() {
                        let k_owned = <Vec<u8> as Clone>::clone(k);
                        let v_owned = <Vec<u8> as Clone>::clone(v);
                        Self::db_append_annotated(
                            self,
                            &mut batch,
                            &k_owned,
                            &v_owned,
                            height as u32,
                        );
                        let update_key: Vec<u8> =
                            <Vec<u8> as TryFrom<[u8; 4]>>::try_from((height as u32).to_le_bytes())
                                .unwrap();
                        Self::db_append(self, &mut batch, &update_key, &k_owned);
                    }
                    debug!(
                        "saving {:?} k/v pairs for block {:?}",
                        decoded.len() / 2,
                        height
                    );
                    (self.db).write(batch).unwrap();
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
                    let key_vec = Self::read_arraybuffer_as_vec(data, key);
                    let length = Self::db_length_at_key(self, &Self::db_make_length_key(&key_vec));
                    if length != 0 {
                        let indexed_key = Self::db_make_list_key(&key_vec, length - 1);
                        let mut value_vec = (self.db).get(&indexed_key).unwrap().unwrap();
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
                    let key_vec = Self::read_arraybuffer_as_vec(data, key);
                    let length = Self::db_length_at_key(self, &Self::db_make_length_key(&key_vec));
                    if length != 0 {
                        let indexed_key = Self::db_make_list_key(&key_vec, length - 1);
                        let value_vec = (self.db).get(&indexed_key).unwrap().unwrap();
                        return (value_vec.len() - 4).try_into().unwrap();
                    } else {
                        return 0;
                    }
                },
            )
            .unwrap();
    }
}

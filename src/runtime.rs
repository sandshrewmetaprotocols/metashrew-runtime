use std::path::PathBuf;
use wasmtime::{Extern, Caller, Instance, Memory, MemoryType, SharedMemory, Config, Engine, Linker, Module, Store, Mutability, GlobalType, Global, Val, ValType};
use rlp::{Rlp};
use rlp;
use wasmtime_wasi::sync::WasiCtxBuilder;
use itertools::Itertools;
use hex;
use electrs_rocksdb as rocksdb;
use std::collections::HashSet;


pub trait BatchLike {
  fn put(key: &Vec<u8>, value: &Vec<u8>);
}

pub trait KeyValueStoreLike<T: BatchLike> {
  fn write(batch: T) -> Result<()>;
  fn get(key: &Vec<u8>) -> Result<Option<Vec<u8>>>;
}

pub trait HostInterfaceLike<'a> {
  fn __host_len(self: &Self, mut caller: Caller<'a, ()>) -> i32;
  fn __load_input(self: &Self, mut caller: Caller<'a, ()>, ptr: i32);
  fn __log(self: &Self, mut caller: Caller<'a, ()>, ptr: i32);
  fn abort(self: &Self, mut caller: Caller<'a, ()>, ptr: i32);
  fn __flush(self: &Self, mut caller: Caller<'a, ()>, ptr: i32);
  fn __get(self: &Self, mut caller: Caller<'a, ()>, key: i32, value: i32);
  fn __get_len(self: &Self, mut caller: Caller<'a, ()>, key: i32) -> i32;
}

pub struct KeyValueHostInterface<'a, T: KeyValueStoreLike> {
  db: T;
  engine: Engine;
  module: Module;
  input: Vec<u8>;
  caller: &mut Caller<'a, ()>;
};

impl<'a, T: KeyValueStoreLike> HostInterfaceLike for KeyValueHostInterface<KeyValueStoreLike> {
  pub __host_len(self: &Self, mut caller: Caller<'a, ()>) -> i32 {
    self.input.len().try_into().unwrap();
  }
  pub __load_input(self: &Self, mut caller: Caller<'a, ()>, ptr: i32) {
    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
    let _ = mem.write(&mut caller, ptr.try_into().unwrap(), self.input.as_slice());
  }
  pub __log(self: &Self, mut caller: Caller<'a, ()>, ptr: i32) {
    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
    let data = mem.data(&caller);
    let bytes = read_arraybuffer_as_vec(data, ptr);
    println!("{}", std::str::from_utf8(bytes.as_slice()).unwrap());
  }
  pub abort(self: &Self, mut caller: Caller<'a, ()>) {
    panic!("abort!");
  }
}

pub struct Runtime<T: HostInterface> {
  interface: T;
  module: Module;
  engine: Engine;
}

impl for Runtime<T: HostInterface> {
  
}

pub fn db_annotate_value(v: &Vec<u8>, block_height: u32) -> Vec<u8> {
  let mut entry: Vec<u8> = v.clone();
  let height: Vec<u8> = block_height.to_le_bytes().try_into().unwrap();
  entry.extend(height);
  return entry;
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

pub fn db_append<T: BatchLike, K: KeyValueStoreLike<T>>(dbstore: &T, batch: &mut K, key: &Vec<u8> , value: &Vec<u8>) {
  let mut length_key = db_make_length_key(key);
  let length: u32 = db_length_at_key(dbstore, &length_key);
  let entry_key: Vec<u8> = db_make_list_key(key, length);
  batch.put(&entry_key, &value);
  let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
  batch.put(&length_key, &new_length_bits);
}


pub fn db_length_at_key<T: BatchLike, K: KeyValueStoreLike<T>>(dbstore: &K, length_key: &Vec<u8>) -> u32 {
  return match dbstore.get(length_key).unwrap() {
    Some(v) => u32::from_le_bytes(v.try_into().unwrap()),
    None => 0
  }
}

pub fn read_arraybuffer_as_vec(data: &[u8], data_start: i32) -> Vec<u8> {
      let len = u32::from_le_bytes((data[((data_start - 4) as usize)..(data_start as usize)]).try_into().unwrap());
      return Vec::<u8>::from(&data[(data_start as usize)..(((data_start as u32) + len) as usize)]);
}

pub fn db_append_annotated<T: BatchLike, K: KeyValueStoreLike<T>>(dbstore: &K, batch: &mut T, key: &Vec<u8> , value: &Vec<u8>, block_height: u32) {
  let mut length_key = db_make_length_key(key);
  let length: u32 = db_length_at_key(dbstore, &length_key);
  let entry = db_annotate_value(value, block_height);

  let entry_key: Vec<u8> = db_make_list_key(key, length);
  batch.put(&entry_key, &entry);
  let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
  batch.put(&length_key, &new_length_bits);
}

pub fn db_create_empty_update_list<T: BatchLike, K: KeyValueStoreLike<T>>(batch: &mut T, height: u32){
    let height_vec: Vec<u8> = height.to_le_bytes().try_into().unwrap();
    let key: Vec<u8> = db_make_length_key(&db_make_updated_key(&height_vec));
    let value_vec: Vec<u8> = (0 as u32).to_le_bytes().try_into().unwrap();
    batch.put(&key, &value_vec);
}
pub fn setup_linker_indexer(linker: &mut Linker<()>, dbstore: &'static DBStore, height: usize) {
    linker.func_wrap("env", "__flush", move |mut caller: Caller<'_, ()>, encoded: i32| {
      let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
      let data = mem.data(&caller);
      let encoded_vec = read_arraybuffer_as_vec(data, encoded);
      let mut batch = rocksdb::WriteBatch::default();
      let _ = db_create_empty_update_list(&mut batch, height as u32);
      let decoded: Vec<Vec<u8>> = rlp::decode_list(&encoded_vec);
      decoded.iter().tuple_windows().inspect(|(k, v)| {
        let k_owned = <Vec<u8> as Clone>::clone(k);
        let v_owned = <Vec<u8> as Clone>::clone(v);
        db_append_annotated(dbstore, &mut batch, &k_owned, &v_owned, height as u32);
        let update_key: Vec<u8> = <Vec<u8> as TryFrom<[u8; 4]>>::try_from((height as u32).to_le_bytes()).unwrap();
        db_append(dbstore, &mut batch, &update_key, &k_owned);
      });
      debug!("saving {:?} k/v pairs for block {:?}", decoded.len() / 2, height);
      (dbstore.db).write(batch).unwrap();
    }).unwrap();
    linker.func_wrap("env", "__get", move |mut caller: Caller<'_, ()>, key: i32, value: i32| {
      let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
      let data = mem.data(&caller);
      let key_vec = read_arraybuffer_as_vec(data, key);
      let length = db_length_at_key(dbstore, &key_vec);
      if length != 0 {
        let indexed_key = db_make_list_key(&key_vec, length - 1);
        let mut value_vec = (dbstore.db).get(&indexed_key).unwrap().unwrap();
        value_vec.truncate(value_vec.len().saturating_sub(4));
        let _ = mem.write(&mut caller, value.try_into().unwrap(), value_vec.as_slice());
      }
    }).unwrap();
    linker.func_wrap("env", "__get_len", move |mut caller: Caller<'_, ()>, key: i32| -> i32 {
      let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
      let data = mem.data(&caller);
      let key_vec = read_arraybuffer_as_vec(data, key);
      let length = db_length_at_key(dbstore, &key_vec);
      if length != 0 {
        let indexed_key = db_make_list_key(&key_vec, length - 1);
        let value_vec = (dbstore.db).get(&indexed_key).unwrap().unwrap();
        return (value_vec.len() - 4).try_into().unwrap();
      } else {
        return 0;
      }
    }).unwrap();
}

pub fn db_set_length(dbstore: &'static DBStore, key: &Vec<u8>, length: u32) {
  let mut length_key = db_make_length_key(key);
  if length == 0 {
    dbstore.db.delete(&length_key).unwrap();
    return;
  }
  let new_length_bits: Vec<u8> = (length + 1).to_le_bytes().try_into().unwrap();
  dbstore.db.put(&length_key, &new_length_bits).unwrap();
}

pub fn db_updated_keys_for_block(dbstore: &'static DBStore, height: u32) -> HashSet<Vec<u8>> {
  let key: Vec<u8> = db_make_length_key(&db_make_updated_key(&u32_to_vec(height)));
  let length: i32 = (db_length_at_key(dbstore, &key) as i32);
  let mut i: i32 = 0;
  let mut set: HashSet<Vec<u8>> = HashSet::<Vec<u8>>::new();
  while i < length {
    set.insert(dbstore.db.get(&db_make_list_key(&key, i as u32)).unwrap().unwrap());
    i = i + 1;
  }
  return set;
}

pub fn db_updated_keys_for_block_range(dbstore: &'static DBStore, from: u32, to: u32) -> HashSet<Vec<u8>> {
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
    let _ = match dbstore.db.get(&list_key).unwrap() {
      Some(value) => {
        let value_height: u32 = u32::from_le_bytes(value.as_slice()[(value.len() - 4)..].try_into().unwrap());
        if to_block <= value_height.try_into().unwrap() {
          dbstore.db.delete(&list_key).unwrap();
          end_length = end_length - 1;
        } else {
          break;
        }
      },
      None => { break; }
    };
  }
  if end_length != length {
    db_set_length(dbstore, key, end_length as u32);
  }
}

pub fn db_value_at_block(dbstore: &'static DBStore, key: &Vec<u8>, height: i32) -> Vec<u8> {
  let length: i32 = db_length_at_key(dbstore, &key).try_into().unwrap();
  let mut index: i32 = length - 1;
  while index >= 0 {
    let value: Vec<u8> = match dbstore.db.get(db_make_list_key(key, index.try_into().unwrap())).unwrap() {
      Some(v) => v,
      None => db_make_list_key(&Vec::<u8>::new(), 0)
    };

    let value_height: u32 = u32::from_le_bytes(value.as_slice()[(value.len() - 4)..].try_into().unwrap());
    /*
      Ok(v) => u32::from_le_bytes(v).try_into().unwrap(),
      Err(e) => 0
    };
    */
    if height >= value_height.try_into().unwrap() {
      value.clone().truncate(value.len().saturating_sub(4));
    }
  }
  return vec![];
}

pub fn setup_linker_view(linker: &mut Linker<()>, dbstore: &'static DBStore, height: i32) {
    linker.func_wrap("env", "__flush", move |mut caller: Caller<'_, ()>, encoded: i32| {}).unwrap();
    linker.func_wrap("env", "__get", move |mut caller: Caller<'_, ()>, key: i32, value: i32| {
      let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
      let data = mem.data(&caller);
      let key_vec = read_arraybuffer_as_vec(data, key);
      let value = db_value_at_block(dbstore, &key_vec, height);
      let _ = mem.write(&mut caller, value.len(), value.as_slice());
    }).unwrap();
    linker.func_wrap("env", "__get_len", move |mut caller: Caller<'_, ()>, key: i32| -> i32 {
      let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
      let data = mem.data(&caller);
      let key_vec = read_arraybuffer_as_vec(data, key);
      let value = db_value_at_block(dbstore, &key_vec, height);
      return value.len().try_into().unwrap();
    }).unwrap();
}

pub fn u32_to_vec(v: u32) -> Vec<u8> {
  return v.to_le_bytes().try_into().unwrap();
}

pub fn check_latest_block_for_reorg(dbstore: &'static DBStore, height: u32) -> u32 {
    match dbstore.db.get(db_make_length_key(&db_make_updated_key(&u32_to_vec(height as u32)))).unwrap() {
        Some(v) => check_latest_block_for_reorg(dbstore, height + 1),
        None => return height
    }
}
pub fn db_make_updated_key(key: &Vec<u8>)-> Vec<u8>{
    return key.clone();
}

pub fn handle_reorg(dbstore: &'static DBStore, from: u32) {
    let latest: u32 = check_latest_block_for_reorg(dbstore, from);
    let set: HashSet<Vec<u8>> = db_updated_keys_for_block_range(dbstore, from, latest);
    for key in set.iter() {
      db_rollback_key(dbstore, &key, from);
    }
}

fn index_single_block(
    dbstore: &'static DBStore,
    engine: Arc<&wasmtime::Engine>,
    module: Arc<&wasmtime::Module>,
    block_hash: BlockHash,
    block: Arc<&SerBlock>,
    height: usize,
    batch: &mut WriteBatch,
) {

    let mut store = Store::new(*engine, ());
    let mut linker = Linker::new(*engine);
    setup_linker(&mut linker, &mut store, dbstore, *block, height as u32);
    setup_linker_indexer(&mut linker, dbstore, height);
    let instance = linker.instantiate(&mut store, &module).unwrap();
    let start = instance.get_typed_func::<(), ()>(&mut store, "_start").unwrap();
    handle_reorg(dbstore, height as u32);
    instance.get_memory(&mut store, "memory").unwrap().grow(&mut store,  32767).unwrap();

    start.call(&mut store, ()).unwrap();
    batch.tip_row = serialize(&block_hash).into_boxed_slice();
}

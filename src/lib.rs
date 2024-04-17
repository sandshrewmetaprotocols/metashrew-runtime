use anyhow::{Context, Result};
use rocksdb::{DBWithThreadMode, ThreadMode, WriteBatchWithTransaction, DB};
// use bitcoin::consensus::{deserialize, serialize};
// use bitcoin::{BlockHash, OutPoint, Txid};
// use itertools::Itertools;
// use rlp;
// use std::collections::HashSet;
// use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use wasmtime::{Caller, Linker, Store, StoreLimits, StoreLimitsBuilder};

pub trait BatchLike {
    fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;
}

pub trait KeyValueStoreLike {
    type Error;
    type Batch;
    fn write(&self, batch: Self::Batch) -> Result<(), Self::Error>;
    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Self::Error>;
}

impl<T: ThreadMode> KeyValueStoreLike for DBWithThreadMode<T> {
    type Error = rocksdb::Error;
    type Batch = WriteBatchWithTransaction<false>;

    fn write(&self, batch: WriteBatchWithTransaction<false>) -> Result<(), Self::Error> {
        self.write(batch)
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        self.get(key)
    }
}

// pub struct KeyValueHostInterface<'a, T: KeyValueStoreLike> {
//   db: T;
// };
//
// impl KeyValueHostInterface<'a, T: KeyValueStoreLike> {
// }

pub struct State {
    limits: StoreLimits,
}

pub struct MetashrewRuntime {
    pub store: &'static DB,
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

// impl MetashrewRuntime {
//     pub(crate) fn load(
//         indexer: PathBuf,
//         store: &'static DBStore,
//         mut chain: Chain,
//         metrics: &Metrics,
//         batch_size: usize,
//         lookup_limit: Option<usize>,
//         reindex_last_blocks: usize,
//     ) -> Result<Self> {
//         if let Some(row) = store.get_tip() {
//             let tip = deserialize(&row).expect("invalid tip");
//             let headers = store
//                 .read_headers()
//                 .into_iter()
//                 .map(|row| HeaderRow::from_db_row(&row).header)
//                 .collect();
//             chain.load(headers, tip);
//             chain.drop_last_headers(reindex_last_blocks);
//         };
//         let stats = Stats::new(metrics);
//         stats.observe_chain(&chain);
//         stats.observe_db(store);
//         let engine = wasmtime::Engine::default();
//         let module = wasmtime::Module::from_file(&engine, indexer.into_os_string()).unwrap();
//         let wasmstore = Arc::new(Mutex::new(Store::<State>::new(&engine, State::new())));
//         {
//           (*wasmstore.lock().unwrap()).limiter(|state| &mut state.limits)
//         }
//         Ok(MetashrewRuntime {
//             store,
//             batch_size,
//             lookup_limit,
//             chain,
//             stats,
//             is_ready: false,
//             flush_needed: false,
//             engine,
//             module,
//             wasmstore
//         })
//     }
//
// }
//
//

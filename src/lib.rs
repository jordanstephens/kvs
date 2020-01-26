#![deny(missing_docs)]
//! A simple key/value store.

pub use error::{KvError, Result};
pub use kv::KvStore;

mod error;
mod kv;
mod kvreader;
mod kvwriter;

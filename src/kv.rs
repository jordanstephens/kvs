use crate::kvreader::KvReader;
use crate::kvwriter::KvWriter;
use crate::{KvError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy)]
struct KvIndexRecord {
	offset: u64,
	length: u64,
}

type KvIndex = HashMap<String, KvIndexRecord>;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
	path: PathBuf,
	index: KvIndex,
	writer: KvWriter,
	reader: KvReader,
}

#[derive(Debug, Serialize, Deserialize)]
enum KvAction {
	Set(String, String),
	Remove(String),
}

impl KvStore {
	/// Opens a KvStore from a file
	pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		let path = path.into();
		let filepath = path.join("db.log");
		let writer = KvWriter::new(&filepath)?;
		let mut reader = KvReader::new(&filepath)?;

		let index = load_index(&mut reader)?;

		Ok(KvStore {
			path,
			index,
			writer,
			reader,
		})
	}

	/// Sets the value of a string key to a string.
	///
	/// If the key already exists, the previous value will be overwritten.
	pub fn set(&mut self, key: String, value: String) -> Result<()> {
		let action = KvAction::Set(key.clone(), value.clone());
		let offset = self.writer.pos;
		serde_json::to_writer(&mut self.writer, &action)?;
		let length = self.writer.pos - offset;
		self.writer.flush()?;
		self.index.insert(key, KvIndexRecord { offset, length });
		Ok(())
	}

	/// Gets the string value of a given string key.
	///
	/// Returns `None` if the given key does not exist.
	pub fn get(&mut self, key: String) -> Result<Option<String>> {
		let index = &self.index;
		let reader = &mut self.reader;
		if let Some(KvIndexRecord { offset, length }) = index.get(&key) {
			reader.seek(SeekFrom::Start(*offset))?;
			let take = reader.take(*length);
			if let KvAction::Set(_key, value) = serde_json::from_reader(take)? {
				Ok(Some(value))
			} else {
				Err(KvError::UnexpectedAction)
			}
		} else {
			Ok(None)
		}
	}

	/// Remove a given key.
	pub fn remove(&mut self, key: String) -> Result<()> {
		if self.index.contains_key(&key) {
			let action = KvAction::Remove(key);
			serde_json::to_writer(&mut self.writer, &action)?;
			self.writer.flush()?;
			if let KvAction::Remove(key) = action {
				self.index.remove(&key).expect("key not found");
			}
			Ok(())
		} else {
			Err(KvError::KeyNotFound)
		}
	}
}

fn load_index(reader: &mut KvReader) -> Result<KvIndex> {
	let mut stream = Deserializer::from_reader(reader).into_iter::<KvAction>();
	let mut index = KvIndex::new();
	let mut offset = 0;

	while let Some(action) = stream.next() {
		let next_offset = stream.byte_offset() as u64;
		let length = next_offset - offset;
		match action? {
			KvAction::Set(key, _value) => index.insert(key, KvIndexRecord { offset, length }),
			KvAction::Remove(key) => index.remove(&key),
		};
		offset = next_offset;
	}

	Ok(index)
}

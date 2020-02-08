use crate::kvreader::KvReader;
use crate::kvwriter::KvWriter;
use crate::{KvError, Result};
use glob::glob;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

const THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
enum KvAction {
	Set(String, String),
	Remove(String),
}

fn generation_path(dirpath: &PathBuf, generation: u64) -> PathBuf {
	dirpath.join(format!("{}.db", generation))
}

fn active_generations(dirpath: &PathBuf) -> Result<Vec<u64>> {
	let glob_pattern = format!("{}/*.db", &dirpath.to_str().expect("dirpath not utf8"));
	let generation_pattern = Regex::new(r"/(\d+)\.db$").unwrap();
	let mut generations: Vec<u64> = glob(&glob_pattern)?
		.filter_map(std::result::Result::ok)
		.filter_map(|entry| {
			let path = entry.to_str().expect("path not utf8");
			generation_pattern
				.captures(path)
				.and_then(|cap| cap.get(1))
				.map(|m| m.as_str())
				.map(|s| str::parse::<u64>(s).unwrap())
		})
		.collect();
	generations.sort();
	Ok(generations)
}

#[derive(Debug, Clone, Copy)]
struct KvIndexRecord {
	generation: u64,
	offset: u64,
	length: u64,
}

type KvIndex = HashMap<String, KvIndexRecord>;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
pub struct KvStore {
	path: PathBuf,
	generation: u64,
	index: KvIndex,
	writer: KvWriter,
	readers: HashMap<u64, KvReader>,
	compactable: u64,
}

impl KvStore {
	/// Opens a KvStore from a file
	pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		let path = path.into();
		fs::create_dir_all(&path)?;

		let mut readers = HashMap::new();
		let mut index = KvIndex::new();
		let mut compactable = 0;
		let generations = active_generations(&path)?;
		let generation = *generations.last().unwrap_or(&0);

		let writer = KvWriter::new(&generation_path(&path, generation))?;

		if generations.len() == 0 {
			let reader = KvReader::new(&generation_path(&path, 0))?;
			readers.insert(0, reader);
		} else {
			for &gen in &generations {
				let mut reader = KvReader::new(&generation_path(&path, gen))?;
				compactable += load(&mut index, generation, &mut reader)?;
				readers.insert(gen, reader);
			}
		}

		Ok(KvStore {
			path,
			generation,
			index,
			writer,
			readers,
			compactable,
		})
	}

	/// Sets the value of a string key to a string.
	///
	/// If the key already exists, the previous value will be overwritten.
	pub fn set(&mut self, key: String, value: String) -> Result<()> {
		let action = KvAction::Set(key.clone(), value);
		let offset = self.writer.pos;
		serde_json::to_writer(&mut self.writer, &action)?;
		let length = self.writer.pos - offset;
		self.writer.flush()?;
		let generation = self.generation;
		let record = KvIndexRecord {
			generation,
			offset,
			length,
		};
		if let Some(outdated) = self.index.insert(key, record) {
			self.compactable += outdated.length;
		}

		if self.compactable > THRESHOLD {
			self.compact()?;
		}

		Ok(())
	}

	/// Gets the string value of a given string key.
	///
	/// Returns `None` if the given key does not exist.
	pub fn get(&mut self, key: String) -> Result<Option<String>> {
		let index = &self.index;
		if let Some(KvIndexRecord {
			generation,
			offset,
			length,
		}) = index.get(&key)
		{
			let reader = self
				.readers
				.get_mut(generation)
				.expect("No reader for generation");
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
				let outdated = self.index.remove(&key).expect("key not found");
				self.compactable += outdated.length
			}
			Ok(())
		} else {
			Err(KvError::KeyNotFound)
		}
	}

	/// walk
	fn compact(&mut self) -> Result<()> {
		self.generation += 1;
		let new_path = generation_path(&self.path, self.generation);
		self.writer = KvWriter::new(&new_path)?;
		self
			.readers
			.insert(self.generation, KvReader::new(&new_path)?);

		for entry in self.index.values_mut() {
			let KvIndexRecord {
				generation,
				offset,
				length,
			} = entry;

			let mut reader = self
				.readers
				.get_mut(generation)
				.expect("No reader for generation");

			reader.seek(SeekFrom::Start(*offset))?;
			reader.take(*offset);

			io::copy(&mut reader, &mut self.writer)?;

			entry.generation = self.generation;
			entry.offset = self.writer.pos - *length;
		}

		self.writer.flush()?;

		let removable: Vec<_> = self
			.readers
			.keys()
			.filter(|gen| **gen < self.generation)
			.cloned()
			.collect();

		for generation in removable {
			self.readers.remove(&generation);
			fs::remove_file(generation_path(&self.path, generation))?;
		}

		self.compactable = 0;

		Ok(())
	}
}

fn load(index: &mut KvIndex, generation: u64, reader: &mut KvReader) -> Result<u64> {
	let mut stream = Deserializer::from_reader(reader).into_iter::<KvAction>();
	let mut offset = 0;
	let mut compactable = 0;

	while let Some(action) = stream.next() {
		let next_offset = stream.byte_offset() as u64;
		let length = next_offset - offset;
		match action? {
			KvAction::Set(key, _value) => {
				let record = KvIndexRecord {
					generation,
					offset,
					length,
				};
				if let Some(outdated) = index.insert(key, record) {
					compactable += outdated.length;
				}
			}

			KvAction::Remove(key) => {
				if let Some(outdated) = index.remove(&key) {
					compactable += outdated.length;
				}
				compactable += length
			}
		};
		offset = next_offset;
	}

	Ok(compactable)
}

use crate::Result;
use std::fs;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

pub struct KvReader {
  pub pos: u64,
  reader: BufReader<fs::File>,
}

impl KvReader {
  pub fn new(path: &PathBuf) -> Result<Self> {
    let mut file = fs::File::open(&path)?;
    let pos = file.seek(SeekFrom::Current(0))?;
    let reader = BufReader::new(file);
    Ok(KvReader { reader, pos })
  }
}

impl Read for KvReader {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let len = self.reader.read(buf)?;
    self.pos += len as u64;
    Ok(len)
  }
}

impl Seek for KvReader {
  fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
    self.pos = self.reader.seek(pos)?;
    Ok(self.pos)
  }
}

use crate::Result;
use std::fs;
use std::io;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub struct KvWriter {
  pub pos: u64,
  writer: BufWriter<fs::File>,
}

impl KvWriter {
  pub fn new(path: &PathBuf) -> Result<Self> {
    let mut file = fs::OpenOptions::new()
      .create(true)
      .write(true)
      .append(true)
      .open(&path)?;
    let pos = file.seek(SeekFrom::Current(0))?;
    let writer = BufWriter::new(file);
    Ok(KvWriter { writer, pos })
  }
}

impl Write for KvWriter {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    let len = self.writer.write(buf)?;
    self.pos += len as u64;
    Ok(len)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.writer.flush()
  }
}

impl Seek for KvWriter {
  fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
    self.pos = self.writer.seek(pos)?;
    Ok(self.pos)
  }
}

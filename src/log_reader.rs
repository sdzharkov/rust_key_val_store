use std::io::{Write, BufReader, BufRead, Read, Seek, SeekFrom};
use std::path::{Path};
use std::fs::File;

use crate::kv::Result;

#[derive(Debug)]
pub struct LogReader<T: Read> {
  pub reader: BufReader<T>,
  pub pos: u64 // the position of the log
}

impl<T: Read + Seek> LogReader<T> {
  pub fn new(mut reader: T) -> Result<Self> {
    let pos = reader.seek(SeekFrom::Start(0))?;
    Ok(LogReader {
      reader: BufReader::new(reader),
      pos: pos
    })
  }
}

impl<T: Read + Seek> Read for LogReader<T> {
  fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let len = self.reader.read(buf)?;
    self.pos = len as u64;
    Ok(len)
  }
}

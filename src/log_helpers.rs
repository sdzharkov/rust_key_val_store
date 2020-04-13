use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use std::fmt;

use crate::kv::Result;

#[derive(Debug)]
pub struct LogReader<T: Read> {
  pub reader: BufReader<T>,
  pub pos: u64, // the position of the log
}

impl<T: Read + Seek> LogReader<T> {
  pub fn new(mut reader: T) -> Result<Self> {
    let pos = reader.seek(SeekFrom::Start(0))?;
    Ok(LogReader {
      reader: BufReader::new(reader),
      pos: pos,
    })
  }
}

impl<T: Read + Seek> Read for LogReader<T> {
  fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let len = self.reader.read(buf)?;
    // let mut test_str = String::new();
    // let len = 10 as usize;
    // let len2 = self.reader.read_line(&mut test_str)?;
    // println!("Inside: {}: {}", len2, test_str);
    self.pos = len as u64;
    Ok(len)
  }
}

impl<T: Read + Seek> Seek for LogReader<T> {
  fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
    self.pos = self.reader.seek(pos)?;
    Ok(self.pos)
  }
}

pub struct LogWriter<T: Write> {
  pub writer: BufWriter<T>,
  pub filename: String,
  pub pos: u64,
}

impl<T: Write + Seek> LogWriter<T> {
  pub fn new(mut writer: T, filename: String) -> Result<Self> {
    let pos = writer.seek(SeekFrom::Start(0))?;
    Ok(LogWriter {
      writer: BufWriter::new(writer),
      pos: pos,
      filename: filename,
    })
  }
}

impl<T: Write + Seek> Write for LogWriter<T> {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    let bytes_written = self.writer.write(buf)?;
    self.pos = bytes_written as u64;
    Ok(bytes_written)
  }

  fn flush(&mut self) -> std::io::Result<()> {
    self.writer.flush()?;
    Ok(())
  }
}

impl<T: Write> fmt::Debug for LogWriter<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Point")
      .field("writer", &"writer")
      .field("pos", &self.pos)
      .finish()
  }
}

impl<T: Write + Seek> Seek for LogWriter<T> {
  fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
    self.pos = self.writer.seek(pos)?;
    Ok(self.pos)
  }
}
use std::io::{Write, BufReader, BufRead, Read};
use std::path::{Path};
use std::fs::File;
use std::io::SeekFrom;

#[derive(Debug)]
pub struct LogPointer<T: Read> {
  pub reader: BufReader<T>,
  pos: u64 // the position of the log
}

impl<T: Read> LogPointer<T> {
  pub fn new(file: T) -> Self {
    LogPointer {
      reader: BufReader::new(file),
      pos: 0
    }
  }
}

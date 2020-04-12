use chrono::prelude::Utc;
use chrono::DateTime;
use failure::{format_err, Error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{create_dir, read_dir, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::log_helpers::{LogReader, LogWriter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
enum Log {
  Set { key: String, value: String },
  Rm { key: String },
}

// This is what the store would keep as the value
#[derive(Serialize, Deserialize, Debug)]
struct LogReference {
  filename: String,
  pos: u64,
  size: u64,
  // timestamp: String,
}

impl LogReference {
  pub fn new(filename: String, pos: u64, size: u64) -> Self {
    LogReference {
      filename: filename,
      pos: pos,
      size: size,
    }
  }
}

#[derive(Debug)]
pub struct KvStore {
  store: HashMap<String, String>,
  store_2: HashMap<String, LogReference>,
  writer: LogWriter<File>,
  readers: HashMap<String, LogReader<File>>,
}

impl KvStore {
  pub fn set(&mut self, key: String, value: String) -> Result<()> {
    self.store.insert(key.clone(), value.clone());

    let log = Log::Set {
      key: key.clone(),
      value: value.clone(),
    };
    self.write(&log)
  }

  pub fn get(&self, key: String) -> Result<Option<String>> {
    Ok(self.store.get(&key).cloned())
  }

  pub fn remove(&mut self, key: String) -> Result<()> {
    if self.store.contains_key(&key) {
      self.store.remove(&key);
      let log = Log::Rm {
        key: key.to_string(),
      };

      return self.write(&log);
    } else {
      Err(format_err!("Key not found"))
    }
  }

  pub fn new(readers: HashMap<String, LogReader<File>>, writer: LogWriter<File>) -> KvStore {
    KvStore {
      store: HashMap::new(),
      store_2: HashMap::new(),
      writer: writer,
      readers: readers,
    }

    // let hashmap = &mut store.store;

    // let log_reader = LogReader::new(&store.file);

    // reader.lines()
    //       .map(|l| l.unwrap())
    //       .map(|l| serde_json::from_str(&l).unwrap())
    //       .for_each(|l| match l {
    //         Log::Rm { key } => {
    //           if hashmap.contains_key(&key) {
    //             hashmap.remove(&key);
    //           }
    //         },
    //         Log::Set { key, value } => {
    //           hashmap.insert(key, value);
    //         }
    //       });
  }

  fn write(&mut self, log: &Log) -> Result<()> {
    let serialized = serde_json::to_string(log).unwrap();

    let start_pos = self.writer.pos;
    let end_pos = self.writer.write(format!("{}\n", &serialized).as_bytes())? as u64;
    let size = (end_pos - start_pos) as u64;
    let file_name = self.writer.filename.clone();

    match log {
      Log::Rm { key } => {
        let log_ref = LogReference::new(file_name, start_pos as u64, size);
        self.store_2.insert(key.clone(), log_ref);
      }
      Log::Set { key, value: _ } => {
        let log_ref = LogReference::new(file_name, start_pos as u64, size);
        self.store_2.insert(key.clone(), log_ref);
      }
    }

    Ok(())
  }

  pub fn open(mut current_dir: PathBuf) -> Result<KvStore> {
    // Creates a new instance of the KvStore struct.
    // Check if a log file exists in the dir:
    //   * if exists: ingest the list and return back a KV store
    //   * else: Create a new list and return back a new instance
    current_dir.push("data");
    // let data_folder_path = format!("{}/data", current_dir.display());
    // let data_folder = Path::new(&current_dir);
    let data_folder = &current_dir.as_path();

    let mut index: HashMap<String, LogReader<File>> = HashMap::new();
    let mut entries: Vec<PathBuf> = Vec::new();
    let mut store: HashMap<String, String> = HashMap::new();

    if data_folder.is_dir() {
      entries = read_dir(data_folder)?
        .map(|res| res.unwrap())
        .map(|res| res.path())
        .collect::<Vec<_>>();

      entries.sort();
      for entry in &entries {
        let file = File::open(entry)?;
        let file_name = entry.file_name().unwrap().to_str().unwrap();
        let log_reader = LogReader::new(file)?;

        // log_reader.reader.seek(SeekFrom::Start(38))?;
        // let mut line = String::new();
        // let len = log_reader.reader.read_line(&mut line)?;
        // println!("First line is {} bytes long: {}", len, line);
        index.insert(String::from(file_name), log_reader);
        // let pointer = index.get_mut(&String::from(file_name)).unwrap();

        // reader.lines()
        //   .map(|l| l.unwrap())
        //   .map(|l| serde_json::from_str(&l).unwrap())
        //   .for_each(|l| match l {
        //     Log::Rm { key } => {
        //       if store.contains_key(&key) {
        //         store.remove(&key);
        //       }
        //     },
        //     Log::Set { key, value } => {
        //       store.insert(key, value);
        //     }
        //   });
      }
    } else {
      create_dir(data_folder)?;
    }

    let filename = format!("{}.txt", Utc::now());
    current_dir.push(&filename);

    let file = OpenOptions::new()
      .read(true)
      .append(true)
      .create(true)
      .open(current_dir)?;

    let writer = LogWriter::new(file, filename)?;

    let store = KvStore::new(index, writer);

    Ok(store)
  }
}

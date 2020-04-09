use std::collections::HashMap;
use failure::{Error, format_err};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions, create_dir, read_dir};
use std::io::{Write, BufReader, BufRead};
use serde::{Serialize, Deserialize};
use chrono::prelude::{Utc};

use crate::log_pointer::LogPointer;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
enum Log {
  Set { key: String, value: String },
  Rm { key: String }
}

struct LogReference {
  filename: str;
  pos: u64;
}

#[derive(Debug)]
pub struct KvStore {
  store: HashMap<String, String>,
  file: File,
  readers: HashMap<String, LogPointer<File>>
}

impl KvStore {
  pub fn set(&mut self, key: String, value: String) -> Result<()> {
    self.store.insert(key.clone(), value.clone());

    let log = Log::Set { key: key.clone(), value: value.clone() };
    self.write(&log)
  }

  pub fn get(&self, key: String) -> Result<Option<String>>{
    Ok(self.store.get(&key).cloned())
  }

  pub fn remove(&mut self, key: String) -> Result<()> {
    if self.store.contains_key(&key) {
      self.store.remove(&key);
      let log = Log::Rm { key: key.to_string() };
  
      return self.write(&log)
    } else {
      Err(format_err!("Key not found"))
    }
  }

  pub fn new(file: File, readers: HashMap<String, LogPointer<File>>) -> KvStore {
    let mut store = KvStore {
      store: HashMap::new(),
      file: file,
      readers: readers
    };

    let hashmap = &mut store.store;
    let reader = BufReader::new(&store.file);

    // let log_pointer = LogPointer::new(&store.file);

    reader.lines()
          .map(|l| l.unwrap())
          .map(|l| serde_json::from_str(&l).unwrap())
          .for_each(|l| match l {
            Log::Rm { key } => {
              if hashmap.contains_key(&key) {
                hashmap.remove(&key);
              }
            }, 
            Log::Set { key, value } => {
              hashmap.insert(key, value);
            }
          });

    store
  }

  fn write(&mut self, log: &Log) -> Result<()> {
    let serialized = serde_json::to_string(log).unwrap();

    // @TODO: does one need to handle this?
    self.file.write(format!("{}\n", &serialized).as_bytes())?;
    Ok(())
  }

  pub fn open(mut current_dir: PathBuf) -> Result<KvStore>{
    // Creates a new instance of the KvStore struct.
    // Check if a log file exists in the dir:
    //   * if exists: ingest the list and return back a KV store
    //   * else: Create a new list and return back a new instance

    let data_folder_path = format!("{}/data", current_dir.display());
    let data_folder = Path::new(&data_folder_path);

    let mut index: HashMap<String, LogPointer<File>> = HashMap::new();

    if data_folder.is_dir() {
      for entry in read_dir(data_folder)? {
        let entry = entry?;
        let path = entry.path();
        let file = File::open(path)?;
        let file_name = match entry.file_name().into_string() {
          Ok(v) => v,
          Err(e) => {
            return Err(format_err!("{:?}", e));
          }
        };

        let log_pointer = LogPointer::new(file);
        index.insert(file_name, log_pointer);
      }
    } else {
      create_dir(data_folder)?;
    }

    for (key, val) in &index {
      println!("{} has {:?}", key, val);
  }

    current_dir.push(format!("data/{}.txt", Utc::now()));

    let file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create(true)
                    .open(current_dir)?;

    Ok(KvStore::new(file, index))
  }
}

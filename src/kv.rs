use std::collections::HashMap;
use failure::{Error, format_err};
use std::path::{Path};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Write, BufReader, BufRead};
use serde::{Serialize, Deserialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
enum Log {
  Set { key: String, value: String },
  Rm { key: String }
}

#[derive(Debug)]
pub struct KvStore {
  store: HashMap<String, String>,
  file: File
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

  pub fn new(file: File) -> KvStore {
    let mut store = KvStore {
      store: HashMap::new(),
      file: file,
    };

    let hashmap = &mut store.store;
    let reader = BufReader::new(&store.file);

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

  pub fn open(current_dir: &Path) -> Result<KvStore>{
    // Creates a new instance of the KvStore struct.
    // Check if a log file exists in the dir:
    //   * if exists: ingest the list and return back a KV store
    //   * else: Create a new list and return back a new instance

    let mut data_file = current_dir.to_path_buf();
    data_file.push("data.txt");

    let file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create(true)
                    .open(data_file)?;

    Ok(KvStore::new(file))
  }
}

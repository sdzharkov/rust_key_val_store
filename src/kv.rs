use std::collections::HashMap;
use failure::Error;
use std::io;
use std::path::{Path};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Write, BufReader, BufRead, BufWriter};
use serde::{Serialize, Deserialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
enum Log {
  Get { key: String },
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
    // self.store.insert(key.clone(), value.clone());

    let log = Log::Set { key: key.clone(), value: value.clone() };
    self.write(&log)
  }

  pub fn get(&self, key: String) -> Result<Option<String>>{
    Ok(self.store.get(&key).cloned())
  }

  pub fn remove(&mut self, key: String) -> Result<()> {
    let log = Log::Rm { key: key.to_string() };

    // @TODO: Check if the key exists first before writing to the log

    self.write(&log)
  }

  pub fn new(file: File) -> KvStore {
    let mut store = KvStore {
      store: HashMap::new(),
      file: file,
    };

    let reader = BufReader::new(&store.file);

    let lines: Vec<Log> = reader.lines()
                      .map(|l| l.unwrap())
                      .map(|l| serde_json::from_str(&l).unwrap())
                      .collect();

    for line in lines {
      match line {
        Log::Rm { key } => {
          if store.store.contains_key(&key) {
            store.store.remove(&key);
          }
        }, 
        Log::Set { key, value } => {
          store.store.insert(key, value);
        },
        _ => {

        }
      }
    }

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

fn read_file<R: BufRead>(mut reader: R) -> Result<()> {
  for line in reader.lines() {
    println!("{}", line.unwrap());
  }

  Ok(())
}

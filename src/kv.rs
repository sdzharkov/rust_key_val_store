use std::collections::HashMap;
use failure::{Error, format_err};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions, create_dir, read_dir};
use std::io::{Write, BufReader, BufRead, Seek, SeekFrom, BufWriter};
use serde::{Serialize, Deserialize};
use chrono::prelude::{Utc};
use chrono::{DateTime, TimeZone, NaiveDateTime};

use crate::log_reader::LogReader;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
enum Log {
  Set { key: String, value: String },
  Rm { key: String }
}

// This is what the store would keep as the value
struct LogReference {
  filename: String,
  value_pos: u64,
  value_size: u64,
  timestamp: DateTime<Utc>
}

#[derive(Debug)]
pub struct KvStore {
  store: HashMap<String, String>,
  writer: BufWriter<File>,
  readers: HashMap<String, LogReader<File>>
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

  pub fn new(readers: HashMap<String, LogReader<File>>, writer: BufWriter<File>) -> KvStore {
    KvStore {
      store: HashMap::new(),
      writer: writer,
      readers: readers
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

    // @TODO: does one need to handle this?
    self.writer.write(format!("{}\n", &serialized).as_bytes())?;
    let t = self.writer.seek(SeekFrom::Current(0)).unwrap();
    println!("POS: {}", t);
    Ok(())

    // @TODO: now write this to the reference table
  }

  fn read_entries(&mut self, file_name: &str) {
    let pointer = self.readers.get_mut(&String::from(file_name)).expect("Fuck");

    
  }

  pub fn open(mut current_dir: PathBuf) -> Result<KvStore>{
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

    for (key, val) in &index {
      println!("{} has {:?}", key, val);
    }

    current_dir.push(format!("{}.txt", Utc::now()));

    let file = OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create(true)
                    .open(current_dir)?;

    let writer = BufWriter::new(file);

    let store = KvStore::new(index, writer);

    Ok(store)
  }
}

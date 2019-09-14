#![deny(missing_docs)]
//! `KvStore` is a simple key/value store.

extern crate failure;
extern crate serde;
extern crate serde_json;

use failure::Fail;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Value};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::result;

// Threshold in bytes to compact logs.
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// Error types used by `KvStore`.
#[derive(Debug, Fail)]
pub enum KvsError {
    /// IO error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serialization or deserialization error.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// Removing non-exist key error.
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Unexpected command type error, which indicates either a corrupted log
    /// or a prograM bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

/// Result<T> is the custom error type for `KvStore`.
pub type Result<T> = result::Result<T, KvsError>;

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

// LogID is used to compose log file name, i.e.,, <log_id>.log.
type LogID = u64;
// LogOffset represents a Command's offset resides in a log file.
type LogOffset = u64;

/// `KvStore` stores string key/value pairs.
///
/// Key/value string pairs are stored in a `HashMap` in memory but not
/// persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # use std::env::current_dir;
///
/// fn main() -> Result<()> {
///     let mut store = KvStore::open(current_dir()?.as_path())?;
///     store.set("key".to_owned(), "value".to_owned())?;
///     let val = store.get("key".to_owned())?;
///     assert_eq!(val, Some("value".to_owned()));
///     Ok(())
/// }
/// ```
pub struct KvStore {
    // Root dir where KvStore resides.
    root_dir: PathBuf,
    // Log readers.
    log_readers: HashMap<LogID, File>,
    // Log writer.
    log_writer: File,
    // Log id being used by the log writer.
    log_id: LogID,
    // In-memory index containig all keys and corresponding value entries
    // pointing to log files.
    key_dir: BTreeMap<String, ValueEntry>,
    // Size of all log files.
    size: u64,
}

/// `ValueEntry` describes how value is stored on disk, e.g., <log_id>.log
/// and the corresponding offset within that file.
struct ValueEntry {
    log_id: LogID,
    log_offset: LogOffset,
}

// Command to be persisted in log files.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, val: String },
    Remove { key: String },
}

impl KvStore {
    /// Creates a `KvStore` from a path.
    pub fn open(path: &Path) -> Result<Self> {
        // Create the path if it does not exist yet.
        if !path.exists() {
            fs::create_dir_all(path)?
        }

        // Traverse the root dir and derive the existing log ids among log
        // files.
        let mut log_ids: Vec<LogID> = path
            .read_dir()?
            .flat_map(|entry| -> Result<_> { Ok(entry?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(".log"))
                    .map(str::parse::<LogID>)
            })
            .flatten()
            .collect();
        log_ids.sort_unstable();

        // Open the existing log files to read, reconstruct key_dir and update
        // size.
        let mut log_readers = HashMap::new();
        let mut key_dir = BTreeMap::new();
        let mut size = 0;
        for &log_id in &log_ids {
            let mut log_reader = OpenOptions::new().read(true).open(log_path(path, log_id))?;
            size += load(log_id, &mut log_reader, &mut key_dir)?;
            log_readers.insert(log_id, log_reader);
        }

        // Create a new log file for appending Set and Remove Commands.
        let log_id = log_ids.last().unwrap_or(&0) + 1;
        let log_writer = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(log_path(path, log_id))?;

        // Update log readers with respec to the newly created log file.
        let log_reader = OpenOptions::new().read(true).open(log_path(path, log_id))?;
        log_readers.insert(log_id, log_reader);

        // Return an instance of KvStore.
        Ok(KvStore {
            root_dir: path.to_path_buf(),
            log_readers,
            log_id,
            log_writer,
            key_dir,
            size,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the existing value will be overwritten.
    ///
    /// Returns an error if the value is not written successfully.
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        // Serialize the Set Command and append to the log file by log_writer.
        let cmd = Command::Set { key, val };
        let log_offset = self.log_writer.seek(SeekFrom::Current(0))?;
        serde_json::to_writer(&self.log_writer, &cmd)?;

        // Update the size after appending the Set Command.
        let next_log_offset = self.log_writer.seek(SeekFrom::Current(0))?;
        self.size += next_log_offset - log_offset;

        // Update in-memory key_dir with respect to the newly added
        // <key, val_entry>.
        if let Command::Set { key, .. } = cmd {
            self.key_dir.insert(
                key,
                ValueEntry {
                    log_id: self.log_id,
                    log_offset,
                },
            );
        }

        // Compact logs if needed.
        if self.size > COMPACTION_THRESHOLD {
            self.compact()?
        }

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    /// Returns an error if the value is not read successfully.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // Check whether the key exists in key_dir.
        if let Some(val_entry) = self.key_dir.get(&key) {
            // Identify the proper log_reader by log_id from ValueEntry.
            let log_reader = self
                .log_readers
                .get_mut(&val_entry.log_id)
                .expect("Could not find log reader!");

            // Adjust log_reader's cursor by log_offset from ValueEntry.
            log_reader.seek(SeekFrom::Start(val_entry.log_offset))?;

            // Parse the value from targeted Set Command as read from the stream.
            let mut stream = Deserializer::from_reader(log_reader).into_iter::<Value>();
            if let Some(entry) = stream.next() {
                let entry = entry?;
                let cmd: Command = serde_json::from_value(entry)?;
                if let Command::Set { val, .. } = cmd {
                    Ok(Some(val))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            } else {
                // Reach the end of stream.
                Ok(None)
            }
        } else {
            // Return None given the key does not exist.
            Ok(None)
        }
    }

    /// Removes a given string key.
    ///
    /// Return an error if the key does not exist or is not removed
    /// successfully.
    pub fn remove(&mut self, key: String) -> Result<()> {
        // Check whether the key exists in key_dir.
        if self.key_dir.contains_key(&key) {
            // Serialize the Remove Command and append to the log file by
            // log_writer.
            let cmd = Command::Remove { key };
            let log_offset = self.log_writer.seek(SeekFrom::Current(0))?;
            serde_json::to_writer(&self.log_writer, &cmd)?;

            // Update the size after appending the Remove Command.
            let next_log_offset = self.log_writer.seek(SeekFrom::Current(0))?;
            self.size += next_log_offset - log_offset;

            // Update in-memory key_dir by removing the key.
            if let Command::Remove { key } = cmd {
                self.key_dir.remove(&key);
            }

            // Compact logs if needed.
            if self.size > COMPACTION_THRESHOLD {
                self.compact()?
            }

            Ok(())
        } else {
            // Return an error given the key does not exist.
            Err(KvsError::KeyNotFound)
        }
    }

    // Creates a new log file to read & write based on a given log_id.
    fn new_log_file(&mut self, log_id: LogID) -> Result<File> {
        let log_path = log_path(&self.root_dir, log_id);

        let log_writer = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(log_path.as_path())?;

        self.log_readers.insert(
            log_id,
            OpenOptions::new().read(true).open(log_path.as_path())?,
        );

        Ok(log_writer)
    }

    // Compact `KvStore` by removing stale commands in logs.
    fn compact(&mut self) -> Result<()> {
        // New log file for compacted logs.
        let next_log_id = self.log_id + 1;
        // Update the log file as pointed by log_writer for actively appending
        // Commands.
        self.log_id += 2;
        // Reset the size for log files.
        self.size = 0;

        let mut compact_log_writer = self.new_log_file(next_log_id)?;
        self.log_writer = self.new_log_file(self.log_id)?;

        // Travese key_dir values to reconstruct compacted log file from all
        // existing logs.
        for (key, val_entry) in self.key_dir.iter_mut() {
            // Identify the log_reader by log_id from ValueEntry.
            let log_reader = self
                .log_readers
                .get_mut(&val_entry.log_id)
                .expect("Could not find log reader!");
            // Adjust log_reader's cursor by log_offset from ValueEntry.
            log_reader.seek(SeekFrom::Start(val_entry.log_offset))?;
            let mut stream = Deserializer::from_reader(log_reader).into_iter::<Value>();
            if let Some(entry) = stream.next() {
                let entry = entry?;
                // Parse the value from targeted Set Command as read from the stream.
                let cmd: Command = serde_json::from_value(entry)?;
                if let Command::Set { val, .. } = cmd {
                    let cmd = Command::Set {
                        key: key.to_string(),
                        val,
                    };

                    // Update key_dir's ValueEntry by pointing to the updated
                    // log_id and log_offset.
                    let log_offset = compact_log_writer.seek(SeekFrom::Current(0))?;
                    *val_entry = ValueEntry {
                        log_offset,
                        log_id: next_log_id,
                    };

                    // Serialize the Set Command and append to the log file by
                    // compact_log_writer.
                    serde_json::to_writer(&compact_log_writer, &cmd)?;

                    // Update the size after appending the Set Command.
                    let next_log_offset = compact_log_writer.seek(SeekFrom::Current(0))?;
                    self.size += next_log_offset - log_offset;
                }
            }
        }

        // Delete the stale log files.
        let stale_log_ids: Vec<_> = self
            .log_readers
            .keys()
            .filter(|&&log_id| log_id < next_log_id)
            .cloned()
            .collect();
        for stale_log_id in stale_log_ids {
            self.log_readers.remove(&stale_log_id);
            fs::remove_file(log_path(&self.root_dir, stale_log_id))?;
        }

        Ok(())
    }
}

// Constructs the name of the log path from the given path and log_id.
fn log_path(dir: &Path, log_id: LogID) -> PathBuf {
    dir.join(format!("{}.log", log_id))
}

// Constructs key_dir from a log file.
fn load(
    log_id: LogID,
    log_reader: &mut File,
    key_dir: &mut BTreeMap<String, ValueEntry>,
) -> Result<u64> {
    let mut log_offset = log_reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(log_reader).into_iter::<Value>();
    let mut size = 0;
    while let Some(entry) = stream.next() {
        let next_log_offset = stream.byte_offset() as LogOffset;
        let entry = entry?;
        let entry: Command = serde_json::from_value(entry)?;
        match entry {
            Command::Set { key, .. } => {
                key_dir.insert(key, ValueEntry { log_id, log_offset });
            }
            Command::Remove { key } => {
                key_dir.remove(&key);
            }
        }
        size += next_log_offset - log_offset;
        log_offset = next_log_offset;
    }
    Ok(size)
}

extern crate structopt;

use kvs::{KvStore, KvsError, Result};
use std::env::current_dir;
use std::process::exit;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = env!("CARGO_PKG_NAME"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
enum Kvs {
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", required = true, help = "KEY to set")]
        key: String,
        #[structopt(name = "VALUE", required = true, help = "VALUE to set")]
        val: String,
    },
    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", required = true, help = "KEY to get")]
        key: String,
    },
    #[structopt(name = "rm", about = "Remove a given string key")]
    Remove {
        #[structopt(name = "KEY", required = true, help = "KEY to remove")]
        key: String,
    },
}

fn main() -> Result<()> {
    match Kvs::from_args() {
        Kvs::Set { key, val } => {
            let mut store = KvStore::open(current_dir()?.as_path())?;
            store.set(key, val)?;
        }
        Kvs::Get { key } => {
            let mut store = KvStore::open(current_dir()?.as_path())?;
            if let Some(val) = store.get(key)? {
                println!("{}", val);
            } else {
                println!("Key not found");
            }
        }
        Kvs::Remove { key } => {
            let mut store = KvStore::open(current_dir()?.as_path())?;
            match store.remove(key) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}

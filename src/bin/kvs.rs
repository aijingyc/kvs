extern crate structopt;

use std::process;
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

fn main() {
    match Kvs::from_args() {
        Kvs::Set { key, val } => {
            eprintln!("set is unimplemented for key: {} val: {}", key, val);
            process::exit(1);
        }
        Kvs::Get { key } => {
            eprintln!("get is unimplemented for key {}", key);
            process::exit(1);
        }
        Kvs::Remove { key } => {
            eprintln!("remove is unimplemented for key {}", key);
            process::exit(1);
        }
    }
}

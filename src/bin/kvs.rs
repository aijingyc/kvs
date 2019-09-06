extern crate clap;
use clap::{App, Arg, SubCommand};
use std::process;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").help("KEY to set").required(true))
                .arg(Arg::with_name("VALUE").help("VALUE to set").required(true)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("KEY to get").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given string key")
                .arg(Arg::with_name("KEY").help("KEY to remove").required(true)),
        )
        .get_matches();

    match matches.subcommand_name() {
        Some("set") => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        Some("get") => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        Some("rm") => {
            eprintln!("unimplemented");
            process::exit(1);
        }
        _ => unimplemented!(),
    }
}

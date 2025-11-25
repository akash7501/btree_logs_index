mod btree_node;

use crate::btree_node::{BTree, RecordPointer};
use std::fs::File;
use std::io::{self, Write, Read, Seek, SeekFrom};
use std::path::Path;

fn main() {
    println!("Rust Log Search Tool (LOCAL MODE)");
    println!("----------------------------------");
    println!("Index file: /data/index.data");
    println!("WARNING: This search reads from a SINGLE log file only.");
    println!("Type a key to search, or 'exit' to quit.\n");

    // Open B-tree index
    let mut btree = BTree::open(Path::new("/data/index.data"));

    loop {
        print!("search> ");
        io::stdout().flush().unwrap();

        let mut key = String::new();
        io::stdin().read_line(&mut key).unwrap();
        let key = key.trim().to_string();

        if key == "exit" {
            println!("Exiting search tool.");
            break;
        }

        match btree.search(&key) {
            Some(ptr) => {
                println!(
                    "\nFOUND '{}' at offset={} length={}",
                    key, ptr.offset, ptr.length
                );

                let actual = read_log_entry(ptr);
                println!("Log Entry:\n{}\n", actual);
            }
            None => {
                println!("Not found: {}\n", key);
            }
        }
    }
}

pub fn read_log_entry(ptr: RecordPointer) -> String {
    // CHANGE THIS PATH TO YOUR LOG FILE IF NEEDED
    let log_path = "/data/logs/app.log";

    let mut file = File::open(log_path)
        .unwrap_or_else(|_| panic!("Cannot open local log file: {}", log_path));

    file.seek(SeekFrom::Start(ptr.offset))
        .expect("seek failed");

    let mut buf = vec![0u8; ptr.length as usize];
    file.read_exact(&mut buf)
        .expect("failed to read log bytes");

    String::from_utf8_lossy(&buf).into_owned()
}

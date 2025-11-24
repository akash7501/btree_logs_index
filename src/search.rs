

mod btree_node;

use crate::btree_node::{BTree, RecordPointer};
use std::fs::File;
use std::io::{self, Write, Read, Seek, SeekFrom};
use std::path::Path;

fn main() {
    let mut btree = BTree::open(Path::new("index.db"));

    println!("Search Agent started.");
    println!("Type a key to search, or 'exit' to quit.\n");

    loop {
        print!("search> ");
        io::stdout().flush().unwrap();

        let mut key = String::new();
        io::stdin().read_line(&mut key).unwrap();
        let key = key.trim().to_string();

        if key == "exit" {
            println!("Exiting search agent.");
            break;
        }

        if let Some(ptr) = btree.search(&key) {
            println!("FOUND '{}' at offset={} length={}",
                key, ptr.offset, ptr.length);

            let actual = read_log_entry(ptr);
            println!("\nActual log line:\n{}", actual);
        } else {
            println!("Not found: {}", key);
        }
    }
}

pub fn read_log_entry(ptr: RecordPointer) -> String {
    let mut file = File::open("logs/app.log").unwrap();

    file.seek(SeekFrom::Start(ptr.offset)).unwrap();

    let mut buf = vec![0u8; ptr.length as usize];
    file.read_exact(&mut buf).unwrap();

    String::from_utf8(buf).unwrap()
}

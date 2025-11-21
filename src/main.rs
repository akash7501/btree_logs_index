mod btree_node;

use crate::btree_node::{BTree, RecordPointer};
use serde::Serialize;
use std::fs::{OpenOptions, create_dir_all, File};
use std::io::{Write, Seek, SeekFrom, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const PAGE_SIZE: u64 = 4096;

#[derive(Serialize)]
struct LogEntry {
    ts: u128,
    level: String,
    msg: String,
}

fn main() {
    // Ensure logs folder exists
    create_dir_all("logs").unwrap();

    // Open or create the index file
    let path = Path::new("index.db");
    let mut btree = BTree::open(path);

    println!("Opened index.db");
    println!("root_page = {}", btree.root_page);
    println!("next_page = {}", btree.next_page);
    println!("----------------------------------");

    // Single log file
    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("logs/app.log")
        .unwrap();

    for i in 0..11 {
        // Current file offset (this is the pointer!)
        let offset = log_file.seek(SeekFrom::Current(0)).unwrap();

        // Create log string
        let msg_key = format!("Unique log message #{}", i);

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH).unwrap()
            .as_millis();

        let entry = LogEntry {
            ts,
            level: "INFO".to_string(),
            msg: msg_key.clone(),
        };

        let json_line = serde_json::to_string(&entry).unwrap() + "\n";

        // Write log to file
        log_file.write_all(json_line.as_bytes()).unwrap();
        log_file.flush().unwrap();

        // Build pointer for B-tree
        let ptr = RecordPointer {
            offset,
            length: json_line.len() as u32,
        };

        // Insert into B-tree
        btree.insert(msg_key.clone(), ptr);
    }

    println!("\nFinished writing logs.\n");

    // ------------------------------------------------
    // SEARCH for a log and read its actual content
    // ------------------------------------------------
    let search_key = "Unique log message #27";

    if let Some(ptr) = btree.search(search_key) {
        println!("FOUND '{}' at offset={} length={}",
            search_key, ptr.offset, ptr.length);

        let actual = read_log_entry(ptr);
        println!("\nActual log line:\n{}", actual);
    } else {
        println!("Not found: {}", search_key);
    }
    println!("\n========= PAGE-WISE VISUALIZATION =========");
for page in 1..btree.next_page {
    btree.debug_print_page(page);
}

println!("\n========= FULL TREE STRUCTURE =========");
btree.debug_print_tree();

}

// Read log from logs/app.log using the pointer
pub fn read_log_entry(ptr: RecordPointer) -> String {
    let mut file = File::open("logs/app.log").unwrap();

    file.seek(SeekFrom::Start(ptr.offset)).unwrap();

    let mut buf = vec![0u8; ptr.length as usize];
    file.read_exact(&mut buf).unwrap();

    String::from_utf8(buf).unwrap()
}

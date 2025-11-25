mod btree_node;

use crate::btree_node::{BTree, RecordPointer};
use serde::Serialize;
use std::fs::{OpenOptions, create_dir_all, File};
use std::io::{Write, Seek, SeekFrom, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::thread;
use std::time;
use btree_node::{DISK_READS, DISK_WRITES};
use std::sync::atomic::Ordering;

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
    let path = Path::new("index.data");
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

    for i in 0..2000 {
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
println!("================ Disk Access Summary ================");
println!("Disk Reads  : {}", DISK_READS.load(Ordering::Relaxed));
println!("Disk Writes : {}", DISK_WRITES.load(Ordering::Relaxed));
println!("=====================================================");

}
// Read log from logs/app.log using the pointer
pub fn read_log_entry(ptr: RecordPointer) -> String {
    let mut file = File::open("logs/app.log").unwrap();

    file.seek(SeekFrom::Start(ptr.offset)).unwrap();

    let mut buf = vec![0u8; ptr.length as usize];
    file.read_exact(&mut buf).unwrap();

    String::from_utf8(buf).unwrap()
}

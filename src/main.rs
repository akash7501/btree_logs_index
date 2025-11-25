mod btree_node;

use crate::btree_node::{BTree, RecordPointer};
use serde::Serialize;
use std::fs::{OpenOptions, create_dir_all, File};
use std::io::{Write, Seek, SeekFrom, Read};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use btree_node::{DISK_READS, DISK_WRITES};
use std::sync::atomic::Ordering;


const LOG_PATH: &str = "logs/app.log";

#[derive(Serialize)]
struct LogEntry {
    ts: u128,
    level: String,
    msg: String,
}

fn main() {
    // Ensure logs directory exists
    create_dir_all("logs").unwrap();

    // Open or create B-Tree index file
    let path = Path::new("index.data");
    let mut btree = BTree::open(path);

    println!("Opened index.data");
    println!("root_page = {}", btree.root_page);
    println!("next_page = {}", btree.next_page);
    println!("----------------------------------");

    // Open or create the log file
    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(LOG_PATH)
        .unwrap();

    // Insert 2000 log entries
    for i in 0..2000 {
        // Current write offset -> pointer for B-tree
        let offset = log_file.seek(SeekFrom::Current(0)).unwrap();

        let msg_key = format!("Unique log message #{}", i);

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let entry = LogEntry {
            ts,
            level: "INFO".to_string(),
            msg: msg_key.clone(),
        };

        let json_line = serde_json::to_string(&entry).unwrap() + "\n";

        // Write the actual log line
        log_file.write_all(json_line.as_bytes()).unwrap();
        log_file.flush().unwrap();

        // Build B-tree pointer
        let ptr = RecordPointer {
            offset,
            length: json_line.len() as u32,
        };

        // Insert into B-tree index
        btree.insert(msg_key.clone(), ptr);
    }

    println!("\nFinished writing logs.\n");

    // -------------------------------
    // Test a search
    // -------------------------------
    let search_key = "Unique log message #27";

    if let Some(ptr) = btree.search(search_key) {
        println!("FOUND '{}' at offset={} length={}",
            search_key, ptr.offset, ptr.length);

        let actual = read_log_entry(ptr);
        println!("\nActual log line:\n{}", actual);
    } else {
        println!("Not found: {}", search_key);
    }

    // Flush B-Tree pages to disk
    btree.flush();
     let reads = DISK_READS.load(Ordering::Relaxed);
    let writes = DISK_WRITES.load(Ordering::Relaxed);

    println!("----------------------------------");
    println!("Disk Access Summary");
    println!("Disk Reads  : {}", reads);
    println!("Disk Writes : {}", writes);
    println!("----------------------------------");
}

// Read log entry from log file using RecordPointer
pub fn read_log_entry(ptr: RecordPointer) -> String {
    let mut file = File::open(LOG_PATH).unwrap();

    file.seek(SeekFrom::Start(ptr.offset)).unwrap();

    let mut buf = vec![0u8; ptr.length as usize];
    file.read_exact(&mut buf).unwrap();

    String::from_utf8(buf).unwrap()
}

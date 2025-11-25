use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::btree_node::{BTree, RecordPointer};
use serde_json::Value;

pub struct LogTailer {
    offsets: HashMap<PathBuf, u64>,
}

impl LogTailer {
    pub fn new() -> Self {
        Self { offsets: HashMap::new() }
    }

    pub fn tail_file(&mut self, path: &PathBuf, btree: &mut BTree) {
        // Open file
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return,
        };

        // Previous offset
        let old_offset = *self.offsets.get(path).unwrap_or(&0);

        // Jump to last read byte
        if file.seek(SeekFrom::Start(old_offset)).is_err() {
            return;
        }

        // Read only newly appended bytes
        let mut buf = Vec::new();
        if file.read_to_end(&mut buf).is_err() {
            return;
        }
        if buf.is_empty() {
            return;
        }

        // Process new logs
        let mut pos_in_buf: usize = 0;

        for line_bytes in buf.split(|&b| b == b'\n') {
            if line_bytes.is_empty() {
                pos_in_buf += 1;
                continue;
            }

            // Convert bytes → string
            let line_str = match String::from_utf8(line_bytes.to_vec()) {
                Ok(s) => s,
                Err(_) => String::from_utf8_lossy(line_bytes).into_owned(),
            };

            // Compute absolute file offset
            let line_offset = old_offset + pos_in_buf as u64;
            let line_len = line_bytes.len();

            // -------------------------
            // Extract "msg" from JSON
            // -------------------------
            let key = match serde_json::from_str::<Value>(&line_str) {
                Ok(v) => v.get("msg")
                          .and_then(|m| m.as_str())
                          .unwrap_or(&line_str)
                          .to_string(),
                Err(_) => line_str.clone(),
            };

            // Build pointer
            let ptr = RecordPointer {
                offset: line_offset,
                length: line_len as u32,
            };

            // Insert into B-tree
            btree.insert(key.clone(), ptr);

            // Print log for debug
            println!("{}", line_str);

            pos_in_buf += line_len + 1;
        }

        // Update offset to EOF
        if let Ok(new_offset) = file.seek(SeekFrom::End(0)) {
            self.offsets.insert(path.clone(), new_offset);
        }
    }
}

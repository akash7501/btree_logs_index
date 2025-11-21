// src/btree_node.rs
use std::convert::TryInto;
use std::fs::{OpenOptions, File};
use std::io::{Seek, SeekFrom, Read, Write};
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;
pub const ORDER: usize =4;
pub const MAX_KEYS: usize = 2 * ORDER - 1;

#[derive(Clone, Copy, Debug)]
pub struct RecordPointer {
    pub offset: u64,
    pub length: u32,
}

#[derive(Debug)]
pub struct BTreeNode {
    pub is_leaf: bool,
    pub keys: Vec<String>,
    pub values: Vec<RecordPointer>, // only used for leaf nodes
    pub children: Vec<u64>,         // page ids, only used for internal nodes
}

impl BTreeNode {
    pub fn new_leaf() -> Self {
        BTreeNode {
            is_leaf: true,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn new_internal() -> Self {
        BTreeNode {
            is_leaf: false,
            keys: Vec::new(),
            values: Vec::new(), // unused for internal nodes
            children: Vec::new(),
        }
    }
}

pub struct BTree {
    pub index: File,
    pub root_page: u64,
    pub next_page: u64,
}

impl BTree {
    // -----------------------
    // open/create index file
    // -----------------------
    pub fn open(path: &Path) -> Self {
        let mut file = OpenOptions::new()
            .read(true).write(true).create(true)
            .open(path)
            .unwrap();

        let metadata = file.metadata().unwrap();
        let file_len = metadata.len();

        let (root_page, next_page) = if file_len == 0 {
            // brand new file: write header page
            let mut header = vec![0u8; PAGE_SIZE];
            header[0..8].copy_from_slice(&0u64.to_le_bytes()); // root_page = 0
            header[8..16].copy_from_slice(&1u64.to_le_bytes()); // next_page = 1
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&header).unwrap();
            file.sync_all().unwrap();
            (0u64, 1u64)
        } else {
            // read header
            let mut header = [0u8; PAGE_SIZE];
            file.seek(SeekFrom::Start(0)).unwrap();
            file.read_exact(&mut header).unwrap();
            let root = u64::from_le_bytes(header[0..8].try_into().unwrap());
            let next = u64::from_le_bytes(header[8..16].try_into().unwrap());

            // reconcile next_page with actual file length to avoid EOF reads
            let actual_pages = if file_len == 0 { 0 } else { (file_len / PAGE_SIZE as u64) };
            let reconciled_next = if actual_pages == 0 { 1 } else { actual_pages };
            let next_page_final = if next > reconciled_next { reconciled_next } else { next };

            (root, next_page_final)
        };

        BTree { index: file, root_page, next_page }
    }

    // -----------------------
    // low level page ops
    // -----------------------
    pub fn alloc_page(&mut self) -> u64 {
        let new_page = self.next_page;
        let empty = [0u8; PAGE_SIZE];
        self.write_raw_page(new_page, &empty);
        self.next_page += 1;
        self.update_header();
        new_page
    }

    fn write_raw_page(&mut self, page_id: u64, buf: &[u8; PAGE_SIZE]) {
        let offset = page_id * PAGE_SIZE as u64;
        self.index.seek(SeekFrom::Start(offset)).unwrap();
        self.index.write_all(buf).unwrap();
        self.index.flush().unwrap();
    }

    fn read_raw_page(&mut self, page_id: u64) -> [u8; PAGE_SIZE] {
        let offset = page_id * PAGE_SIZE as u64;
        let mut buf = [0u8; PAGE_SIZE];

        // auto-extend file if necessary to avoid UnexpectedEof in dev
        let file_len = self.index.metadata().unwrap().len();
        if file_len < offset + PAGE_SIZE as u64 {
            // extend with zeros up to that page
            self.index.seek(SeekFrom::End(0)).unwrap();
            let mut remaining = (offset + PAGE_SIZE as u64).saturating_sub(file_len);
            let zeros = vec![0u8; PAGE_SIZE];
            while remaining > 0 {
                let write_len = std::cmp::min(remaining, PAGE_SIZE as u64) as usize;
                self.index.write_all(&zeros[..write_len]).unwrap();
                remaining -= write_len as u64;
            }
            self.index.flush().unwrap();
        }

        self.index.seek(SeekFrom::Start(offset)).unwrap();
        self.index.read_exact(&mut buf).unwrap();
        buf
    }

    fn update_header(&mut self) {
        let mut header = [0u8; PAGE_SIZE];
        header[0..8].copy_from_slice(&self.root_page.to_le_bytes());
        header[8..16].copy_from_slice(&self.next_page.to_le_bytes());
        self.write_raw_page(0, &header);
    }

    // -----------------------
    // encode/decode nodes
    // -----------------------
    // Layout (per page):
    // [1 byte is_leaf][2 bytes key_count][for each key: 2 bytes klen | klen bytes key | (if leaf -> 8 bytes offset + 4 bytes length)]
    // If internal: after keys write (key_count+1) * 8 bytes children
    pub fn write_node(&mut self, page_id: u64, node: &BTreeNode) {
        let mut buf = [0u8; PAGE_SIZE];

        buf[0] = if node.is_leaf { 1 } else { 0 };
        buf[1..3].copy_from_slice(&(node.keys.len() as u16).to_le_bytes());

        let mut pos: usize = 3;

        for i in 0..node.keys.len() {
            let kb = node.keys[i].as_bytes();
            let klen = kb.len() as u16;

            buf[pos..pos + 2].copy_from_slice(&klen.to_le_bytes());
            pos += 2;

            buf[pos..pos + klen as usize].copy_from_slice(kb);
            pos += klen as usize;

            if node.is_leaf {
                // value: offset (8) + length (4)
                buf[pos..pos + 8].copy_from_slice(&node.values[i].offset.to_le_bytes());
                pos += 8;
                buf[pos..pos + 4].copy_from_slice(&node.values[i].length.to_le_bytes());
                pos += 4;
            }
        }

        if !node.is_leaf {
            // children (key_count + 1)
            for child in &node.children {
                buf[pos..pos + 8].copy_from_slice(&child.to_le_bytes());
                pos += 8;
            }
        }

        self.write_raw_page(page_id, &buf);
    }

    pub fn read_node(&mut self, page_id: u64) -> BTreeNode {
        let buf = self.read_raw_page(page_id);

        let is_leaf = buf[0] == 1;
        let key_count = u16::from_le_bytes(buf[1..3].try_into().unwrap()) as usize;

        let mut pos: usize = 3;
        let mut keys = Vec::with_capacity(key_count);
        let mut values = Vec::with_capacity(key_count);

        for _ in 0..key_count {
            let klen = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;

            let key = String::from_utf8(buf[pos..pos + klen].to_vec()).unwrap();
            pos += klen;

            keys.push(key);

            if is_leaf {
                let offset = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let length = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
                pos += 4;
                values.push(RecordPointer { offset, length });
            }
        }

        let mut children = Vec::new();
        if !is_leaf {
            for _ in 0..(key_count + 1) {
                let child = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
                pos += 8;
                children.push(child);
            }
        }

        BTreeNode { is_leaf, keys, values, children }
    }

    // -----------------------
    // splitting + insertion
    // -----------------------

    /// split child at parent.children[index]
    pub fn split_child(&mut self, parent_page: u64, index: usize) {
        let t = ORDER;

        let mut parent = self.read_node(parent_page);
        let child_page = parent.children[index];
        let mut y = self.read_node(child_page);

        if y.keys.len() != MAX_KEYS {
            return;
        }

        // new page z
        let z_page = self.alloc_page();
        let mut z = if y.is_leaf { BTreeNode::new_leaf() } else { BTreeNode::new_internal() };

        // middle key
        let middle_key = y.keys[t - 1].clone();

        if y.is_leaf {
            // split leaf: keys[0..t-1] | middle | keys[t..]
            z.keys = y.keys.split_off(t);
            z.values = y.values.split_off(t);

            y.keys.truncate(t - 1);
            y.values.truncate(t - 1);
        } else {
            // split internal: keys[0..t-1] | middle | keys[t..]
            z.keys = y.keys.split_off(t);
            // children split
            z.children = y.children.split_off(t);
            y.keys.truncate(t - 1);
            y.children.truncate(t);
        }

        // insert z_page after child_page
        parent.children.insert(index + 1, z_page);
        // insert middle key into parent.keys
        parent.keys.insert(index, middle_key);

        // write back changes
        self.write_node(child_page, &y);
        self.write_node(z_page, &z);
        self.write_node(parent_page, &parent);
    }

    /// insert into non-full node at page_id
    pub fn insert_nonfull(&mut self, page_id: u64, key: String, ptr: RecordPointer) {
        let mut node = self.read_node(page_id);

        if node.is_leaf {
            let pos = match node.keys.binary_search(&key) {
                Ok(i) => i,
                Err(i) => i,
            };
            node.keys.insert(pos, key);
            node.values.insert(pos, ptr);
            self.write_node(page_id, &node);
            return;
        }

        // internal node: find child index
        let mut idx = match node.keys.binary_search(&key) {
            Ok(i) => i + 1,
            Err(i) => i,
        };

        let child_page = node.children[idx];
        let child = self.read_node(child_page);

        if child.keys.len() == MAX_KEYS {
            self.split_child(page_id, idx);
            // reload parent
            node = self.read_node(page_id);
            // decide which child to go to
            if key > node.keys[idx] {
                idx += 1;
            }
        }

        let next_child = node.children[idx];
        self.insert_nonfull(next_child, key, ptr);
    }

    /// top-level insert
    pub fn insert(&mut self, key: String, ptr: RecordPointer) {
        if self.root_page == 0 {
            let page = self.alloc_page();
            let mut leaf = BTreeNode::new_leaf();
            leaf.keys.push(key);
            leaf.values.push(ptr);
            self.write_node(page, &leaf);
            self.root_page = page;
            self.update_header();
            return;
        }

        let root = self.read_node(self.root_page);

        if root.keys.len() == MAX_KEYS {
            // create new root
            let new_root_page = self.alloc_page();
            let mut new_root = BTreeNode::new_internal();
            new_root.children.push(self.root_page);
            self.write_node(new_root_page, &new_root);

            // set as root
            self.root_page = new_root_page;
            self.update_header();

            // split child 0
            self.split_child(new_root_page, 0);

            // continue insert
            self.insert_nonfull(new_root_page, key, ptr);
        } else {
            self.insert_nonfull(self.root_page, key, ptr);
        }
    }

    // -----------------------
    // search
    // -----------------------
    pub fn search(&mut self, key: &str) -> Option<RecordPointer> {
        if self.root_page == 0 {
            return None;
        }
        self.search_node(self.root_page, key)
    }

    fn search_node(&mut self, page_id: u64, key: &str) -> Option<RecordPointer> {
        let node = self.read_node(page_id);

        match node.keys.binary_search(&key.to_string()) {
            Ok(i) => {
                if node.is_leaf {
                    return Some(node.values[i]);
                } else {
                    // in our layout: internal nodes don't have values, so
                    // when we find equal key in internal, pick child i+1 and descend
                    let child = node.children[i + 1];
                    return self.search_node(child, key);
                }
            }
            Err(i) => {
                if node.is_leaf {
                    return None;
                } else {
                    let child = node.children[i];
                    return self.search_node(child, key);
                }
            }
        }
    }
}

impl BTree {

    /// Print a summary of a single page on disk
    pub fn debug_print_page(&mut self, page_id: u64) {
        let node = self.read_node(page_id);

        println!("-----------------------------------------------");
        println!("PAGE ID: {}", page_id);
        println!("TYPE   : {}", if node.is_leaf { "Leaf" } else { "Internal" });
        println!("KEYS   : {}", node.keys.len());

        if node.is_leaf {
            for (i, k) in node.keys.iter().enumerate() {
                println!("  [{}] KEY='{}', offset={}, length={}",
                    i, k, node.values[i].offset, node.values[i].length);
            }
        } else {
            for (i, k) in node.keys.iter().enumerate() {
                println!("  [{}] KEY='{}'  CHILD_L={}, CHILD_R={}",
                    i, k, node.children[i], node.children[i+1]);
            }
        }
    }


    /// Recursively print the full B-tree structure
    pub fn debug_print_tree(&mut self) {
        if self.root_page == 0 {
            println!("Tree empty.");
            return;
        }

        println!("\n======= B-TREE STRUCTURE (RECURSIVE) =======");
        self.debug_print_node_recursive(self.root_page, 0);
    }


    fn debug_print_node_recursive(&mut self, page_id: u64, depth: usize) {
        let node = self.read_node(page_id);
        let indent = "  ".repeat(depth);

        println!("{}PAGE {} [{}]", indent, page_id,
            if node.is_leaf { "Leaf" } else { "Internal" });

        println!("{}  Keys: {}", indent, node.keys.len());

        for key in &node.keys {
            println!("{}    • {}", indent, key);
        }

        if !node.is_leaf {
            println!("{}  Children:", indent);
            for child in &node.children {
                println!("{}    -> {}", indent, child);
            }

            // Recurse
            for child in node.children.clone() {
                self.debug_print_node_recursive(child, depth + 1);
            }
        }
    }
}

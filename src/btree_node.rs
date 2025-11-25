use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use lru::LruCache;

pub const PAGE_SIZE: usize = 8192;
pub const ORDER: usize = 100;
pub const MAX_KEYS: usize = 2 * ORDER - 1;
pub static DISK_READS: AtomicU64 = AtomicU64::new(0);
pub static DISK_WRITES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
pub struct RecordPointer {
    pub offset: u64,
    pub length: u32,
}

#[derive(Debug)]
pub struct BTreeNode {
    pub is_leaf: bool,
    pub keys: Vec<String>,
    pub values: Vec<RecordPointer>,
    pub children: Vec<u64>,
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
            values: Vec::new(),
            children: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct BufferFrame {
    pub page_id: u64,
    pub data: Vec<u8>, 
    pub is_dirty: bool,
    pub pin_count: usize,
}

impl BufferFrame {
    pub fn new(page_id: u64, data: Vec<u8>) -> Self {
        BufferFrame {
            page_id,
            data,
            is_dirty: false,
            pin_count: 0,
        }
    }
}

pub struct BufferPool {
    pub cache: LruCache<u64, BufferFrame>,
    pub file: File,
}

impl BufferPool {
    pub fn open_file<P: AsRef<Path>>(path: P, capacity: usize) -> std::io::Result<Self> {
        let cap_nz = NonZeroUsize::new(capacity)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "capacity must be > 0"))?;
        let file = File::options().read(true).write(true).create(true).open(path)?;
        Ok(Self {
            cache: LruCache::new(cap_nz),
            file,
        })
    }

    /// Read a page from disk and return bytes (zero-filled for beyond-file pages).
    fn read_page_from_disk(&mut self, page_id: u64) -> std::io::Result<Vec<u8>> {
        DISK_READS.fetch_add(1, Ordering::Relaxed);
        let mut buf = vec![0u8; PAGE_SIZE];
        let offset = page_id
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "page offset overflow"))?;

        let file_len = self.file.metadata()?.len();
        if file_len < offset + PAGE_SIZE as u64 {
            if file_len > offset {
                self.file.seek(SeekFrom::Start(offset))?;
                let to_read = (file_len - offset) as usize;
                self.file.read_exact(&mut buf[..to_read])?;
            }
            return Ok(buf);
        }

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Write page bytes to disk (overwrite).
    fn write_page_to_disk(&mut self, page_id: u64, data: &[u8]) -> std::io::Result<()> {
         DISK_WRITES.fetch_add(1, Ordering::Relaxed);
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let offset = page_id
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "page offset overflow"))?;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }

    /// Pin the page: ensure page is resident and increment pin_count.
    /// DOES NOT return a reference. Use frame_mut() to access the pinned frame.
    pub fn pin_page(&mut self, page_id: u64) -> std::io::Result<()> {
        // Fast path: already resident
        if let Some(frame) = self.cache.get_mut(&page_id) {
            frame.pin_count = frame.pin_count.saturating_add(1);
            return Ok(());
        }

        // Load page bytes from disk (needs &mut self)
        let buf = self.read_page_from_disk(page_id)?;

        // If the cache is full, evict one (may call write_page_to_disk)
        if self.cache.len() >= self.cache.cap().get() {
            self.evict_one()?;
        }

        // Insert and pin
        self.cache.put(page_id, BufferFrame::new(page_id, buf));
        if let Some(frame) = self.cache.get_mut(&page_id) {
            frame.pin_count = 1;
        }
        Ok(())
    }

    /// Unpin the page: decrement pin_count (never negative).
    pub fn unpin_page(&mut self, page_id: u64) {
        if let Some(frame) = self.cache.get_mut(&page_id) {
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
        }
    }

    /// Access a mutable reference to the frame. Caller must ensure the page is pinned.
    /// Returns `None` if the page is not resident.
    pub fn frame_mut(&mut self, page_id: u64) -> Option<&mut BufferFrame> {
        self.cache.get_mut(&page_id)
    }

    /// Mark resident frame dirty.
    pub fn mark_dirty(&mut self, page_id: u64) {
        if let Some(f) = self.cache.get_mut(&page_id) {
            f.is_dirty = true;
        }
    }

    /// Evict one unpinned LRU frame; write back if dirty.
    fn evict_one(&mut self) -> std::io::Result<()> {
        let capacity = self.cache.cap().get();
        for _ in 0..capacity.saturating_add(1) {
            if let Some((pid, frame)) = self.cache.pop_lru() {
                if frame.pin_count == 0 {
                    if frame.is_dirty {
                        self.write_page_to_disk(pid, &frame.data)?;
                    }
                    return Ok(());
                } else {
                    // reinstate pinned frame as MRU
                    self.cache.put(pid, frame);
                }
            } else {
                break;
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "no evictable page (all pages pinned)",
        ))
    }

    /// Read a page into an owned fixed-size array (pin/unpin internally).
    pub fn read_page_copy(&mut self, page_id: u64) -> std::io::Result<[u8; PAGE_SIZE]> {
        self.pin_page(page_id)?;
        let arr = {
            let frame = self.frame_mut(page_id).expect("frame should be present after pin");
            let mut a = [0u8; PAGE_SIZE];
            a.copy_from_slice(&frame.data);
            a
        };
        self.unpin_page(page_id);
        Ok(arr)
    }

    /// Write full page buffer into the pool (pin/unpin internally).
    pub fn write_page(&mut self, page_id: u64, buf: &[u8; PAGE_SIZE]) -> std::io::Result<()> {
        self.pin_page(page_id)?;
        {
            let frame = self.frame_mut(page_id).expect("frame should be present after pin");
            frame.data.copy_from_slice(buf);
            frame.is_dirty = true;
        }
        self.unpin_page(page_id);
        Ok(())
    }

    /// Flush dirty pages to disk (does not fsync).
    pub fn flush_all(&mut self) -> std::io::Result<()> {
        // collect keys first to avoid double-borrow
        let keys: Vec<u64> = self.cache.iter().map(|(k, _)| *k).collect();
        for pid in keys {
            if let Some(frame) = self.cache.get_mut(&pid) {
    if frame.is_dirty {
        // Step 1: copy needed data
        let data = frame.data.clone();
        // Step 2: mark clean inside cache
        frame.is_dirty = false;
        // Step 3: release &mut frame (borrow ends here)
        drop(frame);

        // Step 4: now safe to borrow &mut self again for disk write
        self.write_page_to_disk(pid, &data)?;
    }
}

        }
        Ok(())
    }

    /// Force fsync of the underlying file
    pub fn sync_all(&mut self) -> std::io::Result<()> {
        self.file.sync_all()
    }
}

/// The BTree structure using BufferPool
pub struct BTree {
    pub pool: BufferPool,
    pub root_page: u64,
    pub next_page: u64,
}

impl BTree {
    /// Open BTree with default cache capacity
    pub fn open(path: &Path) -> Self {
        Self::open_with_capacity(path, 1024).expect("open btree")
    }

    pub fn open_with_capacity(path: &Path, capacity: usize) -> std::io::Result<Self> {
        let mut pool = BufferPool::open_file(path, capacity)?;

        // Read header (page 0). If file empty, read_page_from_disk will return zeros.
        let header = pool.read_page_copy(0)?;
        let root = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let next = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let file_len = pool.file.metadata()?.len();
        let actual_pages = if file_len == 0 { 0 } else { file_len / PAGE_SIZE as u64 };
        let reconciled_next = if actual_pages == 0 { 1 } else { actual_pages };
        let next_page_final = if next == 0 { 1 } else { std::cmp::min(next, reconciled_next) };

        let (root_page, next_page) = if file_len == 0 || (root == 0 && next == 0 && actual_pages == 0) {
            let mut header_buf = [0u8; PAGE_SIZE];
            header_buf[0..8].copy_from_slice(&0u64.to_le_bytes());
            header_buf[8..16].copy_from_slice(&1u64.to_le_bytes());
            pool.write_page(0, &header_buf)?;
            pool.sync_all()?;
            (0u64, 1u64)
        } else {
            (root, next_page_final)
        };

        Ok(BTree {
            pool,
            root_page,
            next_page,
        })
    }

    pub fn alloc_page(&mut self) -> u64 {
        let new_page = self.next_page;
        let zero = [0u8; PAGE_SIZE];
        self.pool.write_page(new_page, &zero).expect("alloc write");
        self.next_page += 1;
        self.update_header();
        new_page
    }

    fn write_raw_page(&mut self, page_id: u64, buf: &[u8; PAGE_SIZE]) {
        self.pool
            .write_page(page_id, buf)
            .expect("write_raw_page failed");
    }

    fn read_raw_page(&mut self, page_id: u64) -> [u8; PAGE_SIZE] {
        self.pool
            .read_page_copy(page_id)
            .expect("read_raw_page failed")
    }

    fn update_header(&mut self) {
        let mut header = [0u8; PAGE_SIZE];
        header[0..8].copy_from_slice(&self.root_page.to_le_bytes());
        header[8..16].copy_from_slice(&self.next_page.to_le_bytes());
        self.write_raw_page(0, &header);
    }

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
                buf[pos..pos + 8].copy_from_slice(&node.values[i].offset.to_le_bytes());
                pos += 8;
                buf[pos..pos + 4].copy_from_slice(&node.values[i].length.to_le_bytes());
                pos += 4;
            }
        }

        if !node.is_leaf {
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

    pub fn split_child(&mut self, parent_page: u64, index: usize) {
        let t = ORDER;

        let mut parent = self.read_node(parent_page);
        let child_page = parent.children[index];
        let mut y = self.read_node(child_page);

        if y.keys.len() != MAX_KEYS {
            return;
        }

        let z_page = self.alloc_page();
        let mut z = if y.is_leaf { BTreeNode::new_leaf() } else { BTreeNode::new_internal() };

        let middle_key = y.keys[t - 1].clone();

        if y.is_leaf {
            z.keys = y.keys.split_off(t);
            z.values = y.values.split_off(t);

            y.keys.truncate(t - 1);
            y.values.truncate(t - 1);
        } else {
            z.keys = y.keys.split_off(t);
            z.children = y.children.split_off(t);
            y.keys.truncate(t - 1);
            y.children.truncate(t);
        }

        parent.children.insert(index + 1, z_page);
        parent.keys.insert(index, middle_key);

        self.write_node(child_page, &y);
        self.write_node(z_page, &z);
        self.write_node(parent_page, &parent);
    }

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

        let mut idx = match node.keys.binary_search(&key) {
            Ok(i) => i + 1,
            Err(i) => i,
        };

        let child_page = node.children[idx];
        let child = self.read_node(child_page);

        if child.keys.len() == MAX_KEYS {
            self.split_child(page_id, idx);
            node = self.read_node(page_id);
            if key > node.keys[idx] {
                idx += 1;
            }
        }

        let next_child = node.children[idx];
        self.insert_nonfull(next_child, key, ptr);
    }

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
            let new_root_page = self.alloc_page();
            let mut new_root = BTreeNode::new_internal();
            new_root.children.push(self.root_page);
            self.write_node(new_root_page, &new_root);

            self.root_page = new_root_page;
            self.update_header();

            self.split_child(new_root_page, 0);

            self.insert_nonfull(new_root_page, key, ptr);
        } else {
            self.insert_nonfull(self.root_page, key, ptr);
        }
    }

    /// search
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

    pub fn flush(&mut self) {
        self.pool.flush_all().expect("flush_all failed");
        self.pool.sync_all().expect("sync failed");
    }
}

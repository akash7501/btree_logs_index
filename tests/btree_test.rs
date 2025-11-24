use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use disk_btree::btree_node::{BTree, RecordPointer, PAGE_SIZE};

#[test]
fn insert_test() {
    let path = Path::new("insert_test.idx");

    if path.exists() {
        fs::remove_file(path).unwrap();
    }

    let mut bt = BTree::open(path);

    bt.insert("akash".to_string(), RecordPointer { offset: 10, length: 5 });

    let root = bt.read_node(bt.root_page);

    assert_eq!(root.keys.len(), 1);
    assert_eq!(root.keys[0], "akash");

    assert_eq!(root.values.len(), 1);
    assert_eq!(root.values[0].offset, 10);
    assert_eq!(root.values[0].length, 5);
}


#[test]
fn header_consistency_test() {
    let path = Path::new("header_consistency.idx");

    if path.exists() {
        fs::remove_file(path).unwrap();
    }

    {
        let mut bt = BTree::open(path);

        assert_eq!(bt.root_page, 0);
        assert_eq!(bt.next_page, 1);

        bt.insert("a".into(), RecordPointer { offset: 10, length: 5 });
        bt.insert("b".into(), RecordPointer { offset: 20, length: 5 });
        bt.insert("c".into(), RecordPointer { offset: 30, length: 5 });

        assert!(bt.root_page > 0);
    }

    let mut file = fs::File::open(path).unwrap();
    let mut header = [0u8; PAGE_SIZE];

    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut header).unwrap();

    let root_from_disk = u64::from_le_bytes(header[0..8].try_into().unwrap());
    let next_from_disk = u64::from_le_bytes(header[8..16].try_into().unwrap());

    let mut bt2 = BTree::open(path);

    assert_eq!(bt2.root_page, root_from_disk);
    assert_eq!(bt2.next_page, next_from_disk);

    let ptr = bt2.search("b").unwrap();
    assert_eq!(ptr.offset, 20);
}


#[test]
fn alloc_seq_page() {
    let path = Path::new("alloc_seq_test.idx");

    if path.exists() {
        fs::remove_file(path).unwrap();
    }

    let mut bt = BTree::open(path);

    assert_eq!(bt.next_page, 1);

    let p1 = bt.alloc_page();
    let p2 = bt.alloc_page();

    assert_eq!(p1, 1);
    assert_eq!(p2, 2);
}

#[test]
fn insert_into_empty() {
    let path = Path::new("empty_insert_test.idx");

    if path.exists() {
        fs::remove_file(path).unwrap();
    }

    let mut btree = BTree::open(path);

    assert_eq!(btree.root_page, 0);
    assert_eq!(btree.next_page, 1);

    btree.insert("A".to_string(), RecordPointer { offset: 111, length: 10 });

    assert!(btree.root_page > 0);

    let root = btree.read_node(btree.root_page);

    assert!(root.is_leaf);
    assert_eq!(root.keys.len(), 1);
    assert_eq!(root.keys[0], "A");

    assert_eq!(root.values[0].offset, 111);
    assert_eq!(root.values[0].length, 10);

    let result = btree.search("A").unwrap();
    assert_eq!(result.offset, 111);
    assert_eq!(result.length, 10);
}

#[test]
fn inset_more_key(){
    let path = Path::new("more.idx");
    let mut btree = BTree::open(path);
    let totolkey=200;
    for i in 0..totolkey{
        let key = format!("k{}",i);
         btree.insert(key.clone(), RecordPointer { offset: i as u64, length: 4 });
    }
    assert!(btree.next_page>=20);
    assert!(btree.root_page>0);
    let mut root = btree.read_node(btree.root_page);
    assert!(!root.is_leaf, "Root should NOT be a leaf once tree grows!");
     for test_key in [0, 5, 17, 50, 99, 150, 199] {
        let key = format!("k{}", test_key);
        let found = btree.search(&key);
        assert!(found.is_some(), "Key {} must be found!", key);

        let ptr = found.unwrap();
        assert_eq!(ptr.offset, test_key as u64);
        assert_eq!(ptr.length, 4);
    }



}
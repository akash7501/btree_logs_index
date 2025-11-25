mod k8s_logs;
mod tailer;
mod btree_node;

use notify::{Watcher, RecommendedWatcher, RecursiveMode, Event, EventKind};
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;

fn main() -> notify::Result<()> {
    println!("Starting Rust log-agent...");

    // -----------------------------------------
    // 1. Open your B-Tree index
    // -----------------------------------------
    let mut btree = btree_node::BTree::open(Path::new("/data/index.data"));

    // -----------------------------------------
    // 2. Create tailer
    // -----------------------------------------
    let mut tailer = tailer::LogTailer::new();

    // -----------------------------------------
    // 3. Tail existing logs once (optional)
    // -----------------------------------------
    for path in k8s_logs::discover_ls_logs() {
        tailer.tail_file(&path, &mut btree);
    }

    // -----------------------------------------
    // 4. Setup filesystem watcher
    // -----------------------------------------
    let (tx, rx) = channel();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(
        move |res| {
            if let Ok(event) = res {
                tx.send(event).unwrap();
            }
        }
    )?;

    watcher.watch(Path::new("/host/var/log/pods"), RecursiveMode::Recursive)?;

    println!("Watching for log updates...");

    // -----------------------------------------
    // 5. Main loop — process events
    // -----------------------------------------
    loop {
        if let Ok(event) = rx.recv() {
            if let EventKind::Modify(_) = event.kind {
                for p in event.paths {
                    let path: PathBuf = p.into();

                    // Only log files
                    if path.extension().and_then(|e| e.to_str()) != Some("log") {
                        continue;
                    }

                    // Only namespace "ls_"
                    if !path.to_string_lossy().contains("/ls_") {
                        continue;
                    }

                    // Index new log lines
                    tailer.tail_file(&path, &mut btree);
                }
            }
        }
    }
}

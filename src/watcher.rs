use notify::{Watcher, RecursiveMode, RecommendedWatcher, Event, EventKind};
use std::sync::mpsc::{channel, Receiver};
use std::path::PathBuf;

pub fn create_watcher() -> (RecommendedWatcher, Receiver<Event>) {
    let (tx, rx) = channel();

    let watcher = notify::recommended_watcher(move |res| {
        if let Ok(event) = res {
            tx.send(event).unwrap();
        }
    }).expect("Failed to create watcher");

    (watcher, rx)
}

pub fn is_ls_log_file(path: &PathBuf) -> bool {
    if path.extension().and_then(|e| e.to_str()) != Some("log") {
        return false;
    }

    let s = path.to_string_lossy();
    s.contains("/ls_")
}

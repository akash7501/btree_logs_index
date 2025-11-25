use std::fs;
use std::path::{Path, PathBuf};

pub fn discover_ls_logs() -> Vec<PathBuf> {
    let mut results = Vec::new();
    let pods_root = Path::new("/host/var/log/pods");

    if !pods_root.exists() {
        eprintln!("ERROR: /host/var/log/pods does not exist!");
        return results;
    }

    if let Ok(pod_dirs) = fs::read_dir(pods_root) {
        for pod in pod_dirs.flatten() {
            let pod_name = pod.file_name().to_string_lossy().to_string();

            // Only ls_* namespace
            if !pod_name.starts_with("ls_") {
                continue;
            }

            let pod_path = pod.path();

            if let Ok(container_dirs) = fs::read_dir(&pod_path) {
                for container in container_dirs.flatten() {
                    let container_path = container.path();
                    if !container_path.is_dir() { continue; }

                    if let Ok(files) = fs::read_dir(&container_path) {
                        for f in files.flatten() {
                            let p = f.path();
                            if p.extension().and_then(|e| e.to_str()) == Some("log") {
                                results.push(p);
                            }
                        }
                    }
                }
            }
        }
    }

    results
}

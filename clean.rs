use std::fs;
use std::io;
use std::path::{Path, PathBuf};

fn main() -> io::Result<()> {
    // 1. Get the current directory (root)
    let src = std::env::current_dir()?;
    
    // 2. Define the destination (one level up, named semstrait_llm_clean)
    let mut dst = src.clone();
    dst.pop(); // Go up one level
    let dst = dst.join("semstrait_llm_clean");

    // Define exclusions to keep the copy lean
    let blacklist_dirs = vec!["target", ".git", ".semstrait_demo", ".cursor", ".vscode", "node_modules"];
    let blacklist_extensions = vec!["parquet", "log", "tmp", "lock", "exe"]; // Added exe
    let blacklist_files = vec![".DS_Store", "CACHEDIR.TAG", ".rustc_info.json"];

    println!("Starting clean duplicate...");
    println!("Source: {:?}", src);
    println!("Destination: {:?}", dst);
    
    if dst.exists() {
        println!("Cleaning existing destination folder...");
        fs::remove_dir_all(&dst)?;
    }

    copy_dir_clean(&src, &dst, &blacklist_dirs, &blacklist_extensions, &blacklist_files)?;
    
    println!("✅ Successfully created clean copy at: {:?}", dst);
    Ok(())
}

fn copy_dir_clean(src: &Path, dst: &Path, skip_dirs: &[&str], skip_exts: &[&str], skip_files: &[&str]) -> io::Result<()> {
    if !dst.exists() {
        fs::create_dir_all(dst)?;
    }

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();

        if path.is_dir() {
            if skip_dirs.iter().any(|&d| name_str == d) { continue; }
            copy_dir_clean(&path, &dst.join(file_name), skip_dirs, skip_exts, skip_files)?;
        } else {
            if skip_files.iter().any(|&f| name_str == f) { continue; }
            if let Some(ext) = path.extension() {
                if skip_exts.iter().any(|&e| ext == e) { continue; }
            }
            fs::copy(&path, dst.join(file_name))?;
        }
    }
    Ok(())
}
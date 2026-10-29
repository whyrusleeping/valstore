use std::path::{Path, PathBuf};
use std::process::Command;

fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).current_dir(root).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok().map(|value| value.trim().to_string())
}

fn git_path(root: &Path, name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(git(root, &["rev-parse", "--git-path", name])?);
    Some(if path.is_absolute() { path } else { root.join(path) })
}

// Cargo treats a missing rerun-if-changed path as always stale, which would rebuild valsd on
// every build (a fresh clone has no packed-refs; a packed branch has no loose ref file).
fn watch(path: &Path) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn main() {
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let root = manifest.join("../..");
    println!("cargo:rerun-if-changed=build.rs");
    if let Some(head) = git_path(&root, "HEAD") {
        watch(&head);
        if let Some(reference) = std::fs::read_to_string(&head)
            .ok()
            .and_then(|contents| contents.trim().strip_prefix("ref: ").map(str::to_string))
        {
            if let Some(path) = git_path(&root, &reference) {
                // A packed branch gets a loose ref file again on its next commit.
                if path.exists() {
                    watch(&path);
                } else if let Some(dir) = path.parent() {
                    watch(dir);
                }
            }
            if let Some(packed) = git_path(&root, "packed-refs") {
                watch(&packed);
            }
        }
    }
    let commit = git(&root, &["rev-parse", "--short=12", "HEAD"]).filter(|value| !value.is_empty());
    println!("cargo:rustc-env=VALS_GIT_COMMIT={}", commit.as_deref().unwrap_or("unknown"));
}

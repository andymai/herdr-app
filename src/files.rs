//! Watching the home folder for the files people and agents edit (settings,
//! keys, programs, themes, layouts, mods), so changes apply live without
//! looking at the disk on a timer. One OS watcher (FSEvents, inotify) for all.

use notify::{RecursiveMode, Watcher as _};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// What changed, relative to the home folder.
pub type Changed = Vec<PathBuf>;

/// Starts watching `home`; None if the OS watcher can't start (the app then
/// polls as before).
pub fn watch(home: &Path) -> Option<async_channel::Receiver<Changed>> {
    static WATCHER: OnceLock<std::sync::Mutex<notify::RecommendedWatcher>> = OnceLock::new();
    let (tx, rx) = async_channel::unbounded();
    let root = home.canonicalize().unwrap_or_else(|_| home.to_path_buf());
    let base = root.clone();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        let Ok(event) = event else { return };
        if matches!(event.kind, notify::EventKind::Access(_)) {
            return;
        }
        let changed: Changed = event.paths.iter().filter_map(|p| p.strip_prefix(&base).ok()).filter(|p| watched(p)).map(Path::to_path_buf).collect();
        if !changed.is_empty() {
            let _ = tx.send_blocking(changed);
        }
    })
    .map_err(|e| tracing::warn!("file watching unavailable ({e}); polling instead"))
    .ok()?;
    watcher.watch(&root, RecursiveMode::Recursive).map_err(|e| tracing::warn!("cannot watch {} ({e}); polling instead", root.display())).ok()?;
    WATCHER.set(std::sync::Mutex::new(watcher)).ok()?;
    Some(rx)
}

/// The files that change what the app shows. The app's own state files
/// (view.json, shared.json, data/) change all the time and aren't.
fn watched(rel: &Path) -> bool {
    let mut parts = rel.components();
    let first = parts.next().and_then(|c| c.as_os_str().to_str()).unwrap_or_default();
    match first {
        "mods" => mod_file_counts(parts),
        "layouts" | "themes" => true,
        "settings.json" | "keys.json" | "programs.json" => parts.next().is_none(),
        _ => false,
    }
}

/// A change inside `mods/` that should reload a mod. The runtime writes its
/// typings and jsconfig into each mod on load, and editors drop hidden and
/// backup files: none of those are the mod's code.
fn mod_file_counts(rest: std::path::Components) -> bool {
    let parts: Vec<&str> = rest.filter_map(|c| c.as_os_str().to_str()).collect();
    if parts.iter().any(|p| p.starts_with('.') || p.ends_with('~') || *p == "node_modules") {
        return false;
    }
    !matches!(parts.as_slice(), [_, "jsconfig.json"] | [_, .., "gpui-kit.d.ts"])
}

/// Folders mods watch (`watchFolder`): one OS watcher each, shared by every
/// mod waiting on it. Changes collect while nobody waits, so none are missed.
struct Folder {
    watcher: Watched,
    waiters: Vec<async_channel::Sender<Vec<String>>>,
    pending: Vec<String>,
    flushing: bool,
    /// A directory appeared or a `.gitignore` changed since the last batch:
    /// which directories git sees may have changed.
    rewalk: bool,
}

/// A folder's OS watcher and, on Linux, the directories it watches. The
/// watcher has its own lock: adding a watch waits on the thread that runs
/// the event handler, which takes the folders' lock.
#[derive(Clone)]
struct Watched {
    watcher: std::sync::Arc<std::sync::Mutex<notify::RecommendedWatcher>>,
    dirs: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<PathBuf>>>,
}

static FOLDERS: OnceLock<std::sync::Mutex<std::collections::HashMap<PathBuf, Folder>>> = OnceLock::new();
/// Changes are batched this long (an editor's save is several events).
const BATCH: std::time::Duration = std::time::Duration::from_millis(150);
/// At most this many folders watched for mods at once.
const MAX_FOLDERS: usize = 64;

fn folders() -> &'static std::sync::Mutex<std::collections::HashMap<PathBuf, Folder>> {
    FOLDERS.get_or_init(Default::default)
}

/// The next changes under `path` (recursive), as absolute paths: at once if
/// some came since the last call, else when the next batch lands.
pub async fn next_change(path: String) -> Result<Vec<String>, String> {
    let root = Path::new(&path).canonicalize().map_err(|e| format!("{path}: {e}"))?;
    // The folder is listed before its directories are watched, and they are
    // watched without the folders' lock: an event between two watches runs
    // the handler, which takes that lock while the next watch waits on it.
    let fresh = {
        let mut all = folders().lock().unwrap();
        if all.contains_key(&root) {
            None
        } else {
            if all.len() >= MAX_FOLDERS {
                return Err(format!("already watching {MAX_FOLDERS} folders"));
            }
            let watcher = new_watcher(&root)?;
            all.insert(root.clone(), Folder { watcher: watcher.clone(), waiters: Vec::new(), pending: Vec::new(), flushing: false, rewalk: false });
            Some(watcher)
        }
    };
    if let Some(watcher) = fresh
        && let Err(e) = begin(&watcher, &root)
    {
        folders().lock().unwrap().remove(&root);
        return Err(e);
    }
    let rx = {
        let mut all = folders().lock().unwrap();
        let f = all.get_mut(&root).ok_or_else(|| "stopped watching".to_owned())?;
        if !f.pending.is_empty() && !f.flushing {
            return Ok(std::mem::take(&mut f.pending));
        }
        let (tx, rx) = async_channel::bounded(1);
        f.waiters.push(tx);
        rx
    };
    rx.recv().await.map_err(|_| "stopped watching".to_owned())
}

/// A watcher for `root` that watches nothing yet; `begin` starts it.
fn new_watcher(root: &Path) -> Result<Watched, String> {
    let key = root.to_path_buf();
    let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        let Ok(event) = event else { return };
        if matches!(event.kind, notify::EventKind::Access(_)) {
            return;
        }
        let mut all = folders().lock().unwrap();
        let Some(f) = all.get_mut(&key) else { return };
        let moved = matches!(event.kind, notify::EventKind::Create(_) | notify::EventKind::Modify(notify::event::ModifyKind::Name(_)));
        for p in event.paths {
            if (moved && p.is_dir()) || p.file_name().is_some_and(|n| n == ".gitignore") {
                f.rewalk = true;
            }
            let p = p.to_string_lossy().into_owned();
            if !f.pending.contains(&p) {
                f.pending.push(p);
            }
        }
        if !f.flushing {
            f.flushing = true;
            let key = key.clone();
            std::thread::spawn(move || {
                std::thread::sleep(BATCH);
                // New directories are watched before anyone hears of them, so a
                // change inside one right after is not missed.
                let rewalk = {
                    let mut all = folders().lock().unwrap();
                    let Some(f) = all.get_mut(&key) else { return };
                    std::mem::take(&mut f.rewalk).then(|| f.watcher.clone())
                };
                if let Some(w) = rewalk {
                    watch_tree(&w, &key);
                }
                let mut all = folders().lock().unwrap();
                let Some(f) = all.get_mut(&key) else { return };
                f.flushing = false;
                if f.waiters.is_empty() {
                    return;
                }
                let batch = std::mem::take(&mut f.pending);
                for w in f.waiters.drain(..) {
                    let _ = w.try_send(batch.clone());
                }
            });
        }
    })
    .map_err(|e| format!("cannot watch: {e}"))?;
    Ok(Watched { watcher: std::sync::Arc::new(std::sync::Mutex::new(watcher)), dirs: Default::default() })
}

/// Starts watching `root`.
fn begin(watched: &Watched, root: &Path) -> Result<(), String> {
    if cfg!(target_os = "linux") {
        watch_tree(watched, root);
        if watched.dirs.lock().unwrap().is_empty() {
            return Err(format!("cannot watch {}", root.display()));
        }
        Ok(())
    } else {
        watched.watcher.lock().unwrap().watch(root, RecursiveMode::Recursive).map_err(|e| format!("cannot watch {}: {e}", root.display()))
    }
}

/// Watches each directory under `root` that git sees, one at a time. inotify
/// takes a watch per directory, so a recursive watch over a checkout counts
/// every directory in its `node_modules`, `target` and `.git/objects`, and a
/// few checkouts reach the user's limit. What git ignores changes nothing a
/// file tree or a git panel shows; `.git` itself and its refs still change
/// what `git status` says. FSEvents and Windows watch a whole tree as one,
/// so only Linux comes here.
fn watch_tree(watched: &Watched, root: &Path) {
    let mut dirs = watched_dirs(root);
    {
        let seen = watched.dirs.lock().unwrap();
        dirs.retain(|d| !seen.contains(d));
    }
    if dirs.is_empty() {
        return;
    }
    let mut watcher = watched.watcher.lock().unwrap();
    let mut seen = watched.dirs.lock().unwrap();
    for dir in dirs {
        match watcher.watch(&dir, RecursiveMode::NonRecursive) {
            Ok(()) => {
                seen.insert(dir);
            }
            Err(e) => tracing::debug!("cannot watch {}: {e}", dir.display()),
        }
    }
}

/// The directories under `root` (itself included) that git does not ignore,
/// hidden ones too, and `.git` with its refs; `.git`'s objects and logs
/// change with every commit but say nothing new.
fn watched_dirs(root: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let walk = ignore::WalkBuilder::new(root).hidden(false).filter_entry(|e| e.file_name() != ".git").build();
    for entry in walk.flatten() {
        if entry.file_type().is_some_and(|t| t.is_dir()) {
            dirs.push(entry.into_path());
        }
    }
    let git = root.join(".git");
    if git.is_dir() {
        dirs.push(git.clone());
        let refs = ignore::WalkBuilder::new(git.join("refs")).standard_filters(false).build();
        for entry in refs.flatten() {
            if entry.file_type().is_some_and(|t| t.is_dir()) {
                dirs.push(entry.into_path());
            }
        }
    }
    dirs
}

/// The mod a changed path belongs to (`mods/<name>/...`).
pub fn mod_of(rel: &Path) -> Option<String> {
    let mut parts = rel.components();
    (parts.next()?.as_os_str() == "mods").then_some(())?;
    parts.next()?.as_os_str().to_str().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_what_people_edit_counts() {
        assert!(watched(Path::new("settings.json")));
        assert!(watched(Path::new("mods/agents/main.js")));
        assert!(watched(Path::new("layouts/main.json")));
        assert!(!watched(Path::new("view.json")));
        assert!(!watched(Path::new("shared.json")));
        assert!(!watched(Path::new("data/x")));
        assert_eq!(mod_of(Path::new("mods/agents/main.js")).as_deref(), Some("agents"));
    }

    #[test]
    fn files_the_runtime_or_an_editor_writes_into_a_mod_do_not_reload_it() {
        assert!(watched(Path::new("mods/probe")));
        assert!(watched(Path::new("mods/probe/ui.js")));
        assert!(watched(Path::new("mods/probe/lib/jsconfig.json")));
        assert!(!watched(Path::new("mods/probe/jsconfig.json")));
        assert!(!watched(Path::new("mods/probe/gpui-kit.d.ts")));
        assert!(!watched(Path::new("mods/probe/.main.js.swp")));
        assert!(!watched(Path::new("mods/probe/.vscode/settings.json")));
        assert!(!watched(Path::new("mods/probe/main.js~")));
        assert!(!watched(Path::new("mods/probe/lib/gpui-kit.d.ts")));
        assert!(!watched(Path::new("mods/probe/node_modules/x/index.js")));
        assert!(!watched(Path::new("mods/probe/.git/HEAD")));
        assert!(watched(Path::new("mods/probe/mod.json")));
        assert_eq!(mod_of(Path::new("layouts/main.json")), None);
    }

    /// Drives a future to its end on this thread (the tests have no executor).
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        use std::task::{Context, Poll, Wake, Waker};
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark {
            fn wake(self: std::sync::Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(std::sync::Arc::new(Unpark(std::thread::current())));
        let mut f = std::pin::pin!(f);
        loop {
            match f.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(v) => return v,
                Poll::Pending => std::thread::park_timeout(std::time::Duration::from_millis(50)),
            }
        }
    }

    /// A checkout with `node_modules` ignored: its tracked and hidden folders
    /// and `.git` with its refs are watched; nothing in `node_modules` or
    /// `.git/objects` is.
    #[test]
    fn a_checkout_watches_only_what_git_sees() {
        let dir = std::env::temp_dir().join(format!("herdr-dirs-{}", std::process::id()));
        for d in ["src/a", ".github/workflows", "node_modules/x/y/z", ".git/refs/heads", ".git/objects/ab"] {
            std::fs::create_dir_all(dir.join(d)).unwrap();
        }
        std::fs::write(dir.join(".gitignore"), "node_modules/\n").unwrap();
        let dirs: std::collections::HashSet<PathBuf> = watched_dirs(&dir).into_iter().map(|d| d.strip_prefix(&dir).unwrap().to_path_buf()).collect();
        for d in ["", "src", "src/a", ".github", ".github/workflows", ".git", ".git/refs", ".git/refs/heads"] {
            assert!(dirs.contains(Path::new(d)), "{d} not watched: {dirs:?}");
        }
        assert!(!dirs.iter().any(|d| d.starts_with("node_modules") || d.starts_with(".git/objects")), "{dirs:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A folder made after watching starts is watched before its creation is
    /// reported, so a change in it right after comes back; a change deep in
    /// an ignored folder does not.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_new_folder_is_watched_and_an_ignored_one_is_not() {
        let dir = std::env::temp_dir().join(format!("herdr-new-{}", std::process::id()));
        std::fs::create_dir_all(dir.join(".git")).unwrap();
        std::fs::create_dir_all(dir.join("node_modules/x/y")).unwrap();
        std::fs::write(dir.join(".gitignore"), "node_modules/\n").unwrap();
        let path = dir.to_string_lossy().into_owned();
        let maker = {
            let dir = dir.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(300));
                std::fs::create_dir(dir.join("fresh")).unwrap();
            })
        };
        let first = block_on(next_change(path.clone())).unwrap();
        maker.join().unwrap();
        assert!(first.iter().any(|p| p.ends_with("fresh")), "{first:?}");
        std::fs::write(dir.join("node_modules/x/y/dep.js"), "x").unwrap();
        std::fs::write(dir.join("fresh/note.md"), "y").unwrap();
        std::thread::sleep(BATCH * 3);
        let second = block_on(next_change(path)).unwrap();
        assert!(second.iter().any(|p| p.ends_with("fresh/note.md")), "{second:?}");
        assert!(!second.iter().any(|p| p.contains("node_modules")), "{second:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_watched_folder_reports_changes_and_misses_none_between_waits() {
        let dir = std::env::temp_dir().join(format!("herdr-watch-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.to_string_lossy().into_owned();
        // Start watching, then change something while the first wait is on.
        let writer = {
            let dir = dir.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(300));
                std::fs::write(dir.join("a.md"), "one").unwrap();
            })
        };
        let first = block_on(next_change(path.clone())).unwrap();
        writer.join().unwrap();
        assert!(first.iter().any(|p| p.ends_with("a.md")), "{first:?}");
        // A change while nobody waits comes back on the next call.
        std::fs::write(dir.join("b.md"), "two").unwrap();
        std::thread::sleep(BATCH * 3);
        let second = block_on(next_change(path)).unwrap();
        assert!(second.iter().any(|p| p.ends_with("b.md")), "{second:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

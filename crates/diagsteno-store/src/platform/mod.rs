//! OS-specific pieces. The only module allowed `cfg` on the target OS.

use std::io;
use std::path::Path;

/// Make a rename inside `dir` durable by syncing the directory.
///
/// On Unix a directory can be opened and `fsync`ed. Windows has no
/// equivalent for directories through `std`; NTFS journals the rename, and
/// the file's own data was already flushed before the rename.
#[cfg(unix)]
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// See the Unix version.
#[cfg(not(unix))]
pub(crate) fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

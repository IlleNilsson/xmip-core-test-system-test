//! Where Carried drops and reads, and what arrived there: a folder of the
//! run's on this computer, or a user's directory on the Parties' far end;
//! each arrival recorded once, by the Send Port whose spot it reached. How a
//! file is dropped into a folder and what a folder holds whole are said
//! here once, for the rig and the far ends outside the cluster
//! (`crate::external`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use configure::placed::Placement;
use transport_sftp::directory::{Shared, Told};

/// Each file arrived, by name: the Send Port whose spot it reached and when
/// it was first seen whole there.
pub type Arrived = Arc<Mutex<BTreeMap<String, (String, Instant)>>>;

/// Where a placed Location takes its files from, or a Send Port puts them.
#[derive(Clone)]
pub enum Spot {
    /// A folder on this computer.
    Folder(PathBuf),
    /// A user's directory on the Parties' far end.
    FarEnd(Shared),
    /// The address a Receive Location listens at on its node, which the
    /// far ends on machines of their own push to.
    Listening(String),
}

/// One Receive Location, Send Port or Work Process placed, and its spot: a
/// Work Process's, which the rig never makes or reads, its node's alone.
#[derive(Clone)]
pub struct Binding {
    pub placement: Placement,
    pub spot: Spot,
}

/// A placed Location or Port as a person reads it: its name and its node.
pub(super) fn placed_name(placement: &Placement) -> String {
    format!("{} [{}]", placement.name, placement.node)
}

/// The scope of a placed Receive Location on its node, beneath `root`.
pub(super) fn located(root: &str, placement: &Placement) -> String {
    format!("{root}/node/{}/receive/{}", placement.node, placement.name)
}

/// Where a Receive Location's verdict is published: beneath it, on the node
/// it is placed on.
#[must_use]
pub fn delivery(root: &str, placement: &Placement) -> String {
    format!("{}/delivery", located(root, placement))
}

/// Every file whole in `folder` — none a send is still writing — recorded
/// where it was not, as arrived at `port`, at `now`.
pub(super) fn seen(folder: &Path, port: &str, arrived: &Arrived, now: Instant) {
    let mut arrived = arrived.lock().unwrap_or_else(PoisonError::into_inner);
    for name in whole(folder) {
        arrived
            .entry(name)
            .or_insert_with(|| (port.to_string(), now));
    }
}

/// The name of every file whole in `folder`: none a send is still writing,
/// which goes by a name of the send's own until it is kept. What a Send
/// Port's folder holds as delivered, for the rig and a far end alike.
pub(crate) fn whole(folder: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(folder) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| !name.contains(".xmip-"))
        .collect()
}

/// `bytes` dropped into `folder` as `name` as a producer that means it whole
/// drops a file: written beside the folder and renamed in, so a Receive
/// Location never takes half of it. The one way the Essentials drops a
/// file, the rig's own and a far end's.
///
/// # Errors
///
/// Where it cannot be written or renamed.
pub(crate) fn put(folder: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let beside = folder.with_extension("beside");
    std::fs::create_dir_all(&beside)?;
    std::fs::write(beside.join(name), bytes)?;
    std::fs::rename(beside.join(name), folder.join(name))
}

/// What a far end tells of a file a Send Location of the Send Port `port`
/// finished writing: its arrival there, now.
#[must_use]
pub fn told(arrived: &Arrived, port: &str) -> Told {
    let (arrived, port) = (Arc::clone(arrived), port.to_string());
    Arc::new(move |name: &str| {
        arrived
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(name.to_string())
            .or_insert_with(|| (port.clone(), Instant::now()));
    })
}

/// The bytes of the file `name` at `spot`, where it is there, taken off it
/// as the Party it was delivered to takes it: what a spot holds never grows
/// with the run's age.
pub(super) fn taken(spot: &Spot, name: &str) -> Option<Vec<u8>> {
    match spot {
        Spot::Folder(folder) => {
            let path = folder.join(name);
            let bytes = std::fs::read(&path).ok();
            let _ = std::fs::remove_file(&path);
            bytes
        }
        Spot::FarEnd(directory) => directory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .files
            .remove(name),
        // A Send Port never listens: nothing arrives there to take.
        Spot::Listening(_) => None,
    }
}

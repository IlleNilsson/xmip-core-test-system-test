//! The roll's environment: every switch a roll reads, as the variable it
//! reads it from. The variables are external names and keep the prefix
//! (ADR-0030); `Xmip/New-XmipSystemTestEnvironment.ps1` sets them, and
//! `test/XmipTest.Test.ps1` holds that file against this one, so a switch the
//! roll reads is a switch an operator can reach.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::roster::Roster;
use crate::scenario;

/// The scenarios named in `XMIP_SYSTEM_TEST_SCENARIOS`: none named is every
/// one.
///
/// # Errors
///
/// When a name is no scenario, as [`scenario::chosen`] refuses it.
pub fn scenarios() -> Result<Vec<String>, String> {
    let raw = std::env::var("XMIP_SYSTEM_TEST_SCENARIOS").ok();
    scenario::chosen(raw.as_deref())
}

/// The variable naming `cluster.xmip` a roll spawns the nodes
/// of: the run's own, written by `Start-XmipTest` from `-Cluster`, `-Nodes`,
/// `-NodeRole` and `-NodeCapability`, or the test cluster's where `-Nodes`
/// was not given (ADR-0056, amendments 2026-10-03 and 2026-11-24). The
/// variable the tests' fixtures read.
pub const CLUSTER_FILE: &str = "XMIP_TEST_CLUSTER";

/// The roster a roll spawns: the nodes the cluster's file
/// [`CLUSTER_FILE`] names declares under `[nodes]`, each with the roles its
/// `roles` says and the Node capabilities its `capabilities` says, `online`
/// among them where it may assume the internet ([`Roster::of_cluster`];
/// ADR-0045; ADR-0056, amendment 2026-11-24). Nothing is read out of a
/// node's name, and nothing is counted or numbered: the nodes are
/// configuration.
///
/// # Errors
///
/// When no file is named or it cannot be read, or a word is no role or no
/// Node capability: REFUSED, naming the file (ADR-0055).
pub fn roster() -> Result<Roster, String> {
    let (file, text) = cluster_file()?;
    Roster::of_cluster(&text)
        .map_err(|problem| format!("REFUSED: the cluster file {}: {problem}", file.display()))
}

/// The run's cluster file [`CLUSTER_FILE`] names, and its text.
///
/// # Errors
///
/// When no file is named or it cannot be read: REFUSED, naming the file.
pub fn cluster_file() -> Result<(PathBuf, String), String> {
    let file = std::env::var_os(CLUSTER_FILE)
        .filter(|named| !named.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            format!(
                "REFUSED: a roll spawns the nodes of a cluster's cluster.xmip; \
                 {CLUSTER_FILE} names none."
            )
        })?;
    let text = std::fs::read_to_string(&file)
        .map_err(|error| format!("REFUSED: the cluster file {}: {error}", file.display()))?;
    Ok((file, text))
}

/// The `xmip-service` a roll starts each node it does not simulate as,
/// `XMIP_SYSTEM_TEST_SERVICE`, which `Start-XmipTest` builds and sets: every
/// node of the run then a process of the assembled service, reaching Xmip
/// Storage over the wire as on machines of its own (ADR-0028, amendment
/// 2026-10-22). `None` where nothing named one, and every node not real is
/// simulated as before.
#[must_use]
pub fn service() -> Option<PathBuf> {
    std::env::var_os("XMIP_SYSTEM_TEST_SERVICE")
        .filter(|named| !named.is_empty())
        .map(PathBuf::from)
}

/// Whether the run is the Systems Tests, on an environment's machines:
/// `XMIP_SYSTEM_TEST_MACHINES` is `true`, which `Start-XmipTest -Suite
/// Core.Systems` sets. Every node of the run is then a machine that answers
/// as that node, and none is spawned in its place; unset, the run is the
/// System Tests and no name is resolved (ADR-0028, amendment 2026-11-23).
/// The suite says which, never a name.
#[must_use]
pub fn machines() -> bool {
    std::env::var("XMIP_SYSTEM_TEST_MACHINES").is_ok_and(|said| said.trim() == "true")
}

/// The folder of the operator certificate a roll asks a real node with,
/// `XMIP_SYSTEM_TEST_CERTIFICATE`, which `Start-XmipTest -Certificate` sets:
/// `operator.pem`, the chain the cluster's authority signed, `operator.key`
/// and `anchors.pem`, that authority. `None` where nothing named one.
#[must_use]
pub fn certificate() -> Option<PathBuf> {
    std::env::var_os("XMIP_SYSTEM_TEST_CERTIFICATE")
        .filter(|named| !named.is_empty())
        .map(PathBuf::from)
}

/// The directory a roll publishes in: `XMIP_SYSTEM_TEST_AREA`, where
/// `Start-XmipTest` keeps the run, else the temp directory.
#[must_use]
pub fn publish_area() -> PathBuf {
    std::env::var_os("XMIP_SYSTEM_TEST_AREA")
        .filter(|named| !named.is_empty())
        .map_or_else(std::env::temp_dir, PathBuf::from)
}

/// The System Tests' own certificate authority, `authority` in the area a
/// roll publishes in: the one `Get-XmipSystemTestLayout` names, which the
/// web host is served under and a tester's operator certificate is signed
/// by. The nodes a run starts are issued under it too, so it outlives the
/// run, and a phone or browser that trusts it keeps trusting the next run's
/// nodes (ADR-0028, amendment 2026-10-27). `Start-XmipTest` makes it, as
/// `Initialize-XmipSystemTestAuthority`, before the run starts any node.
#[must_use]
pub fn authority() -> PathBuf {
    publish_area().join("authority")
}

/// Where the roll of `cluster` publishes its snapshot, history and activity
/// in `area`: `<cluster>-snapshot.xmip`, `-history.toml` and
/// `-activity.toml`. The one place the names are decided; `Start-XmipTest`
/// asks `xmip-system-test-roll --publication <cluster> <area>` rather than
/// naming them again.
#[must_use]
pub fn publish_paths(cluster: &str, area: &Path) -> (PathBuf, PathBuf, PathBuf) {
    (
        area.join(format!("{cluster}-snapshot.xmip")),
        area.join(format!("{cluster}-history.toml")),
        area.join(format!("{cluster}-activity.toml")),
    )
}

/// Where the per-instance images of this run go, from
/// `XMIP_SYSTEM_TEST_IMAGES`: the device-local directory a cluster's roll, its
/// cluster process and its nodes are linked into, so each runs under a name
/// that says which cluster and which node it is (ADR-0053, amendment
/// 2026-09-20). `None` where nothing named one, and then nothing is linked and
/// every process keeps the binary's own name.
#[must_use]
pub fn image_directory() -> Option<PathBuf> {
    std::env::var_os("XMIP_SYSTEM_TEST_IMAGES")
        .filter(|named| !named.is_empty())
        .map(PathBuf::from)
}

/// The `HeavyLoad` payload size: `XMIP_SYSTEM_TEST_LOAD_BYTES` if set — a
/// plain number or a human size like `512mb` or `2gb` — else a megabyte. Note
/// the memory: peak is roughly twice this per pair, so a gigabyte wants a few
/// free.
#[must_use]
pub fn load_bytes() -> usize {
    let Some(raw) = std::env::var("XMIP_SYSTEM_TEST_LOAD_BYTES").ok() else {
        return 1024 * 1024;
    };
    let text = raw.trim().to_lowercase();
    let (number, unit) = text
        .find(|c: char| c.is_alphabetic())
        .map_or((text.as_str(), ""), |at| text.split_at(at));
    let scale: usize = match unit {
        "gb" | "g" => 1024 * 1024 * 1024,
        "mb" | "m" => 1024 * 1024,
        "kb" | "k" => 1024,
        _ => 1,
    };
    number
        .trim()
        .parse::<usize>()
        .map_or(1024 * 1024, |value| value.saturating_mul(scale))
}

/// The maximum wall-clock time to roll: `XMIP_SYSTEM_TEST_MAX_SECONDS` if set,
/// else no ceiling.
#[must_use]
pub fn max_seconds() -> Option<Duration> {
    std::env::var("XMIP_SYSTEM_TEST_MAX_SECONDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .filter(|seconds| *seconds > 0.0)
        .map(Duration::from_secs_f64)
}

/// The factor on time: `XMIP_SYSTEM_TEST_TIME_FACTOR` if set, else `1.0` (real
/// time). Below one runs faster than real time, above one slower.
#[must_use]
pub fn time_factor() -> f64 {
    std::env::var("XMIP_SYSTEM_TEST_TIME_FACTOR")
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .unwrap_or(1.0)
}

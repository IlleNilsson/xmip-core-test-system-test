//! The node's configuration a process stage reads its Subscriptions from:
//! the Reference Environment with the run's names and the
//! bindings that place every Application on the run's nodes, sliced as
//! deployment slices it. Split from `subscribing.rs` on 2026-10-13, when the
//! bindings came to be written from the sections. The file names no
//! cluster: the run's is given to the slice as an environment gives its
//! own (`configure::slice_named`; ADR-0028, amendment 2026-10-21), and the
//! bindings' addresses are the run's directories, never the file's.

use std::path::{Path, PathBuf};

use node::{Capability, NodeRole, Scope, Stage};

use configure::placed::{self, Placed};

use crate::reference_environment::CLUSTER;
use crate::roster::Roster;

/// Write this node's configuration, sliced from the Reference
/// Environment with the run's names and the bindings that place every
/// Application the file designs on the run's nodes (`configure::placed`) — each Receive Location
/// on the first node that declared receive, each Send Port on the first
/// that declared send, each Work Process on every node serving process,
/// as configure places it — into `<shared>/configuration`, and say where
/// it is.
pub(super) fn configured(
    node: &str,
    name: &str,
    roster: &Roster,
    shared: &Path,
) -> Result<PathBuf, String> {
    let cluster = Scope::new(node).segments().next().unwrap_or_default();
    let at = |stage: Stage| roster.with(stage).first().map(ToString::to_string);
    let receives = at(Stage::Receive).ok_or("no node declares receive")?;
    let sends = at(Stage::Send).ok_or("no node declares send")?;
    let inbox = |to: &str, stage: Stage| {
        shared
            .join("handoff")
            .join(to)
            .join(stage.name())
            .display()
            .to_string()
    };
    let directory = shared.join("configuration");
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    let (inbox, outbox) = (inbox(&receives, Stage::Process), inbox(&sends, Stage::Send));
    // The roster's nodes, each serving here what the simulated handoff
    // gives it: receive on the first serving receive, send on the first
    // serving send, and process on every node the one rule gives a Work
    // Process (`xcluster::every`); where none serves process, `placements`
    // refuses each Work Process in its own words. A simulated node has
    // every environment the file's work requires: each declares every Node
    // capability any of it requires (ADR-0056, amendment 2026-11-24), so it
    // is placed as the handoff runs it, whatever the roster's nodes are.
    let candidates: Vec<(String, Capability)> = roster
        .names()
        .into_iter()
        .map(|one| (one.to_string(), roster.capability(one)))
        .collect();
    let processing: Vec<&str> = xcluster::every(Placed::Process.requirement(), &candidates)
        .map_or_default(|on| on.into_iter().map(|(one, _)| one.as_str()).collect());
    let mut required: Vec<String> = Vec::new();
    for application in configure::applications(CLUSTER)? {
        let receives = application
            .receive_locations
            .into_iter()
            .map(|one| one.requires);
        let processes = application
            .work_processes
            .into_iter()
            .map(|one| one.requires);
        let sends = application.send_ports.into_iter().map(|one| one.requires);
        for word in receives.chain(processes).chain(sends).flatten() {
            if !required.contains(&word) {
                required.push(word);
            }
        }
    }
    let mut nodes: Vec<(String, Capability)> = Vec::new();
    for (one, _) in &candidates {
        let serving: Vec<NodeRole> = [
            (*one == receives).then_some(NodeRole::Receiving),
            (*one == sends).then_some(NodeRole::Sending),
            processing
                .contains(&one.as_str())
                .then_some(NodeRole::Processing),
        ]
        .into_iter()
        .flatten()
        .collect();
        if !serving.is_empty() {
            nodes.push((
                one.clone(),
                Capability::of(&serving).with_declared(&required)?,
            ));
        }
    }
    let placements = placed::placements(CLUSTER, &nodes)?;
    let bound = placed::written(&placements, |placement| placed::Binding {
        address: match placement.placed {
            Placed::Receive => inbox.clone(),
            Placed::Send => outbox.clone(),
            // Never asked: a Work Process is bound by its node alone.
            Placed::Process => String::new(),
        },
        settings: Vec::new(),
        secret: None,
        accepting: false,
    });
    let file =
        format!("{CLUSTER}\n{bound}\n[nodes.{name}.service]\nname = \"xmip-system-test-{name}\"\n");
    let text = configure::slice_named(&file, name, cluster)?;
    let path = directory.join(format!("{name}.node.xmip"));
    let signed = runtime::signed::signed_beside(&path, cluster, &file, &text)?;
    std::fs::write(&path, signed).map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(path)
}

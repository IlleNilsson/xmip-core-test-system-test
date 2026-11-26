//! Where the nodes on virtual machines or in containers are, the System
//! Tests' and the Systems Tests' alike: each node of the run at the
//! address of its machine as the deployed environment recorded it, or its
//! container as `Start-XmipTest -Container` made it
//! (`environment::machines`), at the port the cluster's own cluster.xmip
//! operates the node on, reached where that port is published in
//! containers, every node looked up before any is asked; no name is
//! resolved (ADR-0028, amendments 2026-11-25).

use std::net::SocketAddr;

use crate::environment::Machines;
use crate::subscribing::CLUSTER;

/// Each of `names` of `cluster` where this computer reaches its operate
/// listener in `machines`: its machine's address, at the port the cluster's
/// own cluster.xmip operates it on, or where that port is published; every
/// name looked up before any node is asked.
///
/// # Errors
///
/// REFUSED, naming in one sentence every node `machines` gives no address,
/// and the `Deploy-XmipEnvironment` that makes them.
pub(super) fn mapped(
    cluster: &str,
    names: &[String],
    machines: &Machines,
) -> Result<Vec<(String, SocketAddr)>, String> {
    let mut mapped = Vec::new();
    let mut unmapped = Vec::new();
    for name in names {
        let port = operate_port(cluster, Some(name))?;
        match machines.reached(name, port) {
            Some(at) => mapped.push((name.clone(), at)),
            None => unmapped.push(name.as_str()),
        }
    }
    if unmapped.is_empty() {
        return Ok(mapped);
    }
    let has = if unmapped.len() == 1 { "has" } else { "have" };
    Err(format!(
        "REFUSED: {} {has} no virtual machine in the environment deployed for cluster \
         {cluster}: on virtual machines every node of the run is the machine \
         Deploy-XmipEnvironment made for it, and none is spawned in its place; \
         Deploy-XmipEnvironment -Path <the Reference Environment> -Cluster {cluster} \
         makes them",
        unmapped.join(", ")
    ))
}

/// The port the cluster's own cluster.xmip says `node` is operated on — the
/// first node it declares where `node` is none of them, or none is named —
/// as `[operate] listen` says it, read through the configuration's own
/// slice and reader.
pub(super) fn operate_port(cluster: &str, node: Option<&str>) -> Result<u16, String> {
    let declared = configure::cluster::nodes(CLUSTER)?;
    let asked = node
        .filter(|name| declared.iter().any(|one| one == name))
        .or_else(|| declared.first().map(String::as_str))
        .ok_or("REFUSED: the Reference Environment declares no node")?;
    let document = configure::parse_toml(&configure::slice_named(CLUSTER, asked, cluster)?)?;
    let listen = document
        .operate
        .and_then(|operate| operate.listen)
        .ok_or_else(|| format!("REFUSED: the Reference Environment operates {asked} nowhere"))?;
    listen
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .ok_or_else(|| format!("REFUSED: [operate] listen {listen} names no port"))
}

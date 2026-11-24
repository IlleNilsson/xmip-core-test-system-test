//! Where the Systems Tests' nodes are: each name of the run resolved
//! through this machine's own resolver, at the port the cluster's own
//! cluster.xmip operates the node on, every name before any node is asked
//! (ADR-0028, amendment 2026-11-24).

use std::net::ToSocketAddrs;

use crate::subscribing::CLUSTER;

/// Each of `names` with the address its name resolves to, at the port the
/// cluster's own cluster.xmip operates it on, every name resolved before
/// any node is asked.
///
/// # Errors
///
/// REFUSED, naming in one sentence every node whose name does not resolve,
/// the cluster file's OP1 on an environment that gives it no machine among
/// them, and the `-Nodes` that run the ones that resolve.
pub(super) fn mapped(cluster: &str, names: &[String]) -> Result<Vec<(String, String)>, String> {
    let mut mapped = Vec::new();
    let mut unmapped = Vec::new();
    let mut ports = Vec::new();
    for name in names {
        let port = operate_port(cluster, Some(name))?;
        match resolving(name, &port) {
            Some(address) => mapped.push((name.clone(), address)),
            None => unmapped.push(name.as_str()),
        }
        if !ports.contains(&port) {
            ports.push(port);
        }
    }
    if unmapped.is_empty() {
        return Ok(mapped);
    }
    let (verb, them) = if unmapped.len() == 1 {
        ("does", "it")
    } else {
        ("do", "each")
    };
    let resolved: Vec<&str> = mapped.iter().map(|(name, _)| name.as_str()).collect();
    let instead = if resolved.is_empty() {
        "none of the run's nodes resolves here".to_string()
    } else {
        format!("-Nodes {} runs the nodes that resolve", resolved.join(", "))
    };
    Err(format!(
        "REFUSED: {} {verb} not resolve, at port {}: the Systems Tests run on the \
         environment's machines and spawn no node in a machine's place; map {them} to a \
         machine of the environment, or {instead}",
        unmapped.join(", "),
        ports.join(", ")
    ))
}

/// The port the cluster's own cluster.xmip says `node` is operated on — the
/// first node it declares where `node` is none of them, or none is named —
/// as `[operate] listen` says it, read through the configuration's own
/// slice and reader.
pub(super) fn operate_port(cluster: &str, node: Option<&str>) -> Result<String, String> {
    let declared = configure::cluster::nodes(CLUSTER)?;
    let asked = node
        .filter(|name| declared.iter().any(|one| one == name))
        .or_else(|| declared.first().map(String::as_str))
        .ok_or("REFUSED: the System Tests' cluster.xmip declares no node")?;
    let document = configure::parse_toml(&configure::slice_named(CLUSTER, asked, cluster)?)?;
    let listen = document
        .operate
        .and_then(|operate| operate.listen)
        .ok_or_else(|| {
            format!("REFUSED: the System Tests' cluster.xmip operates {asked} nowhere")
        })?;
    listen
        .rsplit_once(':')
        .map(|(_, port)| port.to_string())
        .ok_or_else(|| format!("REFUSED: [operate] listen {listen} names no port"))
}

/// `name` at `port`, where this machine's resolver knows the name.
pub(super) fn resolving(name: &str, port: &str) -> Option<String> {
    let address = format!("{name}:{port}");
    address
        .to_socket_addrs()
        .ok()
        .and_then(|mut found| found.next())
        .map(|_| address)
}

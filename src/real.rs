//! The Systems Tests' nodes are machines of their own; the System Tests'
//! are processes on one computer (ADR-0028, amendment 2026-11-23; the owner:
//! *System Test is local. Systems Test test systems together*).
//!
//! The suite says which (`environment::machines`), never a name. On machines
//! every node of the run — the nodes `-Nodes` named, or the cluster file's —
//! and the cluster's own name, `-Cluster`, are resolved through this
//! machine's own resolver, at the port the cluster's own cluster.xmip says
//! the node is operated on. Every node whose name does not resolve is
//! REFUSED, all of them in one sentence before any node is asked, with the
//! `-Nodes` that run the ones that resolve, and none is spawned in its
//! place. A name that resolves is not yet
//! trusted: it must answer over its operate listener, inside Xmip's mutual
//! TLS with the operator certificate the run names
//! (`environment::certificate`), declare itself the node of that name in
//! that cluster (ADR-0053) and say what it can do (ADR-0056); anything else
//! is REFUSED by name. A resolving cluster name must answer as a node of
//! that cluster. On one computer no name is resolved at all.
//!
//! The rig never spawns, configures or stops a real node: an orchestrator
//! owns a node's life (ADR-0052, ruling 8). It follows what the node
//! publishes through the runtime's one gatherer (`runtime::plane::gather`,
//! [`gathered`]), and the run says per node which it is.

use std::path::Path;
use std::sync::Arc;

use node::Capability;
use observe::Publication;
use runtime::plane::gather::{Follow, Gatherer, Reach};
use runtime::plane::{Outcome, Remote};
use xcluster::{ClusterError, Member, Membership};

use crate::environment;
use crate::roster::Roster;

mod mapped;

use mapped::{mapped, operate_port, resolving};

/// A run's nodes, sorted: the real ones as they answered, their names, the
/// roster with each real node's roles as it declared them, and the
/// simulated nodes alone, which are all the run spawns.
pub struct Sorted {
    pub real: Vec<RealNode>,
    pub names: Vec<String>,
    pub roster: Roster,
    pub simulated: Roster,
}

/// The nodes of `roster`, sorted into real and simulated: on machines
/// (`environment::machines`) every one real, asked with the run's operator
/// certificate (`environment::certificate`) as [`found`] does; on one
/// computer none.
///
/// # Errors
///
/// As [`found`].
pub fn sorted(cluster: &str, roster: Roster) -> Result<Sorted, String> {
    let certificate = environment::certificate();
    sort(
        cluster,
        roster,
        environment::machines(),
        certificate.as_deref(),
    )
}

/// [`sorted`], told whether the run is on machines and the folder of the
/// operator certificate it asks them with.
fn sort(
    cluster: &str,
    roster: Roster,
    machines: bool,
    certificate: Option<&Path>,
) -> Result<Sorted, String> {
    let real = if machines {
        let asked: Vec<String> = roster.names().into_iter().map(str::to_string).collect();
        found(cluster, &asked, certificate)?
    } else {
        Vec::new()
    };
    let names: Vec<String> = real.iter().map(|node| node.name.clone()).collect();
    let roster = real.iter().fold(roster, |roster, node| {
        roster.declared(&node.name, node.capability.clone())
    });
    let simulated = roster.without(&names);
    Ok(Sorted {
        real,
        names,
        roster,
        simulated,
    })
}

/// Every node of `real` the run reads, gathered beneath `root` by the
/// runtime's one gatherer (`runtime::plane::gather`; ADR-0028 and ADR-0052,
/// amendments 2026-11-13): the run's roster as the cluster's membership
/// (`xcluster::Membership`), each node related and drawn by the roles it
/// declared and its place in `[storage] nodes` as `roster` says them, and
/// followed over its operate listener through the surface it was found
/// through, heard once before this returns. Each round the roll merges what
/// was heard into its own snapshot, each node's `<node>/reached` said
/// *real, at* where it answered, and what each drew of itself into its
/// drawing of the cluster (`Gathered::merged`).
///
/// # Errors
///
/// REFUSED, where the membership could not be read, which a roster always
/// is.
pub fn gathered(real: &[RealNode], roster: &Roster, root: &str) -> Result<Gatherer, String> {
    let membership = Read { real, roster };
    Gatherer::start(root, None, &membership, &|member| membership.reach(member))
        .map_err(|error| format!("REFUSED: the nodes the run reads: {error}"))
}

/// The nodes the run reads, as the cluster's membership.
struct Read<'a> {
    real: &'a [RealNode],
    roster: &'a Roster,
}

impl Membership for Read<'_> {
    /// Each node the run reads, running, at the scope it declared itself
    /// at, operated where the roll follows it; no sync listener the roll
    /// knows.
    fn members(&self) -> Result<Vec<Member>, ClusterError> {
        let listed = self.roster.members();
        let members = self.real.iter().map(|node| {
            let storage_place = listed
                .iter()
                .find(|one| one.node.eq_ignore_ascii_case(&node.name))
                .and_then(|one| one.storage_place)
                .and_then(|place| usize::try_from(place).ok());
            Member {
                node: node.scope.clone(),
                address: None,
                capability: node.capability.clone(),
                operate: Some(node.remote.address().to_string()),
                storage_place,
                stopped: None,
            }
        });
        Ok(members.collect())
    }
}

impl Read<'_> {
    /// How the roll reaches `member`: through the surface it found the node
    /// through, said *real, at* where it answered.
    fn reach(&self, member: &Member) -> Reach {
        match self.real.iter().find(|node| node.scope == member.node) {
            Some(node) => Reach {
                at: format!("real, at {}", node.address),
                through: Ok(Arc::clone(&node.remote) as Arc<dyn Follow>),
            },
            None => Reach {
                at: member.node.clone(),
                through: Err("the run reads no such node".to_string()),
            },
        }
    }
}

/// A node on a machine of its own, as it answered.
pub struct RealNode {
    /// Its name, as typed and as it declared itself.
    pub name: String,
    /// The scope it declared itself at, `xmip:///<cluster>/node/<name>`:
    /// what a node-scoped act names it by, a secret set on it among them.
    pub scope: String,
    /// Where it was reached: its name and its operate port.
    pub address: String,
    /// What it declared it can do.
    pub capability: Capability,
    remote: Arc<Remote>,
}

impl RealNode {
    /// The node `name` of `cluster` the run started on this computer
    /// (`served`), read at its local endpoint `endpoint`, which the
    /// operating system proves the rig to, declaring `capability`.
    ///
    /// # Errors
    ///
    /// An endpoint that is no name in the endpoints' folder.
    pub fn local(
        (cluster, name): (&str, &str),
        endpoint: &str,
        capability: Capability,
    ) -> Result<Self, String> {
        Ok(Self {
            name: name.to_string(),
            scope: scope_of(cluster, name),
            address: format!("the local endpoint {endpoint}"),
            capability,
            remote: Arc::new(Remote::local(
                endpoint,
                std::time::Duration::ZERO,
                rigs_clock(),
            )?),
        })
    }

    /// The password secret `name` set to `value` on this node, through its
    /// operate listener as the run's operator certificate proves the rig
    /// (`Remote::set_secret`): the one place the value is kept, read by
    /// the Location naming it at its next connection, and never read back.
    ///
    /// # Errors
    ///
    /// REFUSED, in words: the node did not answer, or did not take it.
    pub fn set_password(&self, name: &str, value: &str) -> Result<(), String> {
        let refused = |why: &str| format!("REFUSED: the secret '{name}' on {}: {why}", self.name);
        // Named by its scope, as the node keeps its Storage by it: the bare
        // name was refused, "no node at EX1 runs in this process"
        // (2026-11-21, the first real node driven).
        match self
            .remote
            .set_secret(0, &self.scope, (name, "password", value))
        {
            Ok((Outcome::Taken, _)) => Ok(()),
            Ok((_, said)) => Err(refused(&said)),
            Err(why) => Err(refused(&why)),
        }
    }
}

/// The nodes `names` of `cluster` on machines of their own, each having
/// resolved, answered and declared itself a node of `cluster`. The
/// cluster's own name is asked too where it resolves.
///
/// # Errors
///
/// REFUSED, naming every name that does not resolve at once ([`mapped`]);
/// then, naming the name, a resolving name the rig holds no certificate to
/// ask, one that does not answer, or one that answers as something else.
pub fn found(
    cluster: &str,
    names: &[String],
    certificate: Option<&Path>,
) -> Result<Vec<RealNode>, String> {
    if let Some(address) = resolving(cluster, &operate_port(cluster, None)?) {
        let remote = answering(cluster, &address, certificate)?;
        let declared = publication(cluster, &address, &remote)?.node;
        if !declared.starts_with(&format!("xmip:///{cluster}/node/")) {
            return Err(format!(
                "REFUSED: the cluster name {cluster} resolves, at {address}, and answers as \
                 {declared}, no node of cluster {cluster}"
            ));
        }
    }
    let mut real = Vec::new();
    for (name, address) in mapped(cluster, names)? {
        let remote = answering(&name, &address, certificate)?;
        let capability = declared(cluster, &name, &address, &remote)?;
        real.push(RealNode {
            scope: scope_of(cluster, &name),
            name,
            address,
            capability,
            remote: Arc::new(remote),
        });
    }
    Ok(real)
}

/// The operate listener at `address`, opened with the operator certificate
/// in `folder`, which a resolving name needs to be asked at all.
fn answering(name: &str, address: &str, folder: Option<&Path>) -> Result<Remote, String> {
    let folder = folder.ok_or_else(|| {
        format!(
            "REFUSED: {name} resolves, at {address}, and the run holds no operator certificate \
             to ask it whether it is an Xmip node; name the folder of one with -Certificate"
        )
    })?;
    let identity = operator(folder)?;
    Remote::open(address, &identity, rigs_clock())
        .map_err(|problem| format!("REFUSED: {name} resolves, at {address}: {problem}"))
}

/// The tester's operator certificate in `folder`, as the roll presents it to
/// what it asks: `operator.pem`, `operator.key` and `anchors.pem`, from the
/// cluster's own authority — a node's operate listener, and a far end on a
/// machine of its own (`crate::external::remote`).
///
/// # Errors
///
/// REFUSED, in words, where a file does not read or the three are no
/// certificate.
pub fn operator(folder: &Path) -> Result<tls::Identity, String> {
    let file = |leaf: &str| {
        let path = folder.join(leaf);
        std::fs::read(&path).map_err(|error| format!("REFUSED: {}: {error}", path.display()))
    };
    tls::Identity::from_pem(
        &file("operator.pem")?,
        &file("operator.key")?,
        &file("anchors.pem")?,
    )
    .map_err(|error| format!("REFUSED: the certificate in {}: {error}", folder.display()))
}

/// The scope the node `name` of `cluster` declares itself at (ADR-0053).
fn scope_of(cluster: &str, name: &str) -> String {
    format!(
        "{}/node/{name}",
        runtime::running::publication::cluster_location(cluster)
    )
}

/// What the rig's surfaces tell time by: it is no node, so the operating
/// system's clock (ADR-0068, amendment 2026-11-16).
fn rigs_clock() -> Arc<dyn xcore::Clock> {
    Arc::new(xcore::SystemClock)
}

/// What the node at `address` publishes, or REFUSED where it does not
/// answer with a publication.
fn publication(name: &str, address: &str, remote: &Remote) -> Result<Publication, String> {
    let refused = |what: String| format!("REFUSED: {name} resolves, at {address}, and {what}");
    let (_, text) = remote
        .publication()
        .map_err(|problem| refused(format!("does not answer: {problem}")))?;
    Publication::read(&text)
        .map_err(|problem| refused(format!("answers with no Xmip publication: {problem}")))
}

/// What the node at `address` declares: itself the node `name` of
/// `cluster`, and its capability; REFUSED by name where it does not.
fn declared(
    cluster: &str,
    name: &str,
    address: &str,
    remote: &Remote,
) -> Result<Capability, String> {
    let refused = |what: String| format!("REFUSED: {name} resolves, at {address}, and {what}");
    let publication = publication(name, address, remote)?;
    let scope = scope_of(cluster, name);
    if publication.node != scope {
        return Err(refused(format!(
            "answers as {}, not as {scope}",
            publication.node
        )));
    }
    let record = observe::capability::scope(&scope);
    publication
        .records
        .iter()
        .find(|one| one.scope == record)
        .and_then(|one| observe::capability::declared(&one.scope, &one.evidence))
        .map(|(_, capability)| capability)
        .ok_or_else(|| refused("says nothing of what it can do".to_string()))?
        .map_err(refused)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::test_cluster;

    #[test]
    fn on_machines_a_name_that_does_not_resolve_is_refused_and_the_port_is_the_file_s() {
        let cluster = test_cluster();
        let port = operate_port(&cluster.name, None).expect("the file operates its nodes");
        assert!(port.parse::<u16>().is_ok(), "{port}");
        // A name no resolver knows: `.invalid` is reserved for it (RFC 6761).
        let unknown = vec!["no-such-node.invalid".to_string()];
        let refused = found(&cluster.name, &unknown, None)
            .err()
            .expect("a machine that is not there is refused, never spawned");
        assert!(
            refused.starts_with("REFUSED: no-such-node.invalid does not resolve"),
            "{refused}"
        );
    }

    #[test]
    fn every_node_that_does_not_resolve_is_refused_at_once_naming_the_nodes_that_do() {
        let cluster = test_cluster();
        let names = ["OP1.invalid", "localhost", "XX1.invalid"].map(str::to_string);
        // No certificate: nothing is asked before every name has resolved.
        let refused = found(&cluster.name, &names, None)
            .err()
            .expect("two machines that are not there are refused");
        assert!(
            refused.starts_with("REFUSED: OP1.invalid, XX1.invalid do not resolve"),
            "{refused}"
        );
        assert!(
            refused.contains("-Nodes localhost runs the nodes that resolve"),
            "{refused}"
        );
    }

    #[test]
    fn on_one_computer_no_name_is_resolved_and_every_node_is_simulated() {
        let cluster = test_cluster();
        // A name every resolver knows: on one computer it is still a process.
        let roster = Roster::of(&["localhost".to_string()]);
        let sorted = sort(&cluster.name, roster.clone(), false, None).expect("nothing asked");
        assert!(sorted.real.is_empty(), "no node is real on one computer");
        assert_eq!(sorted.simulated, roster);
    }

    #[test]
    fn a_resolving_name_with_no_certificate_to_ask_it_is_refused_by_name() {
        let refused = answering("R", "localhost:7401", None)
            .err()
            .expect("refused");
        assert!(refused.starts_with("REFUSED: R resolves"), "{refused}");
        assert!(refused.contains("-Certificate"), "{refused}");
    }
}

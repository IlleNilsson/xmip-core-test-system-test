//! The run's nodes as the roll sorts them before anything is spawned: real,
//! started as `xmip-service`, or simulated by the cluster process.

use std::path::Path;

use configure::placed::Placed;

use crate::Roster;
use crate::carried::{Binding, Carried};
use crate::cluster::external_binary;
use crate::environment::{self, Machines};
use crate::external::{Externals, remote};
use crate::real::{self, RealNode};
use crate::served::{self, Served};

/// The run's nodes, sorted: the real ones — on virtual machines or in
/// containers, every node of the run, each having answered; as local
/// processes, every node started as `xmip-service` where the run was given
/// one, driven through its Receive Locations — their names, the roster, and
/// the simulated nodes alone, which are all the cluster process spawns; and
/// what the run runs as, `environment::PROCESS`,
/// `environment::VIRTUAL_MACHINE` or `environment::CONTAINER`.
pub struct Nodes {
    pub real: Vec<RealNode>,
    pub names: Vec<String>,
    pub roster: Roster,
    pub simulated: Roster,
    pub served: Option<Served>,
    pub carried: Option<Carried>,
    pub runs_as: &'static str,
}

/// The nodes of the run of `cluster` beneath `base`, as [`Nodes`] sorts
/// them.
///
/// # Errors
///
/// REFUSED, as [`real::sorted`], [`remote::on_machines`], [`served::stores`]
/// and [`Served::start`] say.
pub fn nodes(cluster: &str, base: &Path) -> Result<Nodes, String> {
    let real::Sorted {
        mut real,
        mut names,
        roster,
        simulated,
    } = environment::roster().and_then(|all| real::sorted(cluster, all))?;
    // Nodes on virtual machines or in containers of their own: driven
    // through the far ends on theirs, and none started here, which could
    // reach no Storage node of theirs (ADR-0028, amendments 2026-10-22 and
    // 2026-11-25).
    if let Some(machines) = remote::on_machines(cluster)? {
        let carried = on_machines(&real, &machines, base)?;
        return Ok(Nodes {
            real,
            names,
            roster,
            simulated,
            served: None,
            carried: Some(carried),
            runs_as: machines.runs_as(),
        });
    }
    let (served, started, simulated) = serve(cluster, &roster, simulated, base)?;
    names.extend(started.iter().map(|node| node.name.clone()));
    real.extend(started);
    // The nodes started here are driven through the far ends outside the
    // cluster, each a process of its own (ADR-0028, amendment 2026-11-22).
    let carried = served
        .as_ref()
        .map(|served| {
            let arrived = Carried::arrivals();
            let program = external_binary().map_err(|error| format!("REFUSED: {error}"))?;
            let far = Externals::start(&program, (cluster, base), &served.bindings, &arrived)?;
            Carried::through(&served.bindings, arrived, far)
        })
        .transpose()?;
    Ok(Nodes {
        real,
        names,
        roster,
        simulated,
        served,
        carried,
        runs_as: environment::PROCESS,
    })
}

/// What drives the nodes `real` on the virtual machines or in the
/// containers `machines` beneath `base`: the far ends on theirs, reached
/// with the run's operator certificate (`environment::certificate`).
///
/// # Errors
///
/// REFUSED where the run holds no certificate, or as
/// [`Externals::connect`] and [`Carried::through`] say.
fn on_machines(real: &[RealNode], machines: &Machines, base: &Path) -> Result<Carried, String> {
    let folder = environment::certificate().ok_or(
        "REFUSED: the far ends run on machines of their own, and the run holds no operator \
         certificate to reach them with; Start-XmipTest -VirtualMachine names the one \
         Deploy-XmipEnvironment made, and -Container the one it made",
    )?;
    let identity = real::operator(&folder)?;
    let arrived = Carried::arrivals();
    let (far, bindings) = Externals::connect(&identity, (real, machines), base, &arrived)?;
    let judged: Vec<Binding> = bindings
        .iter()
        .filter(|one| one.placement.placed == Placed::Send)
        .cloned()
        .collect();
    Carried::through(&bindings, arrived, (far, judged))
}

/// Every node of `simulated` started as the `xmip-service` the run was
/// given (`environment::service`), and the real nodes the rig reads them
/// as; none, and every one still simulated, where it was given none or none
/// of them declares the Storage role, which the rest would reach.
///
/// # Errors
///
/// REFUSED where a node does not start, as [`Served::start`] says.
fn serve(
    cluster: &str,
    roster: &Roster,
    simulated: Roster,
    base: &Path,
) -> Result<(Option<Served>, Vec<RealNode>, Roster), String> {
    let Some(service) = environment::service() else {
        return Ok((None, Vec::new(), simulated));
    };
    let names: Vec<String> = simulated.names().into_iter().map(str::to_string).collect();
    if names.is_empty() || !served::stores(&names)? {
        return Ok((None, Vec::new(), simulated));
    }
    let (served, real) = Served::start((cluster, roster), &names, (base, &service))?;
    Ok((Some(served), real, simulated.without(&names)))
}

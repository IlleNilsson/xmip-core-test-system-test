//! The run's nodes as the roll sorts them before anything is spawned: real,
//! started as `xmip-service`, or simulated by the cluster process.

use std::path::Path;

use configure::placed::Placed;

use crate::Roster;
use crate::carried::{Binding, Carried};
use crate::cluster::external_binary;
use crate::environment;
use crate::external::{Externals, remote};
use crate::far_end::FarEnd;
use crate::real::{self, RealNode};
use crate::served::{self, Served};

/// The run's nodes, sorted: the real ones — on machines, every node of the
/// run, each having answered; on one computer, every node started as
/// `xmip-service` where the run was given one, driven through its Receive
/// Locations — their names, the
/// roster, and the simulated nodes alone, which are all the cluster process
/// spawns.
pub struct Nodes {
    pub real: Vec<RealNode>,
    pub names: Vec<String>,
    pub roster: Roster,
    pub simulated: Roster,
    pub served: Option<Served>,
    pub carried: Option<Carried>,
    /// What the real nodes fetch from and put on, kept served for the run.
    pub far_end: Option<FarEnd>,
}

/// The nodes of the run of `cluster` beneath `base`, as [`Nodes`] sorts
/// them.
///
/// # Errors
///
/// REFUSED, as [`real::sorted`], [`served::stores`] and [`Served::start`]
/// say.
pub fn nodes(cluster: &str, base: &Path) -> Result<Nodes, String> {
    let real::Sorted {
        mut real,
        mut names,
        roster,
        simulated,
    } = environment::roster().and_then(|all| real::sorted(cluster, all))?;
    // Nodes on machines of their own: driven through the Parties' far end
    // their environment bound them to, and none started here, which could
    // reach no Storage node of theirs (ADR-0028, amendment 2026-10-22) —
    // the far ends on machines of their own where they run there.
    if !real.is_empty() {
        let arrived = Carried::arrivals();
        if remote::on_machines()? {
            let folder = environment::certificate().ok_or(
                "REFUSED: the far ends run on machines of their own, and the run holds no \
                 operator certificate to reach them with; name it with -Certificate",
            )?;
            let identity = real::operator(&folder)?;
            let (far, bindings) = Externals::connect(&identity, &real, base, &arrived)?;
            let judged: Vec<Binding> = bindings
                .iter()
                .filter(|one| one.placement.placed == Placed::Send)
                .cloned()
                .collect();
            let carried = Carried::through(&bindings, arrived, (far, judged))?;
            return Ok(Nodes {
                real,
                names,
                roster,
                simulated,
                served: None,
                carried: Some(carried),
                far_end: None,
            });
        }
        let far = FarEnd::serve(&real, &arrived)?;
        let carried = far
            .as_ref()
            .map(|far| Carried::new(&far.bindings, arrived))
            .transpose()?;
        return Ok(Nodes {
            real,
            names,
            roster,
            simulated,
            served: None,
            carried,
            far_end: far,
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
        far_end: None,
    })
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

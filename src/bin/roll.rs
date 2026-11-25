//! Roll the System Tests: run every scenario continuously.
//!
//! `cargo run` ticks all four scenarios on an interval and redraws the combined
//! board each round — the tests as ADR-0028 means them, over time and never
//! stopping:
//!
//!   - **`RoundTrip`** — every transport by every contract round-trips and holds
//!     its contract; the message-path stages, with injected faults.
//!   - **`LowLatency`** — the same pairs, timed against a latency budget (p50/p99).
//!   - **`HeavyLoad`** — a megabyte per pair; does it arrive whole and validate.
//!   - **Retention** — retention and archiving: retain, then archive by age
//!     (Xmip does not delete, ADR-0040).
//!   - **Filing** — every archive technology by every contract: file a probe
//!     item through the real store and restore it whole.
//!   - **`ExclusiveClaim`** — exclusive pickup: one holder per item under contention,
//!     per execution style (sequential, parallel, concurrent).
//!   - **`DailyBacklog`** — drain a backlog as fast as possible; tweak, add a node.
//!
//! Each publishes under its own subtree of `xmip:///<cluster>`, merged into one
//! snapshot so the rollup covers all four and an operator drills scenario →
//! detail → the failing leaf.
//!
//! Pass a number to run that many rounds and stop; omit it to roll until
//! interrupted. `XMIP_SYSTEM_TEST_SCENARIOS` names the scenarios to drive, comma
//! separated (`round-trip,heavy-load`); unset, every one rolls, so nothing changed
//! quietly. Two time limits bound any roll (ADR-0028): a maximum wall-clock
//! time, `XMIP_SYSTEM_TEST_MAX_SECONDS`, and a factor on time,
//! `XMIP_SYSTEM_TEST_TIME_FACTOR`, which stretches a **simulated clock** — `1.0`
//! mimics real time, retracted below one runs simulated time faster, so a long
//! horizon plays out in a short run (three simulated years in fifteen real
//! minutes is `MAX_SECONDS=900` with `TIME_FACTOR≈9.5e-6`). The round cadence
//! stays real; the factor stretches simulated time, which the Retention test ages on.
//!
//! When stdout is a terminal the board is redrawn in place; when it is piped,
//! one summary line per round is appended. After every tick the snapshot,
//! history and activity are written to the TOML files the monitoring GUI reads,
//! named for the cluster in `XMIP_SYSTEM_TEST_AREA` (the temp directory unset);
//! `roll --publication <cluster> <area>` prints where they are and starts
//! nothing. Every variable is read in one place, `environment.rs`.
//!
//! **The cluster is a process too** (the owner, 2026-09-19). The roll spawns
//! exactly one `xmip-system-test-cluster` process, and that process spawns and
//! supervises one `xmip-system-test-node` per node (ADR-0028 clause 2). The
//! roll merges the one file the cluster publishes into its own snapshot each
//! round; the board shows the nodes' rollup row, and a node's leaf only when
//! it is not fine.
//!
//! **The nodes are `cluster.xmip`** (ADR-0056, amendment
//! 2026-10-03): `XMIP_TEST_CLUSTER` names the file, and every node under its
//! `[nodes]` is one process, declaring the roles its `roles` says and the
//! Node capabilities its `capabilities` says, `online` among them where it
//! may assume the internet (`environment::roster`; ADR-0045; ADR-0056,
//! amendment 2026-11-24). `Start-XmipTest` writes the run's file from
//! `-Cluster`, `-Nodes`, `-NodeRole` and `-NodeCapability`, or names the
//! test cluster's where `-Nodes` was not given; nothing is counted or
//! numbered.
//!
//! **A node declares its roles** (ADR-0056, amendment 2026-10-01) —
//! `receiving`, `processing+sending`, or `executing` for all
//! three in one process — and every node runs its part of the scenarios
//! named. Where a node's roles serve a stage, `RoundTrip` is the nodes' —
//! each pair handed receive to process to send between the processes, or
//! kept in an executing node's one process — and the roll does not also run
//! it in-process; a stage no node serves is REFUSED at the start, naming the
//! role. A node serving no stage runs the shared-directory tests whole, as
//! before. A node's name decides none of this.
//!
//! **As local processes, on virtual machines or in containers** (ADR-0028,
//! amendments 2026-11-25), the System Tests and the Systems Tests alike, as
//! `Start-XmipTest -Process`, `-VirtualMachine` or `-Container` chose:
//! on virtual machines `XMIP_SYSTEM_TEST_MACHINES` names each node's and far
//! end's machine, `name=address`, from what `Deploy-XmipEnvironment`
//! recorded, in containers each one's container as `-Container` made it,
//! and `XMIP_SYSTEM_TEST_PUBLISHED` where this computer reaches each
//! listener the roll asks; every node of the run must answer at its machine
//! as that node of this cluster over its operate listener, asked with the
//! operator certificate `XMIP_SYSTEM_TEST_CERTIFICATE` names — never
//! spawned, configured or stopped, its publication followed by the
//! runtime's one gatherer (`runtime::plane::gather`) and merged each round;
//! one with no machine or that does not answer is REFUSED by name, and none
//! is spawned in its place. No name is resolved. `[run] real` and the first
//! lines say which nodes are real (`real.rs`), and `[run] runs_as` which
//! the run is.
//!
//! **As local processes, a node is started as `xmip-service`** where the
//! run was given one, `XMIP_SYSTEM_TEST_SERVICE`, and one of those nodes
//! declares the Storage role (ADR-0028, amendment 2026-10-22): the run gives
//! each its slice, its certificate, its addresses on loopback and its
//! bindings (`served.rs`), reads it as a real node, and drives it through
//! its Receive Locations each round (`carried.rs`); simulated where not.
//!
//! **Real nodes are driven through the far ends outside the cluster**, the
//! System Tests' cluster file's `[environment.far_ends]` (`external.rs`):
//! as local processes each a process of its own here, on virtual machines
//! or in containers each on its own, at the address
//! `XMIP_SYSTEM_TEST_MACHINES` gives it; they
//! drop into the Receive Locations and tell what the Send Ports put
//! (`carried.rs`).
//!
//! **It audits** (ADR-0062): `start` once the roll knows what it is, `stop`
//! when its rounds are done, every refusal and failed write as a failure, and
//! every panic as `unhandled` — into `XMIP_AUDIT_DIRECTORY`, else the
//! operating system's log (`process_audit.rs`).
//!
//! What it decides, sorts, spawns, draws and leaves behind is the library's
//! `roll.rs`; this file is its `main`.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use observe::topology::window::Window;
use observe::{Activity, History, Snapshot, now_unix_nanos};
use xaudit::program_audit::ProgramAudit;
use xmip_core_test_system_test::cluster::Orders;
use xmip_core_test_system_test::drop_box::Dropping;
use xmip_core_test_system_test::environment::{self, max_seconds, time_factor};
use xmip_core_test_system_test::in_process::InProcess;
use xmip_core_test_system_test::publication::Publication;
use xmip_core_test_system_test::real;
use xmip_core_test_system_test::roll::round::{Drawing, Rolling, assembled, scenarios};
use xmip_core_test_system_test::roll::{
    Nodes, Ran, activity, announce, declared, leave, nodes, reading, relayed, say_started, shared,
    spawn_cluster, the_publication,
};
use xmip_core_test_system_test::run::{announced, found};
use xmip_core_test_system_test::{
    Budget, Headroom, Parties, Stress, cluster_name, cluster_root, record_round, redraw, started,
    summarize,
};
use xmip_core_test_system_test::{image, process_audit};

fn main() {
    // The name is the image's own — xmip-system-test-<cluster>-roll where
    // Start-XmipTest linked one — so the declaration and the audit say what
    // Get-Process says (ADR-0053, amendment 2026-09-20).
    let called = image::this_process("xmip-system-test-roll");
    let audit = process_audit::program(&called);
    audit.watch_panics();

    if answered_publication(&audit) {
        return;
    }

    let (cluster, root, base) = or_refuse(&audit, this_cluster());
    let root = root.as_str();
    let _declared = declared(&audit, called, root);
    let stress = Stress::from_env();
    let chosen = or_refuse(&audit, environment::scenarios());
    let Nodes {
        real: real_nodes,
        names: real_names,
        roster,
        simulated,
        served,
        mut carried,
        runs_as,
    } = or_refuse(&audit, nodes(&cluster, &base));
    let relayed = or_refuse(&audit, relayed(&chosen, &simulated));
    // Every node read, none simulated: the configured flows with real
    // documents, and nothing else (ADR-0028, amendment 2026-11-11).
    let served_only = or_refuse(
        &audit,
        reading(&chosen, (&simulated, &real_names), carried.as_mut(), stress),
    );
    let tests = (chosen.as_slice(), served_only);
    let run = started(&cluster, tests, &roster, stress, (&real_names, runs_as));

    let spread = Parties::configured().over_every_transport();
    let first = announced(&cluster, stress, &roster, &spread);
    announce(first, &real_nodes);

    // Every scenario this process runs itself (`in_process.rs`): none where
    // every node is read.
    let mut in_process = scenarios(served_only, || {
        InProcess::new(root, &base, stress, chosen.clone(), relayed)
    });
    // The Parties' bursts into the drop box the receiving nodes share.
    let mut dropping = relayed.then(|| Dropping::new(&shared(&base)));

    // An hour of history at one point a second: enough to watch a shift, bounded
    // so a week-long run does not grow. ADR-0029.
    let mut history = History::with_capacity(3600);

    let limit: Option<u64> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    // The roll's snapshot is read and never acted on: an operator acts on a
    // node the run reads at its door, over its operate listener, and the act
    // reaches its node through Xmip Storage (ADR-0014, amendment 2026-11-13).
    let publication = Publication::of(&cluster, run.clone(), audit.clone());
    say_started(&audit, (&run, &spread), limit, &publication.snapshot);

    // One cluster per roll (ADR-0028), and since 2026-09-19 a process of its
    // own: it spawns and supervises the nodes and publishes beside this
    // roll's snapshot, which merges that one file each round.
    let cluster_path = publication.beside(&format!("{cluster}-cluster.xmip"));
    let orders = Orders::of(stress, simulated.clone(), 0).driving(&chosen);
    let mut spawned = spawn_cluster(&audit, &cluster, &orders, &base, &cluster_path);

    let live = std::io::stdout().is_terminal();
    let real = Duration::from_millis(1000);
    let budget = Budget::new(max_seconds(), time_factor());

    if !live {
        println!("publishing snapshots to {}", publication.snapshot.display());
    }

    let mut round: u64 = 0;
    // What moved over the last half minute of rounds, stated on each
    // round's drawing (ADR-0052, amendment 2026-11-10).
    let mut window = Window::new();
    let idle = Activity::with_capacity(1);
    // What the real nodes and those started as xmip-service published, and
    // what each drew of itself, followed by the runtime's one gatherer.
    let gatherer = or_refuse(&audit, real::gathered(&real_nodes, &roster, root));
    let mut reading = |snapshot: &mut Snapshot, now| gatherer.gathered().merged(snapshot, now);
    let drawing = Drawing::of(&roster, relayed, &spread, &real_names);
    loop {
        round += 1;

        // What everyone else is using, measured now: the levels size this
        // round's pairs to half of what is left (ADR-0028, 2026-09-11). The
        // level's own count of nodes was sized the same way.
        let headroom = Headroom::refresh();

        // The scenarios, the cluster's own file, what the nodes the run
        // reads published and drew, Carried's verdicts, and the drawing from
        // all of them: one round, assembled in one place (`roll::round`).
        let rolling = Rolling {
            in_process: in_process.as_mut(),
            spawned: spawned.as_mut(),
            reading: &mut reading,
            carried: carried.as_mut(),
        };
        let at = (now_unix_nanos(), budget.simulated_elapsed());
        let (mut snapshot, topology) = assembled(root, at, rolling, &drawing, &mut window);

        // Every scope's series, and the rollup at the root that the history
        // file is written from: a scenario counts beneath the node, so a roll
        // that recorded only the snapshot published `points = []` for as long
        // as the System Tests have had a history (curve.rs, 2026-09-19).
        if let Some(dropping) = dropping.as_mut() {
            dropping.round(root, &mut snapshot, now_unix_nanos());
        }
        record_round(&mut history, root, &snapshot);
        let activity = activity((in_process.as_ref(), carried.as_ref()), &idle);
        publication.round(root, &snapshot, topology, &history, activity);

        said(
            (live, round),
            (root, &snapshot),
            &real_nodes,
            &publication.snapshot,
        );
        println!("  headroom: {}", headroom.describe());

        if limit.is_some_and(|limit| round >= limit) || budget.expired() {
            break;
        }
        std::thread::sleep(real);
    }

    let mut ran = Ran {
        round,
        dropping,
        carried,
    };
    leave(
        &audit,
        &cluster,
        &mut ran,
        (spawned, served),
        (&base, &cluster_path),
    );
}

/// What a round says on the console: drawn whole where a person watches it
/// live — the board, which real nodes answered and where it publishes — and
/// summed in a line otherwise.
fn said(
    (live, round): (bool, u64),
    (root, snapshot): (&str, &Snapshot),
    real: &[real::RealNode],
    published: &std::path::Path,
) {
    if live {
        redraw(root, round, snapshot);
        if let Some(line) = found(real) {
            println!("{line}");
        }
        println!("  publishing to {}", published.display());
    } else {
        summarize(root, round, snapshot);
    }
}

/// Asked where a cluster publishes, it answers the same way: the names are
/// decided here (`environment::publish_paths`), and `Start-XmipTest`
/// records what it is told rather than naming the files again. Whether it
/// was asked, and answered.
fn answered_publication(audit: &ProgramAudit) -> bool {
    if std::env::args().nth(1).as_deref() != Some("--publication") {
        return false;
    }
    let told: Vec<String> = std::env::args().skip(2).collect();
    println!("{}", or_refuse(audit, the_publication(&told)));
    true
}

/// What the environment told this roll, or REFUSED: a value it cannot read is
/// said, audited as the failure to `start`, and the roll does not start —
/// until 2026-09-19 an unknown scenario was said on stderr and dropped, and
/// the roll carried on as something nobody asked for. A word that is no
/// role is refused the same way.
fn or_refuse<T>(audit: &ProgramAudit, told: Result<T, String>) -> T {
    told.unwrap_or_else(|refusal| {
        process_audit::fail(audit, "start", &refusal);
        std::process::exit(2);
    })
}

/// One cluster per roll (ADR-0028): its name, its scope root, and its own
/// scratch directory, emptied now, so a second cluster beside it neither wipes
/// nor shares this one's directories. REFUSED when the owner named none.
fn this_cluster() -> Result<(String, String, PathBuf), String> {
    let Some(cluster) = cluster_name() else {
        return Err(
            "REFUSED: a roll is a cluster and the owner names it; set XMIP_SYSTEM_TEST_CLUSTER \
             (Start-XmipTest -Cluster <name>). A test spawns nodes, never a cluster."
                .to_string(),
        );
    };
    let base = std::env::temp_dir().join("system-test").join(&cluster);
    std::fs::remove_dir_all(&base).ok();
    Ok((cluster, cluster_root(), base))
}

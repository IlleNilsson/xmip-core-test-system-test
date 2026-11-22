//! The far ends outside the cluster, each a process of its own, as a run of
//! nodes started as `xmip-service` starts them (ADR-0028, amendment
//! 2026-11-22): every far end the shipped file names started, a sender's
//! turn moving one further each round, a receiver taking what a Send Port
//! delivers and telling of it, and every one ended with its input. The test
//! plays the node, copying what was dropped to where it delivers it.

use std::path::Path;
use std::sync::PoisonError;
use std::time::{Duration, Instant};

use configure::placed::{Placed, Placement};
use xmip_core_test_essentials::carried::{Binding, Carried, Spot};
use xmip_core_test_essentials::external::{Externals, FILE, named};

/// The far ends' program cargo built for this program.
fn external_binary() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_xmip-essentials-external"))
}

#[test]
fn each_far_end_is_a_process_a_sender_drops_in_turn_and_a_receiver_takes_and_tells() {
    let _alone = super::alone();
    let base = super::scratch("external");
    let cluster = configure::fixture::test_cluster();
    let bound = |placed, name: &str, folder: &Path| Binding {
        placement: Placement {
            placed,
            application: "Order to cash".to_string(),
            name: name.to_string(),
            node: cluster.node(0).name.clone(),
        },
        spot: Spot::Folder(folder.to_path_buf()),
    };
    let (dropping, out) = (base.join("drop"), base.join("out"));
    std::fs::create_dir_all(&dropping).expect("the Receive Location's folder");
    let port = "Sales orders to ERP";
    let bindings = [
        bound(Placed::Receive, "Sales orders from Party-A", &dropping),
        bound(Placed::Send, port, &out),
    ];
    let arrived = Carried::arrivals();
    let (mut far, judged) = Externals::start(
        external_binary(),
        (&cluster.name, &base),
        &bindings,
        &arrived,
    )
    .expect("every far end started");
    let [
        Binding {
            spot: Spot::Folder(taken_into),
            ..
        },
    ] = judged.as_slice()
    else {
        panic!(
            "the Send Port judged at its receiver's folder: {}",
            judged.len()
        );
    };

    // Each round the next sender drops into the Location, so as many rounds
    // as there are senders reach it from every one.
    let shipped = named(FILE).expect("the shipped far ends");
    let document = b"UNA:+.? 'UNB+UNOC:3+PARTY-A:ZZZ+ESSENTIALS:ZZZ'";
    let mut senders = Vec::new();
    for round in 0..shipped.senders.len() {
        let name = format!("orders-{round}.edi");
        let round = u64::try_from(round).expect("few rounds");
        let (sender, at) = far
            .dropped((0, round), document, (&dropping, &name))
            .expect("dropped");
        assert!(at <= Instant::now(), "dropped before it answered");
        let sender = sender.to_string();
        assert_eq!(
            std::fs::read(dropping.join(&name)).expect("dropped whole"),
            document
        );
        senders.push(sender);
    }
    senders.sort();
    let mut every = shipped.senders.clone();
    every.sort();
    assert_eq!(senders, every, "each sender in its turn");

    // Delivered at the Send Port's folder: its receiver takes it and tells.
    std::fs::copy(dropping.join("orders-0.edi"), out.join("orders-0.edi")).expect("delivered");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let told = arrived
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get("orders-0.edi")
            .map(|(at, _)| at.clone());
        if let Some(at_port) = told {
            assert_eq!(at_port, port);
            break;
        }
        assert!(Instant::now() < deadline, "the receiver never told of it");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        taken_into.join("orders-0.edi").is_file(),
        "into the receiver's folder"
    );
    assert!(
        !out.join("orders-0.edi").exists(),
        "taken off the Send Port's folder"
    );
    let receiver = far
        .receiver_of(port)
        .expect("delivered to one receiver")
        .to_string();
    let words = far.words();
    assert!(words.contains(&format!("{receiver} received 1")), "{words}");
    for sender in &shipped.senders {
        assert!(words.contains(&format!("{sender} sent 1")), "{words}");
    }
    // Every far end ends with its input, none left running.
    drop(far);
}

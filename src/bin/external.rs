//! `xmip-system-test-external`: one far end outside the cluster, a process of
//! its own (the owner's node plan, 2026-11-22: *the [System Tests'] far ends,
//! processes of their own*; ADR-0028, amendment 2026-11-22). The roll starts
//! each the System Tests' `configuration/external.xmip` names, from an image
//! named for it, `xmip-system-test-<cluster>-external-<name>`: a sender drops
//! each file it is handed into a Receive Location's folder, a receiver takes
//! what the Send Ports it is given deliver. It ends when its input does, as
//! the run ends or the roll with it.
//!
//! **It audits** (ADR-0062) its start and its stop, with how many files it
//! sent or received, and declares itself while it runs (ADR-0053), at
//! `xmip:///<cluster>/external/<name>`. What it does is the library's
//! `external/program.rs`; this file is its `main`.

use std::process::ExitCode;

use xaudit::program_audit::ProgramAudit;
use xmip_core_test_system_test::external::command::{Does, Program};
use xmip_core_test_system_test::external::program;
use xmip_core_test_system_test::external::serving;
use xmip_core_test_system_test::{cluster_root, image, process_audit};

const USAGE: &str = "\
usage: xmip-system-test-external --name TX1 --sender
       xmip-system-test-external --name RX1 --receiver --into <folder>
                                [--port <Send Port> <folder>]...
       xmip-system-test-external --name TX1 --serve --configuration <folder>
       xmip-system-test-external --name TX1 --install --configuration <folder>
       xmip-system-test-external --configuration <folder> --listening

One far end outside the cluster, started by the roll for each the
System Tests' configuration/external.xmip names. A sender reads, a line each,
a document, where it goes and a name, tab-separated, drops it there and
answers `dropped` or `failed`. A receiver moves each whole file a Send Port
delivers into its folder and says `arrived`, the Send Port, the file and
when. Each ends when its input does. On a machine of its own, --serve
serves the roll's sessions over Xmip's mutual TLS on port 7423, its
certificate node.pem, node.key and anchors.pem in --configuration, a
sender pushing to listening Receive Locations and a receiver serving its
Send Ports over SFTP on 7422; --install registers that as a service and
starts it; --listening says the two ports.";

fn main() -> ExitCode {
    // The name is the image's own, xmip-system-test-<cluster>-external-<name>
    // where the roll named one, so the declaration, the audit and the
    // process list agree (ADR-0053, amendment 2026-09-20).
    let called = image::this_process("xmip-system-test-external");
    let audit = process_audit::program(&called);
    audit.watch_panics();

    let started = match Program::parse(std::env::args().skip(1)) {
        Ok(started) => started,
        Err(problem) => {
            process_audit::fail(&audit, "start", &format!("external: {problem}"));
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    // Its ports are said and nothing more: its machine's firewall asks.
    if started.does == Does::Listens {
        for line in serving::listening() {
            println!("{line}");
        }
        return ExitCode::SUCCESS;
    }
    let scope = format!("{}/external/{}", cluster_root(), started.name);
    audit.locate(&scope);
    let _declared = node::Declaration::new(called, &scope, node::Purpose::Test)
        .and_then(|declaration| declaration.with("does", started.does.word()))
        .and_then(|declaration| process_audit::declared(&declaration))
        .map_err(|error| {
            let problem = format!(
                "external {}: could not declare itself: {error}",
                started.name
            );
            process_audit::fail(&audit, "declare", &problem);
        });
    process_audit::start(
        &audit,
        &[("external", &started.name), ("does", started.does.word())],
    );

    let input = std::io::stdin().lock();
    let carried = match &started.does {
        Does::Serves { folder } => return served(&audit, &started.name, folder),
        Does::Installs { folder } => {
            return match serving::install(&started.name, folder) {
                Ok(said) => {
                    println!("OK: {said}");
                    ExitCode::SUCCESS
                }
                Err(problem) => {
                    let problem = format!("external {}: {problem}", started.name);
                    process_audit::fail(&audit, "install", &problem);
                    eprintln!("{problem}");
                    ExitCode::FAILURE
                }
            };
        }
        Does::Listens => return ExitCode::SUCCESS,
        Does::Sends => program::sending(input, &mut std::io::stdout().lock()),
        Does::Receives { into, ports } => {
            match program::receiving(into, ports, input, std::io::stdout()) {
                Ok(taken) => taken,
                Err(problem) => {
                    let problem = format!("external {}: {problem}", started.name);
                    process_audit::fail(&audit, "receive", &problem);
                    return ExitCode::FAILURE;
                }
            }
        }
    };
    process_audit::stop(
        &audit,
        &[
            ("external", &started.name),
            (started.does.counted(), &carried.to_string()),
        ],
    );
    ExitCode::SUCCESS
}

/// The far end `name` served on its machine, its certificate in `folder`:
/// each session audited as it ends, with what the far end was for it and
/// how many files it carried.
fn served(audit: &ProgramAudit, name: &str, folder: &std::path::Path) -> ExitCode {
    let (recording, named) = (audit.clone(), name.to_string());
    let ended = Box::new(
        move |session: Result<(String, u64), String>| match session {
            Ok((what, carried)) => process_audit::stop(
                &recording,
                &[
                    ("external", &named),
                    ("session", &what),
                    ("carried", &carried.to_string()),
                ],
            ),
            Err(problem) => process_audit::fail(
                &recording,
                "session",
                &format!("external {named}: {problem}"),
            ),
        },
    );
    match serving::run(name, folder, ended) {
        Ok(code) => ExitCode::from(code),
        Err(problem) => {
            process_audit::fail(audit, "serve", &format!("external {name}: {problem}"));
            ExitCode::FAILURE
        }
    }
}

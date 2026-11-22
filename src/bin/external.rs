//! `xmip-essentials-external`: one far end outside the cluster, a process of
//! its own (the owner's node plan, 2026-11-22: *the Essentials' far ends,
//! processes of their own*; ADR-0028, amendment 2026-11-22). The roll starts
//! each the Essentials' `configuration/external.xmip` names, from an image
//! named for it, `xmip-essentials-<cluster>-external-<name>`: a sender drops
//! each file it is handed into a Receive Location's folder, a receiver takes
//! what the Send Ports it is given deliver. It ends when its input does, as
//! the run ends or the roll with it.
//!
//! **It audits** (ADR-0062) its start and its stop, with how many files it
//! sent or received, and declares itself while it runs (ADR-0053), at
//! `xmip:///<cluster>/external/<name>`. What it does is the library's
//! `external/program.rs`; this file is its `main`.

use std::process::ExitCode;

use xmip_core_test_essentials::external::program::{self, Does, Program};
use xmip_core_test_essentials::{cluster_root, image, process_audit};

const USAGE: &str = "\
usage: xmip-essentials-external --name TX1 --sender
       xmip-essentials-external --name RX1 --receiver --into <folder>
                                [--port <Send Port> <folder>]...

One far end outside the cluster, started by the roll for each the
Essentials' configuration/external.xmip names. A sender reads, a line each,
a file, a Receive Location's folder and a name, tab-separated, drops the file
there under the name and answers `dropped` or `failed`. A receiver moves each
whole file a Send Port delivers into its folder and says `arrived`, the Send
Port, the file and when. Each ends when its input does.";

fn main() -> ExitCode {
    // The name is the image's own, xmip-essentials-<cluster>-external-<name>
    // where the roll named one, so the declaration, the audit and the
    // process list agree (ADR-0053, amendment 2026-09-20).
    let called = image::this_process("xmip-essentials-external");
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
    let scope = format!("{}/external/{}", cluster_root(), started.name);
    audit.locate(&scope);
    let _declared = node::Declaration::new(called, &scope, node::Purpose::Test)
        .and_then(|declaration| declaration.with("does", started.does.word()))
        .and_then(|declaration| process_audit::declared(&audit, declaration))
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

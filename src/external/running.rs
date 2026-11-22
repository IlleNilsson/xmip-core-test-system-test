//! One far end running, as the roll holds it: a process of this computer it
//! started, told through its input, or one on a machine of its own, told
//! over the connection to it (`remote.rs`); what it says handed on, each
//! arrival it tells recorded and counted, and its readiness waited for.
//! Split from `external.rs` on 2026-11-22.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};

use super::ANSWER;
use super::program::{self, Arrival};
use crate::carried::Arrived;
/// One far end running: its name, its process on this computer or none
/// where it runs on a machine of its own, what it is told through — the
/// process's input, or the connection to it — what it says, and how many
/// files it carried.
pub(super) struct Running {
    pub(super) name: String,
    pub(super) child: Option<Child>,
    pub(super) input: Option<Box<dyn Write + Send>>,
    pub(super) said: Receiver<String>,
    pub(super) carried: Arc<AtomicU64>,
}

impl Running {
    /// The far end `name` of `cluster` started from its image of `program`
    /// with `arguments`: what it says handed on, or, for a receiver, each
    /// arrival it tells recorded in `arrived` and counted. It declares itself
    /// in a folder of the run's beneath `area`, as the nodes the run starts
    /// as `xmip-service` do (`crate::served`): it is the run's, ended with
    /// its input as they are, never ended from outside before it can say
    /// what it carried.
    pub(super) fn start(
        program: &Path,
        (cluster, name, area): (&str, &str, &Path),
        arguments: &[String],
        arrived: Option<&Arrived>,
    ) -> Result<Self, String> {
        let image = crate::image::of(program, cluster, &format!("external-{name}"))
            .map_err(|error| format!("REFUSED: the far end {name}'s image: {error}"))?;
        let mut command = Command::new(&image);
        command
            .arg("--name")
            .arg(name)
            .args(arguments)
            .env(
                runtime::declared::DIRECTORY_VARIABLE,
                area.join("external").join("process"),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
        }
        let mut child = command
            .spawn()
            .map_err(|error| format!("REFUSED: the far end {name} did not start: {error}"))?;
        let output = child.stdout.take().ok_or("the far end's output")?;
        let input = child
            .stdin
            .take()
            .map(|input| Box::new(input) as Box<dyn Write + Send>);
        let told = Told {
            arrived,
            into: None,
        };
        Self::ready((name, Some(child)), input, output, told)
    }

    /// The far end `name` told through `input` and heard on `output`: what
    /// it says handed on, each arrival `told` records counted, and its
    /// [`program::READY`] waited for, as the first thing it says. Ended, its
    /// process — `child`, where it is one of this computer's — is killed.
    pub(super) fn ready(
        (name, child): (&str, Option<Child>),
        input: Option<Box<dyn Write + Send>>,
        output: impl Read + Send + 'static,
        told: Told<'_>,
    ) -> Result<Self, String> {
        let carried = Arc::new(AtomicU64::new(0));
        let (tell, said) = mpsc::channel();
        let counting = Arc::clone(&carried);
        let (arrived, into) = (told.arrived.map(Arc::clone), told.into);
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines().map_while(Result::ok) {
                match (&arrived, Arrival::read(&line)) {
                    (Some(arrived), Some(arrival)) => {
                        counting.fetch_add(1, Ordering::Relaxed);
                        // A receiver on a machine of its own says what it
                        // took, which is kept here where it is judged.
                        if let Some(into) = &into
                            && std::fs::write(into.join(&arrival.name), &arrival.bytes).is_err()
                        {
                            continue;
                        }
                        arrival.told(arrived);
                    }
                    _ => {
                        let _ = tell.send(line);
                    }
                }
            }
        });
        let mut running = Self {
            name: name.to_string(),
            input,
            child,
            said,
            carried,
        };
        // Ready before it is used: a receiver is watching every Send Port's
        // folder it was given, or serving its Send Ports, so nothing
        // delivered is missed.
        match running.said.recv_timeout(ANSWER) {
            Ok(line) if line == program::READY => Ok(running),
            said => {
                if let Some(child) = running.child.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                let what = said.unwrap_or_else(|_| "nothing".to_string());
                Err(format!(
                    "REFUSED: the far end {name} never said it is ready, said {what}; \
                     its audit says why"
                ))
            }
        }
    }
}

/// What a far end's arrivals are recorded in, and, for one on a machine of
/// its own, the folder of this computer the bytes it says are kept in.
pub(super) struct Told<'a> {
    pub(super) arrived: Option<&'a Arrived>,
    pub(super) into: Option<PathBuf>,
}

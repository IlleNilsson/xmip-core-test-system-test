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
use std::thread::JoinHandle;

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
            .stderr(Stdio::piped());
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
        let words = last_words(child.stderr.take().ok_or("the far end's error output")?);
        let input = child
            .stdin
            .take()
            .map(|input| Box::new(input) as Box<dyn Write + Send>);
        let told = Told {
            arrived,
            into: None,
        };
        Self::ready((name, Some(Launched { child, words })), input, output, told)
    }

    /// The far end `name` told through `input` and heard on `output`: what
    /// it says handed on, each arrival `told` records counted, and its
    /// [`program::READY`] waited for, as the first thing it says. Refused,
    /// its process — `launched`, where it is one of this computer's — is
    /// ended.
    pub(super) fn ready(
        (name, launched): (&str, Option<Launched>),
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
        let (child, words) = launched.map_or((None, None), |launched| {
            (Some(launched.child), Some(launched.words))
        });
        let running = Self {
            name: name.to_string(),
            input,
            child,
            said,
            carried,
        };
        // Ready before it is used: a receiver is watching every Send Port's
        // folder it was given, or serving its Send Ports, so nothing
        // delivered is missed. Waited for until it says so, or says
        // anything else, or ends, and never for a time: under load a
        // process may take seconds to start, and the far end's audit says
        // where its start went. Until 2026-11-25 one silent for five
        // seconds was refused and killed before its audit could say why.
        match running.said.recv() {
            Ok(line) if line == program::READY => Ok(running),
            said => Err(running.refused(said.ok(), words)),
        }
    }

    /// Why the far end is refused, having said `said` where it says it is
    /// ready, or ended without a word: its process, where it is one of this
    /// computer's, ended, and how it exited and what it last said on its
    /// error output named.
    fn refused(mut self, said: Option<String>, words: Option<JoinHandle<String>>) -> String {
        let what = said.map_or_else(
            || "ended before it said it is ready".to_string(),
            |line| format!("said '{line}' where it says it is ready"),
        );
        let exited = self.child.as_mut().and_then(|child| {
            let _ = child.kill();
            child.wait().ok()
        });
        let exited = exited.map_or_else(String::new, |status| format!(", {status}"));
        let last = words
            .and_then(|words| words.join().ok())
            .filter(|last| !last.is_empty())
            .map_or_else(String::new, |last| format!(", its last words: {last}"));
        format!(
            "REFUSED: the far end {} {what}{exited}{last}; its audit says where its start went",
            self.name
        )
    }
}

/// A far end's process of this computer, and what it says last on its error
/// output, kept by a thread of its own until it ends.
pub(super) struct Launched {
    child: Child,
    words: JoinHandle<String>,
}

/// The last line `errors`, a far end's error output, says before it ends,
/// read to its end on a thread of its own, so the far end is never kept
/// waiting for a reader.
fn last_words(errors: impl Read + Send + 'static) -> JoinHandle<String> {
    std::thread::spawn(move || {
        BufReader::new(errors)
            .lines()
            .map_while(Result::ok)
            .filter(|line| !line.trim().is_empty())
            .last()
            .unwrap_or_default()
    })
}

/// What a far end's arrivals are recorded in, and, for one on a machine of
/// its own, the folder of this computer the bytes it says are kept in.
pub(super) struct Told<'a> {
    pub(super) arrived: Option<&'a Arrived>,
    pub(super) into: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use std::io::pipe;
    use std::time::Duration;

    use super::*;

    /// What a far end records nothing in.
    const NOWHERE: Told<'static> = Told {
        arrived: None,
        into: None,
    };

    /// A far end slower to say it is ready than a sender is given to answer
    /// is taken once it says so: the wait is for the event, never for a
    /// time, so a loaded computer refuses no far end.
    #[test]
    fn a_far_end_slow_to_start_is_taken_once_it_says_it_is_ready() {
        let (output, mut saying) = pipe().expect("a pipe");
        let speaking = std::thread::spawn(move || {
            std::thread::sleep(super::super::ANSWER + Duration::from_millis(100));
            writeln!(saying, "{}", program::READY).expect("said");
            saying
        });

        let running = Running::ready(("slow", None), None, output, NOWHERE).map(|one| one.name);

        assert_eq!(running.as_deref(), Ok("slow"));
        drop(speaking.join());
    }

    /// One that says anything else first, or ends without a word, is
    /// refused at once, in words that say which.
    #[test]
    fn a_far_end_that_says_another_thing_or_ends_unready_is_refused_in_words() {
        let (output, mut saying) = pipe().expect("a pipe");
        writeln!(saying, "busy").expect("said");
        let Err(other) = Running::ready(("busy", None), None, output, NOWHERE) else {
            panic!("taken without saying it is ready");
        };
        assert!(
            other.contains("busy said 'busy' where it says it is ready"),
            "{other}"
        );

        let (output, saying) = pipe().expect("a pipe");
        drop(saying);
        let Err(ended) = Running::ready(("gone", None), None, output, NOWHERE) else {
            panic!("taken without saying it is ready");
        };
        assert!(
            ended.starts_with("REFUSED: the far end gone ended before it said it is ready;"),
            "{ended}"
        );
    }
}

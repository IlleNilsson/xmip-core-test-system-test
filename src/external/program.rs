//! What one far end does, as `xmip-essentials-external` runs it
//! (`src/bin/external.rs`), and the lines it and the roll say to each other,
//! each written and read here alone.
//!
//! - **A sender** reads an [`Order`] a line — the file to send, the
//!   Receive Location's folder and the name it goes by — drops it there as a
//!   producer that means it whole does, written beside it and renamed in
//!   (`carried::spot::put`, the one way the Essentials drops a file), and
//!   answers each with an [`Answer`].
//! - **A receiver** watches the folders of the Send Ports it is given and,
//!   as each whole file appears there (`carried::spot::whole`), moves it into
//!   a folder of its own, as a Party takes what it is delivered, and says
//!   an [`Arrival`]: the Send Port, the file and when. Told, never looking on
//!   a timer.
//!
//! Each says [`READY`] once it can work, and ends when its input does,
//! saying how many files it carried.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use runtime::directory_watch::DirectoryWatch;

use crate::carried::{Arrived, spot};

/// One file a sender is to drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Order {
    pub source: PathBuf,
    pub folder: PathBuf,
    pub name: String,
}

impl Order {
    /// The order as one line: its three parts, tab-separated.
    ///
    /// # Errors
    ///
    /// Where a part holds a tab or a line break, which no line can carry.
    pub fn line(&self) -> Result<String, String> {
        let parts = [
            self.source.display().to_string(),
            self.folder.display().to_string(),
            self.name.clone(),
        ];
        if let Some(part) = parts.iter().find(|part| part.contains(['\t', '\n', '\r'])) {
            return Err(format!("{part} cannot be ordered on one line"));
        }
        Ok(parts.join("\t"))
    }

    /// The order a line says, if it says one.
    #[must_use]
    pub fn read(line: &str) -> Option<Self> {
        let mut parts = line.split('\t');
        let (source, folder, name) = (parts.next()?, parts.next()?, parts.next()?);
        parts.next().is_none().then(|| Self {
            source: PathBuf::from(source),
            folder: PathBuf::from(folder),
            name: name.to_string(),
        })
    }
}

/// What a sender says of an order: dropped, and when, in nanoseconds since
/// the Unix epoch, the moment it was renamed in; or failed and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    Dropped(String, i64),
    Failed(String, String),
}

impl Answer {
    /// The answer as one line.
    #[must_use]
    pub fn line(&self) -> String {
        match self {
            Self::Dropped(name, at) => format!("dropped\t{name}\t{at}"),
            Self::Failed(name, problem) => {
                format!(
                    "failed\t{name}\t{}",
                    problem.replace(['\t', '\n', '\r'], " ")
                )
            }
        }
    }

    /// The answer a line says, if it says one.
    #[must_use]
    pub fn read(line: &str) -> Option<Self> {
        let mut parts = line.splitn(3, '\t');
        match (parts.next()?, parts.next()?, parts.next()) {
            ("dropped", name, Some(at)) => Some(Self::Dropped(name.to_string(), at.parse().ok()?)),
            ("failed", name, Some(problem)) => {
                Some(Self::Failed(name.to_string(), problem.to_string()))
            }
            _ => None,
        }
    }
}

/// What a receiver says of a file it took: the Send Port it arrived at, its
/// name, and when it was taken, in nanoseconds since the Unix epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Arrival {
    pub port: String,
    pub name: String,
    pub at_unix_nanos: i64,
}

impl Arrival {
    /// The arrival as one line.
    #[must_use]
    pub fn line(&self) -> String {
        format!(
            "arrived\t{}\t{}\t{}",
            self.port, self.name, self.at_unix_nanos
        )
    }

    /// The arrival a line says, if it says one.
    #[must_use]
    pub fn read(line: &str) -> Option<Self> {
        let mut parts = line.split('\t');
        if parts.next()? != "arrived" {
            return None;
        }
        let (port, name, at) = (parts.next()?, parts.next()?, parts.next()?.parse().ok()?);
        parts.next().is_none().then(|| Self {
            port: port.to_string(),
            name: name.to_string(),
            at_unix_nanos: at,
        })
    }

    /// The arrival recorded in `arrived` where its file is not yet there, at
    /// the instant it was taken.
    pub fn told(self, arrived: &Arrived) {
        let at = instant_of(self.at_unix_nanos);
        arrived
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(self.name)
            .or_insert((self.port, at));
    }
}

/// The instant `unix_nanos` says, read on this computer's clock as a far end
/// stamped it: as long before now as the clock says. A drop and an arrival
/// are each timed where they happen, in the far end that did it, so the
/// time between them is not the roll's to delay.
#[must_use]
pub fn instant_of(unix_nanos: i64) -> Instant {
    let ago = observe::now_unix_nanos().saturating_sub(unix_nanos);
    let ago = Duration::from_nanos(u64::try_from(ago).unwrap_or_default());
    Instant::now().checked_sub(ago).unwrap_or_else(Instant::now)
}

/// What one far end was started to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Does {
    /// Drops what it is handed into Receive Locations.
    Sends,
    /// Takes what the Send Ports `ports`, each by its name and folder,
    /// deliver, into `into`.
    Receives {
        into: PathBuf,
        ports: Vec<(String, PathBuf)>,
    },
}

impl Does {
    /// What it does, in a word, as it declares and audits it.
    #[must_use]
    pub const fn word(&self) -> &'static str {
        match self {
            Self::Sends => "sends",
            Self::Receives { .. } => "receives",
        }
    }

    /// What its count of files says, as its stop record says it.
    #[must_use]
    pub const fn counted(&self) -> &'static str {
        match self {
            Self::Sends => "sent",
            Self::Receives { .. } => "received",
        }
    }
}

/// A far end's command line: its name and what it does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Program {
    pub name: String,
    pub does: Does,
}

impl Program {
    /// The command line `arguments`, the program's own name left out:
    /// `--name <name> --sender`, or `--name <name> --receiver --into
    /// <folder>` and `--port <Send Port> <folder>` for each Send Port.
    ///
    /// # Errors
    ///
    /// What is missing or not understood, in words.
    pub fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut arguments = arguments.into_iter();
        let (mut name, mut sends, mut receives, mut into) = (None, false, false, None);
        let mut ports = Vec::new();
        while let Some(argument) = arguments.next() {
            let mut value = |what: &str| {
                arguments
                    .next()
                    .ok_or_else(|| format!("{argument} takes {what}"))
            };
            match argument.as_str() {
                "--name" => name = Some(value("a name")?),
                "--sender" => sends = true,
                "--receiver" => receives = true,
                "--into" => into = Some(PathBuf::from(value("a folder")?)),
                "--port" => {
                    let port = value("a Send Port's name and its folder")?;
                    let folder = value("a Send Port's name and its folder")?;
                    ports.push((port, PathBuf::from(folder)));
                }
                other => return Err(format!("{other} is not understood")),
            }
        }
        let name = name.ok_or("--name is missing")?;
        let does = match (sends, receives, into) {
            (true, false, None) if ports.is_empty() => Does::Sends,
            (false, true, Some(into)) => Does::Receives { into, ports },
            _ => {
                return Err(
                    "a far end is --sender alone, or --receiver with --into and its --port"
                        .to_string(),
                );
            }
        };
        Ok(Self { name, does })
    }
}

/// What a far end says once it can work, the first thing it says.
pub const READY: &str = "ready";

/// A sender: [`READY`] said, then each [`Order`] read from `input` dropped
/// and answered on `output`, until `input` ends; how many it dropped.
pub fn sending(input: impl BufRead, output: &mut impl Write) -> u64 {
    let mut dropped = 0;
    if writeln!(output, "{READY}")
        .and_then(|()| output.flush())
        .is_err()
    {
        return dropped;
    }
    for line in input.lines().map_while(Result::ok) {
        let Some(order) = Order::read(&line) else {
            continue;
        };
        let put = std::fs::read(&order.source)
            .and_then(|bytes| spot::put(&order.folder, &order.name, &bytes));
        let answer = match put {
            Ok(()) => {
                dropped += 1;
                Answer::Dropped(order.name, observe::now_unix_nanos())
            }
            Err(error) => Answer::Failed(order.name, error.to_string()),
        };
        if writeln!(output, "{}", answer.line())
            .and_then(|()| output.flush())
            .is_err()
        {
            break;
        }
    }
    dropped
}

/// A receiver: the folder of each of `ports` watched, then what each holds
/// already taken and [`READY`] said, and each whole file that appears there
/// moved into `into` and said as an [`Arrival`] on `output`, until `input`
/// ends; how many it took.
///
/// # Errors
///
/// Where a folder cannot be made or watched, in words.
pub fn receiving(
    into: &Path,
    ports: &[(String, PathBuf)],
    input: impl BufRead,
    output: impl Write + Send + 'static,
) -> Result<u64, String> {
    std::fs::create_dir_all(into).map_err(|error| format!("{}: {error}", into.display()))?;
    let output = Arc::new(Mutex::new(output));
    let taken = Arc::new(AtomicU64::new(0));
    let mut watching = Vec::new();
    for (port, folder) in ports {
        std::fs::create_dir_all(folder)
            .map_err(|error| format!("{}: {error}", folder.display()))?;
        let taking = Arc::new(Taking {
            port: port.clone(),
            folder: folder.clone(),
            into: into.to_path_buf(),
            output: Arc::clone(&output),
            taken: Arc::clone(&taken),
        });
        // Watched first, then looked in once: a file that arrives between
        // the two is taken either way.
        let watched = Arc::clone(&taking);
        let watch = DirectoryWatch::spawn(folder, move || watched.took())
            .map_err(|error| format!("{}: {error}", folder.display()))?;
        watching.push(watch);
        taking.took();
    }
    {
        let mut said = output.lock().unwrap_or_else(PoisonError::into_inner);
        writeln!(said, "{READY}")
            .and_then(|()| said.flush())
            .map_err(|error| format!("could not say it is ready: {error}"))?;
    }
    for _ in input.lines().map_while(Result::ok) {}
    drop(watching);
    Ok(taken.load(Ordering::Relaxed))
}

/// One Send Port's folder a receiver takes from.
struct Taking<W> {
    port: String,
    folder: PathBuf,
    into: PathBuf,
    output: Arc<Mutex<W>>,
    taken: Arc<AtomicU64>,
}

impl<W: Write> Taking<W> {
    /// Every whole file in the folder moved into the receiver's own and
    /// said; one that does not move yet is taken at the folder's next word.
    fn took(&self) {
        for name in spot::whole(&self.folder) {
            if std::fs::rename(self.folder.join(&name), self.into.join(&name)).is_err() {
                continue;
            }
            self.taken.fetch_add(1, Ordering::Relaxed);
            let arrival = Arrival {
                port: self.port.clone(),
                name,
                at_unix_nanos: observe::now_unix_nanos(),
            };
            let mut output = self.output.lock().unwrap_or_else(PoisonError::into_inner);
            let _ = writeln!(output, "{}", arrival.line()).and_then(|()| output.flush());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::support::scratch;

    #[test]
    fn each_line_reads_back_as_written_and_a_stranger_reads_as_nothing() {
        let order = Order {
            source: PathBuf::from("documents/a/orders-1.edi"),
            folder: PathBuf::from("drop/sales"),
            name: "orders-1-7.edi".to_string(),
        };
        assert_eq!(Order::read(&order.line().expect("a line")), Some(order));
        for answer in [
            Answer::Dropped("x.edi".to_string(), 42),
            Answer::Failed("x.edi".to_string(), "no room".to_string()),
        ] {
            assert_eq!(Answer::read(&answer.line()), Some(answer));
        }
        let arrival = Arrival {
            port: "Sales orders to ERP".to_string(),
            name: "x.edi".to_string(),
            at_unix_nanos: 42,
        };
        assert_eq!(Arrival::read(&arrival.line()), Some(arrival));
        assert_eq!(Arrival::read("dropped\tx.edi"), None);
        assert_eq!(Answer::read("arrived\ta\tb\t1"), None);
        let tabbed = Order {
            name: "a\tb".to_string(),
            ..Order::read("s\tf\tn").expect("an order")
        };
        assert!(tabbed.line().is_err(), "no part carries a tab");
    }

    #[test]
    fn a_command_line_says_a_sender_or_a_receiver_and_nothing_else() {
        let said = |line: &str| Program::parse(line.split(' ').map(str::to_string));
        assert_eq!(
            said("--name a --sender").expect("a sender").does,
            Does::Sends
        );
        let receiver = said("--name b --receiver --into in --port p out").expect("a receiver");
        assert_eq!(
            receiver.does,
            Does::Receives {
                into: PathBuf::from("in"),
                ports: vec![("p".to_string(), PathBuf::from("out"))],
            }
        );
        for wrong in [
            "--sender",
            "--name a",
            "--name a --sender --receiver",
            "--name a --x",
        ] {
            assert!(said(wrong).is_err(), "{wrong}");
        }
    }

    /// A sender drops what it is handed whole, a receiver takes it where it
    /// arrives and says so, each told by its folder, and each ends with its
    /// input: the two far ends over two folders, in this process.
    #[test]
    fn a_sender_drops_whole_and_a_receiver_takes_what_arrives_and_says_so() {
        let base = scratch("external");
        let (dropping, out, into) = (base.join("drop"), base.join("out"), base.join("taken"));
        let source = base.join("orders.edi");
        std::fs::write(&source, b"UNA:+.? 'UNB+UNOC:3").expect("a document");
        // The Receive Location's folder is there, as the run makes it.
        std::fs::create_dir_all(&dropping).expect("the Receive Location's folder");
        let order = Order {
            source,
            folder: dropping.clone(),
            name: "orders-0.edi".to_string(),
        };
        let mut answered = Vec::new();
        let dropped = sending(
            format!("{}\n", order.line().expect("a line")).as_bytes(),
            &mut answered,
        );
        assert_eq!(dropped, 1);
        let answered = String::from_utf8(answered).expect("text");
        let mut lines = answered.lines();
        assert_eq!(lines.next(), Some(READY), "ready first");
        let before = observe::now_unix_nanos();
        assert!(
            matches!(
                lines.next().and_then(Answer::read),
                Some(Answer::Dropped(name, at)) if name == order.name && at <= before
            ),
            "dropped, saying when"
        );
        let bytes = std::fs::read(dropping.join(&order.name)).expect("dropped whole");

        let said = Arc::new(Mutex::new(Vec::new()));
        let (ending, ended) = std::sync::mpsc::channel::<()>();
        let ports = vec![("Sales orders to ERP".to_string(), out.clone())];
        let (into_too, said_too) = (into.clone(), Arc::clone(&said));
        let receiver = std::thread::spawn(move || {
            let input = std::io::BufReader::new(Stopping(ended));
            receiving(&into_too, &ports, input, Shared(said_too)).expect("receiving")
        });
        let said_now = || String::from_utf8(said.lock().expect("said").clone()).expect("text");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !said_now().starts_with(READY) {
            assert!(Instant::now() < deadline, "never ready");
            std::thread::sleep(Duration::from_millis(5));
        }
        std::fs::write(out.join(&order.name), &bytes).expect("delivered");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !into.join(&order.name).is_file() {
            assert!(Instant::now() < deadline, "never taken");
            std::thread::sleep(Duration::from_millis(5));
        }
        drop(ending);
        assert_eq!(receiver.join().expect("ended"), 1);
        let said = said_now();
        let arrival = said.lines().find_map(Arrival::read).expect("an arrival");
        assert_eq!(
            (arrival.port.as_str(), arrival.name.as_str()),
            ("Sales orders to ERP", "orders-0.edi")
        );
        assert_eq!(std::fs::read(into.join(&order.name)).expect("taken"), bytes);
        assert!(
            !out.join(&order.name).exists(),
            "taken off the Send Port's folder"
        );
    }

    /// An input that ends once its sender is dropped.
    struct Stopping(std::sync::mpsc::Receiver<()>);

    impl std::io::Read for Stopping {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            let _ = self.0.recv();
            Ok(0)
        }
    }

    /// An output the test reads after.
    struct Shared(Arc<Mutex<Vec<u8>>>);

    impl Write for Shared {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("said").extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}

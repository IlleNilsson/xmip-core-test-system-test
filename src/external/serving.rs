//! A far end on a machine of its own (the owner's node plan, 2026-11-22: the
//! far ends on six machines of their own in Hyper-V): the service
//! `xmip-system-test-external --serve` runs as there, waiting for the roll.
//! The roll reaches it over Xmip's mutual TLS on [`CONTROL_PORT`], the far
//! end presenting the certificate the cluster's authority issued it and
//! asking the roll for one of the same authority, and tells it what it is
//! for a session; the session ends when the roll ends the connection, and
//! the next roll starts another.
//!
//! - **A sender** says [`program::READY`], then carries out each
//!   [`program::Order`] — on a machine of its own a push over HTTP to the
//!   address a Receive Location listens at on its node, through Xmip's own
//!   send — answering each as on one computer.
//! - **A receiver** is given a user and its password for each Send Port it
//!   takes from, serves them over SFTP on the far end's port with Xmip's own
//!   server (`crate::far_end::serving`, the one the far end on one computer
//!   serves by), says [`program::READY`], and tells each file a Send Port
//!   finished putting as an [`program::Arrival`] with its bytes, taken off
//!   its directory, as a Party takes what it is delivered.
//!
//! The lines are the program's own (`program.rs`), the same on one computer
//! and on a machine; only what carries them differs.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};

use configure::placed::FAR_END_PORT;
use transport_sftp::directory::{Directory, Shared};

use super::program::{self, Answer, Arrival, Order};

/// The port a far end on a machine of its own is reached at by the roll.
pub const CONTROL_PORT: u16 = 7423;

/// What a session begins with: what the far end is for it.
pub const SENDER: &str = "sender";
/// What a session begins with: what the far end is for it.
pub const RECEIVER: &str = "receiver";

/// What ends a receiver's users and has it serve them.
pub const SERVE: &str = "serve";

/// The ports a far end on a machine of its own listens on, one a line, as
/// `xmip-service --listening` says a node's: what its machine's firewall
/// opens (`deploy/dsc`, `Xmip.Node/Opening`).
#[must_use]
pub fn listening() -> Vec<String> {
    vec![
        format!("{FAR_END_PORT}/tcp sftp, the Send Ports a receiver takes from"),
        format!("{CONTROL_PORT}/tcp control, the roll"),
    ]
}

/// One Send Port a receiver takes from: its user, the password it logs in
/// by, and its name, as a line `user\t<user>\t<password>\t<Send Port>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User {
    pub user: String,
    pub password: String,
    pub port: String,
}

impl User {
    /// The user as one line.
    #[must_use]
    pub fn line(&self) -> String {
        format!("user\t{}\t{}\t{}", self.user, self.password, self.port)
    }

    /// The user a line says, if it says one.
    #[must_use]
    pub fn read(line: &str) -> Option<Self> {
        let mut parts = line.split('\t');
        if parts.next()? != "user" {
            return None;
        }
        let (user, password, port) = (parts.next()?, parts.next()?, parts.next()?);
        parts.next().is_none().then(|| Self {
            user: user.to_string(),
            password: password.to_string(),
            port: port.to_string(),
        })
    }
}

/// The far end's own certificate in `folder`, `node.pem`, `node.key` and
/// `anchors.pem` as `New-XmipCertificate -Node` writes them, as a TLS
/// server asking each caller for a certificate of the same authority.
///
/// # Errors
///
/// REFUSED, in words, where a file does not read or they are no
/// certificate.
pub fn identity(folder: &Path) -> Result<Arc<tls::ServerConfig>, String> {
    let file = |leaf: &str| {
        let path = folder.join(leaf);
        std::fs::read(&path).map_err(|error| format!("REFUSED: {}: {error}", path.display()))
    };
    tls::Identity::from_pem(
        &file("node.pem")?,
        &file("node.key")?,
        &file("anchors.pem")?,
    )
    .and_then(|identity| identity.server())
    .map(Arc::new)
    .map_err(|error| format!("REFUSED: the certificate in {}: {error}", folder.display()))
}

/// Serve the roll's sessions at `listener`, each over Xmip's mutual TLS
/// with `config`, a receiver's Send Ports at `sftp`, each told to `ended`
/// with what the far end was for it and how many files it carried. Returns
/// only where the listener fails; the service ends it by ending the
/// process.
pub fn serve(
    listener: &TcpListener,
    config: &Arc<tls::ServerConfig>,
    sftp: &str,
    ended: &(dyn Fn(Result<(String, u64), String>) + Sync),
) {
    // Each session on a thread of its own, so one the roll left open never
    // keeps the next from being answered.
    std::thread::scope(|sessions| {
        for stream in listener.incoming() {
            let Ok(stream) = stream else {
                continue;
            };
            sessions.spawn(move || ended(session(stream, config, sftp)));
        }
    });
}

/// One session: the roll's connection answered, what the far end is for
/// it read, and served until the roll ends it.
fn session(
    stream: std::net::TcpStream,
    config: &Arc<tls::ServerConfig>,
    sftp: &str,
) -> Result<(String, u64), String> {
    let guarded = tls::server(stream, Arc::clone(config)).map_err(|error| error.to_string())?;
    let split = tls::duplex::split(guarded).map_err(|error| error.to_string())?;
    let mut lines = BufReader::new(split.reading).lines().map_while(Result::ok);
    let mut writing = split.writing;
    let what = lines.next().unwrap_or_default();
    match what.as_str() {
        SENDER => Ok((what, sending(lines, &mut writing))),
        RECEIVER => receiving(lines, writing, sftp).map(|taken| (what, taken)),
        other => Err(format!(
            "a session is a {SENDER} or a {RECEIVER}, not '{other}'"
        )),
    }
}

/// A sender's session: ready said, then each order carried out and
/// answered; how many it dropped.
fn sending(lines: impl Iterator<Item = String>, output: &mut impl Write) -> u64 {
    let said = |output: &mut dyn Write, line: &str| {
        writeln!(output, "{line}").and_then(|()| output.flush())
    };
    if said(output, program::READY).is_err() {
        return 0;
    }
    let mut dropped = 0;
    for line in lines {
        let Some(order) = Order::read(&line) else {
            continue;
        };
        let answer = match order.carried() {
            Ok(at) => {
                dropped += 1;
                Answer::Dropped(order.name, at)
            }
            Err(problem) => Answer::Failed(order.name, problem),
        };
        if said(output, &answer.line()).is_err() {
            break;
        }
    }
    dropped
}

/// A receiver's session: its users read until [`SERVE`], served over SFTP,
/// ready said, and each file put told with its bytes until the roll ends
/// the session; how many it took.
fn receiving(
    mut lines: impl Iterator<Item = String>,
    output: impl Write + Send + 'static,
    sftp: &str,
) -> Result<u64, String> {
    let mut users = Vec::new();
    for line in lines.by_ref() {
        if line == SERVE {
            break;
        }
        users.extend(User::read(&line));
    }
    let (tell, puts) = mpsc::channel::<(String, String, i64)>();
    let mut directories: BTreeMap<String, Shared> = BTreeMap::new();
    let mut ports: BTreeMap<String, Shared> = BTreeMap::new();
    let mut enrolled = Vec::new();
    for user in users {
        let (telling, port) = (tell.clone(), user.port.clone());
        let directory = Directory::default().telling(Arc::new(move |name: &str| {
            let at = observe::now_unix_nanos();
            let _ = telling.send((port.clone(), name.to_string(), at));
        }));
        let shared: Shared = Arc::new(Mutex::new(directory));
        directories.insert(user.user.clone(), Arc::clone(&shared));
        ports.insert(user.port, shared);
        enrolled.push((user.user, user.password));
    }
    let serving = crate::far_end::serving(sftp, directories, enrolled)?;
    let output = Arc::new(Mutex::new(output));
    let said = |line: &str| {
        let mut output = output.lock().unwrap_or_else(PoisonError::into_inner);
        writeln!(output, "{line}").and_then(|()| output.flush())
    };
    said(program::READY).map_err(|error| format!("could not say it is ready: {error}"))?;
    // Each put told, taken off its directory and said with its bytes, on a
    // thread of its own: the directory tells while it is held.
    let telling = {
        let output = Arc::clone(&output);
        std::thread::spawn(move || {
            let mut taken = 0_u64;
            // The session's end is told as a put of no Send Port: the
            // directories that tell are this thread's own to the last.
            for (port, name, at_unix_nanos) in puts.iter().take_while(|(port, ..)| !port.is_empty())
            {
                let bytes = ports.get(&port).and_then(|shared| {
                    shared
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .files
                        .remove(&name)
                });
                let Some(bytes) = bytes else {
                    continue;
                };
                taken += 1;
                let arrival = Arrival {
                    port,
                    name,
                    at_unix_nanos,
                    bytes,
                };
                let mut output = output.lock().unwrap_or_else(PoisonError::into_inner);
                let _ = writeln!(output, "{}", arrival.line()).and_then(|()| output.flush());
            }
            taken
        })
    };
    for _ in lines {}
    // The session ended: no Send Port puts here now, and what was told is
    // said before the count is.
    drop(serving);
    let _ = tell.send((String::new(), String::new(), 0));
    Ok(telling.join().unwrap_or_default())
}

/// The far end `name` served on its machine, its certificate in `folder`,
/// each session told to `ended`: under the Windows Service Control Manager
/// through the runtime's one dispatcher, elsewhere until the service
/// manager ends the process. Its exit code.
///
/// # Errors
///
/// REFUSED, in words, where its certificate does not read, its port is not
/// given, or the manager did not start it.
pub fn run(name: &str, folder: &Path, ended: Ended) -> Result<u8, String> {
    let config = identity(folder)?;
    let listener = TcpListener::bind(("0.0.0.0", CONTROL_PORT))
        .map_err(|error| format!("REFUSED: port {CONTROL_PORT}: {error}"))?;
    let sftp = format!("0.0.0.0:{FAR_END_PORT}");
    #[cfg(windows)]
    {
        use runtime::registration::control_manager::{self, Reporter};
        let (stop, stopped) = mpsc::channel::<()>();
        let stopping = Box::new(move |_: &'static str| {
            let _ = stop.send(());
        });
        let serving = Box::new(move |reporter: &Reporter| {
            std::thread::spawn(move || serve(&listener, &config, &sftp, &*ended));
            reporter.running();
            let _ = stopped.recv();
            reporter.stopping();
            0
        });
        let service = runtime::registration::node_service_name(name);
        control_manager::run(&service, stopping, serving)
    }
    // Its unit is the runtime's, `Type=notify`: systemd counts it started
    // once it says it is ready, its port open, and stopped it every 90
    // seconds until 2026-11-25, when it said nothing.
    #[cfg(not(windows))]
    {
        let _ = name;
        let serving = std::thread::spawn(move || serve(&listener, &config, &sftp, &*ended));
        runtime::registration::readiness::ready();
        let _ = serving.join();
        Ok(0)
    }
}

/// What a served far end tells of each session as it ends: what it was for
/// it and how many files it carried, or why it failed.
pub type Ended = Box<dyn Fn(Result<(String, u64), String>) + Send + Sync>;

/// The far end `name` registered as the service that serves, its
/// certificate in `folder`, its audit beside it, and started: through the
/// runtime's one registration of a service, as `xmip-service --install`
/// registers a node's.
///
/// # Errors
///
/// The service manager's refusal, in its words.
pub fn install(name: &str, folder: &Path) -> Result<String, String> {
    use runtime::registration::{ServiceDefinition, apply, node_service_name};
    let executable =
        std::env::current_exe().map_err(|error| format!("this program's own path: {error}"))?;
    let configuration = folder.display().to_string();
    let service = ServiceDefinition {
        name: node_service_name(name),
        display_name: format!("Xmip System Tests far end ({name})"),
        description: format!(
            "The System Tests' far end {name}: serves the roll's sessions on port \
             {CONTROL_PORT}, and a receiver's Send Ports on {FAR_END_PORT}."
        ),
        executable: executable.display().to_string(),
        arguments: ["--name", name, "--serve", "--configuration", &configuration]
            .map(String::from)
            .to_vec(),
        service_identity: None,
        working_directory: configuration.clone(),
        environment: vec![(
            xaudit::audit_store::DIRECTORY_VARIABLE.to_string(),
            folder.join("audit").display().to_string(),
        )],
        local: None,
    };
    apply::register(&service, name, &|| Ok(()))?;
    Ok(format!("{} registered and started", service.name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_user_line_reads_back_as_written_and_a_stranger_as_nothing() {
        let user = User {
            user: "sales-orders-to-erp".to_string(),
            password: "secret".to_string(),
            port: "Sales orders to ERP".to_string(),
        };
        assert_eq!(User::read(&user.line()), Some(user));
        assert_eq!(User::read("serve"), None);
        assert_eq!(User::read("user\ta\tb"), None);
    }

    #[test]
    fn it_says_both_ports_its_machine_opens() {
        let said = listening();
        assert!(said[0].starts_with("7422/tcp"), "{said:?}");
        assert!(said[1].starts_with("7423/tcp"), "{said:?}");
    }
}

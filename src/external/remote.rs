//! The far ends on machines of their own, as the roll reaches them: each
//! the service `xmip-system-test-external --serve` runs as on its machine
//! (`serving.rs`), found by its own name, as a node is found by its own,
//! and told over Xmip's mutual TLS with the tester's operator
//! certificate (`crate::real::operator`).
//!
//! The real nodes' Locations are bound as their environment bound them
//! (`configure::placed::far_ends::bound`, which `xmip-service --slice
//! --bind --listen` slices by): each Receive Location listens on its node,
//! and each round the sender whose turn it is pushes to it, so any sender
//! reaches any node that receives; each Send Port puts on the receiver of
//! its turn, a user of its own there whose password the roll mints and sets
//! on the node that takes it, as the far end on one computer has it set
//! (`crate::far_end`). A receiver tells what it took with its bytes, kept in
//! a folder of the run's, where Carried judges it.

use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use configure::placed::far_ends::{self, far_end_of};
use configure::placed::{self, Placed};

use super::serving::{CONTROL_PORT, RECEIVER, SENDER, SERVE, User};
use super::{Externals, FILE, Running, Told, named};
use crate::carried::{Arrived, Binding, Spot};
use crate::login;
use crate::real::RealNode;
use crate::subscribing::CLUSTER;

/// How long a far end may take to answer a connection.
const REACH: Duration = Duration::from_secs(5);

/// Whether the far ends the shipped file names run on machines of their
/// own: every one's name resolving; none resolving, they do not, and the
/// Parties' far end is served on this computer as before.
///
/// # Errors
///
/// REFUSED, naming them, where some resolve and some do not: the roll
/// would place differently from the environment.
pub fn on_machines() -> Result<bool, String> {
    let named = named(FILE)?;
    let all: Vec<&String> = named.senders.iter().chain(&named.receivers).collect();
    let resolving: Vec<&&String> = all
        .iter()
        .filter(|name| {
            (name.as_str(), CONTROL_PORT)
                .to_socket_addrs()
                .is_ok_and(|mut found| found.next().is_some())
        })
        .collect();
    match resolving.len() {
        0 => Ok(false),
        count if count == all.len() => Ok(true),
        _ => Err(format!(
            "REFUSED: of the far ends, only {} resolve by their names; every one runs on a \
             machine of its own, or none does",
            resolving
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

impl Externals {
    /// Every far end the shipped file names reached on its machine with the
    /// tester's `identity`, for the run of the real nodes `real` beneath
    /// `area`, each arrival told in `arrived`; with them every Receive
    /// Location and Send Port placed over `real`, a Receive Location at the
    /// address it listens at on its node, a Send Port where its receiver's
    /// takings are kept, which Carried judges.
    ///
    /// # Errors
    ///
    /// REFUSED, in words: a far end not reached or not ready, a node that
    /// did not take a password, or a placement the file refuses.
    pub fn connect(
        identity: &tls::Identity,
        real: &[RealNode],
        area: &Path,
        arrived: &Arrived,
    ) -> Result<(Self, Vec<Binding>), String> {
        let named = named(FILE)?;
        let reals: Vec<String> = real.iter().map(|node| node.name.clone()).collect();
        let placements = placed::placements(CLUSTER, &placed::present(CLUSTER, &reals)?)?;
        let hosts = named.receivers.clone();
        let config = Arc::new(
            identity
                .client()
                .map_err(|error| format!("REFUSED: the tester's certificate: {error}"))?,
        );
        let mut started = Self {
            senders: Vec::new(),
            receivers: Vec::new(),
            delivered_to: Vec::new(),
        };
        for name in &named.senders {
            started
                .senders
                .push(reached(name, &config, &[SENDER.to_string()], None)?);
        }
        let mut users: Vec<Vec<String>> = vec![Vec::new(); named.receivers.len()];
        let (mut bindings, mut send) = (Vec::new(), 0);
        for (placement, binding) in far_ends::bound(&placements, &hosts, true) {
            let node = real
                .iter()
                .find(|node| node.name == placement.node)
                .ok_or_else(|| format!("REFUSED: {} is no real node of the run", placement.node))?;
            let spot = match placement.placed {
                Placed::Receive => {
                    let host = node
                        .address
                        .rsplit_once(':')
                        .map_or(&*node.address, |(h, _)| h);
                    Spot::Listening(binding.address.replace("0.0.0.0", host))
                }
                Placed::Send => {
                    let at = far_end_of(send, named.receivers.len());
                    send += 1;
                    let password = login::minted();
                    node.set_password(&placed::far_end_secret(&placement), &password)?;
                    let user = User {
                        user: placement.folded(),
                        password,
                        port: placement.name.clone(),
                    };
                    users[at].push(user.line());
                    let receiver = named.receivers[at].clone();
                    started
                        .delivered_to
                        .push((placement.name.clone(), receiver.clone()));
                    Spot::Folder(area.join("external").join(receiver))
                }
                Placed::Process => continue,
            };
            bindings.push(Binding { placement, spot });
        }
        for (name, mut told) in named.receivers.iter().zip(users) {
            let into = area.join("external").join(name);
            std::fs::create_dir_all(&into)
                .map_err(|error| format!("REFUSED: {}: {error}", into.display()))?;
            told.insert(0, RECEIVER.to_string());
            told.push(SERVE.to_string());
            let receiving = Told {
                arrived: Some(arrived),
                into: Some(into),
            };
            started
                .receivers
                .push(reached(name, &config, &told, Some(receiving))?);
        }
        Ok((started, bindings))
    }
}

/// The far end `name` reached on its machine over Xmip's mutual TLS with
/// `config`, told `said`, a line each, and waited for until it is ready.
fn reached(
    name: &str,
    config: &Arc<tls::ClientConfig>,
    said: &[String],
    told: Option<Told<'_>>,
) -> Result<Running, String> {
    let address = (name, CONTROL_PORT)
        .to_socket_addrs()
        .ok()
        .and_then(|mut found| found.next())
        .ok_or_else(|| format!("REFUSED: the far end {name}: its name does not resolve"))?;
    told_at((name, name, address), config, said, told)
}

/// The far end `name` at `address`, its certificate naming it `host`,
/// reached over Xmip's mutual TLS with `config`, told `said`, a line each,
/// and waited for until it is ready.
fn told_at(
    (name, host, address): (&str, &str, SocketAddr),
    config: &Arc<tls::ClientConfig>,
    said: &[String],
    told: Option<Told<'_>>,
) -> Result<Running, String> {
    let refused = |what: &str| format!("REFUSED: the far end {name} at {host}: {what}");
    let tcp =
        TcpStream::connect_timeout(&address, REACH).map_err(|error| refused(&error.to_string()))?;
    let guarded = tls::client_with(host, tcp, Arc::clone(config))
        .map_err(|error| refused(&error.to_string()))?;
    let split = tls::duplex::split(guarded).map_err(|error| refused(&error.to_string()))?;
    let mut writing = split.writing;
    for line in said {
        writeln!(writing, "{line}").map_err(|error| refused(&error.to_string()))?;
    }
    writing
        .flush()
        .map_err(|error| refused(&error.to_string()))?;
    let told = told.unwrap_or(Told {
        arrived: None,
        into: None,
    });
    Running::ready((name, None), Some(Box::new(writing)), split.reading, told)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::PoisonError;
    use std::time::Instant;

    use super::super::program::{Answer, Order};
    use super::super::serving;
    use super::*;
    use crate::carried::Carried;
    use crate::support::scratch;

    /// A far end on a machine of its own, here on loopback, reached as the
    /// roll reaches one over Xmip's mutual TLS: a sender's session drops
    /// what it is handed, and a receiver's serves its Send Port over SFTP
    /// and tells what a Send Location put there with its bytes, kept where
    /// Carried judges it.
    #[test]
    fn a_far_end_on_a_machine_of_its_own_sends_and_receives_over_mutual_tls() {
        let base = scratch("external-machine");
        let now = observe::now_unix_nanos() / 1_000_000_000;
        let names = ["localhost".to_string(), "127.0.0.1".to_string()];
        for (who, folder) in [("EXTTX1", "far"), ("roll", "roll")] {
            provision::node::issue_into(
                &base.join("authority"),
                "the far ends' authority",
                who,
                &names,
                &base.join(folder),
                now,
            )
            .expect("issued");
        }
        let config = serving::identity(&base.join("far")).expect("the far end's certificate");
        let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
        let control = listener.local_addr().expect("its address");
        let sftp = TcpListener::bind("127.0.0.1:0")
            .and_then(|free| free.local_addr())
            .expect("a port")
            .to_string();
        let serving_at = sftp.clone();
        std::thread::spawn(move || serving::serve(&listener, &config, &serving_at, &|_| {}));
        let file = |leaf: &str| std::fs::read(base.join("roll").join(leaf)).expect("issued");
        let roll = Arc::new(
            tls::Identity::from_pem(&file("node.pem"), &file("node.key"), &file("anchors.pem"))
                .and_then(|identity| identity.client())
                .expect("the roll's certificate"),
        );
        let at = ("EXTTX1", "localhost", control);

        let dropping = base.join("drop");
        std::fs::create_dir_all(&dropping).expect("the Receive Location's folder");
        let mut sender = told_at(at, &roll, &[SENDER.to_string()], None).expect("ready");
        let order = Order {
            bytes: b"UNB+UNOC:3+A+B+261122:1900+X'".to_vec(),
            to: dropping.display().to_string(),
            name: "orders-0.edi".to_string(),
        };
        let input = sender.input.as_mut().expect("its connection");
        writeln!(input, "{}", order.line().expect("a line")).expect("told");
        input.flush().expect("told");
        let answer = sender
            .said
            .recv_timeout(super::super::ANSWER)
            .expect("answered");
        assert!(
            matches!(Answer::read(&answer), Some(Answer::Dropped(name, _)) if name == order.name),
            "{answer}"
        );
        assert_eq!(
            std::fs::read(dropping.join(&order.name)).expect("dropped"),
            order.bytes
        );

        let into = base.join("taken");
        std::fs::create_dir_all(&into).expect("the receiver's folder");
        let arrived = Carried::arrivals();
        let user = User {
            user: "sales-orders-to-erp".to_string(),
            password: "a password".to_string(),
            port: "Sales orders to ERP".to_string(),
        };
        let said = [RECEIVER.to_string(), user.line(), SERVE.to_string()];
        let receiving = Told {
            arrived: Some(&arrived),
            into: Some(into.clone()),
        };
        let receiver = told_at(
            ("EXTRX1", "localhost", control),
            &roll,
            &said,
            Some(receiving),
        )
        .expect("ready");
        let mut client = transport_sftp::Client::connect(
            &sftp,
            &user.user,
            Some(&transport_sftp::Credential::Password(user.password.clone())),
            (Some(REACH), &transport::Pinned::in_memory()),
        )
        .expect("a Send Location logs in");
        client
            .put("orders-0.edi", &transport::Outgoing::held(&order.bytes))
            .expect("put");
        let deadline = Instant::now() + REACH;
        while !arrived
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key("orders-0.edi")
        {
            assert!(Instant::now() < deadline, "the receiver never told of it");
            std::thread::yield_now();
        }
        assert_eq!(
            std::fs::read(into.join("orders-0.edi")).expect("kept where it is judged"),
            order.bytes
        );
        // Each session ends with its connection.
        drop((sender, receiver));
    }
}

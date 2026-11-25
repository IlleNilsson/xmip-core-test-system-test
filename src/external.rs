//! The far ends outside the cluster: the senders that drop the Parties'
//! documents into the Receive Locations and the receivers that take what the
//! Send Ports deliver, each a process of its own (the owner's node plan,
//! 2026-11-22: *the [System Tests'] far ends, processes of their own*; ADR-0028
//! clause 2 and amendment 2026-11-22). They are named in the System Tests'
//! cluster file, `[environment.far_ends]`, never in code, and each runs the
//! program `xmip-system-test-external`: as local processes from an image of
//! its own, `xmip-system-test-<cluster>-external-<name>` (ADR-0053), told
//! through its input; on a virtual machine of its own as the service it
//! serves as there ([`serving`]), told over Xmip's mutual TLS ([`remote`]).
//!
//! **Who reaches what.** Each round, each Receive Location's document goes
//! through the sender whose turn it is, one further every round
//! ([`sender`]), so every sender reaches every node that receives. Each
//! Send Port delivers to one receiver, round robin in the order the Send
//! Ports are placed (`configure::placed::far_ends::far_end_of`, the rule
//! the nodes on machines are bound by), so each receiver is reached by the
//! nodes that send.
//!
//! **What the roll does with them** ([`Externals`]): it hands a sender each
//! file to drop and waits until the sender says it did; each receiver tells
//! it what arrived at which Send Port and when, the file moved into the
//! receiver's own folder, where Carried judges it as before
//! (`crate::carried`). Each ends when its input does, as the run ends or
//! the roll with it, and audits what it carried ([`program`]).

pub mod command;
pub mod program;
pub mod remote;
mod running;
pub mod serving;

use std::io::Write;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use configure::placed::Placed;
use configure::placed::far_ends::far_end_of;

use crate::carried::{Arrived, Binding, Spot};
use crate::subscribing::CLUSTER;
use program::{Answer, Order};
use running::{Running, Told};

/// How long a sender may take to say it dropped a file: a bound on the
/// test, never on Xmip. A far end's readiness has no bound of time: it is
/// waited for until it says so or ends (`running.rs`).
const ANSWER: Duration = Duration::from_secs(5);

/// How long a far end may take to end once its input does.
const STOPPING: Duration = Duration::from_secs(5);

/// Where a cluster file names its far ends.
const FAR_ENDS: &str = "cluster.xmip's [environment.far_ends]";

/// The far ends a cluster file names, each by its name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Named {
    pub senders: Vec<String>,
    pub receivers: Vec<String>,
}

/// The far ends the cluster file `text` names in `[environment.far_ends]`:
/// its `senders` and its `receivers`. The System Tests' is
/// `crate::subscribing::CLUSTER`.
///
/// # Errors
///
/// REFUSED, in words, where the file does not read, names no sender or no
/// receiver, or names one no process can be called or one twice.
pub fn named(text: &str) -> Result<Named, String> {
    let file: toml::Table = text
        .parse()
        .map_err(|error| format!("REFUSED: {FAR_ENDS}: {error}"))?;
    let table = file
        .get("environment")
        .and_then(|environment| environment.get("far_ends"));
    let list = |key: &str| -> Result<Vec<String>, String> {
        let names: Vec<String> = table
            .and_then(|table| table.get(key))
            .and_then(toml::Value::as_array)
            .map_or_default(|names| {
                names
                    .iter()
                    .filter_map(|name| name.as_str().map(str::to_string))
                    .collect()
            });
        if names.is_empty() {
            return Err(format!("REFUSED: {FAR_ENDS} names no {key}"));
        }
        Ok(names)
    };
    let named = Named {
        senders: list("senders")?,
        receivers: list("receivers")?,
    };
    let mut seen: Vec<&String> = Vec::new();
    for name in named.senders.iter().chain(&named.receivers) {
        if !crate::image::shaped(name) {
            return Err(format!(
                "REFUSED: {FAR_ENDS} names {name}: a far end's name is letters, digits and \
                 hyphens starting with a letter, since its process is named for it"
            ));
        }
        if seen.contains(&name) {
            return Err(format!("REFUSED: {FAR_ENDS} names {name} twice"));
        }
        seen.push(name);
    }
    Ok(named)
}

/// Which of `senders` drops into the Receive Location placed `location`-th
/// in round `round`: one further each round, so over as many rounds as
/// there are senders each has dropped into every Location.
#[must_use]
pub fn sender(location: usize, round: u64, senders: usize) -> usize {
    let Some(count) = u64::try_from(senders).ok().filter(|count| *count > 0) else {
        return 0;
    };
    let turn = usize::try_from(round % count).unwrap_or_default();
    (location + turn) % senders
}

/// The far ends of a run, started.
pub struct Externals {
    senders: Vec<Running>,
    receivers: Vec<Running>,
    /// Each Send Port, by name, and the receiver it delivers to.
    delivered_to: Vec<(String, String)>,
}

impl Externals {
    /// The far ends the System Tests' cluster file names, each started as
    /// `program` for the run of `cluster` over `bindings`: every sender, and
    /// every receiver watching the Send Ports' folders it is given, taking
    /// into a folder of its own beneath `area` and telling `arrived`. With them,
    /// each Send Port's binding as Carried judges it: at its receiver's
    /// folder.
    ///
    /// # Errors
    ///
    /// REFUSED, in words, where the file does not read, a folder cannot be
    /// made, or a far end does not start; those already started are ended.
    pub fn start(
        program: &Path,
        (cluster, area): (&str, &Path),
        bindings: &[Binding],
        arrived: &Arrived,
    ) -> Result<(Self, Vec<Binding>), String> {
        let named = named(CLUSTER)?;
        let mut started = Self {
            senders: Vec::new(),
            receivers: Vec::new(),
            delivered_to: Vec::new(),
        };
        for name in &named.senders {
            let sending = ["--sender".to_string()];
            let running = Running::start(program, (cluster, name, area), &sending, None)?;
            started.senders.push(running);
        }
        let ports: Vec<&Binding> = bindings
            .iter()
            .filter(|one| one.placement.placed == Placed::Send)
            .collect();
        let mut judged = Vec::new();
        for (at, name) in named.receivers.iter().enumerate() {
            let into = area.join("external").join(name);
            std::fs::create_dir_all(&into)
                .map_err(|error| format!("REFUSED: {}: {error}", into.display()))?;
            let mut arguments = vec![
                "--receiver".to_string(),
                "--into".to_string(),
                into.display().to_string(),
            ];
            for (place, port) in ports.iter().enumerate() {
                let Spot::Folder(folder) = &port.spot else {
                    continue;
                };
                if far_end_of(place, named.receivers.len()) != at {
                    continue;
                }
                let port_name = port.placement.name.clone();
                arguments.extend([
                    "--port".to_string(),
                    port_name.clone(),
                    folder.display().to_string(),
                ]);
                started.delivered_to.push((port_name, name.clone()));
                judged.push(Binding {
                    placement: port.placement.clone(),
                    spot: Spot::Folder(into.clone()),
                });
            }
            let running =
                Running::start(program, (cluster, name, area), &arguments, Some(arrived))?;
            started.receivers.push(running);
        }
        Ok((started, judged))
    }

    /// `bytes` dropped as `name` where the Receive Location takes them,
    /// `to` — its folder, or the address it listens at on its node — by the
    /// sender whose turn it is for the Location placed `location`-th in
    /// round `round`, waited for until it says so: that sender's name, and
    /// the instant it dropped them.
    ///
    /// # Errors
    ///
    /// What went wrong, in words: the sender said it failed, said nothing in
    /// time, or has ended.
    pub fn dropped(
        &mut self,
        (location, round): (usize, u64),
        bytes: &[u8],
        (to, name): (&str, &str),
    ) -> Result<(&str, Instant), String> {
        let turn = sender(location, round, self.senders.len());
        let sender = self.senders.get_mut(turn).ok_or("no sender is running")?;
        let order = Order {
            bytes: bytes.to_vec(),
            to: to.to_string(),
            name: name.to_string(),
        }
        .line()?;
        let input = sender
            .input
            .as_mut()
            .ok_or_else(|| format!("{} has ended", sender.name))?;
        writeln!(input, "{order}")
            .and_then(|()| input.flush())
            .map_err(|error| format!("{} was not told of {name}: {error}", sender.name))?;
        let deadline = Instant::now() + ANSWER;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = sender
                .said
                .recv_timeout(left)
                .map_err(|_| format!("{} said nothing of {name} within {ANSWER:?}", sender.name))?;
            match Answer::read(&line) {
                Some(Answer::Dropped(of, at)) if of == name => {
                    sender.carried.fetch_add(1, Ordering::Relaxed);
                    return Ok((&sender.name, program::instant_of(at)));
                }
                Some(Answer::Failed(of, problem)) if of == name => {
                    return Err(format!("{} could not drop it: {problem}", sender.name));
                }
                _ => {}
            }
        }
    }

    /// The receiver the Send Port `port` delivers to.
    #[must_use]
    pub fn receiver_of(&self, port: &str) -> Option<&str> {
        self.delivered_to
            .iter()
            .find(|(of, _)| of == port)
            .map(|(_, receiver)| receiver.as_str())
    }

    /// What each far end carried so far, in words: `<sender> sent <n>`,
    /// then `<receiver> received <n>`.
    #[must_use]
    pub fn words(&self) -> String {
        let said = |group: &[Running], verb: &str| {
            group
                .iter()
                .map(|one| {
                    let carried = one.carried.load(Ordering::Relaxed);
                    format!("{} {verb} {carried}", one.name)
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "{}; {}",
            said(&self.senders, "sent"),
            said(&self.receivers, "received")
        )
    }
}

impl Drop for Externals {
    /// Every far end ended by the end of its input, waited for, and killed
    /// where it did not end; one on a machine of its own ends its session
    /// with the connection.
    fn drop(&mut self) {
        for one in self.senders.iter_mut().chain(self.receivers.iter_mut()) {
            drop(one.input.take());
        }
        for one in self.senders.iter_mut().chain(self.receivers.iter_mut()) {
            if let Some(child) = one.child.as_mut()
                && !crate::served::exited(child, STOPPING)
            {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The far ends are the System Tests' cluster file's, in its
    /// `[environment.far_ends]`: no file of their own.
    #[test]
    fn the_cluster_file_names_three_senders_and_three_receivers() {
        let named = named(CLUSTER).expect("the cluster file's far ends");
        assert_eq!((named.senders.len(), named.receivers.len()), (3, 3));
    }

    #[test]
    fn a_file_naming_none_twice_or_what_no_process_can_be_called_is_refused() {
        let file = |senders: &str, receivers: &str| {
            format!("[environment.far_ends]\nsenders = [{senders}]\nreceivers = [{receivers}]\n")
        };
        let refused = named(&file("", "\"a\"")).expect_err("no sender");
        assert!(
            refused.contains("cluster.xmip's [environment.far_ends] names no senders"),
            "{refused}"
        );
        let twice = named(&file("\"a\"", "\"a\"")).expect_err("twice");
        assert!(twice.contains("names a twice"), "{twice}");
        let shapeless = named(&file("\"a b\"", "\"c\"")).expect_err("shape");
        assert!(
            shapeless.contains("letters, digits and hyphens"),
            "{shapeless}"
        );
        let none = named("[nodes]\n").expect_err("a file naming no far end");
        assert!(none.contains("names no senders"), "{none}");
    }

    /// Every sender reaches every Receive Location, one further each round,
    /// and each Location is dropped into by one sender a round.
    #[test]
    fn over_as_many_rounds_as_senders_each_sender_drops_into_every_location() {
        for (locations, senders) in [(8, 3), (2, 3), (4, 4), (1, 1)] {
            for location in 0..locations {
                let mut reached: Vec<usize> = (0..senders)
                    .map(|round| sender(location, u64::try_from(round).expect("small"), senders))
                    .collect();
                reached.sort_unstable();
                assert_eq!(reached, (0..senders).collect::<Vec<_>>(), "{location}");
            }
        }
        assert_eq!(sender(3, 7, 0), 0, "no sender is no turn");
    }
}

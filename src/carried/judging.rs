//! Judging what arrived: each file the Send Ports delivered since the last
//! judgment, paired with its drop by the stamp it carries, judged once and
//! let go of, and how long the rest have waited. Split from `carried.rs` on
//! 2026-11-22, when the far ends outside the cluster came to carry the
//! files (ADR-0028, amendment 2026-11-22).

use std::sync::PoisonError;
use std::time::{Duration, Instant};

use observe::{Item, ItemKind};

use super::spot::{placed_name, taken};
use super::{Came, Carried, LOST, stamp};

impl Carried {
    /// Wait, as the run ends, for every file dropped that a Subscription
    /// takes to arrive, for at most [`LOST`]: what is not carried by then
    /// is lost. Asked each tenth of a second, the carrying being what it
    /// waits out.
    pub fn settle(&mut self) {
        let deadline = Instant::now() + LOST;
        while Instant::now() < deadline {
            self.resolve();
            let mut routed = self
                .dropped
                .iter()
                .filter(|(_, dropped)| dropped.port.is_some());
            if routed.all(|(stamp, _)| self.came.contains_key(stamp)) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// What the spots told of since this was last asked, each file taken
    /// off its Send Port's spot, as its Party takes what it is delivered,
    /// and kept by the stamp its bytes carry where this run dropped one so
    /// stamped: a file is paired with its drop by what it says, whatever it
    /// is called where it arrived. One that carries no stamp of this run's
    /// is let go of as it is seen.
    pub(super) fn resolve(&mut self) {
        let told: Vec<(String, (String, Instant))> =
            std::mem::take(&mut *self.arrived.lock().unwrap_or_else(PoisonError::into_inner))
                .into_iter()
                .collect();
        for (name, (port, seen)) in told {
            let Some(placed) = self.ports.get(&port) else {
                continue;
            };
            let Some(bytes) = taken(&placed.spot, &name) else {
                continue;
            };
            if let Some(stamp) = stamp::read(&bytes).filter(|it| self.dropped.contains_key(it)) {
                self.came.entry(stamp).or_insert(Came { port, seen, bytes });
            }
        }
    }

    /// Every file that arrived since the last judgment, judged once and
    /// let go of: whole where its route sends it, elsewhere, or with other
    /// bytes; each arrival said as a recent item. What has not arrived is
    /// how long it has waited. Nothing it keeps grows with the run's age:
    /// an arrival is let go of once judged, one of no file this run dropped
    /// as it is seen, and a document no Subscription takes once it is older
    /// than [`LOST`].
    pub(super) fn judge(&mut self, root: &str, now: i64) {
        self.resolve();
        let came: Vec<(String, Came)> = std::mem::take(&mut self.came).into_iter().collect();
        let mut judged = Vec::new();
        for (stamp, Came { port, seen, bytes }) in &came {
            let (Some(dropped), Some(placed)) = (self.dropped.get(stamp), self.ports.get(port))
            else {
                continue;
            };
            let name = &dropped.name;
            let whole = *bytes == dropped.bytes;
            let took = seen.saturating_duration_since(dropped.at);
            let routed = (dropped.port.as_deref(), port.as_str());
            self.judged
                .arrived(name, dropped.location, routed, (whole, took));
            let said = format!(
                "from {} → to {}",
                placed_name(&self.drops[dropped.location].binding.placement),
                placed_name(&placed.placement)
            );
            let taker = self.far.as_ref().and_then(|far| far.receiver_of(port));
            self.activity.record(Item {
                kind: ItemKind::Message,
                scope: format!(
                    "{root}/node/{}/send/{}",
                    placed.placement.node, placed.placement.name
                ),
                id: name.clone(),
                bytes: dropped.bytes.len() as u64,
                detail: taker.map_or_else(
                    || said.clone(),
                    |receiver| format!("{said}, received by {receiver}"),
                ),
                observed_unix_nanos: now,
            });
            judged.push(stamp.clone());
        }
        for stamp in judged {
            self.dropped.remove(&stamp);
        }
        let at = Instant::now();
        self.dropped
            .retain(|_, dropped| dropped.port.is_some() || at.duration_since(dropped.at) < LOST);
        let waiting = self
            .dropped
            .values()
            .filter(|dropped| dropped.port.is_some())
            .map(|dropped| {
                (
                    dropped.name.as_str(),
                    dropped.location,
                    at.duration_since(dropped.at),
                )
            });
        self.judged.waiting(waiting);
    }
}

//! Judging what arrived: each file the Send Ports delivered since the last
//! judgment, judged once and let go of, and how long the rest have waited.
//! Split from `carried.rs` on 2026-11-22, when the far ends outside the
//! cluster came to carry the files (ADR-0028, amendment 2026-11-22).

use std::sync::PoisonError;
use std::time::Instant;

use observe::{Item, ItemKind};

use super::spot::{placed_name, taken};
use super::{Carried, LOST};

impl Carried {
    /// Every file that arrived since the last judgment, judged once and
    /// let go of: whole where its route sends it, elsewhere, or with other
    /// bytes; each arrival said as a recent item, and taken off the Send
    /// Port's spot, as its Party takes what it is delivered. What has not
    /// arrived is how long it has waited. Nothing it keeps grows with the
    /// run's age: an arrival is let go of once judged, one of no file this
    /// run dropped as it is seen, and a document no Subscription takes once
    /// it is older than [`LOST`].
    pub(super) fn judge(&mut self, root: &str, now: i64) {
        let came: Vec<(String, (String, Instant))> = {
            let mut arrived = self.arrived.lock().unwrap_or_else(PoisonError::into_inner);
            let came = self
                .dropped
                .keys()
                .filter_map(|name| arrived.remove(name).map(|seen| (name.clone(), seen)))
                .collect();
            arrived.retain(|name, _| self.dropped.contains_key(name));
            came
        };
        let mut judged = Vec::new();
        for (name, (port, seen)) in &came {
            let (Some(dropped), Some(placed)) = (self.dropped.get(name), self.ports.get(port))
            else {
                continue;
            };
            let whole = taken(&placed.spot, name).is_some_and(|bytes| bytes == dropped.bytes);
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
            judged.push(name.clone());
        }
        for name in judged {
            self.dropped.remove(&name);
        }
        let at = Instant::now();
        self.dropped
            .retain(|_, dropped| dropped.port.is_some() || at.duration_since(dropped.at) < LOST);
        let waiting = self
            .dropped
            .iter()
            .filter(|(_, dropped)| dropped.port.is_some())
            .map(|(name, dropped)| {
                (
                    name.as_str(),
                    dropped.location,
                    at.duration_since(dropped.at),
                )
            });
        self.judged.waiting(waiting);
    }
}

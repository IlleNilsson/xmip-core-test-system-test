//! A document dropped into a Receive Location: stamped (`stamp.rs`), handed
//! to the sender whose turn it is where far ends carry the files — into a
//! folder on this computer, or pushed to a Location listening on its node —
//! or dropped by the rig itself, then recorded with the Send Port its route
//! names and said as a recent item. Split from `carried.rs` on 2026-11-22.

use std::time::Instant;

use observe::{Item, ItemKind};

use super::spot::{self, located, placed_name};
use super::{Carried, Dropped, Spot, stamp};
use crate::documents::Document;

impl Carried {
    /// `document` dropped into the Location at `location`, recorded with
    /// the Send Port its route names, and said as a recent item.
    pub(super) fn dropping(
        &mut self,
        root: &str,
        location: usize,
        document: &Document,
        now: i64,
    ) -> Result<(), String> {
        let name = format!("{}-{}.edi", document.stem, self.next);
        let stamp = stamp::of(&self.run, self.next);
        self.next += 1;
        let binding = &self.drops[location].binding;
        let into = |error: String| format!("{name} into {}: {error}", binding.placement.name);
        let bytes = stamp::stamped(&document.bytes, &stamp).map_err(into)?;
        // Who dropped it and when: a far end says both, the moment it
        // renamed the file in; the rig's own drop is now.
        let mut by: Option<(String, Instant)> = None;
        let written = match (&binding.spot, self.far.as_mut()) {
            (Spot::Folder(folder), Some(far)) => far
                .dropped(
                    (location, self.rounds),
                    &bytes,
                    (&folder.display().to_string(), &name),
                )
                .map(|(sender, at)| by = Some((sender.to_string(), at))),
            (Spot::Listening(address), Some(far)) => far
                .dropped((location, self.rounds), &bytes, (address, &name))
                .map(|(sender, at)| by = Some((sender.to_string(), at))),
            (Spot::Listening(address), None) => Err(format!(
                "{address} listens for the far ends' push, and none is running"
            )),
            (Spot::Folder(folder), None) => {
                spot::put(folder, &name, &bytes).map_err(|error| error.to_string())
            }
        };
        written.map_err(into)?;
        let port = self.documents.routed(&document.bytes).into_iter().next();
        let to = port.as_ref().map_or_else(
            || "no Subscription".to_string(),
            |port| self.at(port).map_or_else(|| port.clone(), placed_name),
        );
        let from = placed_name(&binding.placement);
        self.activity.record(Item {
            kind: ItemKind::Stream,
            scope: located(root, &binding.placement),
            id: name.clone(),
            bytes: bytes.len() as u64,
            detail: by.as_ref().map_or_else(
                || format!("from {from} → to {to}"),
                |(sender, _)| format!("from {from} → to {to}, sent by {sender}"),
            ),
            observed_unix_nanos: now,
        });
        self.dropped.insert(
            stamp,
            Dropped {
                name,
                location,
                at: by.map_or_else(Instant::now, |(_, at)| at),
                port,
                bytes,
            },
        );
        Ok(())
    }
}

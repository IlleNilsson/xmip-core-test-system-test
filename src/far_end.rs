//! What a receiving far end on a virtual machine of its own serves its Send
//! Ports by (`crate::external::serving`; ADR-0028, amendments 2026-10-22 and
//! 2026-11-25). The far end is Xmip (clause 5): SFTP with Xmip's own server
//! (`transport_sftp::serving_each`), each Send Port the nodes put on a user
//! of its own, whose directory it is.
//!
//! Each user logs in by the password its Location's secret holds
//! (`configure::placed::far_end_secret`). The roll mints it, sets it on the
//! node that takes the Location through that node's operate listener — the
//! node's secret store, the one place the value is kept — and the far end
//! keeps only its salted verifier, checked by the password gate
//! (`crate::login`; open problem 59). It enrolls no key, so a key is
//! refused whatever it signed. No directory is chosen by a name nothing
//! checked.

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use transport::concurrent::Concurrent;
use transport_sftp::directory::Shared;

use crate::login;

/// How long a node that stops mid-exchange is waited on.
const PATIENCE: Duration = Duration::from_secs(10);

/// SFTP served at `address` with Xmip's own server, each user of
/// `enrolled` logging in by its password to its directory of
/// `directories`. It enrolls no key, so a key is refused whatever it
/// signed, and it keeps the passwords' verifiers alone.
///
/// # Errors
///
/// REFUSED, in words, where the port is not given.
pub fn serving(
    address: &str,
    directories: BTreeMap<String, Shared>,
    enrolled: Vec<(String, String)>,
) -> Result<Concurrent, String> {
    let listener = TcpListener::bind(address)
        .map_err(|error| format!("REFUSED: the far end cannot be served at {address}: {error}"))?;
    let host = SigningKey::from_bytes(&codec::random::array::<32>());
    let chosen = move |user: &str| {
        directories
            .get(user)
            .map_or_else(Shared::default, Arc::clone)
    };
    let check = login::checking(
        enrolled
            .iter()
            .map(|(user, password)| (user.as_str(), password.as_str())),
        Vec::new(),
    );
    // The values go with the list: the far end keeps the verifiers alone.
    drop(enrolled);
    transport_sftp::serving_each(listener, host, chosen, (Some(PATIENCE), check))
        .map_err(|error| format!("REFUSED: the far end at {address}: {error}"))
}

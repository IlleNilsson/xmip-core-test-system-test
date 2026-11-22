//! The stamp each document dropped carries: its interchange control
//! reference, the fifth element of its `UNB` and the second of its `UNZ`,
//! set for each drop. Carried pairs a drop with its arrival by it, by what
//! the document says rather than by what its file is called where it
//! arrives, so any transport carries it to any node and back: the far ends
//! on machines of their own push to a listening Receive Location, which
//! names no file (the coordinator, 2026-11-22, the far ends' design (a)).
//! No Subscription reads the control reference, and a Work Process that
//! assigns to the Message's context leaves the document's bytes as they
//! were, so a stamped document goes where its unstamped one went.
//!
//! Read and written by Xmip's own `edi` path language
//! (`xmip-core-path-edi`), never cut here.

use path::{Content, Namespaces, PathLanguage, Rewriting};
use path_edi::EdiLanguage;
use stream::Stream;
use xcore::{ScalarValue, StreamId};

/// Where the stamp is written: the interchange header's control reference
/// and the trailer's, which repeats it.
const AT: [&str; 2] = ["UNB/5", "UNZ/2"];

/// How long a run's tag is: four of the fourteen characters a control
/// reference holds (an..14), the drop's number the other ten.
const TAG: usize = 4;

/// The tag of a run started `unix_seconds` after the epoch: four base-36
/// characters, so a file a run before left on its way is never taken for
/// one of this run's.
#[must_use]
pub fn run_tag(unix_seconds: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut left = unix_seconds;
    let mut tag = [b'0'; TAG];
    for at in (0..TAG).rev() {
        tag[at] = DIGITS[usize::try_from(left % 36).unwrap_or_default()];
        left /= 36;
    }
    String::from_utf8_lossy(&tag).into_owned()
}

/// The stamp of the drop numbered `number` in the run tagged `run`.
#[must_use]
pub fn of(run: &str, number: u64) -> String {
    format!("{run}{:010}", number % 10_000_000_000)
}

/// `bytes`, an interchange, with `stamp` as its control reference in its
/// header and its trailer.
///
/// # Errors
///
/// Where `bytes` is not an interchange the `edi` language writes, in its
/// words.
pub fn stamped(bytes: &[u8], stamp: &str) -> Result<Vec<u8>, String> {
    let stream = Stream::new(StreamId::new(1), bytes.to_vec(), None);
    let mut rewriting = Rewriting::of(&stream, StreamId::new(2));
    for at in AT {
        compiled(at)?
            .write(&mut rewriting, ScalarValue::Text(stamp.to_string()))
            .map_err(|error| format!("the stamp at {at}: {error}"))?;
    }
    let written = rewriting
        .finish()
        .map_err(|error| format!("the stamped document: {error}"))?;
    written
        .load()
        .map(<[u8]>::to_vec)
        .map_err(|error| format!("the stamped document: {error}"))
}

/// The stamp `bytes` carries: its header's control reference; none where it
/// is not an interchange or names none.
#[must_use]
pub fn read(bytes: &[u8]) -> Option<String> {
    let stream = Stream::new(StreamId::new(1), bytes.to_vec(), None);
    match compiled(AT[0]).ok()?.read(&Content::of(&stream)).ok()?? {
        ScalarValue::Text(stamp) => Some(stamp),
        _ => None,
    }
}

/// `at` compiled by the `edi` language.
fn compiled(at: &str) -> Result<Box<dyn path::CompiledExpression>, String> {
    EdiLanguage
        .compile(at, &Namespaces::default())
        .map_err(|error| format!("{at}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::documents::Documents;

    #[test]
    fn a_stamp_written_reads_back_and_the_document_goes_where_it_went() {
        let documents = Documents::shipped().expect("the shipped documents");
        let mut stamps = Vec::new();
        for (number, document) in documents.routable().iter().enumerate() {
            let stamp = of(&run_tag(1_795_000_000), u64::try_from(number).expect("few"));
            assert!(stamp.len() <= 14, "a control reference holds 14: {stamp}");
            let bytes = stamped(&document.bytes, &stamp).expect("stamped");
            assert_eq!(
                read(&bytes).as_deref(),
                Some(stamp.as_str()),
                "{}",
                document.stem
            );
            assert_eq!(
                documents.routed(&bytes),
                documents.routed(&document.bytes),
                "{} goes where it went",
                document.stem
            );
            stamps.push(stamp);
        }
        stamps.sort();
        stamps.dedup();
        assert_eq!(
            stamps.len(),
            documents.routable().len(),
            "each drop its own"
        );
    }

    #[test]
    fn two_runs_tag_apart_and_what_is_no_interchange_carries_none() {
        assert_ne!(run_tag(1_795_000_000), run_tag(1_795_000_001));
        assert_eq!(run_tag(0), "0000");
        assert_eq!(read(b"not an interchange"), None);
        assert!(stamped(b"not an interchange", "X").is_err());
    }
}

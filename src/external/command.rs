//! A far end's command line, `xmip-essentials-external`'s
//! (`src/bin/external.rs`): its name and what it was started to do — on one
//! computer a sender or a receiver, on a machine of its own the service
//! that serves, its registration, or the ports it serves on. Split from
//! `program.rs` on 2026-11-22.

use std::path::PathBuf;
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
    /// On a machine of its own: serves the roll's sessions, its certificate
    /// in `folder` (`serving.rs`).
    Serves { folder: PathBuf },
    /// Registers itself as the service that serves, its certificate in
    /// `folder`, and starts it.
    Installs { folder: PathBuf },
    /// Says the ports it serves on, which its machine's firewall opens.
    Listens,
}

impl Does {
    /// What it does, in a word, as it declares and audits it.
    #[must_use]
    pub const fn word(&self) -> &'static str {
        match self {
            Self::Sends => "sends",
            Self::Receives { .. } => "receives",
            Self::Serves { .. } => "serves",
            Self::Installs { .. } => "installs",
            Self::Listens => "listens",
        }
    }

    /// What its count of files says, as its stop record says it.
    #[must_use]
    pub const fn counted(&self) -> &'static str {
        match self {
            Self::Sends => "sent",
            Self::Receives { .. } => "received",
            Self::Serves { .. } | Self::Installs { .. } | Self::Listens => "carried",
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
    /// <folder>` and `--port <Send Port> <folder>` for each Send Port; on a
    /// machine of its own `--name <name> --serve` or `--install`, and
    /// `--listening`, each with `--configuration <folder>`, the folder of
    /// its certificate.
    ///
    /// # Errors
    ///
    /// What is missing or not understood, in words.
    pub fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut arguments = arguments.into_iter();
        let (mut name, mut into, mut folder) = (None, None, None);
        let (mut said, mut ports) = (Vec::new(), Vec::new());
        while let Some(argument) = arguments.next() {
            let mut value = |what: &str| {
                arguments
                    .next()
                    .ok_or_else(|| format!("{argument} takes {what}"))
            };
            match argument.as_str() {
                "--name" => name = Some(value("a name")?),
                "--into" => into = Some(PathBuf::from(value("a folder")?)),
                "--configuration" => folder = Some(PathBuf::from(value("a folder")?)),
                "--port" => {
                    let port = value("a Send Port's name and its folder")?;
                    let folder = value("a Send Port's name and its folder")?;
                    ports.push((port, PathBuf::from(folder)));
                }
                "--sender" | "--receiver" | "--serve" | "--install" | "--listening" => {
                    said.push(argument);
                }
                other => return Err(format!("{other} is not understood")),
            }
        }
        let does = match (said.as_slice(), into, folder) {
            ([one], None, None) if one == "--sender" && ports.is_empty() => Does::Sends,
            ([one], Some(into), None) if one == "--receiver" => Does::Receives { into, ports },
            ([one], None, Some(folder)) if one == "--serve" => Does::Serves { folder },
            ([one], None, Some(folder)) if one == "--install" => Does::Installs { folder },
            ([one], None, Some(_)) if one == "--listening" => Does::Listens,
            _ => {
                return Err(
                    "a far end is --sender alone, --receiver with --into and its --port, or \
                     --serve, --install or --listening with --configuration"
                        .to_string(),
                );
            }
        };
        let name = match (name, &does) {
            (Some(name), _) => name,
            (None, Does::Listens) => String::new(),
            (None, _) => return Err("--name is missing".to_string()),
        };
        Ok(Self { name, does })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(
            said("--name c --serve --configuration tls")
                .expect("a far end on a machine of its own")
                .does,
            Does::Serves {
                folder: PathBuf::from("tls")
            }
        );
        assert_eq!(
            said("--configuration tls --listening")
                .expect("its ports")
                .does,
            Does::Listens
        );
        for wrong in [
            "--sender",
            "--name a",
            "--name a --sender --receiver",
            "--name a --x",
            "--name a --serve",
            "--configuration tls --serve",
        ] {
            assert!(said(wrong).is_err(), "{wrong}");
        }
    }
}

//! What a node declares, as the node binary's flags.
//!
//! The declaration is `node::Capability` (ADR-0056): the node's roles and
//! its Node capabilities, the evidence it publishes and the entries a run
//! lists it by are written and read there and nowhere else (open problem
//! 25). What stays here is the System Tests' own: how the cluster tells a
//! node process what it was started with, `--role` and `--capabilities`,
//! which `node_arguments.rs` reads back through `Capability::parse` and
//! `Capability::from_declared_entry`.

use node::Capability;

/// The declaration as the node binary's flags: its roles, and its Node
/// capabilities by `+`, `online` among them where it may assume the internet
/// (ADR-0056, amendment 2026-11-24).
#[must_use]
pub fn flags(capability: &Capability) -> Vec<String> {
    vec![
        "--role".to_string(),
        capability.words(),
        "--capabilities".to_string(),
        capability.declared().join("+"),
    ]
}

/// The Node capabilities `--capabilities` says, by `+`, read by the one
/// reading of a run's entry (`Capability::from_declared_entry`).
///
/// # Errors
///
/// REFUSED, as `Capability::node_capabilities` refuses a word that is none.
pub fn read(said: &str) -> Result<Vec<String>, String> {
    Capability::from_declared_entry(&format!("node={said}")).1
}

#[cfg(test)]
mod tests {
    use super::*;
    use node::NodeRole;

    #[test]
    fn the_flags_say_the_roles_and_the_node_capabilities() {
        let words = [node::ONLINE.to_string(), "erp-direct".to_string()];
        let capability = Capability::of(&[NodeRole::Processing])
            .with_declared(&words)
            .expect("two words");
        assert_eq!(
            flags(&capability),
            [
                "--role",
                "processing",
                "--capabilities",
                "online+erp-direct"
            ]
        );
        assert_eq!(
            flags(&Capability::none()),
            ["--role", "", "--capabilities", ""]
        );
        let read_back = Capability::parse(&flags(&capability)[1])
            .and_then(|roles| roles.with_declared(&read(&flags(&capability)[3])?));
        assert_eq!(
            read_back,
            Ok(capability),
            "the node reads its flags back by the one parse"
        );
        assert_eq!(read(""), Ok(Vec::new()));
        assert!(read("Online").is_err());
    }
}

//! The Reference Environment, Xmip's one definition of an environment
//! (ADR-0028, amendments 2026-11-26 and 2026-11-28), as this repository
//! ships it: its text, its file, and how a roll names its cluster from it.

/// The Reference Environment's file, from this repository's root: written
/// once, for [`CLUSTER`] and [`FILE`] alike.
macro_rules! reference {
    () => {
        "configuration/Xmip-reference-environment.xmip"
    };
}

/// The Reference Environment's text: its nodes, its Parties and its Xmip
/// Applications, the `RoundTrip` Application's section among them.
pub const CLUSTER: &str = include_str!(concat!("../", reference!()));

/// The Reference Environment's file, from the estate's root, where this
/// repository is mounted: the file a roll names as its cluster's.
pub const FILE: &str = concat!("test/core/system-test/", reference!());

/// How a roll names its cluster for a person: the short name the Reference
/// Environment declares, `[service] short_name`, and its file. A file
/// declaring none names the file alone.
#[must_use]
pub fn naming() -> observe::Naming {
    observe::Naming {
        short_name: configure::service::short_name(CLUSTER)
            .ok()
            .flatten()
            .map(String::from)
            .unwrap_or_default(),
        file: FILE.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_roll_names_its_cluster_by_the_short_name_its_file_declares() {
        let declared = configure::service::short_name(CLUSTER)
            .expect("the Reference Environment reads")
            .expect("the Reference Environment declares a short name");
        let naming = naming();
        assert_eq!(naming.short_name, declared.as_str());
        assert!(
            FILE.ends_with(".xmip") && FILE.starts_with("test/"),
            "{FILE}"
        );
        assert_eq!(
            configure::cluster::cluster_name(CLUSTER).expect("reads"),
            None,
            "the short name is no cluster name: the run names the cluster"
        );
    }
}

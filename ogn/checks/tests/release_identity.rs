use semver::{Version, VersionReq};

#[test]
fn fork_build_preserves_stable_compatibility_and_revision_order() {
    let manifest = include_str!("../../../application/Cargo.toml");
    let version = manifest
        .lines()
        .find_map(|line| {
            line.strip_prefix("version = \"")
                .and_then(|value| value.strip_suffix('"'))
        })
        .unwrap();
    let version = Version::parse(version).unwrap();
    assert!(version.pre.is_empty());
    assert!(version.build.as_str().starts_with("ogn."));
    assert!(
        VersionReq::parse(">=1.2.8, <1.3.0")
            .unwrap()
            .matches(&version)
    );
    // Wings uses Version's total ordering for update checks, including build metadata.
    assert!(Version::parse("1.2.8+ogn.10").unwrap() > Version::parse("1.2.8+ogn.2").unwrap());
    assert!(Version::parse("1.2.9+ogn.1").unwrap() > Version::parse("1.2.8+ogn.10").unwrap());
}

#[test]
fn default_updater_stays_on_the_ogn_release_channel() {
    assert!(
        include_str!("../../../application/build.rs")
            .contains("read(\"WINGS_UPGRADE_REPOSITORY\", \"mike9918/ogn-wings\")")
    );
}

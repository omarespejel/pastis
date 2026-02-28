use std::collections::BTreeMap;

use semver::Version;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedConstants {
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct VersionedConstantsResolver {
    bundled: BTreeMap<Version, VersionedConstants>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VersionResolutionError {
    #[error("no constants available for protocol version {requested}")]
    Missing { requested: Version },
}

impl VersionedConstantsResolver {
    pub fn new(entries: impl IntoIterator<Item = (Version, VersionedConstants)>) -> Self {
        Self {
            bundled: entries.into_iter().collect(),
        }
    }

    pub fn resolve_for_protocol(
        &self,
        requested: &Version,
    ) -> Result<&VersionedConstants, VersionResolutionError> {
        self.bundled
            .get(requested)
            .ok_or_else(|| VersionResolutionError::Missing {
                requested: requested.clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(raw: &str) -> Version {
        Version::parse(raw).expect("valid version")
    }

    #[test]
    fn resolves_exact_match() {
        let resolver = VersionedConstantsResolver::new([
            (
                v("0.14.0"),
                VersionedConstants {
                    id: "v14_0".to_string(),
                },
            ),
            (
                v("0.14.2"),
                VersionedConstants {
                    id: "v14_2".to_string(),
                },
            ),
            (
                v("0.15.0"),
                VersionedConstants {
                    id: "v15_0".to_string(),
                },
            ),
        ]);

        let selected = resolver
            .resolve_for_protocol(&v("0.15.0"))
            .expect("resolve");
        assert_eq!(selected.id, "v15_0");
    }

    #[test]
    fn rejects_missing_patch_version_in_same_minor() {
        let resolver = VersionedConstantsResolver::new([
            (
                v("0.14.0"),
                VersionedConstants {
                    id: "v14_0".to_string(),
                },
            ),
            (
                v("0.14.2"),
                VersionedConstants {
                    id: "v14_2".to_string(),
                },
            ),
        ]);

        let err = resolver
            .resolve_for_protocol(&v("0.14.3"))
            .expect_err("must fail closed for missing patch");
        assert_eq!(
            err,
            VersionResolutionError::Missing {
                requested: v("0.14.3"),
            }
        );
    }

    #[test]
    fn rejects_unknown_minor_version() {
        let resolver = VersionedConstantsResolver::new([
            (
                v("0.14.0"),
                VersionedConstants {
                    id: "v14_0".to_string(),
                },
            ),
            (
                v("0.14.2"),
                VersionedConstants {
                    id: "v14_2".to_string(),
                },
            ),
        ]);

        let err = resolver
            .resolve_for_protocol(&v("0.15.0"))
            .expect_err("must fail closed");
        assert_eq!(
            err,
            VersionResolutionError::Missing {
                requested: v("0.15.0"),
            }
        );
    }

    #[test]
    fn prefers_release_over_prerelease_for_same_patch() {
        let resolver = VersionedConstantsResolver::new([
            (
                v("0.14.2-alpha.1"),
                VersionedConstants {
                    id: "v14_2_alpha".to_string(),
                },
            ),
            (
                v("0.14.2"),
                VersionedConstants {
                    id: "v14_2_release".to_string(),
                },
            ),
        ]);

        let selected = resolver
            .resolve_for_protocol(&v("0.14.2"))
            .expect("resolve");
        assert_eq!(selected.id, "v14_2_release");
    }
}

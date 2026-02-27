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
        if let Some(exact) = self.bundled.get(requested) {
            return Ok(exact);
        }

        self.bundled
            .iter()
            .filter(|(version, _)| {
                version.major == requested.major
                    && version.minor == requested.minor
                    && version.patch <= requested.patch
            })
            .max_by(|(left, _), (right, _)| left.patch.cmp(&right.patch))
            .map(|(_, constants)| constants)
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
    fn resolves_newer_patch_to_latest_known_patch_of_same_minor() {
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

        let selected = resolver
            .resolve_for_protocol(&v("0.14.3"))
            .expect("resolve");
        assert_eq!(selected.id, "v14_2");
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
}

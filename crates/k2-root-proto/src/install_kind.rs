//! How a K2 binary was installed, from its own path (moved here from
//! `k2-daemon`'s `boot_status` for K2 distributed runtime P0, so the
//! daemon, the root helper, the App engine install and k2-node share one
//! rule). A `"package"` binary (`/usr/bin/…` in a distro package or the
//! OCI image, or the Nix store) is never swapped by K2 itself: the package
//! manager or a new image tag updates it.

use std::path::Path;

/// Pure classifier: one of `"bundled-app" | "package" | "standalone" |
/// "unknown"`. `k2-daemon`'s `boot_status::install_kind` runs it on its
/// own executable.
pub fn classify_install_kind(exe: Option<&Path>) -> &'static str {
    let Some(exe) = exe else {
        return "unknown";
    };
    // A macOS app bundle nests the executable at
    // `…/K2.app/Contents/MacOS/<bin>` (or, for the daemon since 0.43.2,
    // `…/K2.app/Contents/Helpers/K2 Daemon.app/Contents/MacOS/k2-daemon`).
    // Detect the `.app/Contents/`
    // segment anywhere in the path — robust to the bundle name and to a
    // sidecar daemon binary placed elsewhere under Contents/.
    let s = exe.to_string_lossy();
    if s.contains(".app/Contents/") {
        return "bundled-app";
    }
    if is_package_path(&s) {
        return INSTALL_KIND_PACKAGE;
    }
    "standalone"
}

/// `installKind` of a package-manager install (0.46.3).
pub const INSTALL_KIND_PACKAGE: &str = "package";

/// True when `exe` classifies as [`INSTALL_KIND_PACKAGE`].
pub fn is_package_install(exe: Option<&Path>) -> bool {
    classify_install_kind(exe) == INSTALL_KIND_PACKAGE
}

/// True for a path a package manager owns: under `/usr/` but not
/// `/usr/local/` (FHS: `/usr` is the distribution's, `/usr/local` the
/// admin's — where the standalone installers put K2), or the Nix store.
pub fn is_package_path(s: &str) -> bool {
    (s.starts_with("/usr/") && !s.starts_with("/usr/local/")) || s.starts_with("/nix/store/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn package_paths() {
        for p in ["/usr/bin/k2-node", "/usr/lib/k2/k2-root-helper", "/usr/libexec/k2-app-engine", "/nix/store/x-k2/bin/k2-node"] {
            assert!(is_package_install(Some(Path::new(p))), "{p}");
        }
        for p in ["/usr/local/libexec/k2-root-helper", "/usr/local/bin/k2-node", "/home/k2/.k2/bin/k2-node", "/usrx/bin/k2-node"] {
            assert!(!is_package_install(Some(Path::new(p))), "{p}");
        }
        assert!(!is_package_install(None));
    }

    #[test]
    fn kinds() {
        assert_eq!(classify_install_kind(Some(Path::new("/Applications/K2.app/Contents/MacOS/k2-daemon"))), "bundled-app");
        assert_eq!(classify_install_kind(Some(Path::new("/usr/bin/k2-daemon"))), "package");
        assert_eq!(classify_install_kind(Some(Path::new("/usr/local/bin/k2-daemon"))), "standalone");
        assert_eq!(classify_install_kind(None), "unknown");
    }
}

//! Shared download-client builder (ARCH m5): the reqwest client + profile
//! behind every user-influenced outbound fetch (direct `.zim` downloads,
//! the OPDS catalog). Own module so `opds` and `poller::direct` import it
//! from `crate::torrent::client` instead of a cross-sibling import into
//! `poller::direct`.

use std::time::Duration;

use crate::error::{Error, Result};

/// Build a download HTTP client. Redirects are **not** followed by this
/// client (`Policy::none`): user-influenced URLs (direct `.zim` downloads,
/// the OPDS catalog) follow redirects **manually** via
/// [`crate::netguard::follow_pinned_get`] — every hop is re-validated,
/// re-resolved, and pinned to the exact address that passed the check, so a
/// sub-second DNS flip between check and CONNECT can't steer the fetch at a
/// blocked host (SEC-1). `pin` optionally fixes a domain→addr resolution
/// (validated up front by the caller) for this client; IP-literal hosts are
/// passed with `pin = None` (no DNS → nothing to pin, no rebinding window).
pub(crate) enum ClientProfile {
    /// Control-plane calls (OPDS catalog fetch): bounded end-to-end.
    Control,
    /// Long transfers (multi-GB `.zim` body streams): connect + read-idle
    /// bounds only, **no total timeout** — `reqwest::Client::timeout` caps
    /// the entire request including body reception, which aborted every
    /// realistically-sized download mid-stream (C1).
    Transfer,
}

/// `(total, connect, read-idle)` timeouts per profile. `Transfer` is bounded
/// instead by the `downloads.max_bytes` stream cap on total volume and by the
/// read-idle timeout on stalled connections.
fn client_timeouts(
    profile: ClientProfile,
) -> (Option<Duration>, Option<Duration>, Option<Duration>) {
    match profile {
        ClientProfile::Control => (Some(Duration::from_secs(30)), None, None),
        ClientProfile::Transfer => (
            None,
            Some(Duration::from_secs(10)),
            Some(Duration::from_secs(60)),
        ),
    }
}

pub(crate) fn build_download_client(
    profile: ClientProfile,
    pin: Option<(String, std::net::SocketAddr)>,
) -> Result<reqwest::Client> {
    let (total, connect, read) = client_timeouts(profile);
    // SEC-1: never auto-follow — the manual redirect loop
    // (netguard::follow_pinned_get) owns all following, with per-hop
    // resolve + pin.
    let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
    if let Some(t) = total {
        builder = builder.timeout(t);
    }
    if let Some(c) = connect {
        builder = builder.connect_timeout(c);
    }
    if let Some(r) = read {
        builder = builder.read_timeout(r);
    }
    if let Some((host, ip)) = pin {
        builder = builder.resolve(&host, ip);
    }
    builder.build().map_err(Error::Http)
}

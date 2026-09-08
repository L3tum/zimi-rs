//! Direct-HTTP download path: client build, Range-resume streaming, finalize.

use super::*;

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

/// Resume decision from an existing `.part`: `Some(s)` for a non-empty
/// file (resume from byte `s`), `None` for a fresh download. Bytes on
/// disk are always a true prefix of the source stream (sequential
/// `write_all`, offset advances only after successful writes), so
/// resuming is byte-safe when the source file is unchanged; a changed
/// source is caught by the 416/200 fallback in `stream_part`, and final
/// corruption by `verify_zim` before publish.
fn resume_plan(part: &Path) -> Option<u64> {
    let len = std::fs::metadata(part).ok()?.len();
    (len > 0).then_some(len)
}

/// Outcome of a single `stream_part` call.
struct StreamOutcome {
    /// Final on-disk size of `part` after the stream.
    size: u64,
    /// Declared total size (Content-Length, or the FULL of
    /// Content-Range on 206), if the server gave one.
    total: Option<u64>,
    /// A mid-stream cancel was observed: the row is `cancelled`, the staged
    /// `.part` has already been removed (see [`observe_cancel`]), and the
    /// caller must leave the row alone — no error mark, no finalize (the
    /// same route as the [`finalize_direct_download`] cancel sites).
    cancelled: bool,
}

/// Instantaneous speed from a byte window over its *actual* elapsed time
/// (BUG-19: the old `window_bytes / 2` assumed the 2 s check fired on time;
/// under load a late check understates elapsed time and overstates speed).
pub(crate) fn window_speed(window_bytes: u64, elapsed: std::time::Duration) -> i64 {
    let secs = elapsed.as_secs_f64().max(0.001); // floor: avoid div-by-zero
    (window_bytes as f64 / secs).round() as i64
}

/// Parse `Content-Range: bytes S-T/FULL` (or `bytes */FULL`) into
/// `(S, FULL)`; `S` is `None` for the `*` form (the reply does not say
/// where its range starts). `None` when the unit is not `bytes` (e.g.
/// `items 0-9/100` — a non-byte range set must not be mistaken for one)
/// or the header is malformed.
fn content_range_parts(val: &str) -> Option<(Option<u64>, u64)> {
    let (range, full) = val.split_once('/')?;
    let full = full.trim().parse().ok()?;
    let (unit, spec) = range.split_once(' ')?;
    if unit != "bytes" {
        return None;
    }
    let start = if spec == "*" {
        None
    } else {
        let (s, t) = spec.split_once('-')?;
        let s: u64 = s.parse().ok()?;
        let _t: u64 = t.parse().ok()?;
        Some(s)
    };
    Some((start, full))
}

/// Parse the `FULL` part of `Content-Range: bytes S-T/FULL` (or
/// `bytes */FULL`); `None` when the header is absent, is not a `bytes`
/// range, or the total is unparseable.
fn content_range_total(headers: &axum::http::HeaderMap) -> Option<u64> {
    let val = headers
        .get(axum::http::header::CONTENT_RANGE)?
        .to_str()
        .ok()?;
    content_range_parts(val).map(|(_, full)| full)
}

/// Parse the `S` (first byte) part of `Content-Range: bytes S-T/FULL`;
/// `None` when the header is absent, is not a `bytes` range, S is `*` (the
/// reply does not say where its range starts), or the header is malformed.
fn content_range_start(headers: &axum::http::HeaderMap) -> Option<u64> {
    let val = headers
        .get(axum::http::header::CONTENT_RANGE)?
        .to_str()
        .ok()?;
    content_range_parts(val)?.0
}

/// How to handle a `206 Partial Content` reply to a resume Range request
/// whose `.part` holds `from` bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resume206Decision {
    /// The reply's `Content-Range` starts exactly at the resumed offset
    /// (S == `from`): append to the existing `.part`.
    Append,
    /// S != `from`, or S is unknowable (`bytes */FULL`, missing/malformed
    /// header): a redirected mirror serving a *different* revision also
    /// answers `bytes=from-` with a 206 on its own file, and appending
    /// would splice the old prefix onto the new suffix. Take the 416
    /// route: discard the stale partial, reissue fresh.
    FallbackFresh,
}

/// The 206 dispatch decision (factored out so the check is unit-testable
/// without a server): append only when the server confirms the range
/// starts exactly where the `.part` ends.
fn resume_206_decision(headers: &axum::http::HeaderMap, from: u64) -> Resume206Decision {
    match content_range_start(headers) {
        Some(s) if s == from => Resume206Decision::Append,
        _ => Resume206Decision::FallbackFresh,
    }
}

/// The in-loop cancel observation (the ~5 s check), factored out so the
/// branch is testable without a multi-second stream: re-read the row's
/// status; `None` while it is still `downloading` (keep streaming; fails
/// open on a DB blip like [`status_if_changed`]). Once it has left
/// `downloading`, a CANCEL removes the staged `.part` right here — a
/// CANCEL always removes the staged file, at every cancel-observation
/// site — because a plain `break` would leave it on disk: the partial file
/// is intentionally kept on drop for resume (PERF-11), and the post-loop truncation guard
/// would return `Err` (typical mid-stream cancel: `received < total`) before
/// the `finalize_direct_download` cancel sites — which DO remove the file —
/// are ever reached, so a multi-GB `.part` would linger until the next
/// startup reconcile sweep. Returns the new status.
async fn observe_cancel(db: &Pool, id: i32, part: &Path) -> Option<String> {
    let status = status_if_changed(db, id, "downloading").await?;
    if status == crate::torrent::DownloadStatus::Cancelled.as_str() {
        let _ = std::fs::remove_file(part);
    }
    Some(status)
}

/// File stream + bookkeeping for [`stream_part`]'s body loop.
enum FileStream {
    /// 206 append: file opened in append mode, `from` = resumed offset.
    Append(tokio::fs::File, u64, Option<u64>, reqwest::Response),
    /// Fresh/416-fallback: file opened in create/truncate mode.
    Fresh(tokio::fs::File, Option<u64>, reqwest::Response),
}

/// Reissue a fresh GET (no Range) to the pinned final `url` and stream its
/// body into a re-created `part`. Reached on a 416 (source changed, or the
/// `.part` was already complete) and on a 206 whose `Content-Range` start
/// does not match the resumed offset (a mirror serving a different
/// revision answered our Range — see [`resume_206_decision`]).
///
/// The stale `.part` is removed only **after** the reissue is confirmed a
/// success status within the byte cap: a transient `send()` failure must
/// not throw away resume progress (PERF-11). If the reissue answers
/// non-success, the `.part` is removed and the row error-marked (the
/// fallback has already proven the resume unusable).
#[allow(clippy::too_many_arguments)]
async fn fresh_fallback(
    url: &str,
    client: &reqwest::Client,
    part: &Path,
    max_bytes: u64,
    db: &Pool,
    id: i32,
) -> Result<FileStream> {
    let resp2 = client.get(url).send().await?;
    if !resp2.status().is_success() {
        let _ = std::fs::remove_file(part);
        let msg = format!("HTTP {}", resp2.status());
        mark_error(db, id, &msg).await;
        return Err(Error::Torrent {
            kind: TorrentKind::Other,
            msg,
        });
    }
    // Same up-front cap check as the 200 branch: reject before touching the
    // file (a declared size over the cap must not stream to the cap).
    if let Some(t) = resp2.content_length() {
        if t > max_bytes {
            let msg = format!("file is {t} bytes, exceeds the {max_bytes}-byte limit");
            mark_error(db, id, &msg).await;
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg,
            });
        }
    }
    // Confirmed success — only now discard the stale partial.
    let _ = std::fs::remove_file(part);
    let f = tokio::fs::File::create(part).await?;
    Ok(FileStream::Fresh(f, resp2.content_length(), resp2))
}

/// Stream an in-flight HTTP body into `part`, with Range resume support
/// (PERF-11). `resp` is the terminal (non-redirect) response of the manual
/// redirect chain and `url`/`client` the pinned final hop that produced it
/// (see [`direct_download`] / `netguard::follow_pinned_get`), so the 416
/// fallback re-issues to the FINAL redirected URL with the pinned client.
///
/// Response-shape dispatch:
/// - **206 + Range sent** → append to the existing `.part`, but only when
///   `Content-Range` confirms the range starts exactly at the resumed
///   offset (S == `from`); a mismatch or an unknowable S takes the 416
///   route instead. `total` from `Content-Range` (or
///   `from + Content-Length` fallback).
/// - **416 + Range sent** (and the 206 mismatch above) → source changed
///   (or the `.part` was already complete): reissue once, fresh, without
///   Range. The stale partial is removed only after the reissue is
///   confirmed (a transient `send()` failure keeps it for resume), and the
///   reissue gets the same up-front `max_bytes` check as a 200.
/// - **200** → fresh download (also covers servers that ignore Range);
///   truncates any existing partial.
/// - Anything else → `mark_error` + `Err`.
///
/// In-loop: over-cap check (against `received`, which includes the resumed
/// prefix), 2 s progress updates, ~5 s cancel check — a CANCEL removes the
/// staged `.part` there (see [`observe_cancel`]) so the post-loop guard can
/// not strand it.
/// Post-loop: truncation guard — a declared total that was not fully
/// received means the body was cut short (skipped for a row observed
/// `cancelled` mid-stream: the file is already gone and the row must not be
/// error-marked).
#[allow(clippy::too_many_arguments)]
async fn stream_part(
    url: &str,
    client: &reqwest::Client,
    resp: reqwest::Response,
    part: &Path,
    resume_from: Option<u64>,
    max_bytes: u64,
    db: &Pool,
    id: i32,
) -> Result<StreamOutcome> {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;

    // Determine the file open mode, the starting `received` offset, the
    // declared `total`, and the bytes stream to consume.
    let fs = match (resp.status(), resume_from) {
        (s, Some(from)) if s == axum::http::StatusCode::PARTIAL_CONTENT => {
            if resume_206_decision(resp.headers(), from) != Resume206Decision::Append {
                // S != from (or S unknowable): a mirror serving a different
                // revision answered our `bytes=from-` — appending would
                // splice the old prefix onto the new suffix. Same route as
                // a 416: discard the stale partial, reissue fresh.
                fresh_fallback(url, client, part, max_bytes, db, id).await?
            } else {
                let total = content_range_total(resp.headers())
                    .or_else(|| resp.content_length().map(|l| from.saturating_add(l)));
                if let Some(t) = total {
                    if t > max_bytes {
                        let msg = format!("file is {t} bytes, exceeds the {max_bytes}-byte limit");
                        mark_error(db, id, &msg).await;
                        return Err(Error::Torrent {
                            kind: TorrentKind::Other,
                            msg,
                        });
                    }
                }
                let f = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(part)
                    .await?;
                FileStream::Append(f, from, total, resp)
            }
        }
        (s, Some(_)) if s.as_u16() == 416 => {
            // 416: source changed (or the .part was already complete) —
            // discard the stale partial (once the reissue is confirmed) and
            // reissue once, fresh, without a Range.
            fresh_fallback(url, client, part, max_bytes, db, id).await?
        }
        (s, _) if s.is_success() => {
            // 200: fresh download — also covers servers that ignore Range.
            if let Some(t) = resp.content_length() {
                if t > max_bytes {
                    let msg = format!("file is {t} bytes, exceeds the {max_bytes}-byte limit");
                    mark_error(db, id, &msg).await;
                    return Err(Error::Torrent {
                        kind: TorrentKind::Other,
                        msg,
                    });
                }
            }
            let f = tokio::fs::File::create(part).await?;
            FileStream::Fresh(f, resp.content_length(), resp)
        }
        (s, _) => {
            let msg = format!("HTTP {s}");
            mark_error(db, id, &msg).await;
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg,
            });
        }
    };

    let (mut file, mut received, total, resp) = match fs {
        FileStream::Append(f, from, total, resp) => (f, from, total, resp),
        FileStream::Fresh(f, total, resp) => (f, 0u64, total, resp),
    };

    let mut stream = resp.bytes_stream();
    let mut window_bytes: u64 = 0;
    let mut window_start = Instant::now();
    let mut last_cancel_check = Instant::now();
    let mut cancelled_observed = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        // Enforce the cap even when the server omitted Content-Length.
        // usize → u64 is lossless on every supported platform but has no
        // `From` impl — `as` (hoisted: each conversion happens once).
        let chunk_bytes = chunk.len() as u64;
        if received.saturating_add(chunk_bytes) > max_bytes {
            let msg = format!("download exceeds the {max_bytes}-byte limit");
            mark_error(db, id, &msg).await;
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg,
            });
        }
        file.write_all(&chunk).await?;
        received += chunk_bytes;
        window_bytes += chunk_bytes;

        if window_start.elapsed() >= Duration::from_secs(2) {
            let pct = match total {
                // u64 → f64 has no `From` impl (lossless widening; `as`).
                Some(t) if t > 0 => (received as f64 / t as f64).min(1.0) as f32,
                _ => 0.0,
            };
            let now = Instant::now();
            let speed = window_speed(window_bytes, now - window_start);
            window_bytes = 0;
            window_start = now;
            crate::db::downloads_lifecycle::refresh_progress(db, id, pct, speed).await?;
        }

        // Periodic cancel check (every ~5 s): if the row is no longer
        // 'downloading' (cancelled, errored, etc.), abort the stream. A
        // CANCEL removes the staged `.part` inside `observe_cancel` (a plain
        // break would leave it behind — see that fn).
        if last_cancel_check.elapsed() >= Duration::from_secs(5) {
            last_cancel_check = Instant::now();
            if let Some(status) = observe_cancel(db, id, part).await {
                tracing::info!("direct download {id} aborted: status changed to '{status}'");
                if status == crate::torrent::DownloadStatus::Cancelled.as_str() {
                    cancelled_observed = true;
                }
                break;
            }
        }
    }
    file.flush().await?;
    drop(file);

    // Truncated-stream guard: a declared total that was not fully received
    // means the body was cut short (server lied / connection dropped).
    // Skipped for a mid-stream cancel: the `.part` is already removed and
    // the row must stay `cancelled` with no error mark — the same route as
    // the `finalize_direct_download` cancel sites.
    if let Some(t) = total {
        if received < t && !cancelled_observed {
            let msg = format!("stream ended at {received} of {t} bytes (truncated)");
            mark_error(db, id, &msg).await;
            return Err(Error::Torrent {
                kind: TorrentKind::Other,
                msg,
            });
        }
    }
    Ok(StreamOutcome {
        size: received,
        total,
        cancelled: cancelled_observed,
    })
}

/// Stream a `.zim` file straight into `zim_dir` (as a `.part` file), then
/// hand off to [`finalize_direct_download`] (verify, atomic rename, resync,
/// index).
///
/// Cancel-coverage note: the in-loop cancel observation
/// ([`observe_cancel`]) is covered directly by the `observe_cancel_*` tests
/// and end-to-end by `stream_part_cancel_mid_stream_removes_part` (a raw-TCP
/// server holding the body back — wiremock's delay covers the whole
/// response, which would move the in-loop clock with it).
pub(super) async fn direct_download(
    http: &reqwest::Client,
    db: &Pool,
    zims: &Arc<ZimManager>,
    settings: &SettingsCache,
    id: i32,
    url: &str,
    part: &Path,
) -> Result<()> {
    // PERF-12: snapshot the two download settings we read up front.
    let dp = settings.poller_params_snapshot();
    // SSRF guard before any network I/O (the manual redirect chain below
    // re-validates this URL as hop 0, and every later hop, the same way).
    let allow_private = dp.allow_private_networks;
    validate_download_url(url, allow_private)?;

    // SEC-1: redirects are followed manually (netguard::follow_pinned_get)
    // instead of by a built-in reqwest policy: every hop — initial included
    // — is validated, re-resolved, and the request goes out through a
    // client pinned to the exact address(es) that passed the check, so a
    // sub-second TTL flip between check and CONNECT can no longer steer
    // the fetch at a blocked host. The shared unpinned client `http` is
    // used only for IP-literal hosts (no DNS → nothing to pin).
    // (Per-download clients: downloads are infrequent, so building one per
    // hop is cheap.)
    let settings = settings.clone();
    let provider: std::sync::Arc<dyn Fn() -> bool + Send + Sync> =
        std::sync::Arc::new(move || settings.downloads_allow_private_networks());
    let shared = http.clone();

    let resume_from = resume_plan(part);
    if let Some(s) = resume_from {
        tracing::info!("direct download {id}: resuming from {s} bytes");
    }

    let PinnedResponse {
        url: final_url,
        client,
        response,
    } = follow_pinned_get(
        url,
        provider,
        &[],
        &|pin| match pin {
            Some((host, ip)) => {
                tracing::debug!("pinned {host} -> {ip} for download");
                build_download_client(ClientProfile::Transfer, Some((host, ip)))
            }
            // IP-literal host: reuse the shared Transfer client (built with
            // the same profile and no pin).
            None => Ok(shared.clone()),
        },
        &|c, u| {
            let mut req = c.get(u);
            if let Some(s) = resume_from {
                req = req.header(axum::http::header::RANGE, format!("bytes={s}-"));
            }
            req
        },
    )
    .await?;

    // On a transient failure the `.part` file is intentionally left in place
    // for resume (PERF-11); it is removed only on the explicit cancel/finalize
    // paths (see [`finalize_direct_download`]).
    let max_bytes = dp.max_bytes;
    let outcome = stream_part(
        &final_url,
        &client,
        response,
        part,
        resume_from,
        max_bytes,
        db,
        id,
    )
    .await?;
    if outcome.cancelled {
        // Mid-stream cancel: the staged `.part` was already removed by the
        // in-loop cancel observation ([`observe_cancel`]). Same route as the
        // [`finalize_direct_download`] cancel sites: leave the `cancelled`
        // row alone (no error mark, no verify/rename/index) — the file is
        // gone, so nothing is left to keep for resume.
        tracing::info!(
            "direct download {id} cancelled during transfer — staged part removed, leaving row cancelled"
        );
        return Ok(());
    }
    tracing::debug!(
        "direct download {id}: {} bytes (total: {:?})",
        outcome.size,
        outcome.total
    );

    // Post-stream finalize (TEST-5 seam): cancel re-checks, verify, rename,
    // resync, guarded row update, auto-index.
    finalize_direct_download(db, zims, id, part).await
}

/// Post-stream finalize for a direct download (TEST-5 seam, extracted from
/// [`direct_download`]): the cancel re-check before verify, the libzim
/// verify, the cancel check before rename, the atomic rename + resync, the
/// guarded `status = 'downloading'` row update, and the auto-index.
///
/// Cancel policy (RECONCILE with PERF-11 resume): a CANCEL always removes the
/// staged file — at all four cancel-observation sites (the in-stream loop —
/// [`observe_cancel`], and here: pre-verify, pre-rename, post-rename
/// `updated == 0`). Only transient network failures in the stream loop keep
/// the `.part` for resume.
async fn finalize_direct_download(
    db: &Pool,
    zims: &Arc<ZimManager>,
    id: i32,
    part: &Path,
) -> Result<()> {
    // Cancel re-check BEFORE the expensive verify: a cancelled partial file
    // must never be fed to verify_zim (a multi-GB central-dir parse of a
    // truncated file is wasted work and can fail spuriously). The in-loop
    // check runs every ~5 s; this closes the window between the last loop
    // iteration and verify. The post-verify cancelled check below stays as a
    // harmless second guard.
    {
        if let Some(status) = status_if_changed(db, id, "downloading").await {
            tracing::info!(
                "direct download {id} cancelled/changed ('{status}') before verify — discarding {}",
                part.display()
            );
            let _ = std::fs::remove_file(part);
            return Ok(());
        }
    }

    // Verify before publishing — a corrupt file must not clobber the library.
    // The central-dir parse is blocking (multi-GB) — keep it off the single
    // poller task so all download polling stays responsive.
    let part_owned = part.to_path_buf();
    let verify_res = tokio::task::spawn_blocking(move || verify_zim(&part_owned))
        .await
        .map_err(|e| Error::Internal(anyhow::anyhow!("verify task failed: {e}")))?;
    verify_res?;

    // Re-read the row BEFORE publishing: if the download was cancelled while
    // in flight, don't install it — discard the staged file and leave the DB
    // row alone (cancel wins over the in-flight task finishing). Checking
    // before the rename avoids briefly registering the ZIM in cache + DB.
    {
        if status_checked(db, id).await? == crate::torrent::DownloadStatus::Cancelled.as_str() {
            tracing::info!(
                "direct download {id} was cancelled during transfer — discarding {}",
                part.display()
            );
            let _ = std::fs::remove_file(part);
            return Ok(());
        }
    }

    // Atomic rename into place (same directory).
    let dst =
        zims.zim_dir.join(part.file_stem().ok_or_else(|| {
            Error::InvalidInput(format!("bad part file name: {}", part.display()))
        })?);
    std::fs::rename(part, &dst)?;

    zims.resync().await?;

    // `AND status = 'downloading'`: a cancel landing between the pre-rename
    // check and this UPDATE must still win — never clobber a terminal state
    // (the guard lives in `finalize_direct`).
    let updated =
        crate::db::downloads_lifecycle::finalize_direct(db, id, &dst.display().to_string()).await?;
    if updated == 0 {
        tracing::info!(
            "direct download {id} was cancelled during finalization — discarding {}",
            dst.display()
        );
        let _ = std::fs::remove_file(&dst);
        let _ = zims.resync().await;
        return Ok(());
    }

    tracing::info!("direct download {id} complete: {}", dst.display());

    let name = dst
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    if !name.is_empty() {
        if let Err(e) = index::index_zims(zims, db, Some(&name)).await {
            tracing::error!("auto-index of {name} failed: {e}");
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::tests::download::test_pool;
    use super::{build_download_client, ClientProfile};
    use crate::netguard::{follow_pinned_get, PinnedResponse};
    use crate::testing::dead_pool;

    // ── B5.4: download-client SSRF / redirect / pinning (wiremock) ───────────
    // In-module so the private `build_download_client` / `ClientProfile` are
    // reachable; the builder-failure path is covered by the
    // `client_from_option` unit test (propagation instead of the old silent
    // `unwrap_or_default()`).

    /// Drive the SEC-1 manual chain (netguard::follow_pinned_get) with a
    /// Transfer-profile pinned client. `testhost` is mapped onto the local
    /// mock socket via `host_pins` (the test seam for `pin_for_hop` —
    /// production passes no pins and resolves real DNS).
    async fn pinned_chain(
        url: &str,
        pins: &[(String, std::net::SocketAddr)],
        allow_private: bool,
        request: impl Fn(&reqwest::Client, &str) -> reqwest::RequestBuilder + Sync + 'static,
    ) -> PinnedResponse {
        follow_pinned_get(
            url,
            std::sync::Arc::new(move || allow_private),
            pins,
            &|pin| build_download_client(ClientProfile::Transfer, pin),
            &request,
        )
        .await
        .expect("chain must succeed")
    }

    #[tokio::test]
    async fn download_client_happy_path_pinned() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .respond_with(ResponseTemplate::new(200).set_body_string("zimblob"))
            .mount(&server)
            .await;
        // Pin `testhost` (no real DNS) at the MockServer socket: the fetch of
        // `http://testhost:{port}` succeeds only if the client carries the pin.
        let client = super::build_download_client(
            super::ClientProfile::Transfer,
            Some(("testhost".into(), *server.address())),
        )
        .expect("client builds");
        let body = client
            .get(format!("http://testhost:{}/x.zim", server.address().port()))
            .send()
            .await
            .expect("fetch through pin")
            .text()
            .await
            .unwrap();
        assert_eq!(body, "zimblob");
    }

    /// SEC-1: a 3xx whose `Location` points at a blocked address is rejected
    /// by the manual chain with the existing SSRF error class — before any
    /// connection to the blocked target. (Pre-SEC-1 this was the built-in
    /// policy test; the download clients no longer follow redirects at all,
    /// so the chain — not the client — owns the refusal.)
    #[tokio::test]
    async fn follow_pinned_get_redirect_to_blocked_address() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        // Redirect hops are re-validated with allow_loopback=false for
        // downloads, so both the link-local metadata IP and loopback are
        // refused even though the initial hop is a pinned local socket.
        for (location, why) in [
            ("http://169.254.169.254/", "link-local metadata"),
            (
                "http://127.0.0.1:9/x",
                "loopback (downloads never allow it)",
            ),
            ("http://10.0.0.5/x", "private (allow_private=false)"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/hop"))
                .respond_with(ResponseTemplate::new(302).append_header("Location", location))
                .mount(&server)
                .await;
            let pins = [("testhost".to_string(), *server.address())];
            let err = follow_pinned_get(
                &format!("http://testhost:{}/hop", server.address().port()),
                std::sync::Arc::new(|| false),
                &pins,
                &|pin| build_download_client(ClientProfile::Transfer, pin),
                &|c, u| c.get(u),
            )
            .await
            .expect_err("redirect target must be refused");
            // The existing SSRF error class: validate_download_url →
            // assert_host_not_blocked → InvalidInput.
            match err {
                crate::error::Error::InvalidInput(msg) => assert!(
                    msg.contains("blocked"),
                    "{why}: expected a blocked-range message, got: {msg}"
                ),
                other => panic!("{why}: expected InvalidInput (SSRF class), got {other:?}"),
            }
            // The chain stopped at the redirect: the only request the mock
            // ever saw was the initial /hop (no connection to the target).
            let got = server.received_requests().await.unwrap_or_default();
            assert_eq!(
                got.len(),
                1,
                "{why}: no request may leave for the blocked target"
            );
            assert_eq!(got[0].url.path(), "/hop");
        }
    }

    /// SEC-1: a direct download from URL A that 302-redirects to URL B
    /// succeeds end-to-end at the chain level: the chain lands on the FINAL
    /// URL with a pinned client, and `stream_part` writes B's body.
    #[tokio::test]
    async fn follow_pinned_get_redirect_302_downloads_final_body() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        // /a 302 → /b (same host, different path — the common mirror case).
        Mock::given(method("GET"))
            .and(path("/a.zim"))
            .respond_with(ResponseTemplate::new(302).append_header("Location", "/b.zim"))
            .mount(&server)
            .await;
        let body: &[u8] = b"hello world!"; // 12 bytes
        Mock::given(method("GET"))
            .and(path("/b.zim"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(body.to_vec())
                    .append_header("Content-Length", "12"),
            )
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        let pr = pinned_chain(
            &format!("http://testhost:{}/a.zim", server.address().port()),
            &pins,
            false,
            |c, u| c.get(u),
        )
        .await;
        assert!(
            pr.url.ends_with("/b.zim"),
            "the chain must land on the final redirected URL, got {}",
            pr.url
        );

        // Feed the terminal response into the streamer (dead pool: a 12-byte
        // body never touches the DB).
        let dir = std::env::temp_dir().join(format!("zimi-sec1-302-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("a.zim.part");
        let pool = dead_pool();
        let outcome = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            None,
            1024,
            &pool,
            999_999,
        )
        .await
        .expect("stream_part ok");
        assert_eq!(outcome.size, 12);
        assert_eq!(
            std::fs::read(&part).unwrap(),
            body,
            "B's body must be written"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SEC-1: a redirect loop / chain longer than the hop cap must error out
    /// cleanly — no hang, no infinite follow.
    #[tokio::test]
    async fn follow_pinned_get_redirect_loop_hits_cap() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        // /a → /b → /a → … forever.
        Mock::given(method("GET"))
            .and(path("/a"))
            .respond_with(ResponseTemplate::new(302).append_header("Location", "/b"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/b"))
            .respond_with(ResponseTemplate::new(302).append_header("Location", "/a"))
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        let err = follow_pinned_get(
            &format!("http://testhost:{}/a", server.address().port()),
            std::sync::Arc::new(|| false),
            &pins,
            &|pin| build_download_client(ClientProfile::Transfer, pin),
            &|c, u| c.get(u),
        )
        .await
        .expect_err("a redirect loop must hit the hop cap");
        match err {
            crate::error::Error::InvalidInput(msg) => assert!(
                msg.contains("too many redirects"),
                "expected the hop-cap error, got: {msg}"
            ),
            other => panic!("expected InvalidInput (hop cap), got {other:?}"),
        }
        // Bounded: exactly 1 initial + MAX_REDIRECT_HOPS follows were sent.
        let got = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            got.len(),
            crate::netguard::MAX_REDIRECT_HOPS + 1,
            "the chain must stop at the cap"
        );
    }

    /// SEC-1 + PERF-11: the 416 fresh-start fallback still works AFTER a
    /// redirect — the fallback re-issues to the FINAL redirected URL with the
    /// pinned client (not a re-fetch of hop 1).
    #[tokio::test]
    async fn stream_part_416_fallback_after_redirect() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let body: &[u8] = b"hello world!"; // 12 bytes
        let server = MockServer::start().await;
        // /a 302 → /b.
        Mock::given(method("GET"))
            .and(path("/a.zim"))
            .respond_with(ResponseTemplate::new(302).append_header("Location", "/b.zim"))
            .mount(&server)
            .await;
        // /b with Range → 416 (stale .part / changed source).
        Mock::given(method("GET"))
            .and(path("/b.zim"))
            .and(header("range", "bytes=6-"))
            .respond_with(ResponseTemplate::new(416).append_header("Content-Range", "bytes */12"))
            .mount(&server)
            .await;
        // /b without Range → 200 full body.
        Mock::given(method("GET"))
            .and(path("/b.zim"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(body.to_vec())
                    .append_header("Content-Length", "12"),
            )
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        // Hop 0 goes out WITH the Range header (resume_from = 6); the server
        // answers 416, which is the terminal response of the chain.
        let pr = pinned_chain(
            &format!("http://testhost:{}/a.zim", server.address().port()),
            &pins,
            false,
            |c, u| c.get(u).header("Range", "bytes=6-"),
        )
        .await;
        assert!(pr.url.ends_with("/b.zim"));
        assert_eq!(pr.response.status(), 416);

        let dir = std::env::temp_dir().join(format!("zimi-sec1-416-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("a.zim.part");
        std::fs::write(&part, b"stale!").unwrap();
        let pool = dead_pool();
        let outcome = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            Some(6),
            1024,
            &pool,
            999_999,
        )
        .await
        .expect("stream_part ok");
        assert_eq!(outcome.size, 12);
        assert_eq!(outcome.total, Some(12));
        assert_eq!(
            std::fs::read(&part).unwrap(),
            body,
            "file must be fresh after 416 fallback"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// TEST-5 (CI-DB): the post-stream finalize seam installs a valid part
    /// when the row is still `downloading` — rename into zim_dir, row
    /// `complete` + `file_path`, resynced `zims` row, articles indexed.
    #[tokio::test]
    async fn finalize_direct_download_installs_when_row_still_downloading() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let tmp = tempfile::tempdir().expect("tempdir");
        let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());

        let id: i32 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "INSERT INTO downloads (name, url, status)
                 VALUES ('utiny', 'http://example.net/utiny.zim', 'downloading') RETURNING id",
            |q| q,
        )
        .await
        .expect("insert downloading row")
        .unwrap();
        let part = tmp.path().join("utiny.part");
        std::fs::copy("tests/fixtures/tiny.zim", &part).expect("stage utiny.zim as .part");

        super::finalize_direct_download(&pool, &zims, id, &part)
            .await
            .expect("finalize must succeed");

        let dst = zims.zim_dir.join("utiny.zim");
        assert!(dst.exists(), "part renamed into zim_dir");
        let status: String = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT status FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .unwrap();
        let file_path: Option<String> = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT file_path FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .flatten();
        assert_eq!(status, "complete");
        assert_eq!(file_path, Some(dst.display().to_string()));
        let zims_rows: i64 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM zims WHERE name = 'utiny'",
            |q| q,
        )
        .await
        .expect("zims count")
        .unwrap();
        assert_eq!(
            zims_rows, 1,
            "resync must have registered the installed ZIM"
        );
        let articles: i64 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT count(*) FROM articles
                 WHERE zim_id = (SELECT id FROM zims WHERE name = 'utiny')",
            |q| q,
        )
        .await
        .expect("article count")
        .unwrap();
        assert!(
            articles > 0,
            "auto-index must have indexed the fixture's articles"
        );

        // Cleanup (shared single-DB suite; articles cascade off zims).
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE id = $1", |q| q.bind(id))
            .await
            .unwrap();
        crate::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| {
            q.bind("utiny")
        })
        .await
        .unwrap();
    }

    /// TEST-5 (CI-DB): the cancel-race proof — the row flipped to `cancelled`
    /// before finalize must discard the staged part: nothing installed, row
    /// stays `cancelled` with no `file_path`, `Ok(())`.
    #[tokio::test]
    async fn finalize_direct_download_discards_when_cancelled() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let tmp = tempfile::tempdir().expect("tempdir");
        let zims = crate::zim::ZimManager::new(tmp.path().to_path_buf(), pool.clone());

        let id: i32 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "INSERT INTO downloads (name, url, status)
                 VALUES ('utiny', 'http://example.net/utiny.zim', 'downloading') RETURNING id",
            |q| q,
        )
        .await
        .expect("insert downloading row")
        .unwrap();
        let part = tmp.path().join("utiny.part");
        std::fs::copy("tests/fixtures/tiny.zim", &part).expect("stage utiny.zim as .part");
        // Cancel lands before finalize (the in-loop race, made deterministic).
        crate::db::raw::execute(
            &pool,
            "UPDATE downloads SET status = 'cancelled' WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .unwrap();

        super::finalize_direct_download(&pool, &zims, id, &part)
            .await
            .expect("cancelled finalize still returns Ok");

        assert!(!part.exists(), "cancelled part must be removed");
        assert!(
            !zims.zim_dir.join("utiny.zim").exists(),
            "cancelled download must not be installed"
        );
        let status: String = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT status FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .unwrap();
        let file_path: Option<String> = crate::db::raw::fetch_scalar_optional(
            &pool,
            "SELECT file_path FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .flatten();
        assert_eq!(status, "cancelled");
        assert_eq!(file_path, None, "cancelled row keeps no file_path");

        // Cleanup (shared single-DB suite).
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE id = $1", |q| q.bind(id))
            .await
            .unwrap();
    }

    // ── PERF-11: stream_part + resume_plan + content_range_total ─────────────

    #[test]
    fn resume_plan_unit() {
        let dir = std::env::temp_dir().join(format!("zimi-resume-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        // No file → None.
        let missing = dir.join("missing.part");
        assert_eq!(super::resume_plan(&missing), None);

        // Empty file → None.
        let empty = dir.join("empty.part");
        std::fs::write(&empty, b"").unwrap();
        assert_eq!(super::resume_plan(&empty), None);

        // 7 bytes → Some(7).
        let seven = dir.join("seven.part");
        std::fs::write(&seven, b"1234567").unwrap();
        assert_eq!(super::resume_plan(&seven), Some(7));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn content_range_total_unit() {
        use axum::http::{HeaderMap, HeaderValue};
        let mut h = HeaderMap::new();
        h.insert("Content-Range", HeaderValue::from_static("bytes 6-11/12"));
        assert_eq!(super::content_range_total(&h), Some(12));

        let mut h2 = HeaderMap::new();
        h2.insert("Content-Range", HeaderValue::from_static("bytes */12"));
        assert_eq!(super::content_range_total(&h2), Some(12));

        // Absent → None.
        assert_eq!(super::content_range_total(&HeaderMap::new()), None);

        // Malformed → None.
        let mut h3 = HeaderMap::new();
        h3.insert("Content-Range", HeaderValue::from_static("bytes 6-11/x"));
        assert_eq!(super::content_range_total(&h3), None);

        // A non-`bytes` range unit is not a byte range → None.
        let mut h4 = HeaderMap::new();
        h4.insert("Content-Range", HeaderValue::from_static("items 0-9/100"));
        assert_eq!(super::content_range_total(&h4), None);
    }

    #[test]
    fn content_range_start_unit() {
        use axum::http::{HeaderMap, HeaderValue};
        let with_header = |val: &str| {
            let mut h = HeaderMap::new();
            h.insert("Content-Range", HeaderValue::from_str(val).unwrap());
            h
        };

        assert_eq!(
            super::content_range_start(&with_header("bytes 100-199/1000")),
            Some(100)
        );
        assert_eq!(
            super::content_range_start(&with_header("bytes 6-11/12")),
            Some(6)
        );
        // `*` form: S is unknowable → None.
        assert_eq!(super::content_range_start(&with_header("bytes */12")), None);
        // Absent → None.
        assert_eq!(super::content_range_start(&HeaderMap::new()), None);
        // Non-`bytes` unit → None (not a byte range).
        assert_eq!(
            super::content_range_start(&with_header("items 0-9/100")),
            None
        );
        // Malformed → None.
        assert_eq!(
            super::content_range_start(&with_header("bytes 6-11/x")),
            None
        );
        assert_eq!(super::content_range_start(&with_header("bytes 6/12")), None);
    }

    #[test]
    fn resume_206_decision_unit() {
        use axum::http::{HeaderMap, HeaderValue};
        let with_header = |val: &str| {
            let mut h = HeaderMap::new();
            h.insert("Content-Range", HeaderValue::from_str(val).unwrap());
            h
        };

        // S == from → append.
        assert_eq!(
            super::resume_206_decision(&with_header("bytes 6-11/12"), 6),
            super::Resume206Decision::Append
        );
        // S != from (a mirror serving a different revision) → fresh fallback.
        assert_eq!(
            super::resume_206_decision(&with_header("bytes 0-11/12"), 6),
            super::Resume206Decision::FallbackFresh
        );
        assert_eq!(
            super::resume_206_decision(&with_header("bytes 7-11/12"), 6),
            super::Resume206Decision::FallbackFresh
        );
        // `*` → S unknowable → treat as mismatch.
        assert_eq!(
            super::resume_206_decision(&with_header("bytes */12"), 6),
            super::Resume206Decision::FallbackFresh
        );
        // Absent header → S unknowable → treat as mismatch.
        assert_eq!(
            super::resume_206_decision(&HeaderMap::new(), 6),
            super::Resume206Decision::FallbackFresh
        );
        // Non-`bytes` unit → not a byte range → mismatch.
        assert_eq!(
            super::resume_206_decision(&with_header("items 6-11/12"), 6),
            super::Resume206Decision::FallbackFresh
        );
    }

    #[tokio::test]
    async fn stream_part_fresh_200_writes_full_body() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let body: &[u8] = b"hello world!"; // 12 bytes
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(body.to_vec())
                    .append_header("Content-Length", "12"),
            )
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        let pr = pinned_chain(
            &format!("http://testhost:{}/x.zim", server.address().port()),
            &pins,
            false,
            |c, u| c.get(u),
        )
        .await;
        let dir = std::env::temp_dir().join(format!("zimi-sp-fresh-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        let pool = dead_pool();
        let outcome = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            None,
            1024,
            &pool,
            999_999, // fake id, dead pool never touched for 12-byte body
        )
        .await
        .expect("stream_part ok");
        assert_eq!(outcome.size, 12);
        assert_eq!(outcome.total, Some(12));
        let on_disk = std::fs::read(&part).unwrap();
        assert_eq!(on_disk, body);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stream_part_resume_206_appends() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        // First request (with Range: bytes=6-) → 206.
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .and(wiremock::matchers::header("range", "bytes=6-"))
            .respond_with(
                ResponseTemplate::new(206)
                    .set_body_bytes(b"world!".to_vec())
                    .append_header("Content-Range", "bytes 6-11/12"),
            )
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        // Hop 0 goes out with the Range header (resume_from = 6).
        let pr = pinned_chain(
            &format!("http://testhost:{}/x.zim", server.address().port()),
            &pins,
            false,
            |c, u| c.get(u).header("Range", "bytes=6-"),
        )
        .await;
        let dir = std::env::temp_dir().join(format!("zimi-sp-resume-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        // Pre-write 6 bytes.
        std::fs::write(&part, b"hello ").unwrap();
        let pool = dead_pool();
        let outcome = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            Some(6),
            1024,
            &pool,
            999_999,
        )
        .await
        .expect("stream_part ok");
        assert_eq!(outcome.size, 12);
        assert_eq!(outcome.total, Some(12));
        let on_disk = std::fs::read(&part).unwrap();
        assert_eq!(on_disk, b"hello world!");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stream_part_range_ignored_200_truncates() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let body: &[u8] = b"hello world!";
        let server = MockServer::start().await;
        // Server ignores Range and returns 200 with full body.
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(body.to_vec())
                    .append_header("Content-Length", "12"),
            )
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        // Hop 0 goes out with the Range header; the mock ignores it (200).
        let pr = pinned_chain(
            &format!("http://testhost:{}/x.zim", server.address().port()),
            &pins,
            false,
            |c, u| c.get(u).header("Range", "bytes=6-"),
        )
        .await;
        let dir = std::env::temp_dir().join(format!("zimi-sp-ignored-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        // Pre-write 6 junk bytes.
        std::fs::write(&part, b"junkxx").unwrap();
        let pool = dead_pool();
        let outcome = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            Some(6),
            1024,
            &pool,
            999_999,
        )
        .await
        .expect("stream_part ok");
        assert_eq!(outcome.size, 12);
        assert_eq!(outcome.total, Some(12));
        let on_disk = std::fs::read(&part).unwrap();
        assert_eq!(on_disk, body, "file must be rewritten from 0, not appended");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stream_part_416_falls_back_fresh() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let body: &[u8] = b"hello world!";
        let server = MockServer::start().await;
        // Range request → 416.
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .and(header("range", format!("bytes={}-", 6)))
            .respond_with(ResponseTemplate::new(416).append_header("Content-Range", "bytes */12"))
            .mount(&server)
            .await;
        // Plain GET (no range) → 200 full body.
        // Mounted after the 416 mock; the 416 mock requires the range header,
        // so only un-ranged requests fall through to this one.
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(body.to_vec())
                    .append_header("Content-Length", "12"),
            )
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        // Hop 0 goes out with the Range header; the mock answers 416 (the
        // terminal response of the chain).
        let pr = pinned_chain(
            &format!("http://testhost:{}/x.zim", server.address().port()),
            &pins,
            false,
            |c, u| c.get(u).header("Range", "bytes=6-"),
        )
        .await;
        let dir = std::env::temp_dir().join(format!("zimi-sp-416-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        // Pre-write 6 stale bytes.
        std::fs::write(&part, b"stale!").unwrap();
        let pool = dead_pool();
        let outcome = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            Some(6),
            1024,
            &pool,
            999_999,
        )
        .await
        .expect("stream_part ok");
        assert_eq!(outcome.size, 12);
        assert_eq!(outcome.total, Some(12));
        let on_disk = std::fs::read(&part).unwrap();
        assert_eq!(on_disk, body, "file must be fresh after 416 fallback");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A 206 whose `Content-Range` does NOT start at the resumed offset
    /// (a mirror serving a different revision answered `bytes=6-` with the
    /// start of ITS file) must take the 416 route: discard the stale
    /// partial, reissue fresh without a Range — never append (which would
    /// splice the old prefix onto the new suffix).
    #[tokio::test]
    async fn stream_part_206_start_mismatch_falls_back_fresh() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let body: &[u8] = b"hello world!"; // 12 bytes
        let server = MockServer::start().await;
        // Range request → 206 with S=0 (a different revision answered our
        // `bytes=6-` with the start of its own file).
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .and(header("range", "bytes=6-"))
            .respond_with(
                ResponseTemplate::new(206)
                    .set_body_bytes(body.to_vec())
                    .append_header("Content-Range", "bytes 0-11/12"),
            )
            .mount(&server)
            .await;
        // Plain GET (no range) → 200 full body.
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(body.to_vec())
                    .append_header("Content-Length", "12"),
            )
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        // Hop 0 goes out with the Range header; the mock answers 206 with a
        // mismatched start offset (the terminal response of the chain).
        let pr = pinned_chain(
            &format!("http://testhost:{}/x.zim", server.address().port()),
            &pins,
            false,
            |c, u| c.get(u).header("Range", "bytes=6-"),
        )
        .await;
        assert_eq!(pr.response.status(), 206);

        let dir = std::env::temp_dir().join(format!("zimi-sp-206mm-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        // Pre-write 6 stale bytes.
        std::fs::write(&part, b"stale!").unwrap();
        let pool = dead_pool();
        let outcome = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            Some(6),
            1024,
            &pool,
            999_999,
        )
        .await
        .expect("stream_part ok");
        assert_eq!(outcome.size, 12);
        assert_eq!(
            std::fs::read(&part).unwrap(),
            body,
            "file must be rewritten fresh, not appended onto the stale prefix"
        );
        // The fallback reissue went out WITHOUT a Range header.
        let got = server.received_requests().await.unwrap_or_default();
        assert_eq!(got.len(), 2, "initial ranged GET + one fresh reissue");
        assert!(
            got[0].headers.contains_key("range"),
            "hop 0 carries the Range header"
        );
        assert!(
            !got[1].headers.contains_key("range"),
            "the reissue must be fresh (no Range)"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The 416 fallback must run the same up-front `max_bytes` check as the
    /// 200 branch: a declared size over the cap is rejected immediately —
    /// not streamed to the cap — and the stale `.part` is kept (rejection
    /// happens before the partial is discarded).
    #[tokio::test]
    async fn stream_part_416_fallback_respects_max_bytes() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        // Range request → 416.
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .and(header("range", "bytes=6-"))
            .respond_with(ResponseTemplate::new(416).append_header("Content-Range", "bytes */4096"))
            .mount(&server)
            .await;
        // Plain GET (no range) → 200 with a 4096-byte body (over the 1024
        // cap); the declared Content-Length triggers the up-front check.
        Mock::given(method("GET"))
            .and(path("/x.zim"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 4096]))
            .mount(&server)
            .await;
        let pins = [("testhost".to_string(), *server.address())];
        let pr = pinned_chain(
            &format!("http://testhost:{}/x.zim", server.address().port()),
            &pins,
            false,
            |c, u| c.get(u).header("Range", "bytes=6-"),
        )
        .await;
        assert_eq!(pr.response.status(), 416);

        let dir = std::env::temp_dir().join(format!("zimi-sp-416cap-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        std::fs::write(&part, b"stale!").unwrap();
        let pool = dead_pool();
        let err = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            Some(6),
            1024,
            &pool,
            999_999,
        )
        .await
        .err()
        .expect("an over-cap fallback must be rejected up front");
        assert!(
            err.to_string().contains("exceeds"),
            "expected the cap message, got: {err}"
        );
        assert_eq!(
            std::fs::read(&part).unwrap(),
            b"stale!",
            "a cap rejection must not discard the partial"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A transient `send()` failure of the 416 fallback reissue must NOT
    /// discard the resumable `.part` — the deletion is deferred until the
    /// reissue is confirmed a success. (Raw-TCP server: conn 1 answers the
    /// ranged GET with 416; conn 2 — the reissue — is closed before any
    /// response, i.e. a connection-level failure.)
    #[tokio::test]
    async fn stream_part_416_fallback_send_failure_keeps_part() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            // Conn 1 (the ranged GET): read the head, answer 416, close.
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = sock.read(&mut buf).await.expect("read head");
                if n == 0 {
                    return;
                }
                head.extend_from_slice(&buf[..n]);
                if head.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let _ = sock
                .write_all(
                    b"HTTP/1.1 416 Range Not Satisfiable\r\n\
                      Content-Range: bytes */12\r\n\
                      Content-Length: 0\r\n\
                      Connection: close\r\n\r\n",
                )
                .await;
            let _ = sock.shutdown().await;
            // Conn 2 (the fallback reissue): close before responding.
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let _ = sock.shutdown().await;
        });

        let url = format!("http://127.0.0.1:{}/x.zim", addr.port());
        let client = super::build_download_client(super::ClientProfile::Transfer, None)
            .expect("client builds");
        // Hop 0 (ranged) → 416, the terminal response we feed to stream_part.
        let resp = client
            .get(&url)
            .header("Range", "bytes=6-")
            .send()
            .await
            .expect("initial 416 response");
        assert_eq!(resp.status(), 416);

        let dir = std::env::temp_dir().join(format!("zimi-sp-416fail-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        std::fs::write(&part, b"stale!").unwrap();
        let pool = dead_pool();
        let _ = super::stream_part(&url, &client, resp, &part, Some(6), 1024, &pool, 999_999)
            .await
            .err()
            .expect("a failed reissue must error");
        assert!(
            part.exists(),
            "a transient fallback failure must keep the .part for resume"
        );
        assert_eq!(
            std::fs::read(&part).unwrap(),
            b"stale!",
            "the partial must be byte-identical (untouched)"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Mid-stream cancel: the in-loop cancel observation ────────────────

    /// `observe_cancel` with a DB blip must fail open like
    /// `status_if_changed`: no change reported, the `.part` untouched
    /// (kept for resume). Runs without a database (dead pool).
    #[tokio::test]
    async fn observe_cancel_db_blip_fails_open() {
        let dir = std::env::temp_dir().join(format!("zimi-obs-blip-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        std::fs::write(&part, b"partial bytes").unwrap();
        let pool = dead_pool();
        assert!(
            super::observe_cancel(&pool, 999_999, &part).await.is_none(),
            "a DB blip must not read as a status change (fail-open)"
        );
        assert!(part.exists(), "fail-open must keep the .part for resume");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CI-DB: the cancel-observation branch, driven directly. A CANCEL
    /// removes the staged `.part` and leaves the row `cancelled` with no
    /// error mark; a NON-cancelled status change (`error`) aborts the stream
    /// but KEEPS the `.part` for resume (PERF-11); a row still `downloading`
    /// reads `None` (keep streaming).
    #[tokio::test]
    async fn observe_cancel_removes_part_only_for_cancelled_rows() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");
        let dir = tempfile::tempdir().expect("tempdir");

        let insert = |name: &'static str, status: &'static str| {
            let pool = pool.clone();
            async move {
                crate::db::raw::fetch_scalar_optional(
                    &pool,
                    "INSERT INTO downloads (name, url, status) \
                     VALUES ($1, 'http://example.net/x.zim', $2) RETURNING id",
                    |q| q.bind(name).bind(status),
                )
                .await
                .expect("insert row")
                .unwrap()
            }
        };
        let flip = |id: i32, status: &'static str| {
            let pool = pool.clone();
            async move {
                crate::db::raw::execute(
                    &pool,
                    "UPDATE downloads SET status = $1 WHERE id = $2",
                    |q| q.bind(status).bind(id),
                )
                .await
                .expect("flip status")
            }
        };
        let read = |id: i32| {
            let pool = pool.clone();
            async move {
                crate::db::raw::fetch_optional(
                    &pool,
                    "SELECT status, error FROM downloads WHERE id = $1",
                    |q| q.bind(id),
                )
                .await
                .expect("read row")
                .expect("row present")
            }
        };

        let mut ids = Vec::new();

        // 1) CANCEL: the staged `.part` is removed by the observation itself.
        let id_c = insert("oc-cancelled", "downloading").await;
        ids.push(id_c);
        let part_c = dir.path().join("oc-cancelled.zim.part");
        std::fs::write(&part_c, b"partial bytes").unwrap();
        flip(id_c, "cancelled").await;
        let status = super::observe_cancel(&pool, id_c, &part_c)
            .await
            .expect("cancelled row leaves 'downloading'");
        assert_eq!(status, "cancelled");
        assert!(
            !part_c.exists(),
            "cancelled .part must be removed in the loop"
        );
        let (st, err): (String, Option<String>) = read(id_c).await;
        assert_eq!(st, "cancelled", "the row must stay cancelled");
        assert_eq!(err, None, "a cancel must not be error-marked");

        // 2) Non-cancelled change (error): the `.part` is KEPT for resume.
        let id_e = insert("oc-errored", "downloading").await;
        ids.push(id_e);
        let part_e = dir.path().join("oc-errored.zim.part");
        std::fs::write(&part_e, b"partial bytes").unwrap();
        flip(id_e, "error").await;
        let status = super::observe_cancel(&pool, id_e, &part_e)
            .await
            .expect("errored row leaves 'downloading'");
        assert_eq!(status, "error");
        assert!(
            part_e.exists(),
            "a non-cancelled status change must keep the .part for resume"
        );

        // 3) Still downloading: no change observed, nothing removed.
        let id_d = insert("oc-downloading", "downloading").await;
        ids.push(id_d);
        let part_d = dir.path().join("oc-downloading.zim.part");
        std::fs::write(&part_d, b"partial bytes").unwrap();
        assert!(
            super::observe_cancel(&pool, id_d, &part_d).await.is_none(),
            "a row still downloading must not abort the stream"
        );
        assert!(part_d.exists());

        // Cleanup (shared single-DB suite).
        crate::db::raw::execute(
            &pool,
            "DELETE FROM downloads WHERE id IN ($1, $2, $3)",
            |q| q.bind(id_c).bind(id_e).bind(id_d),
        )
        .await
        .unwrap();
    }

    /// CI-DB: mid-stream cancel, end-to-end. The row is `downloading` and
    /// flips to `cancelled` 2 s after the request is in flight; the raw-TCP
    /// server sends the headers immediately but holds the body back 7 s
    /// (wiremock's delay covers the whole response, which would move the
    /// in-loop clock with it), so the ~5 s in-loop cancel check fires on the
    /// first chunk. The declared total (1000) exceeds the bytes received
    /// (64) — the truncation guard must be SKIPPED for the cancelled row:
    /// `stream_part` returns `Ok` with `cancelled = true`, the `.part` is
    /// removed, and the row stays `cancelled` with `error` NULL. (Pre-fix:
    /// the in-loop `break` hit the truncation guard's `Err` and the `.part`
    /// lingered until the next startup reconcile sweep.)
    #[tokio::test]
    async fn stream_part_cancel_mid_stream_removes_part_no_error() {
        let Some((pool, _db_gate)) = test_pool().await else {
            return;
        };
        crate::db::migrate::run_migrations(&pool)
            .await
            .expect("migrations");

        // Headers now, `SENT` body bytes after `BODY_DELAY_SECS`, then close.
        const BODY_DELAY_SECS: u64 = 7;
        const DECLARED: u64 = 1000;
        const SENT: usize = 64;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            // Read the request head (everything up to the blank line).
            let mut head = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = sock.read(&mut buf).await.expect("read head");
                if n == 0 {
                    return;
                }
                head.extend_from_slice(&buf[..n]);
                if head.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let _ = sock
                .write_all(
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {DECLARED}\r\n\r\n").as_bytes(),
                )
                .await;
            tokio::time::sleep(std::time::Duration::from_secs(BODY_DELAY_SECS)).await;
            let _ = sock.write_all(&[0u8; SENT]).await;
            let _ = sock.shutdown().await;
        });

        let id: i32 = crate::db::raw::fetch_scalar_optional(
            &pool,
            "INSERT INTO downloads (name, url, status) \
             VALUES ('oc-midstream', 'http://example.net/x.zim', 'downloading') RETURNING id",
            |q| q,
        )
        .await
        .expect("insert downloading row")
        .unwrap();

        let tmp = tempfile::tempdir().expect("tempdir");
        let part = tmp.path().join("x.zim.part");

        // The cancel lands 2 s in: the request has been in flight since
        // t≈0, the first chunk (hence the in-loop cancel check) arrives at
        // t≈7 s — well past the 5 s window and well after the flip.
        let flip_pool = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            crate::db::raw::execute(
                &flip_pool,
                "UPDATE downloads SET status = 'cancelled' WHERE id = $1",
                |q| q.bind(id),
            )
            .await
            .expect("flip to cancelled");
        });

        // SEC-1 chain (no redirects here — the raw TCP server answers the
        // single request directly): the pinned client lands on the same
        // final URL and carries the terminal 200 response.
        let pins = [("testhost".to_string(), addr)];
        let pr = pinned_chain(
            &format!("http://testhost:{}/x.zim", addr.port()),
            &pins,
            false,
            |c, u| c.get(u),
        )
        .await;

        let outcome = super::stream_part(
            &pr.url,
            &pr.client,
            pr.response,
            &part,
            None,
            10_000,
            &pool,
            id,
        )
        .await
        .expect("a mid-stream cancel is Ok (no error mark), not an Err");
        assert!(
            outcome.cancelled,
            "the in-loop cancel check must observe the cancelled row"
        );
        assert_eq!(outcome.size, SENT as u64);
        assert_eq!(outcome.total, Some(DECLARED));
        assert!(
            !part.exists(),
            "the cancelled .part must be removed mid-stream (no startup-sweep wait)"
        );

        let (status, error): (String, Option<String>) = crate::db::raw::fetch_optional(
            &pool,
            "SELECT status, error FROM downloads WHERE id = $1",
            |q| q.bind(id),
        )
        .await
        .expect("read row")
        .expect("row present");
        assert_eq!(status, "cancelled", "the row must stay cancelled");
        assert_eq!(error, None, "a mid-stream cancel must not be error-marked");

        // Cleanup (shared single-DB suite).
        crate::db::raw::execute(&pool, "DELETE FROM downloads WHERE id = $1", |q| q.bind(id))
            .await
            .unwrap();
    }
}

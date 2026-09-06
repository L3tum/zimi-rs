//! Direct-HTTP download path: client build, Range-resume streaming, finalize.

use super::*;

/// Build the download HTTP client with a redirect-safe SSRF guard: reqwest
/// follows redirects by default, and a 3xx to an internal host would bypass
/// the initial-URL check, so re-validate every hop with the same rules
/// (reading the live `downloads.allow_private_networks` setting per hop). A
/// blocked hop aborts the whole request. `pin` optionally fixes a
/// domain→addr resolution (validated up front to close the DNS-rebinding
/// window).
/// Timeout profile for poller HTTP clients.
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
    settings: &SettingsCache,
    profile: ClientProfile,
    pin: Option<(String, std::net::SocketAddr)>,
) -> Result<reqwest::Client> {
    let (total, connect, read) = client_timeouts(profile);
    // ARCH-3: the private-range gate is a provider closure over the live
    // settings cache, so netguard itself carries no settings dependency.
    let settings = settings.clone();
    let provider: std::sync::Arc<dyn Fn() -> bool + Send + Sync> =
        std::sync::Arc::new(move || settings.downloads_allow_private_networks());
    let mut builder =
        reqwest::Client::builder().redirect(crate::netguard::redirect_policy(provider));
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
}

/// Instantaneous speed from a byte window over its *actual* elapsed time
/// (BUG-19: the old `window_bytes / 2` assumed the 2 s check fired on time;
/// under load a late check understates elapsed time and overstates speed).
pub(crate) fn window_speed(window_bytes: u64, elapsed: std::time::Duration) -> i64 {
    let secs = elapsed.as_secs_f64().max(0.001); // floor: avoid div-by-zero
    (window_bytes as f64 / secs).round() as i64
}

/// Parse the `FULL` part of `Content-Range: bytes S-T/FULL` (or
/// `bytes */FULL`); `None` when absent/unparseable.
fn content_range_total(headers: &axum::http::HeaderMap) -> Option<u64> {
    let val = headers
        .get(axum::http::header::CONTENT_RANGE)?
        .to_str()
        .ok()?;
    // Expect "bytes S-T/FULL" or "bytes */FULL"
    let slash = val.rfind('/')?;
    val[slash + 1..].trim().parse().ok()
}

/// Stream HTTP body into `part`, with Range resume support (PERF-11).
///
/// Response-shape dispatch:
/// - **206 + Range sent** → append to the existing `.part`; `total` from
///   `Content-Range` (or `from + Content-Length` fallback).
/// - **416 + Range sent** → source changed (or the `.part` was already
///   complete): discard the stale partial, reissue once, fresh, without
///   Range.
/// - **200** → fresh download (also covers servers that ignore Range);
///   truncates any existing partial.
/// - Anything else → `mark_error` + `Err`.
///
/// In-loop: over-cap check (against `received`, which includes the resumed
/// prefix), 2 s progress updates, ~5 s cancel check.
/// Post-loop: truncation guard — a declared total that was not fully
/// received means the body was cut short.
#[allow(clippy::too_many_arguments)]
async fn stream_part(
    client: &reqwest::Client,
    url: &str,
    part: &Path,
    resume_from: Option<u64>,
    max_bytes: u64,
    db: &Pool,
    id: i32,
) -> Result<StreamOutcome> {
    use futures::StreamExt;
    use tokio::io::AsyncWriteExt;

    let mut req = client.get(url);
    if let Some(s) = resume_from {
        req = req.header(axum::http::header::RANGE, format!("bytes={s}-"));
    }
    let resp = req.send().await?;

    // Determine the file open mode, the starting `received` offset, the
    // declared `total`, and the bytes stream to consume.
    enum FileStream {
        /// 206 append: file opened in append mode, `from` = resumed offset.
        Append(tokio::fs::File, u64, Option<u64>, reqwest::Response),
        /// Fresh/416-fallback: file opened in create/truncate mode.
        Fresh(tokio::fs::File, Option<u64>, reqwest::Response),
    }
    let fs = match (resp.status(), resume_from) {
        (s, Some(from)) if s == axum::http::StatusCode::PARTIAL_CONTENT => {
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
        (s, Some(_)) if s.as_u16() == 416 => {
            // 416: source changed (or the .part was already complete) —
            // discard the stale partial and reissue once, fresh, without a Range.
            let _ = std::fs::remove_file(part);
            let resp2 = client.get(url).send().await?;
            if !resp2.status().is_success() {
                let msg = format!("HTTP {}", resp2.status());
                mark_error(db, id, &msg).await;
                return Err(Error::Torrent {
                    kind: TorrentKind::Other,
                    msg,
                });
            }
            let f = tokio::fs::File::create(part).await?;
            FileStream::Fresh(f, resp2.content_length(), resp2)
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
        // 'downloading' (cancelled, errored, etc.), abort the stream.
        if last_cancel_check.elapsed() >= Duration::from_secs(5) {
            last_cancel_check = Instant::now();
            if let Some(status) = status_if_changed(db, id, "downloading").await {
                tracing::info!("direct download {id} aborted: status changed to '{status}'");
                break;
            }
        }
    }
    file.flush().await?;
    drop(file);

    // Truncated-stream guard: a declared total that was not fully received
    // means the body was cut short (server lied / connection dropped).
    if let Some(t) = total {
        if received < t {
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
    })
}

/// Stream a `.zim` file straight into `zim_dir` (as a `.part` file), then
/// hand off to [`finalize_direct_download`] (verify, atomic rename, resync,
/// index).
///
/// Cancel-coverage note (TEST-5): the in-loop cancel check in
/// [`stream_part`] is covered only via the `finalize_direct_download` seam
/// tests — wiremock binds 127.0.0.1 and netguard hard-blocks loopback, so it
/// cannot drive the HTTP stream loop.
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
    // SSRF guard before any network I/O.
    let allow_private = dp.allow_private_networks;
    validate_download_url(url, allow_private)?;

    // DNS-level SSRF guard: for named hosts, resolve up front, reject if any
    // address is blocked, and pin the validated resolution so a rebinding
    // answer can't reach an internal host mid-download. (Per-download client:
    // downloads are infrequent, so building one is cheap.)
    let client = match resolve_download_host(url, allow_private, false).await? {
        Some((host, ip)) => {
            tracing::debug!("pinned {host} -> {} for download", ip);
            build_download_client(settings, ClientProfile::Transfer, Some((host, ip)))?
        }
        None => http.clone(),
    };

    // Keep the .part file on failure for resume (PERF-11). Disabled once
    // the file is safely renamed into place or explicitly removed (inside
    // [`finalize_direct_download`]).
    let part_guard = PartGuard::new(part);

    let resume_from = resume_plan(part);
    if let Some(s) = resume_from {
        tracing::info!("direct download {id}: resuming from {s} bytes");
    }
    let max_bytes = dp.max_bytes;
    let outcome = stream_part(&client, url, part, resume_from, max_bytes, db, id).await?;
    tracing::debug!(
        "direct download {id}: {} bytes (total: {:?})",
        outcome.size,
        outcome.total
    );

    // Post-stream finalize (TEST-5 seam): cancel re-checks, verify, rename,
    // resync, guarded row update, auto-index.
    finalize_direct_download(db, zims, id, part, part_guard).await
}

/// Post-stream finalize for a direct download (TEST-5 seam, extracted from
/// [`direct_download`]): the cancel re-check before verify, the libzim
/// verify, the cancel check before rename, the atomic rename + resync, the
/// guarded `status = 'downloading'` row update, and the auto-index.
///
/// Cancel policy (RECONCILE with PERF-11 resume): a CANCEL always removes the
/// staged file — at all three cancel-observation sites (pre-verify,
/// pre-rename, post-rename `updated == 0`). Only transient network failures
/// in the stream loop keep the `.part` for resume.
async fn finalize_direct_download(
    db: &Pool,
    zims: &Arc<ZimManager>,
    id: i32,
    part: &Path,
    mut part_guard: PartGuard,
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
            part_guard.disable();
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
            part_guard.disable();
            return Ok(());
        }
    }

    // Atomic rename into place (same directory).
    let dst =
        zims.zim_dir.join(part.file_stem().ok_or_else(|| {
            Error::InvalidInput(format!("bad part file name: {}", part.display()))
        })?);
    std::fs::rename(part, &dst)?;
    part_guard.disable(); // renamed into place — no cleanup needed.

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
    use super::super::tests::download::{download_settings, test_pool};
    use crate::testing::dead_pool;

    // ── B5.4: download-client SSRF / redirect / pinning (wiremock) ───────────
    // In-module so the private `build_download_client` / `ClientProfile` are
    // reachable; the builder-failure path is covered by the `client_or_err`
    // unit test (propagation instead of the old silent `unwrap_or_default()`).

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
        let settings = download_settings();
        // Pin `testhost` (no real DNS) at the MockServer socket: the fetch of
        // `http://testhost:{port}` succeeds only if the client carries the pin.
        let client = super::build_download_client(
            &settings,
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

    #[tokio::test]
    async fn download_client_redirect_to_metadata_blocked() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let settings = download_settings();

        // Redirect hops are re-validated with allow_loopback=false for
        // downloads, so both the link-local metadata IP and loopback are
        // refused even though the initial hop is a pinned local socket.
        for (location, why) in [
            ("http://169.254.169.254/", "link-local metadata"),
            (
                "http://127.0.0.1:9/x",
                "loopback (downloads never allow it)",
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/hop"))
                .respond_with(ResponseTemplate::new(302).append_header("Location", location))
                .mount(&server)
                .await;
            let client = super::build_download_client(
                &settings,
                super::ClientProfile::Transfer,
                Some(("testhost".into(), *server.address())),
            )
            .expect("client builds");
            let err = client
                .get(format!("http://testhost:{}/hop", server.address().port()))
                .send()
                .await
                .expect_err("redirect must be refused");
            // The policy's message lives on the error's source chain (reqwest
            // wraps it as a generic "error following redirect" kind).
            let mut chain = Vec::new();
            let mut cur: Option<&dyn std::error::Error> = Some(&err);
            while let Some(e) = cur {
                chain.push(e.to_string());
                cur = e.source();
            }
            let chain_text = chain.join(" | ");
            assert!(
                chain_text.contains("redirect target rejected"),
                "{why}: expected redirect refusal, got: {chain_text}"
            );
        }
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

        super::finalize_direct_download(
            &pool,
            &zims,
            id,
            &part,
            super::super::PartGuard::new(&part),
        )
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

        super::finalize_direct_download(
            &pool,
            &zims,
            id,
            &part,
            super::super::PartGuard::new(&part),
        )
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
        let settings = download_settings();
        let client = super::build_download_client(
            &settings,
            super::ClientProfile::Transfer,
            Some(("testhost".into(), *server.address())),
        )
        .expect("client builds");
        let dir = std::env::temp_dir().join(format!("zimi-sp-fresh-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        let pool = dead_pool();
        let outcome = super::stream_part(
            &client,
            &format!("http://testhost:{}/x.zim", server.address().port()),
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
        let settings = download_settings();
        let client = super::build_download_client(
            &settings,
            super::ClientProfile::Transfer,
            Some(("testhost".into(), *server.address())),
        )
        .expect("client builds");
        let dir = std::env::temp_dir().join(format!("zimi-sp-resume-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        // Pre-write 6 bytes.
        std::fs::write(&part, b"hello ").unwrap();
        let pool = dead_pool();
        let outcome = super::stream_part(
            &client,
            &format!("http://testhost:{}/x.zim", server.address().port()),
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
        let settings = download_settings();
        let client = super::build_download_client(
            &settings,
            super::ClientProfile::Transfer,
            Some(("testhost".into(), *server.address())),
        )
        .expect("client builds");
        let dir = std::env::temp_dir().join(format!("zimi-sp-ignored-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        // Pre-write 6 junk bytes.
        std::fs::write(&part, b"junkxx").unwrap();
        let pool = dead_pool();
        let outcome = super::stream_part(
            &client,
            &format!("http://testhost:{}/x.zim", server.address().port()),
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
        let settings = download_settings();
        let client = super::build_download_client(
            &settings,
            super::ClientProfile::Transfer,
            Some(("testhost".into(), *server.address())),
        )
        .expect("client builds");
        let dir = std::env::temp_dir().join(format!("zimi-sp-416-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("x.zim.part");
        // Pre-write 6 stale bytes.
        std::fs::write(&part, b"stale!").unwrap();
        let pool = dead_pool();
        let outcome = super::stream_part(
            &client,
            &format!("http://testhost:{}/x.zim", server.address().port()),
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
}

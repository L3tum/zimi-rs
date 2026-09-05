//! Per-tick stats helpers for in-progress download rows (M-poller-n1).

use crate::torrent::TorrentInfo;

use super::*;

/// Convert qBittorrent KiB/s speed to bytes per second.
pub(super) fn kibs_to_bps(kibs: i64) -> i64 {
    kibs * 1024
}

/// ETA in seconds: remaining **bytes** / (dlspeed **KiB/s** → bytes/s).
/// `None` when there is no speed to estimate from.
pub(super) fn eta_secs(remaining_bytes: i64, dlspeed_kibs: i64) -> Option<i64> {
    (dlspeed_kibs > 0).then(|| remaining_bytes / dlspeed_kibs.saturating_mul(1024))
}

/// Whether a `downloading` row's stats changed enough this tick to warrant a
/// DB write (M-poller-n1). `old_*` are the values currently in the row; `t`
/// is the fresh qBittorrent snapshot. We skip the write (return `false`) only
/// when nothing meaningful moved — progress under 1%, no speed, and unchanged
/// ratio / seeds. A non-zero download or upload speed always counts as a
/// change (it drives `eta_secs` and the live speed display). This is a
/// heuristic that only affects write *frequency*, never correctness: a skipped
/// write is always corrected on the next meaningful change (completion / fatal
/// / hash-bind / missing transitions stay per-row).
pub(super) fn stats_changed(
    old_progress: f32,
    old_ratio: Option<f32>,
    old_num_seeds: Option<i64>,
    t: &TorrentInfo,
) -> bool {
    (t.progress - f64::from(old_progress)).abs() >= 0.01
        || (t.ratio as f32) != old_ratio.unwrap_or(0.0)
        || t.num_seeds != old_num_seeds.unwrap_or(0)
        || t.dlspeed != 0
        || t.upspeed != 0
}

/// One in-progress row's fresh stats for the tick's stats UPDATE (M-poller-n1):
/// `(id, progress, speed_bps, eta_secs, ratio, up_speed_bps, num_seeds)`. A
/// named alias (not an inline tuple) so the 7-element type stays under clippy's
/// `type_complexity` threshold at its use sites.
pub type StatsRow = (i32, f32, i64, Option<i64>, Option<f32>, i64, i64);

/// Apply this tick's in-progress stat changes to `downloads` as a **single**
/// batched statement (PERF 5): every changed row is written in one round-trip
/// via `UPDATE … FROM unnest(…)` instead of one point `UPDATE` per row
/// (PONY-S4). The single statement is atomic — either all changed rows are
/// written or none is (the old per-row loop committed each row independently,
/// so a mid-flush failure left a partial write). Recovery is identical either
/// way: an un-written row's stats keep changing and are re-computed next tick.
/// Only rows present in `changed` are touched (matched by `id`); omitted rows
/// keep their exact `updated_at`. `eta_secs`/`ratio` are `Option`-typed to
/// match the nullable columns (the `unnest` arrays carry NULLs for those).
/// Exposed as `pub` so the statement is integration-testable without driving
/// the full poller.
pub async fn apply_stats_batch(pool: &Pool, changed: &[StatsRow]) -> Result<()> {
    if changed.is_empty() {
        return Ok(());
    }
    // Split the 7-tuple rows into per-column arrays for a single
    // `UPDATE … FROM unnest(…)`. `eta_secs`/`ratio` keep their NULLs.
    let ids: Vec<i32> = changed.iter().map(|r| r.0).collect();
    let progress: Vec<f32> = changed.iter().map(|r| r.1).collect();
    let speed_bps: Vec<i64> = changed.iter().map(|r| r.2).collect();
    let eta_secs: Vec<Option<i64>> = changed.iter().map(|r| r.3).collect();
    let ratio: Vec<Option<f32>> = changed.iter().map(|r| r.4).collect();
    let up_speed_bps: Vec<i64> = changed.iter().map(|r| r.5).collect();
    let num_seeds: Vec<i64> = changed.iter().map(|r| r.6).collect();

    const STATS_SQL: &str = "UPDATE downloads d\n\
         SET progress = v.progress,\n\
             speed_bps = v.speed_bps,\n\
             eta_secs = v.eta_secs,\n\
             ratio = v.ratio,\n\
             up_speed_bps = v.up_speed_bps,\n\
             num_seeds = v.num_seeds,\n\
             updated_at = now()\n\
         FROM (SELECT * FROM unnest(\n\
                 $1::int[], $2::float4[], $3::bigint[], $4::bigint[],\n\
                 $5::float4[], $6::bigint[], $7::bigint[]\n\
             ) AS u(id, progress, speed_bps, eta_secs, ratio, up_speed_bps, num_seeds)\n\
         ) v\n\
         WHERE d.id = v.id";

    let params: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![
        &ids,
        &progress,
        &speed_bps,
        &eta_secs,
        &ratio,
        &up_speed_bps,
        &num_seeds,
    ];
    let c = pool.get().await.map_err(Error::Pool)?;
    c.execute(STATS_SQL, &params)
        .await
        .map_err(Error::Database)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::state::tinfo;

    use super::*;

    #[test]
    fn seeding_action_refresh_on_live_torrent() {
        assert_eq!(
            super::super::seeding_row_action(Some(&tinfo("uploading", 1.0)), true),
            super::super::SeedingAction::Refresh,
        );
        assert_eq!(
            super::super::seeding_row_action(Some(&tinfo("stalledUP", 1.0)), false),
            super::super::SeedingAction::Refresh,
        );
        // A torrent paused at the ratio cap keeps refreshing (not fatal).
        assert_eq!(
            super::super::seeding_row_action(Some(&tinfo("pausedUP", 1.0)), true),
            super::super::SeedingAction::Refresh,
        );
    }

    #[test]
    fn seeding_action_fatal_settles_to_complete() {
        assert_eq!(
            super::super::seeding_row_action(Some(&tinfo("error", 1.0)), true),
            super::super::SeedingAction::FatalDone,
        );
        assert_eq!(
            super::super::seeding_row_action(Some(&tinfo("missingFiles", 1.0)), false),
            super::super::SeedingAction::FatalDone,
        );
    }

    #[test]
    fn seeding_action_missing_after_grace_settles() {
        // Grace expired (qB reachable, torrent gone) → settle to complete.
        assert_eq!(
            super::super::seeding_row_action(None, true),
            super::super::SeedingAction::Done
        );
        // Within the grace window, or qB unreachable this tick → wait.
        assert_eq!(
            super::super::seeding_row_action(None, false),
            super::super::SeedingAction::Pending,
        );
    }

    #[test]
    fn kibs_to_bps_is_1024x() {
        assert_eq!(super::kibs_to_bps(0), 0);
        assert_eq!(super::kibs_to_bps(1), 1024);
        assert_eq!(super::kibs_to_bps(1024), 1_048_576);
    }

    #[test]
    fn eta_secs_not_inflated() {
        // 1 GiB remaining at 100 KiB/s → 1_073_741_824 / 102_400 = 10_485 s
        // (≈ 2.9 h), **not** the old 1024× value of 10_737_418 s.
        assert_eq!(super::eta_secs(1_073_741_824, 100), Some(10_485));
        assert_eq!(super::eta_secs(0, 100), Some(0));
        assert_eq!(super::eta_secs(1_000, 0), None);
        assert_eq!(super::eta_secs(1_000, -5), None);
    }

    #[test]
    fn stats_changed_matrix() {
        fn ti(
            progress: f64,
            dlspeed: i64,
            upspeed: i64,
            ratio: f64,
            num_seeds: i64,
        ) -> TorrentInfo {
            TorrentInfo {
                hash: "h".into(),
                name: "n".into(),
                progress,
                state: "downloading".into(),
                dlspeed,
                upspeed,
                ratio,
                category: None,
                save_path: None,
                content_path: None,
                size: 0,
                downloaded: 0,
                num_seeds,
                err_str: None,
            }
        }
        // No change: progress within 1%, no speed, ratio + seeds unchanged.
        assert!(!super::stats_changed(
            0.5,
            Some(1.0),
            Some(3),
            &ti(0.5, 0, 0, 1.0, 3)
        ));
        assert!(!super::stats_changed(
            0.5,
            Some(1.0),
            Some(3),
            &ti(0.505, 0, 0, 1.0, 3)
        ));
        // Progress delta >= 0.01 → changed.
        assert!(super::stats_changed(
            0.5,
            Some(1.0),
            Some(3),
            &ti(0.51, 0, 0, 1.0, 3)
        ));
        // Speed (download or upload) now non-zero → changed.
        assert!(super::stats_changed(
            0.5,
            Some(1.0),
            Some(3),
            &ti(0.5, 100, 0, 1.0, 3)
        ));
        assert!(super::stats_changed(
            0.5,
            Some(1.0),
            Some(3),
            &ti(0.5, 0, 50, 1.0, 3)
        ));
        // Ratio changed → changed.
        assert!(super::stats_changed(
            0.5,
            Some(1.0),
            Some(3),
            &ti(0.5, 0, 0, 1.5, 3)
        ));
        // Seeds changed → changed.
        assert!(super::stats_changed(
            0.5,
            Some(1.0),
            Some(3),
            &ti(0.5, 0, 0, 1.0, 4)
        ));
        // NULL old ratio/seeds (None) compare against the zero defaults.
        assert!(!super::stats_changed(
            0.0,
            None,
            None,
            &ti(0.0, 0, 0, 0.0, 0)
        ));
    }

    #[test]
    fn window_speed_exact() {
        // 1 MiB over 2.0 s → 524_288 B/s.
        assert_eq!(
            super::super::direct::window_speed(1_048_576, std::time::Duration::from_secs(2)),
            524_288
        );
    }

    #[test]
    fn window_speed_late_check() {
        // 1 MiB over 30 s → ~34_953 (the old code would have reported 524_288).
        let speed =
            super::super::direct::window_speed(1_048_576, std::time::Duration::from_secs(30));
        assert!((speed - 34_953).abs() < 10, "got {speed}");
    }

    #[test]
    fn window_speed_floor() {
        // 5 bytes / 0 ms → 5_000 (no div-by-zero, no panic).
        assert_eq!(
            super::super::direct::window_speed(5, std::time::Duration::from_millis(0)),
            5_000
        );
    }
}

# qBittorrent Integration

zimservice can use a qBittorrent instance to download (and keep seeding)
ZIM files. Configure it with the `QBITTORRENT_URL` / `QBITTORRENT_USER` /
`QBITTORRENT_PASS` environment variables (there is no config file) and manage
the remaining options in the settings page.

## Download lifecycle

Every download row in the service tracks one of these states:

| State | Meaning |
| --- | --- |
| `queued` | Accepted, waiting for an active-download slot. |
| `downloading` | In progress in qBittorrent (progress, download speed, and ETA are refreshed on every ~5 s poll). |
| `complete` | The `.zim` was verified and installed into the ZIM directory (hardlinked by default, copied across devices) and the torrent is no longer in qBittorrent. |
| `seeding` | The `.zim` is installed **and** the torrent is still in qBittorrent, giving back to the swarm. The row shows the current **seeding ratio**, **upload speed**, **download speed**, and **seeder count**, refreshed on every poll. |
| `error` | A fatal problem occurred (bad URL, install failure, torrent missing, …). |
| `cancelled` | The user cancelled a queued or in-progress download. |

A `downloading` torrent becomes `seeding` when it completes **and**
`torrent.keep_completed` is enabled (the default) — the torrent then stays
in qBittorrent and the per-torrent seed-ratio cap
(`torrent.seed_ratio`, default 2.0; `0` = unlimited) is applied. When
`keep_completed` is disabled the row goes straight to `complete` and the
torrent is deleted from qBittorrent. A `seeding` row settles to `complete`
once the torrent disappears from qBittorrent (e.g. the user removed it, or
qBittorrent stopped it at the ratio cap) after a short grace period.

## Why seeding keeps working after install

The default `torrent.file_strategy` is **hardlink**: the installed ZIM is a
hard link to the file qBittorrent downloaded, so both paths point at the same
inode and the torrent keeps seeding the very file the library serves. When a
hard link is impossible (different filesystems), the file is **copied**
instead — qBittorrent keeps its own copy for seeding.

## Notes

- Direct `.zim` HTTP URLs never go through qBittorrent, so they have no
  seeding state or ratio.
- **Pointing qBittorrent at a LAN address:** the SSRF guard blocks the qB Web
  API from resolving to a private/reserved address (RFC1918, ULA, CGNAT) by
  default, so a misconfigured or reused `torrent.url` can't reach an internal
  service. If your qBittorrent Web API lives on a private/LAN host
  (e.g. `http://192.168.1.100:8080`), enable the `torrent.allow_private_networks`
  setting (Settings → admin) to opt in. Loopback (`localhost` / `127.0.0.1`)
  is always allowed for a local qB install, and the always-blocked ranges
  (cloud-metadata `169.254.169.254`, link-local, IETF-doc, NAT64) stay blocked
  even with the opt-in. Flipping the setting reconnects the client on the next
  poll tick (no restart needed).
- There is no in-UI cancel for `seeding` rows — remove the torrent from
  qBittorrent directly if you want to stop seeding.
- Re-downloading the URL of an already-completed or seeding torrent is
  allowed (the unique-URL constraint only covers `queued`/`downloading`).

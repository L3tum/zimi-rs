// Real-browser smoke tests (Playwright, headless Chromium) for the
// zimservice web UI.
//
// The jsdom suite (tests/web/*.test.mjs) boots the page scripts against a
// simulated DOM; these tests close the last gap — a real browser engine
// against a LIVE zimservice process — so page-level wiring faults (script
// load order, handler attachment, render-path throws, CSS/layout) are
// caught end to end (2026-10 review, Tests: the web UI had no
// real-browser coverage).
//
// Requires a running zimservice instance:
//   CI: the `web-browser-smoke` job (`.github/workflows/ci.yml`).
//   Local: start the server on 127.0.0.1:8877 (open mode, loopback) or set
//   ZIMSERVICE_BASE_URL, then `make web-browser-test`.

import { test, expect } from '@playwright/test';

// ── library page ─────────────────────────────────────────────────────────────

test('library: loads in a real browser and renders the live status bar', async ({
  page,
}) => {
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e)));

  await page.goto('/', { waitUntil: 'networkidle' });

  // loadLibrary() replaced the static "Loading…" placeholder with the live
  // status line (ZIM count / articles indexed / qBittorrent state).
  const bar = page.locator('#statusBar');
  await expect(bar).not.toContainText('Loading');
  await expect(bar).toContainText(/ZIMs/);
  await expect(bar).toContainText(/articles indexed/);

  // No uncaught exceptions during load (script order, handler wiring).
  expect(errors, `uncaught page errors: ${errors.join('; ')}`).toEqual([]);
});

// ── search page ──────────────────────────────────────────────────────────────

test('search: submits a query in a real browser and renders the result state', async ({
  page,
}) => {
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e)));

  await page.goto('/search.html', { waitUntil: 'networkidle' });

  // Filter dropdowns are populated from /list before the query is submitted
  // (empty DB → only the default "All" options).
  const zimOptions = await page.locator('#zim option').count();
  expect(zimOptions, 'at least the default All-ZIMs option').toBeGreaterThanOrEqual(
    1
  );

  await page.fill('#q', 'zimservice');
  await page.click('#go');

  // Empty-DB search: 200 with zero results → the stats line shows "0
  // results" and the empty-state div is visible. A wiring fault would leave
  // the "Searching…" state up, pop the error state, or throw in the page.
  await expect(page.locator('#stats')).toContainText(/0 results/);
  await expect(page.locator('#empty')).toBeVisible();
  await expect(page.locator('#loading')).toBeHidden();

  expect(errors, `uncaught page errors: ${errors.join('; ')}`).toEqual([]);
});

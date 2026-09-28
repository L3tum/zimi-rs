// Playwright config for the real-browser web UI smoke tests
// (tests/web/browser-smoke.spec.mjs).
//
// The tests run against a LIVE zimservice instance — CI starts one in the
// `web-browser-smoke` job; locally point ZIMSERVICE_BASE_URL at your server
// (default http://127.0.0.1:8877). The jsdom suites (*.test.mjs) are
// excluded on purpose: they run under `node --test` (make web-test), not
// Playwright.

import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: /browser-smoke\.spec\.mjs$/,
  timeout: 30_000,
  // One worker: a single live server backs the whole suite.
  workers: 1,
  use: {
    baseURL: process.env.ZIMSERVICE_BASE_URL ?? 'http://127.0.0.1:8877',
  },
});

// Playwright config for the real-browser web UI tests
// (tests/web/*.spec.mjs: smoke + core flows).
//
// The tests run against a LIVE zimservice instance — CI starts one in the
// `web-browser-smoke` job; locally point ZIMSERVICE_BASE_URL at your server
// (default http://127.0.0.1:8877). The password-mode flow spec starts its
// own second server (ZIMSERVICE_PW_* env, see the spec header). The jsdom
// suites (*.test.mjs) are excluded on purpose: they run under `node --test`
// (make web-test), not Playwright.

import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: /\.spec\.mjs$/,
  timeout: 30_000,
  // One worker: a single live server backs the whole suite.
  workers: 1,
  use: {
    baseURL: process.env.ZIMSERVICE_BASE_URL ?? 'http://127.0.0.1:8877',
  },
});

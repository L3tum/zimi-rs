// Real-browser password-mode auth flow (Playwright, headless Chromium):
// the first mutating API call 401s → the browser prompt() for the admin
// password → a wrong password surfaces the error message (no crash) → a
// correct password saves successfully, the token is stored, and the next
// mutation rides along without a new prompt.
//
// In password mode on a loopback bind, reads (pages + API GETs) stay open
// and only mutating requests are gated — so the prompt fires on the first
// save, not on page load (web/common.js apiFetch 401 handler).
//
// The main CI server is open-mode, so this spec starts its OWN zimservice
// in password mode on a second port. access.mode / access.admin_password
// are seeded from the environment only on first settings load and never
// re-seeded, so the server must own a FRESH database to seed its own
// credentials (a stale DB with require_auth_for_reads=true would 401 the
// page HTML itself and is rejected in beforeAll).
//
// Env:
//   ZIMSERVICE_PW_DATABASE_URL — fresh, disposable database URL for the
//   password-mode server (required; the spec skips without it)
//   ZIMSERVICE_PW_PORT — port for the password-mode server (default 8878)
//   ZIMSERVICE_PW_PASSWORD — admin password (default "e2e-password-123")
//   ZIMSERVICE_BIN — zimservice binary (default target/release/zimservice
//   relative to the repo root)

import { test, expect } from '@playwright/test';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const PW_PORT = Number(process.env.ZIMSERVICE_PW_PORT ?? 8878);
const PW_PASSWORD = process.env.ZIMSERVICE_PW_PASSWORD ?? 'e2e-password-123';
const PW_WRONG = 'e2e-wrong-password';
const PW_URL = `http://127.0.0.1:${PW_PORT}`;
// Prompt text from web/common.js _promptToken().
const PROMPT_HINT = 'admin password';

let server;
let zimDir;
let logBuf = '';

function logTail() {
  return logBuf.slice(-4000);
}

async function waitHealthy() {
  for (let i = 0; i < 90; i++) {
    try {
      const r = await fetch(`${PW_URL}/health`);
      if (r.ok) {
        const body = await r.json();
        if (body.db_connected) return;
      }
    } catch {
      // not up yet — keep polling
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error(`password-mode server never became healthy; log tail:\n${logTail()}`);
}

test.beforeAll(async () => {
  const dbUrl = process.env.ZIMSERVICE_PW_DATABASE_URL;
  test.skip(!dbUrl, 'ZIMSERVICE_PW_DATABASE_URL not set — skipping password-mode flow');

  const bin = process.env.ZIMSERVICE_BIN ?? path.join(REPO_ROOT, 'target', 'release', 'zimservice');
  zimDir = mkdtempSync(path.join(tmpdir(), 'zimservice-e2e-pw-'));
  server = spawn(bin, ['serve'], {
    env: {
      ...process.env,
      HOST: '127.0.0.1',
      PORT: String(PW_PORT),
      ACCESS_MODE: 'password',
      AUTH_PASSWORD: PW_PASSWORD,
      ZIM_DIR: zimDir,
      DATABASE_URL: dbUrl,
      ZIMSERVICE_REQUIRE_DB: '1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const grow = (chunk) => {
    logBuf = (logBuf + String(chunk)).slice(-16_000);
  };
  server.stdout.on('data', grow);
  server.stderr.on('data', grow);
  server.on('error', (e) => grow(`spawn error: ${e.message}\n`));
  await waitHealthy();

  // The flow needs loopback open reads (pages + GETs load; only mutations
  // are gated). A stale DB seeded with require_auth_for_reads=true would
  // 401 the page HTML itself — fail loudly instead of hanging on dialogs.
  const pageProbe = await fetch(`${PW_URL}/`);
  if (pageProbe.status !== 200) {
    throw new Error(
      `password-mode server does not serve open reads (GET / → ${pageProbe.status}); ` +
        'the dedicated DB must be fresh so access.require_auth_for_reads seeds false; ' +
        `log tail:\n${logTail()}`
    );
  }
});

test.afterAll(async () => {
  if (server) {
    server.kill('SIGTERM');
    await new Promise((resolve) => {
      const t = setTimeout(() => {
        server.kill('SIGKILL');
        resolve();
      }, 10_000);
      server.on('close', () => {
        clearTimeout(t);
        resolve();
      });
    });
    server = undefined;
  }
  if (zimDir) {
    rmSync(zimDir, { recursive: true, force: true });
    zimDir = undefined;
  }
});

test('password mode: 401 prompts, wrong password errors, correct password saves', async ({
  page,
}) => {
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e)));

  // Every 401 from apiFetch hits the prompt path; prompt() blocks the page,
  // so dialogs arrive strictly one at a time and the queue order matches:
  // wrong (first save) → correct (second save).
  const dialogs = [];
  const answers = [PW_WRONG, PW_PASSWORD];
  page.on('dialog', (dialog) => {
    dialogs.push({ type: dialog.type(), message: dialog.message() });
    const answer = answers.shift();
    if (answer === undefined) {
      dialog.dismiss().catch(() => {});
    } else {
      dialog.accept(answer).catch(() => {});
    }
  });

  // Note: use the explicit PW_URL here — a relative goto would resolve
  // against the shared baseURL (the main, open-mode server), not this spec's
  // password-mode server.
  await page.goto(`${PW_URL}/settings.html`, { waitUntil: 'networkidle' });

  const input = page.locator('input[data-key="search.default_limit"]');
  await expect(input).toBeVisible();
  const original = await input.inputValue();
  const changed = String(Number(original) + 1);

  // First save: the PUT 401s (no token yet) → the admin-password prompt.
  await input.fill(changed);
  const save = page.locator('#save');
  await expect(save).toBeEnabled();
  await save.click();

  await expect.poll(() => dialogs.length, { timeout: 15_000 }).toBe(1);
  expect(dialogs[0].type).toBe('prompt');
  expect(dialogs[0].message).toContain(PROMPT_HINT);

  // Wrong password → the Bearer retry also 401s → the error message
  // surfaces in the save bar (no crash, no uncaught exception).
  await expect(page.locator('#msg')).toHaveClass(/error/);
  await expect(page.locator('#msg')).toHaveText('Error: authentication required');

  // Second save of the same change: 401 again → prompt → correct password.
  await save.click();
  await expect.poll(() => dialogs.length, { timeout: 15_000 }).toBe(2);
  expect(dialogs[1].type).toBe('prompt');
  expect(dialogs[1].message).toContain(PROMPT_HINT);

  // Correct password → the retried PUT authenticates and saves.
  await expect(page.locator('#msg')).toHaveClass(/success/);
  await expect(page.locator('#msg')).toHaveText('Saved 1 setting.');

  // The accepted password is stored (sessionStorage) and rides along as the
  // Bearer token: the restore save authenticates with NO new prompt, and the
  // value round-trips through the settings table across a full reload.
  await input.fill(original);
  await save.click();
  await expect(page.locator('#msg')).toHaveClass(/success/);
  await expect(page.locator('#msg')).toHaveText('Saved 1 setting.');
  expect(dialogs, 'no auth prompt after the token was stored').toHaveLength(2);
  await page.reload({ waitUntil: 'networkidle' });
  await expect(input).toHaveValue(original);

  expect(errors, `uncaught page errors: ${errors.join('; ')}`).toEqual([]);
});

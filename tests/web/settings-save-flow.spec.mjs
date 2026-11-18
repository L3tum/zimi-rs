// Real-browser settings round-trip (Playwright, headless Chromium): change a
// setting in the form, save it, and verify the value round-trips through the
// settings table (survives a full page reload), then restore the original
// value so the instance is left as found.
//
// Requires the main live server (see playwright.config.mjs). Runs in open
// mode in CI, where the settings write is unauthenticated — the auth path is
// covered by password-auth-flow.spec.mjs against its own password-mode
// server.

import { test, expect } from '@playwright/test';

// A non-secret, non-env-locked int key with a known server-side default.
const KEY = 'search.default_limit';

test('settings: saving a changed value persists across a page reload', async ({
  page,
}) => {
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e)));

  await page.goto('/settings.html', { waitUntil: 'networkidle' });

  // The form renders from GET /settings once the page is up.
  const input = page.locator(`input[data-key="${KEY}"]`);
  await expect(input).toBeVisible();
  const original = await input.inputValue();
  const changed = String(Number(original) + 1);

  await input.fill(changed);
  const save = page.locator('#save');
  // The save button arms with a change counter as soon as the field diverges
  // from the server value.
  await expect(save).toBeEnabled();
  await expect(save).toHaveText('Save 1 change');

  await save.click();
  const msg = page.locator('#msg');
  await expect(msg).toHaveClass(/success/);
  await expect(msg).toHaveText('Saved 1 setting.');

  // The value survives a full page reload — it round-tripped through the
  // settings table, not just an in-memory re-render.
  await page.reload({ waitUntil: 'networkidle' });
  await expect(input).toHaveValue(changed);

  // Restore the original value (leave the instance as we found it).
  await input.fill(original);
  await save.click();
  await expect(msg).toHaveClass(/success/);
  await expect(msg).toHaveText('Saved 1 setting.');
  await page.reload({ waitUntil: 'networkidle' });
  await expect(input).toHaveValue(original);

  expect(errors, `uncaught page errors: ${errors.join('; ')}`).toEqual([]);
});

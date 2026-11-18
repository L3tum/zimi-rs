// Real-browser search round-trip (Playwright, headless Chromium): a query
// that matches an article in the seeded ZIM renders a result row, and
// clicking the row opens the raw article.
//
// Requires the main live server (see playwright.config.mjs) to have the
// `tests/fixtures/tiny.zim` fixture discovered AND indexed — CI runs
// `zimservice list --sync` + `zimservice index --zim tiny` before starting
// the server; locally, run the same two commands (or index from the
// library/settings UI) before `npx playwright test`.
//
// The fixture's single indexed article is "Test ZIM file"
// (path main.html), which every query below is chosen to match exactly once.

import { test, expect } from '@playwright/test';

const QUERY = 'test zim file';
const EXPECTED_TITLE = 'Test ZIM file';
const EXPECTED_HREF = '/w/tiny/main.html';

test('search: a matching query renders a result and clicking it opens the article', async ({
  page,
}) => {
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e)));

  await page.goto('/search.html', { waitUntil: 'networkidle' });

  // Filter dropdowns are populated from /list: the seeded fixture ZIM shows
  // up as a filter option (a wiring fault or missing index would leave only
  // the default "All ZIMs" option).
  await expect(page.locator('#zim option[value="tiny"]')).toHaveCount(1);

  await page.fill('#q', QUERY);
  await page.click('#go');

  // One real result: the stats line counts it and the result card carries
  // the fixture article's title and the /w/… article link.
  const stats = page.locator('#stats');
  await expect(stats).toContainText(/1 results in \d+ms/);
  await expect(page.locator('#results .result')).toHaveCount(1);

  const link = page.locator('#results .result h3 a').first();
  await expect(link).toContainText(EXPECTED_TITLE);
  await expect(link).toHaveAttribute('href', EXPECTED_HREF);
  await expect(page.locator('#empty')).toBeHidden();
  await expect(page.locator('#loading')).toBeHidden();

  // Clicking the result navigates to the raw ZIM article served by /w/…
  await link.click();
  await page.waitForURL('**/w/tiny/main.html');
  await page.waitForLoadState('load');
  await expect(page).toHaveTitle(EXPECTED_TITLE);
  await expect(page.locator('body')).toContainText(EXPECTED_TITLE);

  expect(errors, `uncaught page errors: ${errors.join('; ')}`).toEqual([]);
});

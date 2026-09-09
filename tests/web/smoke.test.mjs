// Real-DOM smoke tests (jsdom) for the zimservice web UI.
//
// The hand-rolled shim suites (common/index/search/settings .test.mjs) exercise
// the page scripts' pure logic in isolation against a fake DOM. They do NOT run
// the scripts against a real DOM, so a page-level wiring fault — a script that
// loads in the wrong order, an event handler that never attaches, a render path
// that throws on the real HTML — slips through. These tests close that gap:
// they boot each page in jsdom and evaluate the <script src> files in the
// HTML's own load order (see tests/web/jsdom.mjs), with a stubbed fetch.
//
// Canned API payloads mirror the REAL handler JSON shapes:
//   /health    -> HealthResponse            (src/serve/handlers/zims.rs)
//   /list      -> { zims: ZimMeta[] }       (src/zim/mod.rs)
//   /downloads -> { downloads: [] }         (src/serve/handlers/downloads.rs)
//   /settings  -> { cat: { key: {value,locked?,locked_by?} } }  (settings cache)
//
// Run: node --test tests/web/*.test.mjs   (or: make web-test)

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { bootPage, tick, res } from './jsdom.mjs';

// ── Canned payloads (match the Rust handler shapes) ──────────────────────────

// ZimMeta (src/zim/mod.rs). `file_path` uses the unauthenticated-redacted value
// the open-mode /list handler returns (see list_zims in zims.rs).
function zim(over = {}) {
  return {
    id: 1,
    name: 'wikipedia_en',
    display_title: 'Wikipedia (EN)',
    description: null,
    language: 'en',
    creator: null,
    publisher: null,
    date: '2024-01-01',
    entry_count: 100000,
    article_count: 50000,
    file_path: '[redacted]',
    file_size: 123456789,
    category: null,
    index_status: 'ready',
    index_progress: 1,
    indexed_entries: 50000,
    embed_enabled: false,
    ...over,
  };
}

// HealthResponse (src/serve/handlers/zims.rs).
const HEALTH = {
  status: 'ok',
  version: '0.0.0-smoke',
  zims_count: 0,
  articles_count: 0,
  db_connected: true,
  qbit_connected: false,
  multi_instance: false,
};

// ── index.html ───────────────────────────────────────────────────────────────

function indexFetch({ zims = [], health = HEALTH } = {}) {
  const calls = [];
  const handler = (url, opts) => {
    calls.push({ url, opts });
    if (url === '/health') return res(health);
    if (url === '/list') return res({ zims });
    if (url === '/downloads') return res({ downloads: [] });
    if (url.startsWith('/settings/zim/')) return res({});
    throw new Error('unexpected fetch: ' + url);
  };
  return { handler, calls };
}

test('index: page scripts boot without throwing and render the status bar', async (t) => {
  const booted = bootPage('index.html', { fetchHandler: indexFetch().handler });
  t.after(() => booted.window.close());
  assert.equal(
    booted.errors.length,
    0,
    'no script should throw during load: ' + booted.errors.map((e) => e.message).join('; ')
  );
  await tick();
  const bar = booted.doc.getElementById('statusBar');
  // loadLibrary() replaced the static "Loading…" with the live status line.
  assert.ok(!bar.textContent.includes('Loading'), 'status bar still shows "Loading"');
  assert.match(bar.textContent, /0 ZIMs/);
  assert.match(bar.textContent, /articles indexed/);
  assert.match(bar.textContent, /qBittorrent/);
});

test('index: renders ZIM cards + the shared zimControls markup into a real DOM', async (t) => {
  const zims = [
    zim({ category: 'reference', embed_enabled: true }),
    zim({ id: 2, name: 'wiktionary_fr', display_title: 'Wiktionnaire (FR)', language: 'fr' }),
  ];
  const booted = bootPage('index.html', { fetchHandler: indexFetch({ zims }).handler });
  t.after(() => booted.window.close());
  assert.equal(booted.errors.length, 0, booted.errors.map((e) => e.message).join('; '));
  await tick();

  const cards = booted.doc.getElementById('zims').querySelectorAll('.zim-card');
  assert.equal(cards.length, 2, 'one card per ZIM');

  // Card 1: the shared zimControls() helper (web/common.js) rendered its embed
  // toggle (data-embed), category input (data-cat) and saved marker
  // (data-catmsg) into the real DOM with the ZIM's values.
  const first = cards[0];
  const embed = first.querySelector('input[data-embed]');
  assert.ok(embed, 'embed toggle rendered');
  assert.equal(embed.getAttribute('data-embed'), 'wikipedia_en');
  assert.equal(embed.checked, true, 'embed_enabled: true -> checked');
  const cat = first.querySelector('input[data-cat]');
  assert.ok(cat, 'category input rendered');
  assert.equal(cat.getAttribute('data-cat'), 'wikipedia_en');
  assert.equal(cat.value, 'reference', 'category value echoed into input');
  assert.ok(first.querySelector('[data-catmsg="wikipedia_en"]'), 'saved marker rendered');

  // Card 2: embed disabled + empty category (default).
  assert.equal(cards[1].querySelector('input[data-embed]').checked, false);
  assert.equal(cards[1].querySelector('input[data-cat]').value, '');
});

test('index: delegating change handler on the grid fires a per-ZIM PUT', async (t) => {
  const rec = indexFetch({ zims: [zim({ embed_enabled: true })] });
  const booted = bootPage('index.html', { fetchHandler: rec.handler });
  t.after(() => booted.window.close());
  assert.equal(booted.errors.length, 0, booted.errors.map((e) => e.message).join('; '));
  await tick();
  const box = booted.doc.querySelector('.zim-card input[data-embed]');
  assert.ok(box, 'embed toggle present');
  box.checked = false; // toggle off
  box.dispatchEvent(new booted.window.Event('change', { bubbles: true }));
  await tick();

  const put = rec.calls.find(
    (c) => c.url === '/settings/zim/wikipedia_en' && c.opts && c.opts.method === 'PUT'
  );
  assert.ok(put, 'a PUT to /settings/zim/wikipedia_en was issued');
  assert.equal(JSON.parse(put.opts.body).embed_enabled, false);
});

// ── search.html ──────────────────────────────────────────────────────────────

function searchFetch({ zims = [] } = {}) {
  return (url) => {
    if (url === '/list') return res({ zims });
    if (url.startsWith('/suggest')) return res({ suggestions: [] });
    if (url.startsWith('/search')) return res({ results: [], total: 0 });
    throw new Error('unexpected fetch: ' + url);
  };
}

test('search: page scripts boot and populate the ZIM + language filters', async (t) => {
  const zims = [
    zim(),
    zim({ id: 2, name: 'wiktionary_fr', display_title: 'Wiktionnaire (FR)', language: 'fr' }),
  ];
  const booted = bootPage('search.html', { fetchHandler: searchFetch({ zims }) });
  t.after(() => booted.window.close());
  assert.equal(booted.errors.length, 0, booted.errors.map((e) => e.message).join('; '));
  await tick();

  const zimSel = booted.doc.getElementById('zim');
  // The HTML ships one default "<option value="">All ZIMs</option>" plus one
  // per ZIM from /list.
  assert.equal(zimSel.querySelectorAll('option').length, 1 + zims.length);
  assert.ok([...zimSel.querySelectorAll('option')].some((o) => o.value === 'wikipedia_en'));
  assert.ok([...zimSel.querySelectorAll('option')].some((o) => o.value === 'wiktionary_fr'));

  const langs = [...booted.doc.getElementById('lang').querySelectorAll('option')].map((o) => o.value);
  assert.ok(langs.includes('en'), 'en language option present');
  assert.ok(langs.includes('fr'), 'fr language option present');
  assert.equal(booted.doc.getElementById('q').value, '', 'q starts empty with no ?q=');
});

// ── settings.html ────────────────────────────────────────────────────────────

// Shape from all_grouped_for: { cat: { key: { value, locked?, locked_by? } } }.
const SETTINGS = {
  general: {
    zim_dir: { value: '/srv/zims', locked: true, locked_by: 'ZIMSERVICE_ZIM_DIR' },
    port: { value: 8080 },
    log_level: { value: 'info' },
  },
  search: {
    fts_weight: { value: 0.6 },
    default_limit: { value: 20 },
  },
  torrent: {
    enabled: { value: false },
    password: { value: '', locked: true, locked_by: 'ZIMSERVICE_TORRENT_PASSWORD' },
  },
  access: {
    mode: { value: 'open' },
  },
};

function settingsFetch({ settings = SETTINGS, zims = [] } = {}) {
  return (url) => {
    if (url === '/settings') return res(settings);
    if (url === '/list') return res({ zims });
    throw new Error('unexpected fetch: ' + url);
  };
}

test('settings: page scripts boot, render sections + inputs + per-ZIM rows', async (t) => {
  const booted = bootPage('settings.html', {
    fetchHandler: settingsFetch({ zims: [zim()] }),
  });
  t.after(() => booted.window.close());
  assert.equal(booted.errors.length, 0, booted.errors.map((e) => e.message).join('; '));
  await tick();

  // The static "Loading…" placeholder was replaced by real category sections.
  const sections = booted.doc.getElementById('sections').querySelectorAll('.section');
  assert.ok(sections.length >= 3, `expected >=3 category sections, got ${sections.length}`);
  assert.ok(!booted.doc.getElementById('sections').textContent.includes('Loading…'));

  // A numeric field rendered with its server value.
  const port = booted.doc.querySelector('input[data-key="general.port"]');
  assert.ok(port, 'port input rendered');
  assert.equal(port.value, '8080');

  // A locked secret renders as a disabled input with its value NOT echoed.
  const pw = booted.doc.querySelector('input[data-key="torrent.password"]');
  assert.ok(pw, 'password input rendered');
  assert.equal(pw.disabled, true, 'locked secret is disabled');
  assert.equal(pw.value, '', 'locked secret value not echoed into the DOM');

  // The per-ZIM rows section rendered from /list.
  assert.equal(booted.doc.getElementById('zimRows').querySelectorAll('.zim-row').length, 1);
  assert.ok(booted.doc.getElementById('zimRows').textContent.includes('Wikipedia (EN)'));
});

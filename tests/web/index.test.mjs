// Behavioral unit tests for web/index.js (library + downloads page).
//
// web/index.js is a classic browser script (no module exports): it reads
// document/fetch/timers and uses web/common.js helpers as globals. We boot
// common.js + index.js as real top-level classic scripts in a jsdom window
// against the minimal HTML the page scripts touch (tests/web/jsdom.mjs
// #bootScripts), with a stubbed fetch and controllable timers, then exercise
// the top-level functions and the delegated event handlers.
//
// NOTE: like on the real page, the top-level loadLibrary()/loadDownloads()/
// setInterval() fire at script boot, so every route those calls need must be
// part of each test's routes.
//
// Run: node --test tests/web/index.test.mjs   (or: make web-test)

import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { bootScripts, tick } from './jsdom.mjs';

// Minimal skeleton of web/index.html: every element index.js touches by id.
const HTML = `<!doctype html>
<html><head><title>index</title></head>
<body>
  <div id="statusBar"></div>
  <div id="zims"></div>
  <input type="text" id="dlUrl">
  <input type="text" id="dlName">
  <button id="dlAdd">Add</button>
  <div id="dlList"></div>
  <div class="toast" id="toast"></div>
</body></html>`;

// Default fetch: loud failure if a test forgets to stub one.
const NO_FETCH = () => {
  throw new Error('fetch should not be called in these tests');
};

const ok = (body) => ({ status: 200, ok: true, json: async () => body });

/**
 * Boot common.js + index.js in a fresh jsdom window with controllable timers.
 * `close` MUST be registered with `t.after` (an open window hangs the run).
 */
function loadIndex({ fetch: fetchFn, prompt } = {}) {
  const { window, doc, errors, timers } = bootScripts(
    HTML,
    ['common.js', 'index.js'],
    { fetchHandler: fetchFn ?? NO_FETCH, promptHandler: prompt, timers: true },
  );
  assert.deepEqual(errors, [], errors.map((e) => e.message).join('; '));
  return { window, doc, timers, close: () => window.close() };
}

// Stubbed fetch that routes by URL path and records every call. A route
// body may be a plain value (wrapped as 200 + json) or a function of
// { url, opts, n } (may throw, or return a full response object); a value
// carrying `status`/`json` is passed through as-is (non-2xx responses).
// Last matching route wins (per-test overrides are appended after defaults).
function routingFetch(routes) {
  const calls = [];
  const fetchStub = async (url, opts) => {
    calls.push({ url, opts });
    const path = String(url).split('?')[0];
    const n = calls.length;
    let hit;
    for (let i = routes.length - 1; i >= 0; i--) {
      if (routes[i].path === path) { hit = routes[i]; break; }
    }
    assert.ok(hit, `unexpected fetch: ${url}`);
    const body = typeof hit.body === 'function' ? hit.body({ url, opts, n }) : hit.body;
    if (body && typeof body === 'object' && typeof body.status === 'number') return body;
    return ok(body);
  };
  return { fetchStub, calls };
}

const HEALTH = { version: '1.2.3', articles_count: 12345, qbit_connected: true };
// Shared JS<->Rust response contract (Tests Major #5): the canned /list
// payload is loaded from the SAME fixture file the Rust integration test
// asserts the live handler response against (tests/integration/contract.rs,
// tests/web/fixtures/list.json) - a handler field rename breaks both halves.
const here = dirname(fileURLToPath(import.meta.url));
const LIST_FIXTURE = JSON.parse(
  readFileSync(join(here, 'fixtures', 'list.json'), 'utf8'),
);
const ZIMS = LIST_FIXTURE.zims;

function libraryRoutes(extra = []) {
  return [
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/downloads', body: { downloads: [] } },
    ...extra,
  ];
}

// ── loadLibrary / renderZims ────────────────────────────────────────────

test('loadLibrary: renders status bar from /health + /list', async (t) => {
  const { fetchStub } = routingFetch(libraryRoutes());
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const bar = doc.getElementById('statusBar').innerHTML;
  assert.ok(bar.includes('v1.2.3'));
  assert.ok(bar.includes('1 ZIMs'));
  assert.ok(bar.includes('12,345 articles indexed'));
  assert.ok(bar.includes('qBittorrent connected'));
  assert.ok(bar.includes('dot ok'), 'healthy qbit dot');
});

test('loadLibrary: qbit_connected=false shows bad dot + "not configured"', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: { ...HEALTH, qbit_connected: false } },
    { path: '/list', body: { zims: [] } },
  ]);
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const bar = doc.getElementById('statusBar').innerHTML;
  assert.ok(bar.includes('dot bad'));
  assert.ok(bar.includes('qBittorrent not configured'));
});

test('loadLibrary: error → status bar shows error message, HTML-escaped', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: () => { throw new Error('boom <x>'); } },
    { path: '/downloads', body: { downloads: [] } },
  ]);
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const bar = doc.getElementById('statusBar').innerHTML;
  assert.ok(bar.includes('Error loading library: boom &lt;x&gt;'));
});

test('renderZims: empty library → empty message', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
  ]);
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  assert.ok(doc.getElementById('zims').innerHTML.includes('No ZIM files found'));
});

test('renderZims: full card with badges, progress bar, embed toggle, links', async (t) => {
  const { fetchStub } = routingFetch(libraryRoutes());
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const html = doc.getElementById('zims').innerHTML;
  assert.ok(html.includes('<h3>Wiki EN</h3>'));
  assert.ok(html.includes('<span class="badge">eng</span>'));
  assert.ok(html.includes('<span class="badge">Wiki</span>'));
  assert.ok(html.includes('<span class="badge">2024</span>'));
  assert.ok(html.includes('100 entries · 1.0 MB'));
  assert.ok(html.includes('50 indexed · 50%'));
  assert.ok(html.includes('width:50%'), 'index progress bar at 50%');
  assert.ok(html.includes('status status-indexing'));
  assert.ok(html.includes('embedding on'));
  assert.ok(html.includes('data-embed="wiki_en" checked'));
  assert.ok(html.includes('data-cat="wiki_en" value="Wiki"'));
  const searchHref = 'href="/search.html?q=&amp;zim=wiki_en"';
  assert.ok(html.includes(searchHref), 'q= serializes with escaped &');
  assert.ok(html.includes('href="/settings.html#zim-wiki_en"'));
});

test('renderZims: no progress bar when indexing is done; name is URL-encoded', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: {
      zims: [{ ...ZIMS[0], name: 'a/b', index_status: 'done', index_progress: 1 }],
    } },
  ]);
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const html = doc.getElementById('zims').innerHTML;
  assert.ok(!html.includes('class="prog"'), 'no progress bar for done ZIM');
  assert.ok(html.includes('zim=a%2Fb'), 'search link encodes slash');
  assert.ok(html.includes('#zim-a%2Fb'), 'settings anchor encodes slash');
  assert.ok(html.includes('data-embed="a/b"'), 'raw name in data attribute');
});

// ── Per-ZIM embed toggle (delegated change event on #zims) ──────────────

test('embed toggle: change → PUT /settings/zim/<name> + success toast', async (t) => {
  const { fetchStub, calls } = routingFetch([
    ...libraryRoutes(),
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const cb = doc.querySelector('[data-embed="wiki_en"]');
  cb.checked = true;
  cb.dispatchEvent(new window.Event('change', { bubbles: true }));
  await tick();
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.ok(put, 'PUT sent');
  assert.equal(put.opts.method, 'PUT');
  assert.deepEqual(JSON.parse(put.opts.body), { embed_enabled: true });
  assert.equal(doc.getElementById('toast').textContent, 'Embedding enabled for wiki_en');
  assert.ok(doc.getElementById('toast').className.includes('ok'));
});

test('embed toggle: API failure → error toast + checkbox reverted', async (t) => {
  const { fetchStub } = routingFetch(libraryRoutes([
    { path: '/settings/zim/wiki_en', body: () => { throw new Error('denied'); } },
  ]));
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const cb = doc.querySelector('[data-embed="wiki_en"]');
  cb.checked = false; // user toggles off
  cb.dispatchEvent(new window.Event('change', { bubbles: true }));
  await tick();
  assert.equal(cb.checked, true, 'checkbox reverted');
  assert.equal(doc.getElementById('toast').textContent, 'Save failed: denied');
  assert.ok(doc.getElementById('toast').className.includes('err'));
});

// ── Per-ZIM category editing (delegated input event, 800ms debounce) ────

test('saveCategory: unchanged value → no request', async (t) => {
  const { fetchStub, calls } = routingFetch(libraryRoutes());
  const { window, doc, timers, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const input = doc.querySelector('[data-cat="wiki_en"]');
  input.dispatchEvent(new window.Event('input', { bubbles: true }));
  await timers.flush(1000);
  assert.equal(calls.filter((c) => c.url.startsWith('/settings/zim/')).length, 0);
});

test('saveCategory: changed value → PUT {category}, flash saved marker', async (t) => {
  const { fetchStub, calls } = routingFetch([
    ...libraryRoutes(),
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { window, doc, timers, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  // The rendered "saved ✓" marker (data-catmsg) is what saveCategory flashes.
  const msg = doc.querySelector('[data-catmsg="wiki_en"]');
  const input = doc.querySelector('[data-cat="wiki_en"]');
  input.value = 'Reference ';
  input.dispatchEvent(new window.Event('input', { bubbles: true }));
  // Debounced at 800ms: not fired at 100ms, fired at 1000ms.
  await timers.flush(100);
  assert.equal(
    calls.filter((c) => c.url.startsWith('/settings/zim/')).length, 0, 'still debounced',
  );
  await timers.flush(1000);
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.deepEqual(JSON.parse(put.opts.body), { category: 'Reference' }, 'trimmed');
  assert.equal(msg.style.opacity, '1', 'saved marker flashed');
  await timers.flush(2000);
  assert.equal(msg.style.opacity, '0', 'marker faded');
});

test('saveCategory: cleared value → PUT {category: null}', async (t) => {
  const { fetchStub, calls } = routingFetch([
    ...libraryRoutes(),
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { window, doc, timers, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const input = doc.querySelector('[data-cat="wiki_en"]');
  input.value = '  ';
  input.dispatchEvent(new window.Event('input', { bubbles: true }));
  await timers.flush(1000);
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.deepEqual(JSON.parse(put.opts.body), { category: null });
});

test('saveCategory: API failure → error toast, no crash', async (t) => {
  const { fetchStub } = routingFetch([
    ...libraryRoutes(),
    { path: '/settings/zim/wiki_en', body: () => { throw new Error('locked'); } },
  ]);
  const { window, doc, timers, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  const input = doc.querySelector('[data-cat="wiki_en"]');
  input.value = 'Nope';
  input.dispatchEvent(new window.Event('input', { bubbles: true }));
  await timers.flush(1000);
  assert.equal(doc.getElementById('toast').textContent, 'Category save failed: locked');
});

// ── Downloads: loadDownloads / renderDownloads / polling ────────────────

test('loadDownloads: fetch error is silent (no status update)', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    { path: '/downloads', body: () => { throw new Error('nope'); } },
  ]);
  const { window, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadLibrary();
  await window.loadDownloads(); // must not throw
});

test('loadDownloads: no downloads → empty message, no polling', async (t) => {
  const { fetchStub } = routingFetch(libraryRoutes());
  const { window, doc, timers, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadDownloads();
  assert.ok(doc.getElementById('dlList').innerHTML.includes('No downloads yet'));
  const noPoll = timers.intervals.filter((ms) => ms === 3000).length;
  assert.equal(noPoll, 0, 'no polling when nothing active');
});

test('loadDownloads: active download starts polling once; none stops it', async (t) => {
  let active = true;
  const dlBody = () => ({
    downloads: [{ id: 1, status: active ? 'downloading' : 'complete' }],
  });
  const { fetchStub } = routingFetch(libraryRoutes([
    { path: '/downloads', body: dlBody },
  ]));
  const { window, timers, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.loadDownloads();
  assert.equal(
    timers.intervals.filter((ms) => ms === 3000).length, 1,
    'top-level + this call share one poll (dlPoll guard)',
  );
  active = false;
  await window.loadDownloads();
  assert.ok(timers.cleared.length >= 1, 'polling stopped');
});

test('renderDownloads: empty list message', (t) => {
  const { window, doc, close } = loadIndex();
  t.after(close);
  window.renderDownloads([]);
  assert.ok(doc.getElementById('dlList').innerHTML.includes('No downloads yet'));
});

test('renderDownloads: queued item has muted progress bar + Cancel button', (t) => {
  const { window, doc, close } = loadIndex();
  t.after(close);
  const queued = { id: 7, name: 'a.zim', url: 'magnet:?xt=1', status: 'queued', progress: 0.45 };
  window.renderDownloads([queued]);
  const html = doc.getElementById('dlList').innerHTML;
  assert.ok(html.includes('45%'));
  assert.ok(html.includes('data-id="7"'), 'cancel button carries id');
  assert.ok(html.includes('var(--muted)'));
  assert.ok(html.includes('>queued</span>'));
});

test('renderDownloads: downloading item shows speed + ETA in detail line', (t) => {
  const { window, doc, close } = loadIndex();
  t.after(close);
  window.renderDownloads([{
    id: 1, name: 'b.zim', url: 'https://x/b.zim', status: 'downloading',
    progress: 1.2, // > 1 must clamp
    speed_bps: 1048576, up_speed_bps: 2048, eta_secs: 90,
  }]);
  const html = doc.getElementById('dlList').innerHTML;
  assert.ok(html.includes('width:100%'), 'progress clamped to 100%');
  assert.ok(html.includes('100% · 1.0 MB/s · ▲ 2.0 KB/s · ETA 1m 30s'));
});

test('renderDownloads: seeding item shows upload speed, ratio, seeders', (t) => {
  const { window, doc, close } = loadIndex();
  t.after(close);
  window.renderDownloads([{
    id: 2, name: 'c.zim', url: 'u', status: 'seeding',
    up_speed_bps: 4096, ratio: 1.256, num_seeds: 3,
  }]);
  const html = doc.getElementById('dlList').innerHTML;
  assert.ok(html.includes('▲ 4.0 KB/s · ratio 1.26 · 3 seeders'));
  assert.ok(!html.includes('Cancel'), 'no cancel button when seeding');
});

test('renderDownloads: error download renders escaped error detail', (t) => {
  const { window, doc, close } = loadIndex();
  t.after(close);
  const errDl = { id: 3, name: 'd.zim', url: 'u', status: 'error', error: '404 <gone>' };
  window.renderDownloads([errDl]);
  const dlHtml = doc.getElementById('dlList').innerHTML;
  assert.ok(dlHtml.includes('<span class="err">404 &lt;gone&gt;</span>'));
});

// ── addDownload ─────────────────────────────────────────────────────────

test('addDownload: empty URL → error toast, no request', async (t) => {
  const { fetchStub, calls } = routingFetch(libraryRoutes());
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('dlUrl').value = '   ';
  await window.addDownload();
  assert.equal(calls.filter((c) => c.opts && c.opts.method === 'POST').length, 0);
  assert.equal(doc.getElementById('toast').textContent, 'Enter a URL or magnet link');
  assert.equal(doc.getElementById('dlAdd').disabled, false);
});

test('addDownload: posts URL (+name), clears inputs, toasts, refreshes', async (t) => {
  const { fetchStub, calls } = routingFetch(libraryRoutes());
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('dlUrl').value = ' magnet:?xt=abc ';
  doc.getElementById('dlName').value = 'My ZIM';
  await window.addDownload();
  await tick();
  const post = calls.find((c) => c.opts && c.opts.method === 'POST');
  assert.equal(post.url, '/downloads');
  assert.deepEqual(JSON.parse(post.opts.body), { url: 'magnet:?xt=abc', name: 'My ZIM' });
  assert.equal(doc.getElementById('dlUrl').value, '');
  assert.equal(doc.getElementById('dlName').value, '');
  assert.equal(doc.getElementById('toast').textContent, 'Download queued');
  const dlListHtml = doc.getElementById('dlList').innerHTML;
  assert.ok(dlListHtml.includes('No downloads yet'), 'list refreshed from server');
});

test('addDownload: name left blank → omitted from body', async (t) => {
  const { fetchStub, calls } = routingFetch(libraryRoutes());
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('dlUrl').value = 'u';
  await window.addDownload();
  await tick();
  const post = calls.find((c) => c.opts && c.opts.method === 'POST');
  const body = JSON.parse(post.opts.body);
  assert.equal(body.url, 'u');
  assert.equal('name' in body, false);
});

test('addDownload: API error → "Failed: <msg>" toast', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    {
      path: '/downloads',
      body: ({ opts }) => (opts && opts.method === 'POST'
        ? { status: 400, ok: false, json: async () => ({ error: 'bad magnet' }) }
        : { downloads: [] }),
    },
  ]);
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('dlUrl').value = 'u';
  await window.addDownload();
  await tick();
  assert.equal(doc.getElementById('toast').textContent, 'Failed: bad magnet');
});

// ── cancelDownload ──────────────────────────────────────────────────────

test('cancelDownload: DELETE ok → list refreshed', async (t) => {
  const { fetchStub, calls } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    { path: '/downloads/5', body: {} },
    { path: '/downloads', body: { downloads: [] } },
  ]);
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.cancelDownload(5);
  await tick(); // cancelDownload refreshes the list without awaiting it
  assert.equal(calls.find((c) => c.url === '/downloads/5').opts.method, 'DELETE');
  assert.ok(doc.getElementById('dlList').innerHTML.includes('No downloads yet'));
});

test('cancelDownload: non-ok response → error toast', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    { path: '/downloads/5', body: { status: 404, ok: false, json: async () => ({}) } },
    { path: '/downloads', body: { downloads: [] } },
  ]);
  const { window, doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  await window.cancelDownload(5);
  assert.equal(doc.getElementById('toast').textContent, 'Cancel failed: 404');
});

test('dlList click: delegated to cancelDownload via data-id', async (t) => {
  const { fetchStub, calls } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    { path: '/downloads/9', body: {} },
    { path: '/downloads', body: { downloads: [] } },
  ]);
  const { doc, close } = loadIndex({ fetch: fetchStub });
  t.after(close);
  // Materialize a cancel button (the empty list from /downloads has none).
  doc.getElementById('dlList').innerHTML = '<button class="btn" data-id="9">Cancel</button>';
  doc.querySelector('[data-id="9"]').click();
  await tick();
  assert.equal(calls.find((c) => c.url === '/downloads/9').opts.method, 'DELETE');
});

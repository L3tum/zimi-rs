// Behavioral unit tests for web/index.js (library + downloads page).
//
// web/index.js is a classic browser script (no module exports): it reads
// document/fetch/timers and uses web/common.js helpers as globals. We load
// common.js + index.js verbatim into a vm.runInNewContext sandbox with a
// minimal fake DOM (tests/web/dom.mjs) and a stubbed fetch, then exercise
// the top-level functions and event handlers by name.
//
// Run: node --test tests/web/index.test.mjs   (or: make web-test)

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import vm from 'node:vm';
import { makeDocument, makeTimers } from './dom.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const webDir = join(here, '..', '..', 'web');
const commonSource = readFileSync(join(webDir, 'common.js'), 'utf8');
const indexSource = readFileSync(join(webDir, 'index.js'), 'utf8');

const ok = (body) => ({ status: 200, ok: true, json: async () => body });

// Yield to the event loop so untracked async handlers (event dispatch) can
// finish their microtask chains.
const tick = () => new Promise((r) => setImmediate(r));

function loadIndex(overrides = {}) {
  const { doc, elements } = makeDocument();
  const timers = makeTimers();
  const store = new Map();
  const sandbox = {
    sessionStorage: {
      getItem: (k) => (store.has(k) ? store.get(k) : null),
      setItem: (k, v) => store.set(k, String(v)),
      removeItem: (k) => store.delete(k),
    },
    document: doc,
    location: { search: '', hash: '' },
    // The page scripts call CSS.escape(name) when building querySelector
    // attribute selectors; our names have no CSS-special chars, so identity.
    CSS: { escape: (s) => String(s) },
    prompt: () => null,
    fetch: async () => {
      throw new Error('fetch should not be called in these tests');
    },
    setTimeout: timers.setTimeout,
    clearTimeout: timers.clearTimeout,
    setInterval: timers.setInterval,
    clearInterval: timers.clearInterval,
    ...overrides,
  };
  vm.createContext(sandbox);
  vm.runInContext(commonSource, sandbox, { filename: 'common.js' });
  vm.runInContext(indexSource, sandbox, { filename: 'index.js' });
  return { sandbox, elements, timers };
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
const ZIMS = [{
  name: 'wiki_en',
  display_title: 'Wiki EN',
  language: 'eng',
  category: 'Wiki',
  date: '2024',
  entry_count: 100,
  file_size: 1048576,
  index_status: 'indexing',
  index_progress: 0.5,
  indexed_entries: 50,
  embed_enabled: true,
}];

function libraryRoutes(extra = []) {
  return [
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/downloads', body: { downloads: [] } },
    ...extra,
  ];
}

// ── loadLibrary / renderZims ────────────────────────────────────────────

test('loadLibrary: renders status bar from /health + /list', async () => {
  const { fetchStub } = routingFetch(libraryRoutes());
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  const bar = elements.get('statusBar').innerHTML;
  assert.ok(bar.includes('v1.2.3'));
  assert.ok(bar.includes('1 ZIMs'));
  assert.ok(bar.includes('12,345 articles indexed'));
  assert.ok(bar.includes('qBittorrent connected'));
  assert.ok(bar.includes('dot ok'), 'healthy qbit dot');
});

test('loadLibrary: qbit_connected=false shows bad dot + "not configured"', async () => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: { ...HEALTH, qbit_connected: false } },
    { path: '/list', body: { zims: [] } },
  ]);
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  const bar = elements.get('statusBar').innerHTML;
  assert.ok(bar.includes('dot bad'));
  assert.ok(bar.includes('qBittorrent not configured'));
});

test('loadLibrary: error → status bar shows error message, HTML-escaped', async () => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: () => { throw new Error('boom <x>'); } },
    { path: '/downloads', body: { downloads: [] } },
  ]);
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  const bar = elements.get('statusBar').innerHTML;
  assert.ok(bar.includes('Error loading library: boom &lt;x&gt;'));
});

test('renderZims: empty library → empty message', async () => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
  ]);
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  assert.ok(elements.get('zims').innerHTML.includes('No ZIM files found'));
});

test('renderZims: full card with badges, progress bar, embed toggle, links', async () => {
  const { fetchStub } = routingFetch(libraryRoutes());
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  const html = elements.get('zims').innerHTML;
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
  assert.ok(html.includes('href="/search.html?q=&zim=wiki_en"'));
  assert.ok(html.includes('href="/settings.html#zim-wiki_en"'));
});

test('renderZims: no progress bar when indexing is done; name is URL-encoded', async () => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [{ ...ZIMS[0], name: 'a/b', index_status: 'done', index_progress: 1 }] } },
  ]);
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  const html = elements.get('zims').innerHTML;
  assert.ok(!html.includes('class="prog"'), 'no progress bar for done ZIM');
  assert.ok(html.includes('zim=a%2Fb'), 'search link encodes slash');
  assert.ok(html.includes('#zim-a%2Fb'), 'settings anchor encodes slash');
  assert.ok(html.includes('data-embed="a/b"'), 'raw name in data attribute');
});

// ── Per-ZIM embed toggle (change event on #zims) ─────────────────────────

test('embed toggle: change → PUT /settings/zim/<name> + success toast', async () => {
  const { fetchStub, calls } = routingFetch([
    ...libraryRoutes(),
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { sandbox, elements, timers } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  elements.get('zims').dispatch('change', { target: { dataset: { embed: 'wiki_en' }, checked: true } });
  await tick();
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.ok(put, 'PUT sent');
  assert.equal(put.opts.method, 'PUT');
  assert.deepEqual(JSON.parse(put.opts.body), { embed_enabled: true });
  assert.equal(elements.get('toast').textContent, 'Embedding enabled for wiki_en');
  assert.ok(elements.get('toast').className.includes('ok'));
});

test('embed toggle: API failure → error toast + checkbox reverted', async () => {
  const { fetchStub } = routingFetch(libraryRoutes([
    { path: '/settings/zim/wiki_en', body: () => { throw new Error('denied'); } },
  ]));
  const { sandbox, elements, timers } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  const target = { dataset: { embed: 'wiki_en' }, checked: true };
  elements.get('zims').dispatch('change', { target });
  await tick();
  assert.equal(target.checked, false, 'checkbox reverted');
  assert.equal(elements.get('toast').textContent, 'Save failed: denied');
  assert.ok(elements.get('toast').className.includes('err'));
});

// ── Per-ZIM category editing (input event, 800ms debounce) ──────────────

test('saveCategory: unchanged value → no request', async () => {
  const { fetchStub, calls } = routingFetch(libraryRoutes());
  const { sandbox, elements, timers } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  elements.get('zims').dispatch('input', { target: { dataset: { cat: 'wiki_en' }, value: 'Wiki' } });
  await timers.flush(1000);
  assert.equal(calls.filter((c) => c.url.startsWith('/settings/zim/')).length, 0);
});

test('saveCategory: changed value → PUT {category}, flash saved marker', async () => {
  const { fetchStub, calls } = routingFetch([
    ...libraryRoutes(),
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { sandbox, elements, timers } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  // Register the rendered "saved ✓" marker so document.querySelector finds it.
  const msg = { dataset: { catmsg: 'wiki_en' }, style: { opacity: 0 } };
  elements.set('__catmsg__', msg);
  elements.get('zims').dispatch('input', { target: { dataset: { cat: 'wiki_en' }, value: 'Reference ' } });
  // Debounced at 800ms: not fired at 100ms, fired at 1000ms.
  await timers.flush(100);
  assert.equal(calls.filter((c) => c.url.startsWith('/settings/zim/')).length, 0, 'still debounced');
  await timers.flush(1000);
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.deepEqual(JSON.parse(put.opts.body), { category: 'Reference' }, 'trimmed');
  assert.equal(msg.style.opacity, 1, 'saved marker flashed');
  await timers.flush(2000);
  assert.equal(msg.style.opacity, 0, 'marker faded');
});

test('saveCategory: cleared value → PUT {category: null}', async () => {
  const { fetchStub, calls } = routingFetch([
    ...libraryRoutes(),
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { sandbox, elements, timers } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  elements.get('zims').dispatch('input', { target: { dataset: { cat: 'wiki_en' }, value: '  ' } });
  await timers.flush(1000);
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.deepEqual(JSON.parse(put.opts.body), { category: null });
});

test('saveCategory: API failure → error toast, no crash', async () => {
  const { fetchStub } = routingFetch([
    ...libraryRoutes(),
    { path: '/settings/zim/wiki_en', body: () => { throw new Error('locked'); } },
  ]);
  const { sandbox, elements, timers } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  elements.get('zims').dispatch('input', { target: { dataset: { cat: 'wiki_en' }, value: 'Nope' } });
  await timers.flush(1000);
  assert.equal(elements.get('toast').textContent, 'Category save failed: locked');
});

// ── Downloads: loadDownloads / renderDownloads / polling ────────────────

test('loadDownloads: fetch error is silent (no status update)', async () => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    { path: '/downloads', body: () => { throw new Error('nope'); } },
  ]);
  const { sandbox } = loadIndex({ fetch: fetchStub });
  await sandbox.loadLibrary();
  await sandbox.loadDownloads(); // must not throw
});

test('loadDownloads: no downloads → empty message, no polling', async () => {
  const { fetchStub } = routingFetch(libraryRoutes());
  const { sandbox, elements, timers } = loadIndex({ fetch: fetchStub });
  await sandbox.loadDownloads();
  assert.ok(elements.get('dlList').innerHTML.includes('No downloads yet'));
  assert.equal(timers.intervals.filter((ms) => ms === 3000).length, 0, 'no polling when nothing active');
});

test('loadDownloads: active download starts polling once; none stops it', async () => {
  let active = true;
  const { fetchStub } = routingFetch(libraryRoutes([
    { path: '/downloads', body: () => ({ downloads: [{ id: 1, status: active ? 'downloading' : 'complete' }] }) },
  ]));
  const { sandbox, timers } = loadIndex({ fetch: fetchStub });
  await sandbox.loadDownloads();
  assert.equal(timers.intervals.filter((ms) => ms === 3000).length, 1, 'top-level + this call share one poll (dlPoll guard)');
  active = false;
  await sandbox.loadDownloads();
  assert.ok(timers.cleared.length >= 1, 'polling stopped');
});

test('renderDownloads: empty list message', () => {
  const { sandbox, elements } = loadIndex();
  sandbox.renderDownloads([]);
  assert.ok(elements.get('dlList').innerHTML.includes('No downloads yet'));
});

test('renderDownloads: queued item has muted progress bar + Cancel button', () => {
  const { sandbox, elements } = loadIndex();
  sandbox.renderDownloads([{ id: 7, name: 'a.zim', url: 'magnet:?xt=1', status: 'queued', progress: 0.45 }]);
  const html = elements.get('dlList').innerHTML;
  assert.ok(html.includes('45%'));
  assert.ok(html.includes('data-id="7"'), 'cancel button carries id');
  assert.ok(html.includes('var(--muted)'));
  assert.ok(html.includes('>queued</span>'));
});

test('renderDownloads: downloading item shows speed + ETA in detail line', () => {
  const { sandbox, elements } = loadIndex();
  sandbox.renderDownloads([{
    id: 1, name: 'b.zim', url: 'https://x/b.zim', status: 'downloading',
    progress: 1.2, // > 1 must clamp
    speed_bps: 1048576, up_speed_bps: 2048, eta_secs: 90,
  }]);
  const html = elements.get('dlList').innerHTML;
  assert.ok(html.includes('width:100%'), 'progress clamped to 100%');
  assert.ok(html.includes('100% · 1.0 MB/s · ▲ 2.0 KB/s · ETA 1m 30s'));
});

test('renderDownloads: seeding item shows upload speed, ratio, seeders', () => {
  const { sandbox, elements } = loadIndex();
  sandbox.renderDownloads([{
    id: 2, name: 'c.zim', url: 'u', status: 'seeding',
    up_speed_bps: 4096, ratio: 1.256, num_seeds: 3,
  }]);
  const html = elements.get('dlList').innerHTML;
  assert.ok(html.includes('▲ 4.0 KB/s · ratio 1.26 · 3 seeders'));
  assert.ok(!html.includes('Cancel'), 'no cancel button when seeding');
});

test('renderDownloads: error download renders escaped error detail', () => {
  const { sandbox, elements } = loadIndex();
  sandbox.renderDownloads([{ id: 3, name: 'd.zim', url: 'u', status: 'error', error: '404 <gone>' }]);
  assert.ok(elements.get('dlList').innerHTML.includes('<span class="err">404 &lt;gone&gt;</span>'));
});

// ── addDownload ─────────────────────────────────────────────────────────

test('addDownload: empty URL → error toast, no request', async () => {
  const { fetchStub, calls } = routingFetch(libraryRoutes());
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  elements.get('dlUrl').value = '   ';
  await sandbox.addDownload();
  assert.equal(calls.filter((c) => c.method === 'POST').length, 0);
  assert.equal(elements.get('toast').textContent, 'Enter a URL or magnet link');
  assert.equal(elements.get('dlAdd').disabled, false);
});

test('addDownload: posts URL (+name), clears inputs, toasts, refreshes', async () => {
  const { fetchStub, calls } = routingFetch(libraryRoutes());
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  // dlName is only referenced inside addDownload(), so materialize it.
  const dlUrl = elements.get('dlUrl');
  const dlName = sandbox.document.getElementById('dlName');
  dlUrl.value = ' magnet:?xt=abc ';
  dlName.value = 'My ZIM';
  await sandbox.addDownload();
  await tick();
  const post = calls.find((c) => c.opts && c.opts.method === 'POST');
  assert.equal(post.url, '/downloads');
  assert.deepEqual(JSON.parse(post.opts.body), { url: 'magnet:?xt=abc', name: 'My ZIM' });
  assert.equal(dlUrl.value, '');
  assert.equal(dlName.value, '');
  assert.equal(elements.get('toast').textContent, 'Download queued');
  assert.ok(elements.get('dlList').innerHTML.includes('No downloads yet'), 'list refreshed from server');
});

test('addDownload: name left blank → omitted from body', async () => {
  const { fetchStub, calls } = routingFetch(libraryRoutes());
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  elements.get('dlUrl').value = 'u';
  await sandbox.addDownload();
  await tick();
  const post = calls.find((c) => c.opts && c.opts.method === 'POST');
  const body = JSON.parse(post.opts.body);
  assert.equal(body.url, 'u');
  assert.equal('name' in body, false);
});

test('addDownload: API error → "Failed: <msg>" toast', async () => {
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
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  elements.get('dlUrl').value = 'u';
  await sandbox.addDownload();
  await tick();
  assert.equal(elements.get('toast').textContent, 'Failed: bad magnet');
});

// ── cancelDownload ──────────────────────────────────────────────────────

test('cancelDownload: DELETE ok → list refreshed', async () => {
  const { fetchStub, calls } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    { path: '/downloads/5', body: {} },
    { path: '/downloads', body: { downloads: [] } },
  ]);
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  await sandbox.cancelDownload(5);
  await tick(); // cancelDownload refreshes the list without awaiting it
  assert.equal(calls.find((c) => c.url === '/downloads/5').opts.method, 'DELETE');
  assert.ok(elements.get('dlList').innerHTML.includes('No downloads yet'));
});

test('cancelDownload: non-ok response → error toast', async () => {
  const { fetchStub } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    { path: '/downloads/5', body: { status: 404, ok: false, json: async () => ({}) } },
    { path: '/downloads', body: { downloads: [] } },
  ]);
  const { sandbox, elements } = loadIndex({ fetch: fetchStub });
  await sandbox.cancelDownload(5);
  assert.equal(elements.get('toast').textContent, 'Cancel failed: 404');
});

test('dlList click: delegated to cancelDownload via data-id', async () => {
  const { fetchStub, calls } = routingFetch([
    { path: '/health', body: HEALTH },
    { path: '/list', body: { zims: [] } },
    { path: '/downloads/9', body: {} },
    { path: '/downloads', body: { downloads: [] } },
  ]);
  const { elements } = loadIndex({ fetch: fetchStub });
  elements.get('dlList').dispatch('click', {
    target: { closest: (sel) => (sel === '[data-id]' ? { dataset: { id: '9' } } : null) },
  });
  await new Promise((r) => setImmediate(r));
  assert.equal(calls.find((c) => c.url === '/downloads/9').opts.method, 'DELETE');
});

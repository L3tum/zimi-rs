// Behavioral unit tests for web/settings.js (admin settings page).
//
// web/settings.js is a classic browser script (no module exports): it reads
// document/location/window/timers and uses web/common.js helpers as globals.
// We load common.js + settings.js verbatim into a vm.runInNewContext sandbox
// with a minimal fake DOM (tests/web/dom.mjs), a stubbed fetch, and a
// controllable prompt, then exercise the top-level functions, the rendered
// markup, and the event handlers by name.
//
// The admin-auth flow (401 → password prompt → single retry with Bearer
// token) lives in common.js's apiFetch but is exercised here end-to-end
// through settings.js's saveAll().
//
// Note: the fake DOM does not parse element.innerHTML strings. Tests that
// need "constructed" elements (the inputs inside rendered fields) register
// them manually in the elements map so document.querySelectorAll finds them.
//
// Run: node --test tests/web/settings.test.mjs   (or: make web-test)

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import vm from 'node:vm';
import { makeDocument, makeEl, makeTimers } from './dom.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const webDir = join(here, '..', '..', 'web');
const commonSource = readFileSync(join(webDir, 'common.js'), 'utf8');
const settingsSource = readFileSync(join(webDir, 'settings.js'), 'utf8');

const ok = (body) => ({ status: 200, ok: true, json: async () => body });

// Yield to the event loop so untracked async handlers (event dispatch, the
// fire-and-forget top-level load()) can finish their microtask chains.
const tick = () => new Promise((r) => setImmediate(r));

// Server-side /settings payload. `general.port` (int), `search.fts_weight`
// (float), `torrent.enabled` (bool), `torrent.password` (password),
// `torrent.file_strategy` (select), `access.admin_password` (locked) cover
// every inputFor() branch; `torrent.unknown` is a key without FIELDS meta;
// `extra_cat` is a category outside CAT_ORDER.
const SETTINGS = {
  general: {
    zim_dir: { value: '/d&ata/zims' },
    port: { value: 4567 },
  },
  search: {
    fts_weight: { value: 0.75 },
    default_limit: { value: 20 },
  },
  torrent: {
    enabled: { value: true },
    password: { value: 'sekret' },
    file_strategy: { value: 'hardlink' },
    unknown: { value: 'x' },
  },
  access: {
    admin_password: { value: 'pw', locked: true, locked_by: 'ZIMSERVICE_ADMIN_PASSWORD' },
  },
  extra_cat: {
    custom_field: { value: 'v' },
  },
};
// After a successful save: default_limit 20 → 25.
const SETTINGS_SAVED = {
  ...SETTINGS,
  search: { ...SETTINGS.search, default_limit: { value: 25 } },
};

function loadSettings(overrides = {}) {
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
    location: { search: '', hash: overrides.locationHash ?? '' },
    window: { addEventListener: () => {} },
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
  vm.runInContext(settingsSource, sandbox, { filename: 'settings.js' });
  return { sandbox, doc, elements, timers, store };
}

// Stubbed fetch that routes by URL path (last match wins) and records every
// call. A route body may be a plain value (wrapped as 200 + json) or a
// function of { url, opts, n } (may throw, or return a full response object).
function routingFetch(routes) {
  const calls = [];
  const fetchStub = async (url, opts) => {
    // Snapshot headers at call time: apiFetch reuses (and mutates) the same
    // opts object for the 401 retry, so a stored reference would show the
    // post-retry state on the first call too.
    calls.push({ url, opts, auth: opts && opts.headers ? opts.headers.Authorization : undefined });
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

const listRoute = { path: '/list', body: { zims: [] } };
const settingsRoutes = (extra = []) => [
  { path: '/settings', body: SETTINGS },
  listRoute,
  ...extra,
];

// Register a fake rendered .field whose [data-key] input is `inputEl`, so
// changedEntries()/bindChanges() (document.querySelectorAll) find it.
function makeField(elements, fullKey, inputEl) {
  const field = makeEl({
    dataset: { field: fullKey },
    querySelector: (sel) => (sel === '[data-key]' ? inputEl : null),
  });
  elements.set(fullKey, field);
  return field;
}

// ── load / render / inputFor ────────────────────────────────────────────

test('load: renders sections in CAT_ORDER with unknown category last', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const html = elements.get('sections').innerHTML;
  const pos = (s) => html.indexOf(s);
  assert.ok(pos('General') < pos('Search'), 'General before Search');
  assert.ok(pos('Search') < pos('Torrent (qBittorrent)'), 'Search before Torrent');
  assert.ok(pos('Torrent (qBittorrent)') < pos('Access'), 'Torrent before Access');
  assert.ok(pos('Access') < pos('<h2>extra_cat</h2>'), 'unknown category appended last');
  // load() also fetches the ZIM list for the per-ZIM section.
  assert.ok(elements.get('zimRows').innerHTML.includes('No ZIMs in the library.'));
});

test('inputFor: label, desc, restart tag, and text input for zim_dir', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const html = elements.get('sections').innerHTML;
  assert.ok(html.includes('<div class="flabel">ZIM directory</div>'));
  assert.ok(html.includes('<div class="desc">Directory scanned for .zim files (watched for changes)</div>'));
  assert.ok(html.includes('⚠ restart'), 'restart tag for general.zim_dir');
  assert.ok(html.includes('data-field="general.zim_dir"'));
});

test('inputFor: numeric values → number input (float gets step="any")', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const html = elements.get('sections').innerHTML;
  assert.ok(html.includes('<input type="number"  data-key="general.port" value="4567" >'), 'int → plain number input');
  assert.ok(html.includes('<input type="number" step="any" data-key="search.fts_weight" value="0.75" >'), 'float → step="any"');
});

test('inputFor: boolean → checked checkbox', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  assert.ok(elements.get('sections').innerHTML.includes(
    '<input type="checkbox" data-key="torrent.enabled" checked '));
});

test('inputFor: select renders options with the current value selected', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const html = elements.get('sections').innerHTML;
  assert.ok(html.includes('<select data-key="torrent.file_strategy" >'));
  assert.ok(html.includes('<option selected>hardlink</option>'));
  assert.ok(html.includes('<option >copy</option>'));
});

test('inputFor: password renders masked input + show button', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const html = elements.get('sections').innerHTML;
  assert.ok(html.includes('<input type="password" data-key="torrent.password" value="sekret"  autocomplete="new-password">'));
  assert.ok(html.includes('<button class="show-pass" >show</button>'));
});

test('inputFor: unknown key falls back to the raw key as label, text input', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const html = elements.get('sections').innerHTML;
  assert.ok(html.includes('<div class="flabel">unknown</div>'));
  assert.ok(html.includes('<input type="text"  data-key="torrent.unknown" value="x" >'));
});

test('inputFor: field values are HTML-escaped', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  assert.ok(elements.get('sections').innerHTML.includes('value="/d&amp;ata/zims"'));
});

test('inputFor: locked fields render disabled with a lock tag naming the source', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const html = elements.get('sections').innerHTML;
  assert.ok(html.includes('🔒 ZIMSERVICE_ADMIN_PASSWORD'));
  // Locked secret: value must NOT be echoed into the DOM.
  assert.ok(html.includes('data-key="access.admin_password" value="" placeholder="locked (set via env)" disabled autocomplete="new-password">'));
  assert.ok(!html.includes('value="pw"'));
  assert.ok(html.includes('<button class="show-pass" disabled>show</button>'));
  // Locked field is disabled → must not count as a pending change.
  assert.equal(elements.get('save').textContent, 'Save');
  assert.ok(elements.get('save').disabled);
});

test('load: /settings failure → error message in #sections', async () => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: () => { throw new Error('nope <x>'); } },
  ]);
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  assert.equal(elements.get('sections').innerHTML,
    '<div class="sub">Failed to load settings: nope &lt;x&gt;</div>');
});

// ── currentValue ────────────────────────────────────────────────────────

test('currentValue: checkbox → checked, SELECT → value, text → raw string', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { sandbox } = loadSettings({ fetch: fetchStub });
  await tick();
  assert.equal(sandbox.currentValue({ type: 'checkbox', checked: true }), true);
  assert.equal(sandbox.currentValue({ type: 'checkbox', checked: false }), false);
  assert.equal(sandbox.currentValue({ type: 'text', tagName: 'SELECT', value: 'info', dataset: { key: 'k' } }), 'info');
  assert.equal(sandbox.currentValue({ type: 'text', value: '  keep  ', dataset: { key: 'k' } }), '  keep  ');
});

test('currentValue: number inputs parse, blank → null', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { sandbox } = loadSettings({ fetch: fetchStub });
  await tick();
  assert.equal(sandbox.currentValue({ type: 'number', value: '7', dataset: { key: 'k' } }), 7);
  assert.equal(sandbox.currentValue({ type: 'number', value: '', dataset: { key: 'k' } }), null);
  assert.equal(sandbox.currentValue({ type: 'number', value: '  ', dataset: { key: 'k' } }), null);
  // Original was numeric → empty input reports null even for text type.
  assert.equal(sandbox.currentValue({
    type: 'text', value: '', dataset: { key: 'search.default_limit' },
  }), null);
});

// ── change detection / save button ──────────────────────────────────────

// One changed fake field (search.default_limit 20 → 25) wired into the DOM.
function withOneChangedField(elements) {
  const input = makeEl({ type: 'number', value: '25', dataset: { key: 'search.default_limit' } });
  makeField(elements, 'search.default_limit', input);
  return input;
}

test('save button: no changes → "Save", disabled', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const btn = elements.get('save');
  assert.equal(btn.textContent, 'Save');
  assert.equal(btn.disabled, true);
});

test('save button: 1 change → "Save 1 change"; 2 → "Save 2 changes", enabled', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { sandbox, elements } = loadSettings({ fetch: fetchStub });
  await tick();
  withOneChangedField(elements);
  sandbox.updateSaveBtn();
  assert.equal(elements.get('save').textContent, 'Save 1 change');
  assert.equal(elements.get('save').disabled, false);

  makeField(elements, 'general.port', makeEl({ type: 'number', value: '8080', dataset: { key: 'general.port' } }));
  sandbox.updateSaveBtn();
  assert.equal(elements.get('save').textContent, 'Save 2 changes');
  assert.equal(elements.get('save').disabled, false);
});

test('saveAll: no changes → no PUT', async () => {
  const { fetchStub, calls } = routingFetch(settingsRoutes());
  const { sandbox } = loadSettings({ fetch: fetchStub });
  await tick();
  await sandbox.saveAll();
  assert.equal(calls.filter((c) => c.opts && c.opts.method === 'PUT').length, 0);
});

test('saveAll: PUTs only changed keys, shows success, reloads, re-enables button', async () => {
  let saved = false;
  const { fetchStub, calls } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => (opts && opts.method === 'PUT'
        ? (() => { saved = true; return ok({ updated: 1, errors: [] }); })()
        : ok(saved ? SETTINGS_SAVED : SETTINGS)),
    },
  ]);
  const { elements, timers } = loadSettings({ fetch: fetchStub });
  await tick();
  withOneChangedField(elements);

  // Drive saveAll through the real delegated click listener on #save.
  elements.get('save').dispatch('click');
  await tick();

  const put = calls.find((c) => c.opts && c.opts.method === 'PUT');
  assert.equal(put.url, '/settings');
  assert.deepEqual(JSON.parse(put.opts.body), { 'search.default_limit': 25 }, 'only the changed key');
  assert.equal(put.opts.headers['Content-Type'], 'application/json');
  const msg = elements.get('msg');
  assert.equal(msg.className, 'msg success');
  assert.equal(msg.textContent, 'Saved 1 setting.');
  // After the post-save reload, the server value matches the input → button
  // is back to the idle state.
  assert.equal(elements.get('save').textContent, 'Save');
  assert.equal(elements.get('save').disabled, true);
  assert.ok(calls.some((c) => c.url === '/settings' && !(c.opts && c.opts.method === 'PUT')), 'reloaded /settings');
  // The success message fades after 6s.
  await timers.flush(7000);
  assert.equal(msg.textContent, '');
  assert.equal(msg.className, 'msg');
});

test('saveAll: per-field errors from the server → error message', async () => {
  const { fetchStub } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => (opts && opts.method === 'PUT'
        ? ok({ updated: 0, errors: ['torrent.url: bad', 'search.max_limit: too small'] })
        : ok(SETTINGS)),
    },
  ]);
  const { sandbox, elements } = loadSettings({ fetch: fetchStub });
  await tick();
  withOneChangedField(elements);
  await sandbox.saveAll();
  const msg = elements.get('msg');
  assert.equal(msg.className, 'msg error');
  assert.equal(msg.textContent, 'torrent.url: bad\nsearch.max_limit: too small');
});

// ── Admin auth flow through saveAll (common.js apiFetch) ────────────────

test('saveAll: 401 → prompt → one retry with Bearer token → saved', async () => {
  let prompts = 0;
  let putCalls = 0;
  let saved = false;
  const { fetchStub, calls } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') {
          putCalls += 1;
          if (putCalls === 1) return { status: 401, ok: false, json: async () => ({ error: 'unauthorized' }) };
          saved = true;
          return ok({ updated: 1, errors: [] });
        }
        return ok(saved ? SETTINGS_SAVED : SETTINGS);
      },
    },
  ]);
  const { sandbox, elements, store } = loadSettings({
    fetch: fetchStub,
    prompt: () => { prompts += 1; return 'admin-tok'; },
  });
  await tick();
  withOneChangedField(elements);
  await sandbox.saveAll();

  const puts = calls.filter((c) => c.opts && c.opts.method === 'PUT');
  assert.equal(puts.length, 2, 'initial 401 + exactly one retry');
  assert.equal(puts[0].auth, undefined, 'first PUT had no token');
  assert.equal(puts[1].auth, 'Bearer admin-tok', 'retry carries the Bearer token');
  assert.equal(prompts, 1, 'prompted exactly once');
  assert.equal(store.get('zimservice_token'), 'admin-tok', 'token persisted in sessionStorage');
  // The post-save reload inherits the stored token automatically.
  const gets = calls.filter((c) => c.url === '/settings' && !(c.opts && c.opts.method === 'PUT'));
  assert.ok(gets.length >= 1);
  assert.equal(gets.at(-1).auth, 'Bearer admin-tok', 'reload reuses stored token');
  assert.equal(elements.get('msg').textContent, 'Saved 1 setting.');
});

test('saveAll: 401 with cancelled prompt → no retry, error shown', async () => {
  let putCalls = 0;
  const { fetchStub } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') { putCalls += 1; return { status: 401, ok: false, json: async () => ({ error: 'unauthorized' }) }; }
        return ok(SETTINGS);
      },
    },
  ]);
  const { sandbox, elements } = loadSettings({
    fetch: fetchStub,
    prompt: () => null, // user cancels
  });
  await tick();
  withOneChangedField(elements);
  await sandbox.saveAll();
  assert.equal(putCalls, 1, 'no retry when the prompt is cancelled');
  assert.equal(elements.get('msg').className, 'msg error');
  assert.equal(elements.get('msg').textContent, 'Error: unauthorized');
});

test('saveAll: 403 → no auth retry at all, error shown', async () => {
  let prompts = 0;
  let putCalls = 0;
  const { fetchStub } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') { putCalls += 1; return { status: 403, ok: false, json: async () => ({ error: 'forbidden' }) }; }
        return ok(SETTINGS);
      },
    },
  ]);
  const { sandbox, elements } = loadSettings({
    fetch: fetchStub,
    prompt: () => { prompts += 1; return 'admin-tok'; },
  });
  await tick();
  withOneChangedField(elements);
  await sandbox.saveAll();
  assert.equal(putCalls, 1, '403 is never retried');
  assert.equal(prompts, 0, '403 never prompts');
  assert.equal(elements.get('msg').className, 'msg error');
  assert.equal(elements.get('msg').textContent, 'Error: forbidden');
  // The save button is restored (saving flag cleared) and still shows the change.
  assert.equal(elements.get('save').textContent, 'Save 1 change');
  assert.equal(elements.get('save').disabled, false);
});

test('saveAll: network error → "Error: <msg>" message', async () => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/settings', body: ({ opts }) => { if (opts && opts.method === 'PUT') throw new TypeError('Failed to fetch'); return ok(SETTINGS); } },
  ]);
  const { sandbox, elements } = loadSettings({ fetch: fetchStub });
  await tick();
  withOneChangedField(elements);
  await sandbox.saveAll();
  assert.equal(elements.get('msg').className, 'msg error');
  assert.equal(elements.get('msg').textContent, 'Error: Failed to fetch');
});

// ── Password show/hide ──────────────────────────────────────────────────

test('togglePass: flips input type password↔text and the button label', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { sandbox } = loadSettings({ fetch: fetchStub });
  await tick();
  const inp = makeEl({ type: 'password', value: 'sekret' });
  const btn = makeEl({ previousElementSibling: inp, textContent: 'show' });
  sandbox.togglePass(btn);
  assert.equal(inp.type, 'text');
  assert.equal(btn.textContent, 'hide');
  sandbox.togglePass(btn);
  assert.equal(inp.type, 'password');
  assert.equal(btn.textContent, 'show');
});

test('sections click: delegated .show-pass toggles the sibling input', async () => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const inp = makeEl({ type: 'password', value: 'pw' });
  const btn = makeEl({
    previousElementSibling: inp,
    textContent: 'show',
    closest: (sel) => (sel === '.show-pass' ? btn : null),
  });
  elements.get('sections').dispatch('click', { target: btn });
  assert.equal(inp.type, 'text', 'password revealed via delegation');
  assert.equal(btn.textContent, 'hide');
  // A click on a non-button target does nothing.
  const plain = makeEl({ closest: () => null });
  elements.get('sections').dispatch('click', { target: plain });
  assert.equal(inp.type, 'text', 'unchanged');
});

// ── Per-ZIM rows ────────────────────────────────────────────────────────

const ZIMS = [{ name: 'wiki_en', display_title: 'Wiki EN', embed_enabled: true, category: 'Wiki' }];

test('loadZimRows: renders one row per ZIM with embed + category controls', async () => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
  ]);
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const html = elements.get('zimRows').innerHTML;
  assert.ok(html.includes('id="zim-wiki_en"'));
  assert.ok(html.includes('data-zembed="wiki_en" checked'));
  assert.ok(html.includes('data-zcat="wiki_en" value="Wiki"'));
});

test('loadZimRows: /list failure → error sub-message', async () => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: () => { throw new Error('nope <x>'); } },
  ]);
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  assert.equal(elements.get('zimRows').innerHTML, '<div class="sub">Could not load ZIM list: nope &lt;x&gt;</div>');
});

test('zim embed toggle: change → PUT embed_enabled + toast', async () => {
  const { fetchStub, calls } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  elements.get('zimRows').dispatch('change', { target: { dataset: { zembed: 'wiki_en' }, checked: false } });
  await tick();
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.equal(put.opts.method, 'PUT');
  assert.deepEqual(JSON.parse(put.opts.body), { embed_enabled: false });
  assert.equal(elements.get('toast').textContent, 'Embedding disabled for wiki_en');
});

test('zim embed toggle: API failure → error toast + checkbox reverted', async () => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/settings/zim/wiki_en', body: () => { throw new Error('denied'); } },
  ]);
  const { elements } = loadSettings({ fetch: fetchStub });
  await tick();
  const target = { dataset: { zembed: 'wiki_en' }, checked: false };
  elements.get('zimRows').dispatch('change', { target });
  await tick();
  assert.equal(target.checked, true, 'checkbox reverted');
  assert.equal(elements.get('toast').textContent, 'Save failed: denied');
  assert.ok(elements.get('toast').className.includes('err'));
});

test('zim category: debounced PUT {category}; unchanged value → no request', async () => {
  const { fetchStub, calls } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { elements, timers } = loadSettings({ fetch: fetchStub });
  await tick();
  // Unchanged ("Wiki") → no PUT even after the debounce window.
  elements.get('zimRows').dispatch('input', { target: { dataset: { zcat: 'wiki_en' }, value: 'Wiki' } });
  await timers.flush(1000);
  assert.equal(calls.filter((c) => c.url.startsWith('/settings/zim/')).length, 0, 'unchanged is a no-op');
  // Changed → PUT after 800ms.
  elements.get('zimRows').dispatch('input', { target: { dataset: { zcat: 'wiki_en' }, value: '  Reference  ' } });
  await timers.flush(800);
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.deepEqual(JSON.parse(put.opts.body), { category: 'Reference' }, 'trimmed');
});

test('zim category: PUT failure → error toast, no crash', async () => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/settings/zim/wiki_en', body: () => { throw new Error('locked'); } },
  ]);
  const { elements, timers } = loadSettings({ fetch: fetchStub });
  await tick();
  elements.get('zimRows').dispatch('input', { target: { dataset: { zcat: 'wiki_en' }, value: 'Nope' } });
  await timers.flush(800);
  assert.equal(elements.get('toast').textContent, 'Category save failed: locked');
});

// ── Anchor flash (#zim-<name>) ──────────────────────────────────────────

test('anchor #zim-<name>: flashes the row, then removes the flash class', async () => {
  let scrolled = 0;
  const row = makeEl({});
  row.scrollIntoView = () => { scrolled += 1; };
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
  ]);
  const { elements, timers } = loadSettings({ fetch: fetchStub, locationHash: '#zim-wiki_en' });
  elements.set('zim-wiki_en', row); // pre-rendered row the page would look up
  await tick();
  assert.equal(row.classList.contains('flash'), false, 'flash is applied on the 60ms timer');
  await timers.flush(100);
  assert.equal(row.classList.contains('flash'), true);
  assert.equal(scrolled, 1, 'row scrolled into view');
  await timers.flush(2000);
  assert.equal(row.classList.contains('flash'), false, 'flash cleared after 1600ms');
});

test('anchor without #zim- prefix is ignored', async () => {
  const row = makeEl({});
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
  ]);
  const { elements, timers } = loadSettings({ fetch: fetchStub, locationHash: '#general' });
  elements.set('zim-general', row);
  await tick();
  await timers.flush(1000);
  assert.equal(row.classList.contains('flash'), false);
});

// Behavioral unit tests for web/settings.js (admin settings page).
//
// web/settings.js is a classic browser script (no module exports): it reads
// document/location/window/timers and uses web/common.js helpers as globals.
// We boot common.js + settings.js as real top-level classic scripts in a
// jsdom window against the minimal HTML the page scripts touch
// (tests/web/jsdom.mjs #bootScripts), with a stubbed fetch, a controllable
// prompt, and controllable timers.
//
// The admin-auth flow (401 → password prompt → single retry with Bearer
// token) lives in common.js's apiFetch but is exercised here end-to-end
// through settings.js's saveAll().
//
// The top-level load() fires at script boot, so every test's routes must
// include /settings (and /list for the per-ZIM section).
//
// Run: node --test tests/web/settings.test.mjs   (or: make web-test)

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { bootScripts, tick } from './jsdom.mjs';

// Minimal skeleton of web/settings.html: everything settings.js touches by
// id (sections, zimRows, save, msg) plus the #toast element used by the
// shared toast() helper in common.js.
const HTML = `<!doctype html>
<html><head><title>settings</title></head>
<body>
  <div id="sections"><div class="sub">Loading…</div></div>
  <div class="section">
    <h2>Per-ZIM</h2>
    <div id="zimRows"><div class="sub">Loading…</div></div>
  </div>
  <div class="save-bar">
    <button class="save-btn" id="save" disabled>Save</button>
    <div class="msg" id="msg"></div>
  </div>
  <div class="toast" id="toast"></div>
</body></html>`;

// Default fetch: loud failure if a test forgets to stub one.
const NO_FETCH = () => {
  throw new Error('fetch should not be called in these tests');
};

/**
 * Boot common.js + settings.js in a fresh jsdom window. `hash` is appended to
 * http://localhost/settings.html (include the "#") to exercise the
 * #zim-<name> anchor-flash path. `close` MUST be registered with `t.after`
 * (an open window hangs the run).
 */
function loadSettings({ fetch: fetchFn, prompt, hash = '' } = {}) {
  const scrollCalls = [];
  const { window, doc, errors, timers } = bootScripts(
    HTML,
    ['common.js', 'settings.js'],
    {
      fetchHandler: fetchFn ?? NO_FETCH,
      promptHandler: prompt,
      timers: true,
      url: `http://localhost/settings.html${hash}`,
      beforeParse: (w) => {
        // jsdom has no scrollIntoView implementation (it would log
        // "not implemented"); record the calls the anchor-flash makes.
        w.Element.prototype.scrollIntoView = function () { scrollCalls.push(this); };
      },
    },
  );
  assert.deepEqual(errors, [], errors.map((e) => e.message).join('; '));
  return { window, doc, timers, scrollCalls, close: () => window.close() };
}

// Stubbed fetch that routes by URL path (last match wins) and records every
// call. A route body may be a plain value (wrapped as 200 + json) or a
// function of { url, opts, n } (may throw, or return a full response object).
function routingFetch(routes) {
  const calls = [];
  const fetchStub = async (url, opts) => {
    // Snapshot the Authorization header at call time: apiFetch reuses (and
    // mutates) the same opts object for the 401 retry, so a stored reference
    // would show the post-retry state on the first call too.
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
    return { status: 200, ok: true, json: async () => body };
  };
  return { fetchStub, calls };
}

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

const listRoute = { path: '/list', body: { zims: [] } };
const settingsRoutes = (extra = []) => [
  { path: '/settings', body: SETTINGS },
  listRoute,
  ...extra,
];

const ZIMS = [{ name: 'wiki_en', display_title: 'Wiki EN', embed_enabled: true, category: 'Wiki' }];

// ── load / render / inputFor ────────────────────────────────────────────

test('load: renders sections in CAT_ORDER with unknown category last', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const html = doc.getElementById('sections').innerHTML;
  const pos = (s) => html.indexOf(s);
  assert.ok(pos('General') < pos('Search'), 'General before Search');
  assert.ok(pos('Search') < pos('Torrent (qBittorrent)'), 'Search before Torrent');
  assert.ok(pos('Torrent (qBittorrent)') < pos('Access'), 'Torrent before Access');
  assert.ok(pos('Access') < pos('<h2>extra_cat</h2>'), 'unknown category appended last');
  // load() also fetches the ZIM list for the per-ZIM section.
  assert.ok(doc.getElementById('zimRows').innerHTML.includes('No ZIMs in the library.'));
});

test('inputFor: label, desc, restart tag, and text input for zim_dir', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const html = doc.getElementById('sections').innerHTML;
  assert.ok(html.includes('<div class="flabel">ZIM directory</div>'));
  const desc = '<div class="desc">Directory scanned for .zim files (watched for changes)</div>';
  assert.ok(html.includes(desc));
  assert.ok(html.includes('⚠ restart'), 'restart tag for general.zim_dir');
  assert.ok(html.includes('data-field="general.zim_dir"'));
  const inp = doc.querySelector('[data-key="general.zim_dir"]');
  assert.equal(inp.type, 'text');
});

test('inputFor: numeric values → number input (float gets step="any")', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const port = doc.querySelector('[data-key="general.port"]');
  assert.equal(port.type, 'number', 'int → number input');
  assert.equal(port.value, '4567');
  assert.equal(port.getAttribute('step'), null, 'int → no step attr');
  const fw = doc.querySelector('[data-key="search.fts_weight"]');
  assert.equal(fw.type, 'number');
  assert.equal(fw.getAttribute('step'), 'any', 'float → step="any"');
  assert.equal(fw.value, '0.75');
});

test('inputFor: boolean → checked checkbox', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const cb = doc.querySelector('[data-key="torrent.enabled"]');
  assert.equal(cb.type, 'checkbox');
  assert.equal(cb.checked, true);
});

test('inputFor: select renders options with the current value selected', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const sel = doc.querySelector('[data-key="torrent.file_strategy"]');
  assert.equal(sel.value, 'hardlink');
  const opts = [...sel.options].map((o) => [o.text, o.selected]);
  assert.deepEqual(opts, [['hardlink', true], ['copy', false]]);
});

test('inputFor: password renders masked input + show button', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const inp = doc.querySelector('[data-key="torrent.password"]');
  assert.equal(inp.type, 'password');
  assert.equal(inp.value, 'sekret');
  assert.equal(inp.getAttribute('autocomplete'), 'new-password');
  const btn = inp.nextElementSibling;
  assert.ok(btn.classList.contains('show-pass'), 'show button follows the input');
  assert.equal(btn.textContent, 'show');
});

test('inputFor: unknown key falls back to the raw key as label, text input', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  assert.ok(doc.getElementById('sections').innerHTML.includes('<div class="flabel">unknown</div>'));
  const inp = doc.querySelector('[data-key="torrent.unknown"]');
  assert.equal(inp.type, 'text');
  assert.equal(inp.value, 'x');
});

test('inputFor: field values are HTML-escaped', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const inp = doc.querySelector('[data-key="general.zim_dir"]');
  assert.equal(inp.value, '/d&ata/zims');
  // The ampersand must be escaped in the serialized markup (real parser).
  assert.ok(doc.getElementById('sections').innerHTML.includes('value="/d&amp;ata/zims"'));
});

test('inputFor: locked fields render disabled with a lock tag naming the source', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const html = doc.getElementById('sections').innerHTML;
  assert.ok(html.includes('🔒 ZIMSERVICE_ADMIN_PASSWORD'));
  const inp = doc.querySelector('[data-key="access.admin_password"]');
  // Locked secret: value must NOT be echoed into the DOM.
  assert.equal(inp.value, '');
  assert.ok(inp.disabled);
  assert.equal(inp.getAttribute('placeholder'), 'locked (set via env)');
  assert.ok(!html.includes('value="pw"'));
  const btn = inp.nextElementSibling;
  assert.ok(btn.disabled, 'show button disabled for a locked field');
  // Locked field is disabled → must not count as a pending change.
  assert.equal(doc.getElementById('save').textContent, 'Save');
  assert.ok(doc.getElementById('save').disabled);
});

test('load: /settings failure → error message in #sections', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: () => { throw new Error('nope <x>'); } },
  ]);
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const sub = doc.querySelector('#sections .sub');
  assert.equal(sub.textContent, 'Failed to load settings: nope <x>');
});

// ── currentValue ────────────────────────────────────────────────────────

test('currentValue: checkbox → checked, SELECT → value, text → raw string', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const cb = doc.createElement('input');
  cb.type = 'checkbox';
  cb.checked = true;
  assert.equal(window.currentValue(cb), true);
  cb.checked = false;
  assert.equal(window.currentValue(cb), false);
  const sel = doc.createElement('select');
  sel.innerHTML = '<option value="info">i</option>';
  sel.value = 'info';
  sel.dataset.key = 'k';
  assert.equal(window.currentValue(sel), 'info');
  const txt = doc.createElement('input');
  txt.type = 'text';
  txt.value = '  keep  ';
  txt.dataset.key = 'k';
  assert.equal(window.currentValue(txt), '  keep  ');
});

test('currentValue: number inputs parse, blank → null', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const num = (v) => {
    const el = doc.createElement('input');
    el.type = 'number';
    el.value = v;
    el.dataset.key = 'k';
    return window.currentValue(el);
  };
  assert.equal(num('7'), 7);
  assert.equal(num(''), null);
  assert.equal(num('  '), null);
  // Original was numeric → empty input reports null even for text type.
  const txt = doc.createElement('input');
  txt.type = 'text';
  txt.value = '';
  txt.dataset.key = 'search.default_limit';
  assert.equal(window.currentValue(txt), null);
});

// ── change detection / save button ──────────────────────────────────────

test('save button: no changes → "Save", disabled', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const btn = doc.getElementById('save');
  assert.equal(btn.textContent, 'Save');
  assert.equal(btn.disabled, true);
});

test('save button: 1 change → "Save 1 change"; 2 → "Save 2 changes", enabled', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  doc.querySelector('[data-key="search.default_limit"]').value = '25';
  window.updateSaveBtn();
  assert.equal(doc.getElementById('save').textContent, 'Save 1 change');
  assert.equal(doc.getElementById('save').disabled, false);

  doc.querySelector('[data-key="general.port"]').value = '8080';
  window.updateSaveBtn();
  assert.equal(doc.getElementById('save').textContent, 'Save 2 changes');
  assert.equal(doc.getElementById('save').disabled, false);
});

test('saveAll: no changes → no PUT', async (t) => {
  const { fetchStub, calls } = routingFetch(settingsRoutes());
  const { window, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  await window.saveAll();
  assert.equal(calls.filter((c) => c.opts && c.opts.method === 'PUT').length, 0);
});

test('saveAll: PUTs only changed keys, shows success, reloads, re-enables button', async (t) => {
  let saved = false;
  const ok = (body) => ({ status: 200, ok: true, json: async () => body });
  const { fetchStub, calls } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') {
          saved = true;
          return ok({ updated: 1, errors: [] });
        }
        return ok(saved ? SETTINGS_SAVED : SETTINGS);
      },
    },
  ]);
  const { window, doc, timers, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  doc.querySelector('[data-key="search.default_limit"]').value = '25';

  // Drive saveAll through the real listener registered on #save. jsdom
  // suppresses .click() on disabled buttons, so enable the button first
  // (it would be enabled by updateSaveBtn() in a real interaction) and
  // dispatch a plain event to hit the handler.
  doc.getElementById('save').disabled = false;
  doc.getElementById('save').dispatchEvent(new window.Event('click'));
  await tick();

  const put = calls.find((c) => c.opts && c.opts.method === 'PUT');
  assert.equal(put.url, '/settings');
  const putBody = { 'search.default_limit': 25 };
  assert.deepEqual(JSON.parse(put.opts.body), putBody, 'only the changed key');
  assert.equal(put.opts.headers['Content-Type'], 'application/json');
  const msg = doc.getElementById('msg');
  assert.equal(msg.className, 'msg success');
  assert.equal(msg.textContent, 'Saved 1 setting.');
  // After the post-save reload, the server value matches the input → button
  // is back to the idle state.
  assert.equal(doc.getElementById('save').textContent, 'Save');
  assert.equal(doc.getElementById('save').disabled, true);
  assert.ok(
    calls.some((c) => c.url === '/settings' && !(c.opts && c.opts.method === 'PUT')),
    'reloaded /settings',
  );
  // The success message fades after 6s.
  await timers.flush(7000);
  assert.equal(msg.textContent, '');
  assert.equal(msg.className, 'msg');
});

test('saveAll: per-field errors from the server → error message', async (t) => {
  const ok = (body) => ({ status: 200, ok: true, json: async () => body });
  const { fetchStub } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') {
          return ok({ updated: 0, errors: ['torrent.url: bad', 'search.max_limit: too small'] });
        }
        return ok(SETTINGS);
      },
    },
  ]);
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  doc.querySelector('[data-key="search.default_limit"]').value = '25';
  await window.saveAll();
  const msg = doc.getElementById('msg');
  assert.equal(msg.className, 'msg error');
  assert.equal(msg.textContent, 'torrent.url: bad\nsearch.max_limit: too small');
});

// ── Admin auth flow through saveAll (common.js apiFetch) ────────────────

test('saveAll: 401 → prompt → one retry with Bearer token → saved', async (t) => {
  let prompts = 0;
  let putCalls = 0;
  let saved = false;
  const ok = (body) => ({ status: 200, ok: true, json: async () => body });
  const { fetchStub, calls } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') {
          putCalls += 1;
          const unauthorized = {
            status: 401, ok: false, json: async () => ({ error: 'unauthorized' }),
          };
          if (putCalls === 1) return unauthorized;
          saved = true;
          return ok({ updated: 1, errors: [] });
        }
        return ok(saved ? SETTINGS_SAVED : SETTINGS);
      },
    },
  ]);
  const { window, doc, close } = loadSettings({
    fetch: fetchStub,
    prompt: () => { prompts += 1; return 'admin-tok'; },
  });
  t.after(close);
  await tick();
  doc.querySelector('[data-key="search.default_limit"]').value = '25';
  await window.saveAll();

  const puts = calls.filter((c) => c.opts && c.opts.method === 'PUT');
  assert.equal(puts.length, 2, 'initial 401 + exactly one retry');
  assert.equal(putCalls, 2);
  assert.equal(puts[0].auth, undefined, 'first PUT had no token');
  assert.equal(puts[1].auth, 'Bearer admin-tok', 'retry carries the Bearer token');
  assert.equal(prompts, 1, 'prompted exactly once');
  assert.equal(window.sessionStorage.getItem('zimservice_token'), 'admin-tok',
    'token persisted in sessionStorage');
  // The post-save reload inherits the stored token automatically.
  const gets = calls.filter((c) => c.url === '/settings' && !(c.opts && c.opts.method === 'PUT'));
  assert.ok(gets.length >= 1);
  assert.equal(gets.at(-1).auth, 'Bearer admin-tok', 'reload reuses stored token');
  assert.equal(doc.getElementById('msg').textContent, 'Saved 1 setting.');
});

test('saveAll: 401 with cancelled prompt → no retry, error shown', async (t) => {
  let putCalls = 0;
  const { fetchStub } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') {
          putCalls += 1;
          return { status: 401, ok: false, json: async () => ({ error: 'unauthorized' }) };
        }
        return { status: 200, ok: true, json: async () => SETTINGS };
      },
    },
  ]);
  const { window, doc, close } = loadSettings({
    fetch: fetchStub,
    prompt: () => null, // user cancels
  });
  t.after(close);
  await tick();
  doc.querySelector('[data-key="search.default_limit"]').value = '25';
  await window.saveAll();
  assert.equal(putCalls, 1, 'no retry when the prompt is cancelled');
  assert.equal(doc.getElementById('msg').className, 'msg error');
  assert.equal(doc.getElementById('msg').textContent, 'Error: unauthorized');
});

test('saveAll: 403 → no auth retry at all, error shown', async (t) => {
  let prompts = 0;
  let putCalls = 0;
  const { fetchStub } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') {
          putCalls += 1;
          return { status: 403, ok: false, json: async () => ({ error: 'forbidden' }) };
        }
        return { status: 200, ok: true, json: async () => SETTINGS };
      },
    },
  ]);
  const { window, doc, close } = loadSettings({
    fetch: fetchStub,
    prompt: () => { prompts += 1; return 'admin-tok'; },
  });
  t.after(close);
  await tick();
  doc.querySelector('[data-key="search.default_limit"]').value = '25';
  await window.saveAll();
  assert.equal(putCalls, 1, '403 is never retried');
  assert.equal(prompts, 0, '403 never prompts');
  assert.equal(doc.getElementById('msg').className, 'msg error');
  assert.equal(doc.getElementById('msg').textContent, 'Error: forbidden');
  // The save button is restored (saving flag cleared) and still shows the change.
  assert.equal(doc.getElementById('save').textContent, 'Save 1 change');
  assert.equal(doc.getElementById('save').disabled, false);
});

test('saveAll: network error → "Error: <msg>" message', async (t) => {
  const { fetchStub } = routingFetch([
    listRoute,
    {
      path: '/settings',
      body: ({ opts }) => {
        if (opts && opts.method === 'PUT') throw new TypeError('Failed to fetch');
        return { status: 200, ok: true, json: async () => SETTINGS };
      },
    },
  ]);
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  doc.querySelector('[data-key="search.default_limit"]').value = '25';
  await window.saveAll();
  assert.equal(doc.getElementById('msg').className, 'msg error');
  assert.equal(doc.getElementById('msg').textContent, 'Error: Failed to fetch');
});

// ── Password show/hide ──────────────────────────────────────────────────

test('togglePass: flips input type password↔text and the button label', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const btn = doc.querySelector('.field[data-field="torrent.password"] .show-pass');
  const inp = btn.previousElementSibling;
  assert.equal(inp.type, 'password');
  window.togglePass(btn);
  assert.equal(inp.type, 'text');
  assert.equal(btn.textContent, 'hide');
  window.togglePass(btn);
  assert.equal(inp.type, 'password');
  assert.equal(btn.textContent, 'show');
});

test('sections click: delegated .show-pass toggles the sibling input', async (t) => {
  const { fetchStub } = routingFetch(settingsRoutes());
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const btn = doc.querySelector('.field[data-field="torrent.password"] .show-pass');
  const inp = btn.previousElementSibling;
  btn.dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
  assert.equal(inp.type, 'text', 'password revealed via delegation');
  assert.equal(btn.textContent, 'hide');
  // A click on a non-button target does nothing.
  const plain = doc.createElement('div');
  doc.getElementById('sections').appendChild(plain);
  plain.dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
  assert.equal(inp.type, 'text', 'unchanged');
});

// ── Per-ZIM rows ────────────────────────────────────────────────────────

test('loadZimRows: renders one row per ZIM with embed + category controls', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
  ]);
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const row = doc.getElementById('zim-wiki_en');
  assert.ok(row, 'row id built from the ZIM name');
  assert.equal(row.querySelector('[data-zembed="wiki_en"]').checked, true);
  assert.equal(row.querySelector('[data-zcat="wiki_en"]').value, 'Wiki');
});

test('loadZimRows: /list failure → error sub-message', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: () => { throw new Error('nope <x>'); } },
  ]);
  const { doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const sub = doc.querySelector('#zimRows .sub');
  assert.equal(sub.textContent, 'Could not load ZIM list: nope <x>');
});

test('zim embed toggle: change → PUT embed_enabled + toast', async (t) => {
  const { fetchStub, calls } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const cb = doc.querySelector('[data-zembed="wiki_en"]');
  cb.checked = false;
  cb.dispatchEvent(new window.Event('change', { bubbles: true }));
  await tick();
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.equal(put.opts.method, 'PUT');
  assert.deepEqual(JSON.parse(put.opts.body), { embed_enabled: false });
  assert.equal(doc.getElementById('toast').textContent, 'Embedding disabled for wiki_en');
});

test('zim embed toggle: API failure → error toast + checkbox reverted', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/settings/zim/wiki_en', body: () => { throw new Error('denied'); } },
  ]);
  const { window, doc, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const cb = doc.querySelector('[data-zembed="wiki_en"]');
  cb.checked = false;
  cb.dispatchEvent(new window.Event('change', { bubbles: true }));
  await tick();
  assert.equal(cb.checked, true, 'checkbox reverted');
  const toastEl = doc.getElementById('toast');
  assert.equal(toastEl.textContent, 'Save failed: denied');
  assert.ok(toastEl.className.includes('err'));
});

test('zim category: debounced PUT {category}; unchanged value → no request', async (t) => {
  const { fetchStub, calls } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/settings/zim/wiki_en', body: {} },
  ]);
  const { window, doc, timers, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const cat = doc.querySelector('[data-zcat="wiki_en"]');
  // Unchanged ("Wiki") → no PUT even after the debounce window.
  cat.value = 'Wiki';
  cat.dispatchEvent(new window.Event('input', { bubbles: true }));
  await timers.flush(1000);
  assert.equal(
    calls.filter((c) => c.url.startsWith('/settings/zim/')).length, 0, 'unchanged is a no-op',
  );
  // Changed → PUT after 800ms.
  cat.value = '  Reference  ';
  cat.dispatchEvent(new window.Event('input', { bubbles: true }));
  await timers.flush(800);
  const put = calls.find((c) => c.url === '/settings/zim/wiki_en');
  assert.deepEqual(JSON.parse(put.opts.body), { category: 'Reference' }, 'trimmed');
});

test('zim category: PUT failure → error toast, no crash', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
    { path: '/settings/zim/wiki_en', body: () => { throw new Error('locked'); } },
  ]);
  const { window, doc, timers, close } = loadSettings({ fetch: fetchStub });
  t.after(close);
  await tick();
  const cat = doc.querySelector('[data-zcat="wiki_en"]');
  cat.value = 'Nope';
  cat.dispatchEvent(new window.Event('input', { bubbles: true }));
  await timers.flush(800);
  assert.equal(doc.getElementById('toast').textContent, 'Category save failed: locked');
});

// ── Anchor flash (#zim-<name>) ──────────────────────────────────────────

test('anchor #zim-<name>: flashes the row, then removes the flash class', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
  ]);
  const hash = '#zim-wiki_en';
  const { doc, timers, scrollCalls, close } = loadSettings({ fetch: fetchStub, hash });
  t.after(close);
  await tick();
  const row = doc.getElementById('zim-wiki_en');
  assert.equal(row.classList.contains('flash'), false, 'flash is applied on the 60ms timer');
  await timers.flush(100);
  assert.equal(row.classList.contains('flash'), true);
  assert.equal(scrollCalls.length, 1, 'row scrolled into view');
  assert.equal(scrollCalls[0], row);
  await timers.flush(2000);
  assert.equal(row.classList.contains('flash'), false, 'flash cleared after 1600ms');
});

test('anchor without #zim- prefix is ignored', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/settings', body: SETTINGS },
    { path: '/list', body: { zims: ZIMS } },
  ]);
  const { doc, timers, close } = loadSettings({ fetch: fetchStub, hash: '#general' });
  t.after(close);
  await tick();
  await timers.flush(1000);
  assert.equal(doc.getElementById('zim-wiki_en').classList.contains('flash'), false);
});

// Behavioral unit tests for the pure helpers in web/common.js.
//
// web/common.js is browser code (no module exports, relies on sessionStorage /
// document / prompt / fetch / timers). We load it verbatim via
// vm.runInNewContext into a sandbox with those globals stubbed, then exercise
// the top-level functions by name.
//
// Run: node --test tests/web/common.test.mjs   (or: make web-test)

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import vm from 'node:vm';

const here = dirname(fileURLToPath(import.meta.url));
const source = readFileSync(join(here, '..', '..', 'web', 'common.js'), 'utf8');

function loadCommon(overrides = {}) {
  const store = new Map();
  const sandbox = {
    // sessionStorage stub backed by a Map (token lifetime: per "tab").
    sessionStorage: {
      getItem: (k) => (store.has(k) ? store.get(k) : null),
      setItem: (k, v) => store.set(k, String(v)),
      removeItem: (k) => store.delete(k),
    },
    // document stub: every getElementById returns the same inert element.
    document: {
      getElementById: () => ({ textContent: '', className: '' }),
    },
    prompt: () => null,
    fetch: async () => {
      throw new Error('fetch should not be called in these tests');
    },
    setTimeout: () => 0,
    clearTimeout: () => {},
    // Per-test seams (e.g. controllable fetch / prompt) override the defaults.
    ...overrides,
  };
  vm.createContext(sandbox);
  vm.runInContext(source, sandbox, { filename: 'common.js' });
  return sandbox;
}

// ── esc ─────────────────────────────────────────────────────────────────

test('esc: escapes all five special characters', () => {
  const { esc } = loadCommon();
  assert.equal(esc('&'), '&amp;');
  assert.equal(esc('<'), '&lt;');
  assert.equal(esc('>'), '&gt;');
  assert.equal(esc('"'), '&quot;');
  assert.equal(esc("'"), '&#39;');
});

test('esc: mixed string', () => {
  const { esc } = loadCommon();
  assert.equal(esc(`<a href="x">&'</a>`), '&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;');
});

test('esc: non-special characters unchanged', () => {
  const { esc } = loadCommon();
  assert.equal(esc('plain text 123 {}[];:,./'), 'plain text 123 {}[];:,./');
});

test('esc: null/undefined/numbers coerced safely', () => {
  const { esc } = loadCommon();
  assert.equal(esc(null), '');
  assert.equal(esc(undefined), '');
  assert.equal(esc(0), '0');
  assert.equal(esc(12345), '12345');
});

// ── fmtBytes ────────────────────────────────────────────────────────────
// Impl: units B/KB/MB/GB/TB, divide by 1024 while n >= 1024; decimal places
// = 0 when (n >= 100 after scaling) or unit index is 0 (B), else 1.

test('fmtBytes: null/NaN → "–"', () => {
  const { fmtBytes } = loadCommon();
  assert.equal(fmtBytes(null), '–');
  assert.equal(fmtBytes(undefined), '–');
  assert.equal(fmtBytes(NaN), '–');
});

test('fmtBytes: byte range (0–1023 B, always integer)', () => {
  const { fmtBytes } = loadCommon();
  assert.equal(fmtBytes(0), '0 B');
  assert.equal(fmtBytes(999), '999 B');
  assert.equal(fmtBytes(1023), '1023 B');
});

test('fmtBytes: 1024 boundaries', () => {
  const { fmtBytes } = loadCommon();
  assert.equal(fmtBytes(1024), '1.0 KB');
  assert.equal(fmtBytes(1536), '1.5 KB');
  assert.equal(fmtBytes(2048), '2.0 KB');
  assert.equal(fmtBytes(1048576), '1.0 MB');
  assert.equal(fmtBytes(1073741824), '1.0 GB');
  assert.equal(fmtBytes(1099511627776), '1.0 TB');
});

test('fmtBytes: ≥100 after scaling → 0 decimals', () => {
  const { fmtBytes } = loadCommon();
  assert.equal(fmtBytes(102400), '100 KB');
});

// ── fmtNum ──────────────────────────────────────────────────────────────

test('fmtNum: null → "–"', () => {
  const { fmtNum } = loadCommon();
  assert.equal(fmtNum(null), '–');
  assert.equal(fmtNum(undefined), '–');
});

test('fmtNum: passes through Number.prototype.toLocaleString in this env', () => {
  const { fmtNum } = loadCommon();
  assert.equal(fmtNum(0), '0');
  assert.equal(fmtNum(1234), String((1234).toLocaleString()));
});

// ── fmtEta ──────────────────────────────────────────────────────────────
// Impl: <60 → "Ns"; <3600 → "Xm Ys"; else → "Xh Ym" (seconds dropped).

test('fmtEta: null/negative/Infinity → ""', () => {
  const { fmtEta } = loadCommon();
  assert.equal(fmtEta(null), '');
  assert.equal(fmtEta(undefined), '');
  assert.equal(fmtEta(-1), '');
  assert.equal(fmtEta(Infinity), '');
  assert.equal(fmtEta(-Infinity), '');
});

test('fmtEta: seconds branch', () => {
  const { fmtEta } = loadCommon();
  assert.equal(fmtEta(0), '0s');
  assert.equal(fmtEta(59), '59s');
});

test('fmtEta: minutes branch', () => {
  const { fmtEta } = loadCommon();
  assert.equal(fmtEta(60), '1m 0s');
  assert.equal(fmtEta(3599), '59m 59s');
});

test('fmtEta: hours branch drops the seconds', () => {
  const { fmtEta } = loadCommon();
  assert.equal(fmtEta(3600), '1h 0m');
  // NOTE: 3659s is 1h 0m 59s, but the implementation only renders
  // hours + minutes, so the current behavior is '1h 0m'.
  assert.equal(fmtEta(3659), '1h 0m');
  assert.equal(fmtEta(7261), '2h 1m');
});

// ── _getToken / _setToken (against stubbed sessionStorage) ──────────────

test('_getToken/_setToken: default is empty string', () => {
  const { _getToken } = loadCommon();
  assert.equal(_getToken(), '');
});

test('_getToken/_setToken: set then get round-trips', () => {
  const { _getToken, _setToken } = loadCommon();
  _setToken('secret');
  assert.equal(_getToken(), 'secret');
  _setToken('other');
  assert.equal(_getToken(), 'other');
});

test('_getToken/_setToken: falsy arg clears the token', () => {
  const { _getToken, _setToken, sessionStorage } = loadCommon();
  _setToken('secret');
  assert.equal(sessionStorage.getItem('zimservice_token'), 'secret');
  _setToken('');
  assert.equal(_getToken(), '');
  _setToken(null);
  assert.equal(_getToken(), '');
  _setToken(undefined);
  assert.equal(_getToken(), '');
});

// ── apiJson ─────────────────────────────────────────────────────────────

test('apiJson: happy path returns parsed JSON body', async () => {
  const body = { results: [1, 2, 3], total: 3 };
  const { apiJson } = loadCommon({
    fetch: async (url, opts) => ({ status: 200, ok: true, json: async () => body }),
  });
  const data = await apiJson('/search?q=test');
  assert.deepEqual(data, body);
});

test('apiJson: passes opts (method, headers) through to apiFetch→fetch', async () => {
  const calls = [];
  const { apiJson } = loadCommon({
    fetch: async (url, opts) => {
      calls.push({ url, opts });
      return { status: 200, ok: true, json: async () => ({}) };
    },
  });
  await apiJson('/settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: '{"a":1}' });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, '/settings');
  assert.equal(calls[0].opts.method, 'PUT');
  assert.equal(calls[0].opts.headers['Content-Type'], 'application/json');
  assert.equal(calls[0].opts.body, '{"a":1}');
});

test('apiJson: 400 with error field throws Error with that message', async () => {
  const { apiJson } = loadCommon({
    fetch: async () => ({ status: 400, ok: false, json: async () => ({ error: 'bad input' }) }),
  });
  await assert.rejects(() => apiJson('/x'), { message: 'bad input' });
});

test('apiJson: 500 with no error field falls back to "HTTP <status>"', async () => {
  const { apiJson } = loadCommon({
    fetch: async () => ({ status: 500, ok: false, json: async () => ({}) }),
  });
  await assert.rejects(() => apiJson('/x'), { message: 'HTTP 500' });
});

test('apiJson: 404 with error field throws with that message', async () => {
  const { apiJson } = loadCommon({
    fetch: async () => ({ status: 404, ok: false, json: async () => ({ error: 'not found' }) }),
  });
  await assert.rejects(() => apiJson('/downloads/99'), { message: 'not found' });
});

test('apiJson: 200 always resolves (even if body is falsy-ish)', async () => {
  const { apiJson } = loadCommon({
    fetch: async () => ({ status: 200, ok: true, json: async () => null }),
  });
  const data = await apiJson('/x');
  assert.equal(data, null);
});

test('apiJson: 401 triggers the apiFetch auth-retry path', async () => {
  let calls = 0;
  const { apiJson } = loadCommon({
    prompt: () => 'newtoken',
    fetch: async (url, opts) => {
      calls += 1;
      if (calls === 1) return { status: 401, ok: false, json: async () => ({ error: 'unauthorized' }) };
      return { status: 200, ok: true, json: async () => ({ token: 'newtoken' }) };
    },
  });
  const data = await apiJson('/settings');
  assert.equal(calls, 2, 'initial 401 + one retry');
  assert.deepEqual(data, { token: 'newtoken' });
});

test('apiJson: network error propagates as-is', async () => {
  const { apiJson } = loadCommon({
    fetch: async () => { throw new TypeError('Failed to fetch'); },
  });
  await assert.rejects(() => apiJson('/x'), { message: 'Failed to fetch' });
});

// ── apiFetch admin-auth flow (stubbed fetch + prompt) ────────────────────

test('apiFetch: attaches stored token as Bearer header', async () => {
  const calls = [];
  const { apiFetch, _setToken } = loadCommon({
    fetch: async (url, opts) => { calls.push(opts.headers); return { status: 200 }; },
  });
  _setToken('secret');
  const r = await apiFetch('/api/things');
  assert.equal(r.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].Authorization, 'Bearer secret');
});

test('apiFetch: 401 → prompt → retries exactly once with the new token', async () => {
  let prompts = 0;
  const calls = [];
  const { apiFetch } = loadCommon({
    prompt: () => { prompts += 1; return 'newtok'; },
    fetch: async (url, opts) => {
      calls.push(opts.headers && opts.headers.Authorization);
      return { status: calls.length === 1 ? 401 : 200 };
    },
  });
  const r = await apiFetch('/api/things');
  assert.equal(r.status, 200);
  assert.equal(calls.length, 2, 'initial + exactly one retry');
  assert.equal(calls[0], undefined, 'first call had no token');
  assert.equal(calls[1], 'Bearer newtok');
  assert.equal(prompts, 1, 'prompted exactly once');
});

test('apiFetch: never retries more than once (second 401 returned as-is)', async () => {
  let prompts = 0;
  const calls = [];
  const { apiFetch } = loadCommon({
    prompt: () => { prompts += 1; return 'again'; },
    fetch: async (url, opts) => { calls.push(opts.headers && opts.headers.Authorization); return { status: 401 }; },
  });
  const r = await apiFetch('/api/things');
  assert.equal(r.status, 401);
  assert.equal(calls.length, 2, 'no second retry');
  assert.equal(prompts, 1, 'prompted at most once');
});

test('apiFetch: 401 with cancelled prompt does not retry', async () => {
  const calls = [];
  const { apiFetch } = loadCommon({
    prompt: () => null, // user cancels
    fetch: async () => { calls.push(1); return { status: 401 }; },
  });
  const r = await apiFetch('/api/things');
  assert.equal(r.status, 401);
  assert.equal(calls.length, 1);
});

// ── snippetHtml ─────────────────────────────────────────────────────────
// Converts <b>…</b> API highlight tags to <mark>…</mark>, escaping all other
// content. Unbalanced tags are rendered as literal escaped text.

test('snippetHtml: empty/null input returns empty string', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(snippetHtml(''), '');
  assert.equal(snippetHtml(null), '');
  assert.equal(snippetHtml(undefined), '');
});

test('snippetHtml: plain text (no tags) is escaped and returned as-is', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(snippetHtml('hello world'), 'hello world');
  assert.equal(snippetHtml('a < b & c > d'), 'a &lt; b &amp; c &gt; d');
});

test('snippetHtml: single <b>…</b> pair becomes <mark>…</mark>', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(snippetHtml('before <b>match</b> after'), 'before <mark>match</mark> after');
});

test('snippetHtml: multiple <b>…</b> pairs', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(
    snippetHtml('<b>alpha</b> mid <b>beta</b>'),
    '<mark>alpha</mark> mid <mark>beta</mark>',
  );
});

test('snippetHtml: content inside <b> is still escaped', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(snippetHtml('<b>a &amp; "b"</b>'), '<mark>a &amp;amp; &quot;b&quot;</mark>');
  // ^ the inner & is from the source; after esc() it becomes &amp;amp; (literal ampersand in HTML source)
  // Actually: raw source is <b>a & "b"</b>. The "a & \"b\"" gets esc'd to "a &amp; &quot;b\"".
});

test('snippetHtml: unbalanced opening <b> (no close) still emits <mark>', () => {
  const { snippetHtml } = loadCommon();
  // The first <b> opens a mark; without a closing </b> the mark is never closed.
  assert.equal(snippetHtml('start <b> no close'), 'start <mark> no close');
});

test('snippetHtml: unbalanced closing </b> renders as escaped text', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(snippetHtml('no open </b> end'), 'no open &lt;/b&gt; end');
});

test('snippetHtml: extra closing tag after balanced pair is escaped', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(snippetHtml('<b>ok</b></b>'), '<mark>ok</mark>&lt;/b&gt;');
});

test('snippetHtml: extra opening tag while already inMark is escaped', () => {
  const { snippetHtml } = loadCommon();
  // First <b> opens a mark; second <b> (inMark=true) → literal; </b> closes the mark.
  assert.equal(snippetHtml('<b><b>ok</b>'), '<mark>&lt;b&gt;ok</mark>');
});

test('snippetHtml: adjacent pairs with no text between', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(snippetHtml('<b>a</b><b>b</b>'), '<mark>a</mark><mark>b</mark>');
});

test('snippetHtml: special chars outside tags are escaped', () => {
  const { snippetHtml } = loadCommon();
  assert.equal(snippetHtml('5 < 6 & 7 > 6 <b>hi</b>'), '5 &lt; 6 &amp; 7 &gt; 6 <mark>hi</mark>');
});

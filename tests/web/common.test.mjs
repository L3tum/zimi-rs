// Behavioral unit tests for the pure helpers in web/common.js.
//
// web/common.js is browser code (no module exports, relies on sessionStorage /
// document / prompt / fetch / timers). We boot it in a real jsdom window as a
// top-level classic script (tests/web/jsdom.mjs #bootScripts) and exercise the
// top-level functions via the window globals. Each test gets a fresh window,
// so sessionStorage and the DOM start clean.
//
// Run: node --test tests/web/common.test.mjs   (or: make web-test)

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { bootScripts } from './jsdom.mjs';

// Minimal skeleton: common.js only needs a #toast element (toast()) and a
// working sessionStorage (provided natively by jsdom per window).
const HTML = '<!doctype html><html><head><title>common</title></head>'
  + '<body><div class="toast" id="toast"></div></body></html>';

// Default fetch: loud failure if a test forgets to stub one (same policy as
// the old vm-sandbox harness).
const NO_FETCH = () => {
  throw new Error('fetch should not be called in these tests');
};

/**
 * Boot web/common.js in a fresh jsdom window. `fetch`/`prompt` are per-test
 * seams installed before the script runs. The returned `close` MUST be
 * registered with `t.after` — an open jsdom window keeps node's event loop
 * alive and would hang `node --test`.
 */
function loadCommon({ fetch: fetchFn, prompt } = {}) {
  const { window, doc, errors } = bootScripts(HTML, ['common.js'], {
    fetchHandler: fetchFn ?? NO_FETCH,
    promptHandler: prompt,
  });
  assert.deepEqual(errors, [], errors.map((e) => e.message).join('; '));
  return { window, doc, close: () => window.close() };
}

// ── esc ─────────────────────────────────────────────────────────────────

test('esc: escapes all five special characters', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.esc('&'), '&amp;');
  assert.equal(w.esc('<'), '&lt;');
  assert.equal(w.esc('>'), '&gt;');
  assert.equal(w.esc('"'), '&quot;');
  assert.equal(w.esc("'"), '&#39;');
});

test('esc: mixed string', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.esc(`<a href="x">&'</a>`), '&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;');
});

test('esc: non-special characters unchanged', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.esc('plain text 123 {}[];:,./'), 'plain text 123 {}[];:,./');
});

test('esc: null/undefined/numbers coerced safely', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.esc(null), '');
  assert.equal(w.esc(undefined), '');
  assert.equal(w.esc(0), '0');
  assert.equal(w.esc(12345), '12345');
});

// ── fmtBytes ────────────────────────────────────────────────────────────
// Impl: units B/KB/MB/GB/TB, divide by 1024 while n >= 1024; decimal places
// = 0 when (n >= 100 after scaling) or unit index is 0 (B), else 1.

test('fmtBytes: null/NaN → "–"', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtBytes(null), '–');
  assert.equal(w.fmtBytes(undefined), '–');
  assert.equal(w.fmtBytes(NaN), '–');
});

test('fmtBytes: byte range (0–1023 B, always integer)', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtBytes(0), '0 B');
  assert.equal(w.fmtBytes(999), '999 B');
  assert.equal(w.fmtBytes(1023), '1023 B');
});

test('fmtBytes: 1024 boundaries', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtBytes(1024), '1.0 KB');
  assert.equal(w.fmtBytes(1536), '1.5 KB');
  assert.equal(w.fmtBytes(2048), '2.0 KB');
  assert.equal(w.fmtBytes(1048576), '1.0 MB');
  assert.equal(w.fmtBytes(1073741824), '1.0 GB');
  assert.equal(w.fmtBytes(1099511627776), '1.0 TB');
});

test('fmtBytes: ≥100 after scaling → 0 decimals', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtBytes(102400), '100 KB');
});

// ── fmtNum ──────────────────────────────────────────────────────────────

test('fmtNum: null → "–"', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtNum(null), '–');
  assert.equal(w.fmtNum(undefined), '–');
});

test('fmtNum: passes through Number.prototype.toLocaleString in this env', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtNum(0), '0');
  assert.equal(w.fmtNum(1234), String((1234).toLocaleString()));
});

// ── fmtEta ──────────────────────────────────────────────────────────────
// Impl: <60 → "Ns"; <3600 → "Xm Ys"; else → "Xh Ym" (seconds dropped).

test('fmtEta: null/negative/Infinity → ""', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtEta(null), '');
  assert.equal(w.fmtEta(undefined), '');
  assert.equal(w.fmtEta(-1), '');
  assert.equal(w.fmtEta(Infinity), '');
  assert.equal(w.fmtEta(-Infinity), '');
});

test('fmtEta: seconds branch', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtEta(0), '0s');
  assert.equal(w.fmtEta(59), '59s');
});

test('fmtEta: minutes branch', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtEta(60), '1m 0s');
  assert.equal(w.fmtEta(3599), '59m 59s');
});

test('fmtEta: hours branch drops the seconds', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.fmtEta(3600), '1h 0m');
  // NOTE: 3659s is 1h 0m 59s, but the implementation only renders
  // hours + minutes, so the current behavior is '1h 0m'.
  assert.equal(w.fmtEta(3659), '1h 0m');
  assert.equal(w.fmtEta(7261), '2h 1m');
});

// ── _getToken / _setToken (against jsdom's real sessionStorage) ─────────

test('_getToken/_setToken: default is empty string', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w._getToken(), '');
});

test('_getToken/_setToken: set then get round-trips', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  w._setToken('secret');
  assert.equal(w._getToken(), 'secret');
  w._setToken('other');
  assert.equal(w._getToken(), 'other');
});

test('_getToken/_setToken: falsy arg clears the token', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  w._setToken('secret');
  assert.equal(w.sessionStorage.getItem('zimservice_token'), 'secret');
  w._setToken('');
  assert.equal(w._getToken(), '');
  w._setToken(null);
  assert.equal(w._getToken(), '');
  w._setToken(undefined);
  assert.equal(w._getToken(), '');
});

// ── apiJson ─────────────────────────────────────────────────────────────

test('apiJson: happy path returns parsed JSON body', async (t) => {
  const body = { results: [1, 2, 3], total: 3 };
  const { window: w, close } = loadCommon({
    fetch: async () => ({ status: 200, ok: true, json: async () => body }),
  });
  t.after(close);
  const data = await w.apiJson('/search?q=test');
  assert.deepEqual(data, body);
});

test('apiJson: passes opts (method, headers) through to apiFetch→fetch', async (t) => {
  const calls = [];
  const { window: w, close } = loadCommon({
    fetch: async (url, opts) => {
      calls.push({ url, opts });
      return { status: 200, ok: true, json: async () => ({}) };
    },
  });
  t.after(close);
  await w.apiJson(
    '/settings',
    { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: '{"a":1}' },
  );
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, '/settings');
  assert.equal(calls[0].opts.method, 'PUT');
  assert.equal(calls[0].opts.headers['Content-Type'], 'application/json');
  assert.equal(calls[0].opts.body, '{"a":1}');
});

test('apiJson: 400 with error field throws Error with that message', async (t) => {
  const { window: w, close } = loadCommon({
    fetch: async () => ({ status: 400, ok: false, json: async () => ({ error: 'bad input' }) }),
  });
  t.after(close);
  await assert.rejects(() => w.apiJson('/x'), { message: 'bad input' });
});

test('apiJson: 500 with no error field falls back to "HTTP <status>"', async (t) => {
  const { window: w, close } = loadCommon({
    fetch: async () => ({ status: 500, ok: false, json: async () => ({}) }),
  });
  t.after(close);
  await assert.rejects(() => w.apiJson('/x'), { message: 'HTTP 500' });
});

test('apiJson: 404 with error field throws with that message', async (t) => {
  const { window: w, close } = loadCommon({
    fetch: async () => ({ status: 404, ok: false, json: async () => ({ error: 'not found' }) }),
  });
  t.after(close);
  await assert.rejects(() => w.apiJson('/downloads/99'), { message: 'not found' });
});

test('apiJson: 200 always resolves (even if body is falsy-ish)', async (t) => {
  const { window: w, close } = loadCommon({
    fetch: async () => ({ status: 200, ok: true, json: async () => null }),
  });
  t.after(close);
  const data = await w.apiJson('/x');
  assert.equal(data, null);
});

test('apiJson: 401 triggers the apiFetch auth-retry path', async (t) => {
  let calls = 0;
  const { window: w, close } = loadCommon({
    prompt: () => 'newtoken',
    fetch: async () => {
      calls += 1;
      const unauthorized = {
        status: 401, ok: false, json: async () => ({ error: 'unauthorized' }),
      };
      if (calls === 1) return unauthorized;
      return { status: 200, ok: true, json: async () => ({ token: 'newtoken' }) };
    },
  });
  t.after(close);
  const data = await w.apiJson('/settings');
  assert.equal(calls, 2, 'initial 401 + one retry');
  assert.deepEqual(data, { token: 'newtoken' });
});

test('apiJson: network error propagates as-is', async (t) => {
  const { window: w, close } = loadCommon({
    fetch: async () => { throw new TypeError('Failed to fetch'); },
  });
  t.after(close);
  await assert.rejects(() => w.apiJson('/x'), { message: 'Failed to fetch' });
});

// ── apiFetch admin-auth flow (stubbed fetch + prompt) ────────────────────

test('apiFetch: attaches stored token as Bearer header', async (t) => {
  const calls = [];
  const { window: w, close } = loadCommon({
    fetch: async (url, opts) => { calls.push(opts.headers); return { status: 200 }; },
  });
  t.after(close);
  w._setToken('secret');
  const r = await w.apiFetch('/api/things');
  assert.equal(r.status, 200);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].Authorization, 'Bearer secret');
});

test('apiFetch: 401 → prompt → retries exactly once with the new token', async (t) => {
  let prompts = 0;
  const calls = [];
  const { window: w, close } = loadCommon({
    prompt: () => { prompts += 1; return 'newtok'; },
    fetch: async (url, opts) => {
      calls.push(opts.headers && opts.headers.Authorization);
      return { status: calls.length === 1 ? 401 : 200 };
    },
  });
  t.after(close);
  const r = await w.apiFetch('/api/things');
  assert.equal(r.status, 200);
  assert.equal(calls.length, 2, 'initial + exactly one retry');
  assert.equal(calls[0], undefined, 'first call had no token');
  assert.equal(calls[1], 'Bearer newtok');
  assert.equal(prompts, 1, 'prompted exactly once');
});

test('apiFetch: never retries more than once (second 401 returned as-is)', async (t) => {
  let prompts = 0;
  const calls = [];
  const { window: w, close } = loadCommon({
    prompt: () => { prompts += 1; return 'again'; },
    fetch: async (_url, opts) => {
      calls.push(opts.headers && opts.headers.Authorization);
      return { status: 401 };
    },
  });
  t.after(close);
  const r = await w.apiFetch('/api/things');
  assert.equal(r.status, 401);
  assert.equal(calls.length, 2, 'no second retry');
  assert.equal(prompts, 1, 'prompted at most once');
});

test('apiFetch: 401 with cancelled prompt does not retry', async (t) => {
  const calls = [];
  const { window: w, close } = loadCommon({
    prompt: () => null, // user cancels
    fetch: async () => { calls.push(1); return { status: 401 }; },
  });
  t.after(close);
  const r = await w.apiFetch('/api/things');
  assert.equal(r.status, 401);
  assert.equal(calls.length, 1);
});

// ── snippetHtml ─────────────────────────────────────────────────────────
// Converts <b>…</b> API highlight tags to <mark>…</mark>, escaping all other
// content. Unbalanced tags are rendered as literal escaped text.

test('snippetHtml: empty/null input returns empty string', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.snippetHtml(''), '');
  assert.equal(w.snippetHtml(null), '');
  assert.equal(w.snippetHtml(undefined), '');
});

test('snippetHtml: plain text (no tags) is escaped and returned as-is', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.snippetHtml('hello world'), 'hello world');
  assert.equal(w.snippetHtml('a < b & c > d'), 'a &lt; b &amp; c &gt; d');
});

test('snippetHtml: single <b>…</b> pair becomes <mark>…</mark>', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.snippetHtml('before <b>match</b> after'), 'before <mark>match</mark> after');
});

test('snippetHtml: multiple <b>…</b> pairs', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(
    w.snippetHtml('<b>alpha</b> mid <b>beta</b>'),
    '<mark>alpha</mark> mid <mark>beta</mark>',
  );
});

test('snippetHtml: content inside <b> is still escaped', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.snippetHtml('<b>a &amp; "b"</b>'), '<mark>a &amp;amp; &quot;b&quot;</mark>');
  // ^ the inner & is from the source; after esc() it becomes &amp;amp; (a literal
  // ampersand in HTML source). Raw source is <b>a & "b"</b>; "a & \"b\"" is
  // esc'd to "a &amp; &quot;b\"".
});

test('snippetHtml: unbalanced opening <b> (no close) still emits <mark>', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  // The first <b> opens a mark; without a closing </b> the mark is never closed.
  assert.equal(w.snippetHtml('start <b> no close'), 'start <mark> no close');
});

test('snippetHtml: unbalanced closing </b> renders as escaped text', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.snippetHtml('no open </b> end'), 'no open &lt;/b&gt; end');
});

test('snippetHtml: extra closing tag after balanced pair is escaped', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.snippetHtml('<b>ok</b></b>'), '<mark>ok</mark>&lt;/b&gt;');
});

test('snippetHtml: extra opening tag while already inMark is escaped', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  // First <b> opens a mark; second <b> (inMark=true) → literal; </b> closes the mark.
  assert.equal(w.snippetHtml('<b><b>ok</b>'), '<mark>&lt;b&gt;ok</mark>');
});

test('snippetHtml: adjacent pairs with no text between', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.snippetHtml('<b>a</b><b>b</b>'), '<mark>a</mark><mark>b</mark>');
});

test('snippetHtml: special chars outside tags are escaped', (t) => {
  const { window: w, close } = loadCommon();
  t.after(close);
  assert.equal(w.snippetHtml('5 < 6 & 7 > 6 <b>hi</b>'), '5 &lt; 6 &amp; 7 &gt; 6 <mark>hi</mark>');
});

// Behavioral tests for shared page helpers extracted into web/common.js:
//   - apiJson(url, opts): fetch → parse JSON → throw on !ok
//   - snippetHtml(raw): convert <b>…</b> highlights to <mark>…</mark>
//
// Uses the same vm.runInNewContext sandbox pattern as common.test.mjs.
// Run: node --test tests/web/pages.test.mjs   (or: make web-test)

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
    sessionStorage: {
      getItem: (k) => (store.has(k) ? store.get(k) : null),
      setItem: (k, v) => store.set(k, String(v)),
      removeItem: (k) => store.delete(k),
    },
    document: {
      getElementById: () => ({ textContent: '', className: '' }),
    },
    prompt: () => null,
    fetch: async () => {
      throw new Error('fetch should not be called in these tests');
    },
    setTimeout: () => 0,
    clearTimeout: () => {},
    ...overrides,
  };
  vm.createContext(sandbox);
  vm.runInContext(source, sandbox, { filename: 'common.js' });
  return sandbox;
}

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
  // Actually: raw source is <b>a & "b"</b>. The "a & \"b\"" gets esc'd to "a &amp; &quot;b&quot;".
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

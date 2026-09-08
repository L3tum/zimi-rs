// Behavioral unit tests for web/search.js (search page).
//
// web/search.js is a classic browser script (no module exports): it reads
// document/location/performance/history and uses web/common.js helpers as
// globals. We load common.js + search.js verbatim into a
// vm.runInNewContext sandbox with a minimal fake DOM (tests/web/dom.mjs), a
// stubbed fetch, and a fixed performance clock, then exercise the top-level
// functions and event handlers by name.
//
// Run: node --test tests/web/search.test.mjs   (or: make web-test)

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
const searchSource = readFileSync(join(webDir, 'search.js'), 'utf8');

const ok = (body) => ({ status: 200, ok: true, json: async () => body });

// Yield to the event loop so untracked async handlers (event dispatch) can
// finish their microtask chains.
const tick = () => new Promise((r) => setImmediate(r));

function loadSearch(overrides = {}) {
  const { doc, elements } = makeDocument();
  const timers = makeTimers();
  const store = new Map();
  const clock = { t: 0 };
  const historyCalls = [];
  const sandbox = {
    sessionStorage: {
      getItem: (k) => (store.has(k) ? store.get(k) : null),
      setItem: (k, v) => store.set(k, String(v)),
      removeItem: (k) => store.delete(k),
    },
    document: doc,
    location: { search: '', hash: '' },
    history: { replaceState: (state, title, url) => historyCalls.push(url) },
    performance: { now: () => clock.t },
    URLSearchParams,
    prompt: () => null,
    fetch: async () => {
      throw new Error('fetch should not be called in these tests');
    },
    setTimeout: timers.setTimeout,
    clearTimeout: timers.clearTimeout,
    ...overrides,
  };
  vm.createContext(sandbox);
  vm.runInContext(commonSource, sandbox, { filename: 'common.js' });
  vm.runInContext(searchSource, sandbox, { filename: 'search.js' });
  return { sandbox, doc, elements, timers, clock, historyCalls };
}

// Stubbed fetch that routes by URL path (last match wins) and records calls.
function routingFetch(routes) {
  const calls = [];
  const fetchStub = async (url, opts) => {
    calls.push({ url, opts });
    const path = String(url).split('?')[0];
    let hit;
    for (let i = routes.length - 1; i >= 0; i--) {
      if (routes[i].path === path) { hit = routes[i]; break; }
    }
    assert.ok(hit, `unexpected fetch: ${url}`);
    const body = typeof hit.body === 'function' ? hit.body() : hit.body;
    if (body && typeof body === 'object' && typeof body.status === 'number') return body;
    return ok(body);
  };
  return { fetchStub, calls };
}

const RESULTS = [{
  zim_name: 'wiki_en',
  path: 'a/b',
  title: 'T & <x>',
  snippet: 'before <b>match</b> after',
  language: 'eng',
  score: 0.12345,
}];

// /list route so the top-level loadZims() that runs at script load time is
// always expected.
const listRoute = { path: '/list', body: { zims: [] } };

// ── URL parameter initialization ────────────────────────────────────────

test('URL params: q and valid mode are pre-filled into the inputs', () => {
  const { fetchStub } = routingFetch([listRoute]);
  const { elements } = loadSearch({
    fetch: fetchStub,
    location: { search: '?q=hello+world&zim=wiki&lang=eng&mode=fts', hash: '' },
  });
  assert.equal(elements.get('q').value, 'hello world');
  assert.equal(elements.get('mode').value, 'fts');
});

test('URL params: invalid mode falls back to "hybrid"', () => {
  const { fetchStub } = routingFetch([listRoute]);
  const { elements } = loadSearch({
    fetch: fetchStub,
    location: { search: '?mode=bogus', hash: '' },
  });
  assert.equal(elements.get('mode').value, 'hybrid');
});

test('URL params: initial q triggers a search once the ZIM list loads', async () => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { sandbox, elements, historyCalls } = loadSearch({
    fetch: fetchStub,
    location: { search: '?q=auto', hash: '' },
  });
  void sandbox;
  await tick(); // top-level loadZims() → doSearch() are fire-and-forget
  const search = calls.find((c) => c.url.startsWith('/search'));
  assert.ok(search, 'search fired from initial q');
  assert.ok(search.url.includes('q=auto'), 'q encoded into URL');
  assert.ok(search.url.includes('highlight=true'));
  assert.ok(elements.get('results').innerHTML.includes('match'), 'results rendered');
  assert.equal(historyCalls.length, 1, 'URL replaced once');
  assert.ok(historyCalls[0].includes('q=auto'));
});

// ── loadZims ────────────────────────────────────────────────────────────

test('loadZims: populates zim + language selects, honors initial selection', async () => {
  const zims = [
    { name: 'z_b', display_title: 'Zed', language: 'fra' },
    { name: 'z_a', display_title: 'Alpha', language: 'eng' },
    { name: 'z_c', display_title: 'No lang' },
  ];
  const { fetchStub } = routingFetch([{ path: '/list', body: { zims } }]);
  const { elements } = loadSearch({
    fetch: fetchStub,
    location: { search: '?zim=z_b&lang=fra', hash: '' },
  });
  await tick();
  const zim = elements.get('zim');
  assert.deepEqual(zim.children.map((o) => o.value), ['z_b', 'z_a', 'z_c']);
  assert.deepEqual(zim.children.map((o) => o.textContent), ['Zed', 'Alpha', 'No lang']);
  assert.equal(zim.children[0].selected, true, 'initial zim selected');
  assert.equal(zim.children[1].selected, false);
  const lang = elements.get('lang');
  assert.deepEqual(lang.children.map((o) => o.value), ['eng', 'fra'], 'sorted, deduped');
  assert.equal(lang.children[1].selected, true, 'initial lang selected');
});

test('loadZims: failure → error state with escaped message', async () => {
  const { fetchStub } = routingFetch([
    { path: '/list', body: () => { throw new Error('nope <x>'); } },
  ]);
  const { elements } = loadSearch({ fetch: fetchStub });
  await tick();
  assert.equal(elements.get('error').style.display, 'block');
  assert.equal(elements.get('error').textContent, 'Could not load ZIM list: nope &lt;x&gt;');
});

// ── doSearch ────────────────────────────────────────────────────────────

function searchSetup() {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS, total: 42 } },
  ]);
  // doc.getElementById (lazy) rather than elements.get: 'zim'/'lang' are only
  // materialized when the fire-and-forget top-level loadZims() resolves, which
  // may not have happened yet at the time we set the filter values.
  const { sandbox, doc, elements } = loadSearch({ fetch: fetchStub });
  doc.getElementById('q').value = 'test';
  doc.getElementById('zim').value = 'wiki_en';
  doc.getElementById('lang').value = 'eng';
  doc.getElementById('mode').value = 'hybrid';
  return { sandbox, elements, calls };
}

test('doSearch: builds URL with all filters and renders results', async () => {
  const { sandbox, elements, calls } = searchSetup();
  await sandbox.doSearch();
  const url = calls.find((c) => c.url.startsWith('/search')).url;
  assert.equal(
    url,
    '/search?q=test&highlight=true&zim=wiki_en&language=eng&mode=hybrid',
  );
  const html = elements.get('results').innerHTML;
  assert.ok(html.includes('<mark>match</mark>'), '<b> highlights rendered as <mark>');
  assert.ok(html.includes('href="/w/wiki_en/a%2Fb"'), 'article link encodes path');
  assert.ok(html.includes('T &amp; &lt;x&gt;'), 'title escaped');
  assert.ok(html.includes('wiki_en · eng · score 0.123'), 'meta line with 3-decimal score');
  // Fixed clock: performance.now() is 0 at both measurements.
  assert.equal(elements.get('stats').textContent, '1 results in 0ms (of 42 total)');
});

test('doSearch: total equal to results length → no "(of N total)" suffix', async () => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { sandbox, elements } = loadSearch({ fetch: fetchStub });
  elements.get('q').value = 'test';
  await sandbox.doSearch();
  void calls;
  assert.equal(elements.get('stats').textContent, '1 results in 0ms');
});

test('doSearch: empty query is a no-op (no fetch, state untouched)', async () => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS } },
  ]);
  const { sandbox, doc, elements } = loadSearch({ fetch: fetchStub });
  elements.get('q').value = '   ';
  await sandbox.doSearch();
  assert.equal(calls.filter((c) => c.url.startsWith('/search')).length, 0);
  // 'results' is never materialized by the page on a no-op search; the lazy
  // getElementById returns the same element the page would render into — empty.
  assert.equal(doc.getElementById('results').innerHTML, '');
});

test('doSearch: failure → error state, stats cleared', async () => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/search', body: () => { throw new Error('boom <x>'); } },
  ]);
  const { sandbox, elements } = loadSearch({ fetch: fetchStub });
  elements.get('q').value = 'test';
  await sandbox.doSearch();
  assert.equal(elements.get('error').style.display, 'block');
  assert.equal(elements.get('error').textContent, 'Search failed: boom &lt;x&gt;');
  assert.equal(elements.get('stats').textContent, '');
});

test('doSearch: zero results → "empty" state shown', async () => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/search', body: { results: [], total: 0 } },
  ]);
  const { sandbox, elements } = loadSearch({ fetch: fetchStub });
  elements.get('q').value = 'test';
  await sandbox.doSearch();
  assert.equal(elements.get('empty').style.display, 'block');
  assert.equal(elements.get('loading').style.display, 'none');
  assert.equal(elements.get('error').style.display, 'none');
});

test('doSearch: stale response (older request resolving late) is discarded', async () => {
  const gate = [];
  const fetchStub = async (url) => {
    const p = String(url).split('?')[0];
    if (p === '/list') return ok({ zims: [] });
    if (p === '/search') {
      let resolve;
      const promise = new Promise((r) => { resolve = r; });
      gate.push({ promise, resolve });
      return promise; // fetch may return the response via a deferred promise
    }
    throw new Error('unexpected fetch: ' + url);
  };
  const { sandbox, doc, elements } = loadSearch({
    fetch: fetchStub,
    location: { search: '', hash: '' },
  });
  elements.get('q').value = 'first';
  const p1 = sandbox.doSearch(); // seq 1 (in flight)
  elements.get('q').value = 'second';
  const p2 = sandbox.doSearch(); // seq 2 (in flight)
  gate[0].resolve(ok({ results: RESULTS, total: 1 })); // first request resolves...
  await p1; // ...but must be ignored (seq mismatch)
  // 'results' is not yet materialized (the fresh request is still in flight).
  assert.equal(doc.getElementById('results').innerHTML, '', 'stale results not rendered');
  gate[1].resolve(ok({ results: [...RESULTS, { ...RESULTS[0], title: 'SECOND' }], total: 2 }));
  await p2;
  assert.ok(elements.get('results').innerHTML.includes('SECOND'), 'fresh results rendered');
});

// ── Autocomplete ────────────────────────────────────────────────────────

test('suggest: query shorter than 2 chars closes suggestions, no fetch', async () => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['A'] } },
  ]);
  const { elements, timers } = loadSearch({ fetch: fetchStub });
  const q = elements.get('q');
  q.value = 'a';
  q.dispatch('input');
  await timers.flush(300);
  assert.equal(calls.filter((c) => c.url.startsWith('/suggest')).length, 0);
  assert.equal(elements.get('suggestions').style.display, 'none');
});

test('suggest: debounced fetch renders item divs, list shown', async () => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['Alpha article', 'Beta article'] } },
  ]);
  const { elements, timers } = loadSearch({ fetch: fetchStub });
  const q = elements.get('q');
  q.value = 'alpha';
  q.dispatch('input');
  // 200ms debounce: not fired at 100ms.
  await timers.flush(100);
  assert.equal(calls.filter((c) => c.url.startsWith('/suggest')).length, 0);
  await timers.flush(300);
  const call = calls.find((c) => c.url.startsWith('/suggest'));
  assert.ok(call.url.includes('q=alpha'), 'query encoded');
  const list = elements.get('suggestions');
  assert.equal(list.style.display, 'block');
  assert.deepEqual(list.children.map((c) => c.textContent), ['Alpha article', 'Beta article']);
});

test('suggest: empty suggestion list keeps the list hidden', async () => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: [] } },
  ]);
  const { elements, timers } = loadSearch({ fetch: fetchStub });
  const q = elements.get('q');
  q.value = 'zzz';
  q.dispatch('input');
  await timers.flush(300);
  assert.equal(elements.get('suggestions').style.display, 'none');
});

test('suggest: user kept typing before the response → result ignored', async () => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['Stale item'] } },
  ]);
  const { doc, elements, timers } = loadSearch({ fetch: fetchStub });
  const q = elements.get('q');
  q.value = 'wiki';
  q.dispatch('input');
  q.value = 'wikip'; // user keeps typing; the in-flight query is now stale
  await timers.flush(300);
  const list = doc.getElementById('suggestions');
  assert.equal(list.children.length, 0, 'stale suggestions not rendered');
  // The stale path bails before closeSuggestions(), so the list was never
  // shown — its display is still the default (CSS hides the empty list).
  assert.notEqual(list.style.display, 'block');
});

test('suggest: mousedown on an item picks it (searches, closes, preventDefault)', async () => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['Picked title'] } },
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { elements, timers } = loadSearch({ fetch: fetchStub });
  const q = elements.get('q');
  q.value = 'pick';
  q.dispatch('input');
  await timers.flush(300);
  const list = elements.get('suggestions');
  const ev = { prevented: false, preventDefault() { this.prevented = true; } };
  list.children[0].dispatch('mousedown', ev);
  await tick();
  assert.equal(ev.prevented, true, 'default prevented (no focus jump)');
  assert.equal(q.value, 'Picked title');
  assert.equal(list.style.display, 'none');
  assert.ok(calls.some((c) => c.url.startsWith('/search') && c.url.includes('q=Picked%20title')));
});

test('suggest: arrow keys cycle selection, wrapping, and fill the input', () => {
  const { fetchStub } = routingFetch([listRoute]);
  const { sandbox, elements } = loadSearch({ fetch: fetchStub });
  const q = elements.get('q');
  q.value = 'alpha';
  sandbox.renderSuggestions(['Alpha', 'Beta']);
  const list = elements.get('suggestions');
  const key = (k) => q.dispatch('keydown', { key: k, preventDefault() {} });

  key('ArrowDown'); // select 0
  assert.equal(q.value, 'Alpha');
  assert.ok(list.children[0].classList.contains('sel'));
  key('ArrowDown'); // select 1
  assert.equal(q.value, 'Beta');
  assert.ok(list.children[1].classList.contains('sel'));
  assert.ok(!list.children[0].classList.contains('sel'));
  key('ArrowUp'); // back to 0
  assert.equal(q.value, 'Alpha');
  key('ArrowUp'); // wraps to last
  assert.equal(q.value, 'Beta');
});

test('suggest: Enter with selection picks; Enter closed searches; Escape closes', async () => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['Chosen'] } },
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { sandbox, elements } = loadSearch({ fetch: fetchStub });
  const q = elements.get('q');
  const key = (k) => q.dispatch('keydown', { key: k, preventDefault() {} });

  // Closed state: Enter → doSearch with current input.
  q.value = 'plain';
  key('Enter');
  await tick();
  assert.ok(calls.some((c) => c.url.includes('q=plain')), 'closed Enter searches');

  // Open with a selection: Enter picks the selected suggestion.
  sandbox.renderSuggestions(['Chosen', 'Other']);
  key('ArrowDown'); // select 0
  key('Enter');
  await tick();
  assert.ok(calls.some((c) => c.url.includes('q=Chosen')), 'open Enter picks selection');
  assert.equal(elements.get('suggestions').style.display, 'none');

  // Escape closes an open list.
  sandbox.renderSuggestions(['Chosen']);
  key('Escape');
  assert.equal(elements.get('suggestions').style.display, 'none');
});

test('suggest: click outside .search-box closes; inside keeps it open', () => {
  const { fetchStub } = routingFetch([listRoute]);
  const { sandbox, doc, elements } = loadSearch({ fetch: fetchStub });
  sandbox.renderSuggestions(['Open item']);
  assert.equal(elements.get('suggestions').style.display, 'block');
  doc.dispatch('click', { target: { closest: () => null } });
  assert.equal(elements.get('suggestions').style.display, 'none', 'outside click closes');
  sandbox.renderSuggestions(['Open item']);
  doc.dispatch('click', { target: { closest: () => ({}) } });
  assert.equal(elements.get('suggestions').style.display, 'block', 'inside click keeps open');
});

test('"go" button click triggers doSearch', async () => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { elements } = loadSearch({ fetch: fetchStub });
  elements.get('q').value = 'goquery';
  elements.get('go').dispatch('click');
  await tick();
  assert.ok(calls.some((c) => c.url.includes('q=goquery')));
});

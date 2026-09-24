// Behavioral unit tests for web/search.js (search page).
//
// web/search.js is a classic browser script (no module exports): it reads
// document/location/performance/history and uses web/common.js helpers as
// globals. We boot common.js + search.js as real top-level classic scripts
// in a jsdom window against the minimal HTML the page scripts touch
// (tests/web/jsdom.mjs #bootScripts), with a stubbed fetch, controllable
// timers, a fixed performance clock, and a recording history.replaceState.
//
// NOTE: the top-level loadZims() fires at script boot, so every test's routes
// must include /list. Real <select> elements only accept values they have as
// options, so searchSetup() adds the filter options the page would load.
//
// Run: node --test tests/web/search.test.mjs   (or: make web-test)

import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { bootScripts, tick } from './jsdom.mjs';

// Minimal skeleton of web/index... of web/search.html: everything search.js
// touches by id/class. The zim/lang selects start EMPTY (the real page ships
// an "All" default option, but the assertions here count the loaded options,
// and smoke.test.mjs covers the default-option case on the real page).
const HTML = `<!doctype html>
<html><head><title>search</title></head>
<body>
  <div class="search-box">
    <input type="text" id="q">
    <button id="go">Search</button>
    <div class="suggest-list" id="suggestions"></div>
  </div>
  <select id="zim"></select>
  <select id="lang"></select>
  <select id="mode">
    <option value="hybrid" selected>Hybrid</option>
    <option value="fts">Full-text</option>
    <option value="trgm">Fuzzy / trigram</option>
    <option value="vector">Semantic (vector)</option>
  </select>
  <div id="stats"></div>
  <div id="loading"></div>
  <div id="error"></div>
  <div id="empty"></div>
  <div id="results"></div>
  <p id="outside">outside the search box</p>
</body></html>`;

// Default fetch: loud failure if a test forgets to stub one.
const NO_FETCH = () => {
  throw new Error('fetch should not be called in these tests');
};

const ok = (body) => ({ status: 200, ok: true, json: async () => body });

/**
 * Boot common.js + search.js in a fresh jsdom window. `query` is appended to
 * http://localhost/search.html (include the "?"). `close` MUST be registered
 * with `t.after` (an open window hangs the run).
 */
function loadSearch({ fetch: fetchFn, prompt, query = '' } = {}) {
  const historyCalls = [];
  const { window, doc, errors, timers } = bootScripts(
    HTML,
    ['common.js', 'search.js'],
    {
      fetchHandler: fetchFn ?? NO_FETCH,
      promptHandler: prompt,
      timers: true,
      // Fixed clock: performance.now() is 0 at both measurements.
      clock: () => 0,
      url: `http://localhost/search.html${query}`,
      beforeParse: (w) => {
        w.history.replaceState = (state, title, u) => { historyCalls.push(String(u)); };
      },
    },
  );
  assert.deepEqual(errors, [], errors.map((e) => e.message).join('; '));
  return { window, doc, timers, historyCalls, close: () => window.close() };
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

// Shared JS<->Rust response contract (Tests Major #5): the canned /search
// payload is loaded from the SAME fixture file the Rust integration test
// asserts the live handler response against (tests/integration/contract.rs,
// tests/web/fixtures/search.json) - a handler field rename breaks both halves.
const here = dirname(fileURLToPath(import.meta.url));
const SEARCH_FIXTURE = JSON.parse(
  readFileSync(join(here, 'fixtures', 'search.json'), 'utf8'),
);
const RESULTS = SEARCH_FIXTURE.results;

// /list route so the top-level loadZims() that runs at script load time is
// always expected.
const listRoute = { path: '/list', body: { zims: [] } };

// ── URL parameter initialization ────────────────────────────────────────

test('URL params: q and valid mode are pre-filled into the inputs', async (t) => {
  const { fetchStub } = routingFetch([listRoute]);
  const { doc, close } = loadSearch({
    fetch: fetchStub,
    query: '?q=hello+world&zim=wiki&lang=eng&mode=fts',
  });
  t.after(close);
  await tick(); // let the fire-and-forget loadZims() settle
  assert.equal(doc.getElementById('q').value, 'hello world');
  assert.equal(doc.getElementById('mode').value, 'fts');
});

test('URL params: invalid mode falls back to "hybrid"', async (t) => {
  const { fetchStub } = routingFetch([listRoute]);
  const { doc, close } = loadSearch({
    fetch: fetchStub,
    query: '?mode=bogus',
  });
  t.after(close);
  await tick(); // let the fire-and-forget loadZims() settle
  assert.equal(doc.getElementById('mode').value, 'hybrid');
});

test('URL params: initial q triggers a search once the ZIM list loads', async (t) => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { doc, historyCalls, close } = loadSearch({
    fetch: fetchStub,
    query: '?q=auto',
  });
  t.after(close);
  await tick(); // top-level loadZims() → doSearch() are fire-and-forget
  const search = calls.find((c) => c.url.startsWith('/search'));
  assert.ok(search, 'search fired from initial q');
  assert.ok(search.url.includes('q=auto'), 'q encoded into URL');
  assert.ok(search.url.includes('highlight=true'));
  assert.ok(doc.getElementById('results').innerHTML.includes('match'), 'results rendered');
  assert.equal(historyCalls.length, 1, 'URL replaced once');
  assert.ok(historyCalls[0].includes('q=auto'));
});

// ── loadZims ────────────────────────────────────────────────────────────

test('loadZims: populates zim + language selects, honors initial selection', async (t) => {
  const zims = [
    { name: 'z_b', display_title: 'Zed', language: 'fra' },
    { name: 'z_a', display_title: 'Alpha', language: 'eng' },
    { name: 'z_c', display_title: 'No lang' },
  ];
  const { fetchStub } = routingFetch([{ path: '/list', body: { zims } }]);
  const { doc, close } = loadSearch({
    fetch: fetchStub,
    query: '?zim=z_b&lang=fra',
  });
  t.after(close);
  await tick();
  const zim = doc.getElementById('zim');
  assert.deepEqual([...zim.children].map((o) => o.value), ['z_b', 'z_a', 'z_c']);
  assert.deepEqual([...zim.children].map((o) => o.textContent), ['Zed', 'Alpha', 'No lang']);
  assert.equal(zim.children[0].selected, true, 'initial zim selected');
  assert.equal(zim.children[1].selected, false);
  const lang = doc.getElementById('lang');
  assert.deepEqual([...lang.children].map((o) => o.value), ['eng', 'fra'], 'sorted, deduped');
  assert.equal(lang.children[1].selected, true, 'initial lang selected');
});

test('loadZims: failure → error state with escaped message', async (t) => {
  const { fetchStub } = routingFetch([
    { path: '/list', body: () => { throw new Error('nope <x>'); } },
  ]);
  const { doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  await tick();
  assert.equal(doc.getElementById('error').style.display, 'block');
  assert.equal(doc.getElementById('error').textContent, 'Could not load ZIM list: nope &lt;x&gt;');
});

// ── doSearch ────────────────────────────────────────────────────────────

function searchSetup() {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS, total: 42 } },
  ]);
  const booted = loadSearch({ fetch: fetchStub });
  const { doc, window } = booted;
  // A real <select> only accepts values it has as options, so add the filter
  // values explicitly (the page would populate them from /list).
  const mkOpt = (value) => {
    const o = doc.createElement('option');
    o.value = value;
    o.textContent = value;
    return o;
  };
  doc.getElementById('zim').appendChild(mkOpt('wiki_en'));
  doc.getElementById('lang').appendChild(mkOpt('eng'));
  doc.getElementById('q').value = 'test';
  doc.getElementById('zim').value = 'wiki_en';
  doc.getElementById('lang').value = 'eng';
  doc.getElementById('mode').value = 'hybrid';
  void window;
  return { ...booted, calls };
}

test('doSearch: builds URL with all filters and renders results', async (t) => {
  const { window, doc, calls, close } = searchSetup();
  t.after(close);
  await window.doSearch();
  const url = calls.find((c) => c.url.startsWith('/search')).url;
  assert.equal(
    url,
    '/search?q=test&highlight=true&zim=wiki_en&language=eng&mode=hybrid',
  );
  const html = doc.getElementById('results').innerHTML;
  assert.ok(html.includes('<mark>match</mark>'), '<b> highlights rendered as <mark>');
  assert.ok(html.includes('href="/w/wiki_en/a%2Fb"'), 'article link encodes path');
  assert.ok(html.includes('T &amp; &lt;x&gt;'), 'title escaped');
  assert.ok(html.includes('wiki_en · eng · score 0.123'), 'meta line with 3-decimal score');
  // Fixed clock: performance.now() is 0 at both measurements.
  assert.equal(doc.getElementById('stats').textContent, '1 results in 0ms (of 42 total)');
});

test('doSearch: total equal to results length → no "(of N total)" suffix', async (t) => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { window, doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('q').value = 'test';
  await window.doSearch();
  assert.equal(doc.getElementById('stats').textContent, '1 results in 0ms');
});

test('doSearch: empty query is a no-op (no fetch, state untouched)', async (t) => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS } },
  ]);
  const { window, doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('q').value = '   ';
  await window.doSearch();
  assert.equal(calls.filter((c) => c.url.startsWith('/search')).length, 0);
  // 'results' is never touched by the page on a no-op search — empty.
  assert.equal(doc.getElementById('results').innerHTML, '');
});

test('doSearch: failure → error state, stats cleared', async (t) => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/search', body: () => { throw new Error('boom <x>'); } },
  ]);
  const { window, doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('q').value = 'test';
  await window.doSearch();
  assert.equal(doc.getElementById('error').style.display, 'block');
  assert.equal(doc.getElementById('error').textContent, 'Search failed: boom &lt;x&gt;');
  assert.equal(doc.getElementById('stats').textContent, '');
});

test('doSearch: zero results → "empty" state shown', async (t) => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/search', body: { results: [], total: 0 } },
  ]);
  const { window, doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('q').value = 'test';
  await window.doSearch();
  assert.equal(doc.getElementById('empty').style.display, 'block');
  assert.equal(doc.getElementById('loading').style.display, 'none');
  assert.equal(doc.getElementById('error').style.display, 'none');
});

test('doSearch: stale response (older request resolving late) is discarded', async (t) => {
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
  const { window, doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('q').value = 'first';
  const p1 = window.doSearch(); // seq 1 (in flight)
  doc.getElementById('q').value = 'second';
  const p2 = window.doSearch(); // seq 2 (in flight)
  gate[0].resolve(ok({ results: RESULTS, total: 1 })); // first request resolves...
  await p1; // ...but must be ignored (seq mismatch)
  assert.equal(doc.getElementById('results').innerHTML, '', 'stale results not rendered');
  gate[1].resolve(ok({ results: [...RESULTS, { ...RESULTS[0], title: 'SECOND' }], total: 2 }));
  await p2;
  assert.ok(doc.getElementById('results').innerHTML.includes('SECOND'), 'fresh results rendered');
});

// ── Autocomplete ────────────────────────────────────────────────────────

test('suggest: query shorter than 2 chars closes suggestions, no fetch', async (t) => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['A'] } },
  ]);
  const { window, doc, timers, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  await tick(); // let the fire-and-forget loadZims() settle
  const q = doc.getElementById('q');
  q.value = 'a';
  q.dispatchEvent(new window.Event('input'));
  await timers.flush(300);
  assert.equal(calls.filter((c) => c.url.startsWith('/suggest')).length, 0);
  assert.equal(doc.getElementById('suggestions').style.display, 'none');
});

test('suggest: debounced fetch renders item divs, list shown', async (t) => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['Alpha article', 'Beta article'] } },
  ]);
  const { window, doc, timers, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  const q = doc.getElementById('q');
  q.value = 'alpha';
  q.dispatchEvent(new window.Event('input'));
  // 200ms debounce: not fired at 100ms.
  await timers.flush(100);
  assert.equal(calls.filter((c) => c.url.startsWith('/suggest')).length, 0);
  await timers.flush(300);
  const call = calls.find((c) => c.url.startsWith('/suggest'));
  assert.ok(call.url.includes('q=alpha'), 'query encoded');
  const list = doc.getElementById('suggestions');
  assert.equal(list.style.display, 'block');
  assert.deepEqual([...list.children].map((c) => c.textContent), ['Alpha article', 'Beta article']);
});

test('suggest: empty suggestion list keeps the list hidden', async (t) => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: [] } },
  ]);
  const { window, doc, timers, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  const q = doc.getElementById('q');
  q.value = 'zzz';
  q.dispatchEvent(new window.Event('input'));
  await timers.flush(300);
  assert.equal(doc.getElementById('suggestions').style.display, 'none');
});

test('suggest: user kept typing before the response → result ignored', async (t) => {
  const { fetchStub } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['Stale item'] } },
  ]);
  const { window, doc, timers, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  const q = doc.getElementById('q');
  q.value = 'wiki';
  q.dispatchEvent(new window.Event('input'));
  q.value = 'wikip'; // user keeps typing; the in-flight query is now stale
  await timers.flush(300);
  const list = doc.getElementById('suggestions');
  assert.equal(list.children.length, 0, 'stale suggestions not rendered');
  // The stale path bails before closeSuggestions(), so the list was never
  // shown — its display is still the default (CSS hides the empty list).
  assert.notEqual(list.style.display, 'block');
});

test('suggest: mousedown on an item picks it (searches, closes, preventDefault)', async (t) => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['Picked title'] } },
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { window, doc, timers, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  const q = doc.getElementById('q');
  q.value = 'pick';
  q.dispatchEvent(new window.Event('input'));
  await timers.flush(300);
  const list = doc.getElementById('suggestions');
  // cancelable: true — a non-cancelable event would silently ignore the
  // handler's preventDefault() in the real DOM (the shim's fake event
  // recorded the flag either way).
  const ev = new window.MouseEvent('mousedown', { cancelable: true });
  list.children[0].dispatchEvent(ev);
  await tick();
  assert.equal(ev.defaultPrevented, true, 'default prevented (no focus jump)');
  assert.equal(q.value, 'Picked title');
  assert.equal(list.style.display, 'none');
  assert.ok(calls.some((c) => c.url.startsWith('/search') && c.url.includes('q=Picked%20title')));
});

test('suggest: arrow keys cycle selection, wrapping, and fill the input', async (t) => {
  const { fetchStub } = routingFetch([listRoute]);
  const { window, doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  await tick(); // let the fire-and-forget loadZims() settle
  const q = doc.getElementById('q');
  q.value = 'alpha';
  window.renderSuggestions(['Alpha', 'Beta']);
  const list = doc.getElementById('suggestions');
  const key = (k) => q.dispatchEvent(new window.KeyboardEvent('keydown', { key: k }));

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

test('suggest: Enter with selection picks; Enter closed searches; Escape closes', async (t) => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/suggest', body: { suggestions: ['Chosen'] } },
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { window, doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  const q = doc.getElementById('q');
  const key = (k) => q.dispatchEvent(new window.KeyboardEvent('keydown', { key: k }));

  // Closed state: Enter → doSearch with current input.
  q.value = 'plain';
  key('Enter');
  await tick();
  assert.ok(calls.some((c) => c.url.includes('q=plain')), 'closed Enter searches');

  // Open with a selection: Enter picks the selected suggestion.
  window.renderSuggestions(['Chosen', 'Other']);
  key('ArrowDown'); // select 0
  key('Enter');
  await tick();
  assert.ok(calls.some((c) => c.url.includes('q=Chosen')), 'open Enter picks selection');
  assert.equal(doc.getElementById('suggestions').style.display, 'none');

  // Escape closes an open list.
  window.renderSuggestions(['Chosen']);
  key('Escape');
  assert.equal(doc.getElementById('suggestions').style.display, 'none');
});

test('suggest: click outside .search-box closes; inside keeps it open', async (t) => {
  const { fetchStub } = routingFetch([listRoute]);
  const { window, doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  await tick(); // let the fire-and-forget loadZims() settle
  window.renderSuggestions(['Open item']);
  assert.equal(doc.getElementById('suggestions').style.display, 'block');
  // Real bubbling: a click on an element outside .search-box reaches the
  // document listener with the real element as target.
  doc.getElementById('outside').dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
  assert.equal(doc.getElementById('suggestions').style.display, 'none', 'outside click closes');
  window.renderSuggestions(['Open item']);
  doc.getElementById('q').dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
  assert.equal(doc.getElementById('suggestions').style.display, 'block', 'inside click keeps open');
});

test('"go" button click triggers doSearch', async (t) => {
  const { fetchStub, calls } = routingFetch([
    listRoute,
    { path: '/search', body: { results: RESULTS, total: 1 } },
  ]);
  const { doc, close } = loadSearch({ fetch: fetchStub });
  t.after(close);
  doc.getElementById('q').value = 'goquery';
  doc.getElementById('go').click();
  await tick();
  assert.ok(calls.some((c) => c.url.includes('q=goquery')));
});

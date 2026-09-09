// Real-DOM smoke harness for the zimservice web UI (tests/web/smoke.test.mjs).
//
// The hand-rolled shim (tests/web/dom.mjs) exercises pure helpers in isolation,
// but never runs the page scripts against a real DOM — so a page-level wiring
// bug (script load order, event-handler attach, render crash on the real HTML)
// slips through. This harness boots each page in jsdom and evaluates its
// <script src> files IN THE HTML'S OWN LOAD ORDER.
//
// Why not `window.eval`? All the page scripts start with `'use strict';`.
// In strict mode, function declarations made by `eval` stay in the eval's own
// scope and are NEVER attached to `window` — so evaluating each file
// separately via `window.eval` leaves the previous file's helpers (esc,
// apiFetch, zimControls, …) invisible to the next: `ReferenceError: esc is
// not defined`. In a real browser each `<script src>` is a separate top-level
// classic script, whose top-level functions become shared globals even under
// `'use strict'`. We reproduce exactly that with `runScripts: 'dangerously'`.
//
// jsdom cannot resolve `src="/common.js"` from a file URL, so we inline each
// referenced script's source in place (preserving document order, so load
// order can never silently drift) and let jsdom run them. `beforeParse`
// installs the `fetch` stub before any script runs.
//
// Stub policy (minimum actually needed, each noted inline):
//   - fetch: jsdom has none. Every page fetches on load, so we install a
//     canned handler BEFORE the page scripts evaluate.
//   - matchMedia: NOT stubbed — none of the page scripts call it.
//   - localStorage/sessionStorage: provided natively by jsdom (common.js uses
//     sessionStorage), so no stub is needed.

import { JSDOM, VirtualConsole } from 'jsdom';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const webDir = join(here, '..', '..', 'web');

const SCRIPT_RE = /<script[^>]*\bsrc="([^"]+)"[^>]*\/?>/gi;

/**
 * Return the page's <script src="..."> values in document order. This is the
 * single source of truth for load order: if the HTML reorders its scripts,
 * the inlined execution reorders with it (and would catch a bad order).
 */
export function scriptSrcs(html) {
  const out = [];
  const re = new RegExp(SCRIPT_RE);
  let m;
  while ((m = re.exec(html)) !== null) out.push(m[1]);
  return out;
}

// Resolve a page script src (e.g. "/common.js") to a file under web/.
function resolveScript(src) {
  const rel = src.startsWith('/') ? src.slice(1) : src;
  return join(webDir, rel);
}

// A resolved macrotask tick: lets the full (arbitrarily nested) promise chain
// kicked off by a page's load function settle before we assert on the DOM.
export const tick = (ms = 0) => new Promise((r) => setTimeout(r, ms));

/**
 * Build a Response-like object the page code actually consumes. The handlers
 * use `r.ok`, `r.status`, and `await r.json()`; none read `r.headers`, so we
 * keep the stub minimal (see stub policy above).
 */
export function res(body, { ok = true, status = 200 } = {}) {
  return { ok, status, json: async () => body };
}

/**
 * Load a page into a real jsdom and run its <script src> files as real
 * top-level classic scripts (in the HTML's document order), with a stubbed
 * fetch installed before any script runs.
 *
 * @param {string} page        page file name, e.g. 'index.html'
 * @param {object} [opts]
 * @param {(url:string, opts?:object, window:object)=>object} [opts.fetch]
 *        canned fetch handler returning a Response-like object (see `res`).
 * @param {string} [opts.url]  window.location URL (default:
 *        http://localhost/<page>; append ?query to exercise the search
 *        page's URL-param boot path).
 * @returns {{ dom, window, doc, files: string[], errors: object[] }}
 *
 * `errors` collects any uncaught exceptions thrown by a page script. Callers
 * MUST `window.close()` when done (e.g. in a test's `t.after`) — an open
 * jsdom window (and any timer a page started) keeps the node event loop alive
 * and would hang `node --test`.
 */
export function bootPage(page, { fetchHandler, url } = {}) {
  const raw = readFileSync(join(webDir, page), 'utf8');
  const files = scriptSrcs(raw).map((s) => s.split('/').pop());

  // Inline each referenced script in place, preserving document order.
  // `</script` -> `<\/script` guards against the (unlikely) literal sequence
  // inside a string; `<\/script>` is a valid string escape, so this is safe
  // both inside and outside string literals.
  const html = raw.replace(SCRIPT_RE, (m, src) => {
    const body = readFileSync(resolveScript(src), 'utf8').replace(/<\/script/gi, '<\\/script');
    return `<script>${body}</script>`;
  });

  const errors = [];
  const virtualConsole = new VirtualConsole();
  virtualConsole.on('jsdomError', (e) => {
    // Uncaught script exceptions surface here as `Uncaught [Error]`.
    errors.push({ message: e && e.detail ? String(e.detail) : String((e && e.message) || e) });
  });

  const dom = new JSDOM(html, {
    url: url || `http://localhost/${page}`,
    runScripts: 'dangerously',
    virtualConsole,
    beforeParse(window) {
      if (fetchHandler) window.fetch = (u, opts) => fetchHandler(String(u), opts, window);
      window.addEventListener('error', (ev) => errors.push({ message: ev.message }));
    },
  });

  return { dom, window: dom.window, doc: dom.window.document, files, errors };
}

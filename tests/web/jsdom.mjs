// Real-DOM harness for the zimservice web UI tests (smoke.test.mjs boots
// full pages; common/index/search/settings .test.mjs boot the page scripts
// against minimal skeletons via bootScripts()).
//
// This harness boots each page in jsdom and evaluates its
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
/**
 * Controllable timers (moved over from the old tests/web/dom.mjs shim):
 * install them on the jsdom window with `bootScripts(..., { timers: true })`
 * and the page scripts' setTimeout/setInterval/clear* are captured here
 * instead of scheduling real timers.
 *
 *   - flush(dueMs): fire every pending setTimeout with delay <= dueMs and
 *     return a promise that settles when all (possibly async) callbacks ran.
 *   - intervals:   setInterval call delays (polling loops) recorded, never
 *     fired — `if (!pollId)` guards still behave like in a browser.
 *   - cleared:     clearTimeout/clearInterval ids.
 */
export function makeTimers() {
  let nextId = 1;
  let nextInt = 1;
  const pending = new Map();
  const intervals = [];
  const cleared = [];
  return {
    pending,
    intervals,
    cleared,
    setTimeout(fn, ms) { const id = nextId++; pending.set(id, { fn, ms }); return id; },
    clearTimeout(id) { cleared.push(id); pending.delete(id); },
    // Page scripts use setInterval for polling loops; record, don't fire.
    // Returns a truthy id so `if (!pollId)` guards behave like in a browser.
    setInterval(_fn, ms) { const id = nextInt++; intervals.push(ms); return id; },
    clearInterval(id) { cleared.push(id); },
    async flush(dueMs = Infinity) {
      const due = [...pending.entries()].filter(([, t]) => t.ms <= dueMs);
      for (const [id] of due) pending.delete(id);
      await Promise.all(due.map(([, t]) => Promise.resolve(t.fn())));
    },
  };
}

// Read a web/ script and wrap it in a <script> tag, escaping the (unlikely)
// literal `</script` sequence inside strings: `<\/script>` is a valid string
// escape, so this is safe both inside and outside string literals.
function inlineScript(name) {
  const body = readFileSync(join(webDir, name), 'utf8').replace(/<\/script/gi, '<\\/script');
  return `<script>${body}</script>`;
}

/**
 * Boot arbitrary web/*.js files as real top-level classic scripts against a
 * minimal HTML skeleton — the unit-test counterpart of bootPage() (which boots
 * a full page and its declared scripts). The scripts land right before
 * `</body>`, so they see the skeleton's elements, just like on the real page.
 *
 * @param {string} html        minimal document; MUST contain `</body>`
 * @param {string[]} scripts   file names under web/, in load order
 * @param {object} [opts]
 * @param {(url:string, opts?:object, window:object)=>object} [opts.fetchHandler]
 *        canned fetch handler (same contract as bootPage's `fetch`).
 * @param {(msg:string)=>(string|null)} [opts.promptHandler]
 *        window.prompt stub. jsdom's prompt would log "not implemented" into
 *        the error collector when the 401 flow calls it, so a `() => null`
 *        default is installed when none is given.
 * @param {boolean} [opts.timers] install makeTimers() controllable timers on
 *        the window (page setTimeout/setInterval are captured, not real).
 * @param {()=>number} [opts.clock]  override performance.now (fixed clock).
 * @param {(window:object)=>void} [opts.beforeParse]
 *        extra window customization, run AFTER the stubs above.
 * @param {string} [opts.url]  window.location URL (query/fragment supported).
 * @returns {{ dom, window, doc, errors: object[], timers: object|null }}
 *
 * Same contract as bootPage: `errors` must be empty for a healthy boot, and
 * callers MUST `window.close()` when done (e.g. in a test's `t.after`).
 */
export function bootScripts(html, scripts, opts = {}) {
  const { fetchHandler, promptHandler, timers = false, clock, beforeParse, url } = opts;
  const body = scripts.map(inlineScript).join('\n');
  const full = html.replace('</body>', `${body}\n</body>`);
  const fakeTimers = timers ? makeTimers() : null;
  const { dom, window, doc, errors } = bootRaw(full, {
    fetchHandler,
    promptHandler,
    fakeTimers,
    clock,
    beforeParse,
    url,
  });
  return { dom, window, doc, errors, timers: fakeTimers };
}

// Shared jsdom construction for bootPage()/bootScripts(): run inlined scripts
// as real top-level classic scripts, with the stubs installed first.
function bootRaw(full, { fetchHandler, promptHandler, fakeTimers, clock, beforeParse, url }) {
  const errors = [];
  const virtualConsole = new VirtualConsole();
  virtualConsole.on('jsdomError', (e) => {
    // Uncaught script exceptions surface here as `Uncaught [Error]`.
    errors.push({ message: e && e.detail ? String(e.detail) : String((e && e.message) || e) });
  });

  const dom = new JSDOM(full, {
    url: url || 'http://localhost/',
    runScripts: 'dangerously',
    virtualConsole,
    beforeParse(window) {
      if (fetchHandler) window.fetch = (u, opts) => fetchHandler(String(u), opts, window);
      // jsdom's prompt() logs "not implemented"; common.js's admin-password
      // flow needs it, so default to a quiet cancel.
      window.prompt = promptHandler || (() => null);
      if (fakeTimers) {
        window.setTimeout = (fn, ms) => fakeTimers.setTimeout(fn, ms);
        window.clearTimeout = (id) => fakeTimers.clearTimeout(id);
        window.setInterval = (fn, ms) => fakeTimers.setInterval(fn, ms);
        window.clearInterval = (id) => fakeTimers.clearInterval(id);
      }
      if (clock) window.performance.now = clock;
      if (beforeParse) beforeParse(window);
      window.addEventListener('error', (ev) => errors.push({ message: ev.message }));
    },
  });

  return { dom, window: dom.window, doc: dom.window.document, errors };
}

export function bootPage(page, { fetchHandler, url } = {}) {
  const raw = readFileSync(join(webDir, page), 'utf8');
  const files = scriptSrcs(raw).map((s) => s.split('/').pop());

  // Inline each referenced script in place, preserving document order (so
  // load order can never silently drift).
  const html = raw.replace(SCRIPT_RE, (m, src) => {
    const body = readFileSync(resolveScript(src), 'utf8').replace(/<\/script/gi, '<\\/script');
    return `<script>${body}</script>`;
  });

  const { dom, window: w, doc, errors } = bootRaw(html, { fetchHandler, url });
  return { dom, window: w, doc, files, errors };
}

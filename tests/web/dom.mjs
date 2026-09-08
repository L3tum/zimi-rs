// Minimal fake DOM + controllable timers shared by the page-script tests
// (index.test.mjs, search.test.mjs, settings.test.mjs).
//
// The page scripts are classic browser scripts that read/write
// element.innerHTML as strings — the fake DOM does not parse that HTML.
// Tests that exercise event-driven logic build the "constructed" elements
// (inputs inside rendered fields) manually and register them so that
// document.querySelectorAll / querySelector find them.

export function makeEl(overrides = {}) {
  const listeners = {};
  const classSet = new Set();
  const el = {
    textContent: '',
    innerHTML: '',
    value: '',
    checked: false,
    disabled: false,
    selected: false,
    style: {},
    dataset: {},
    className: '',
    type: 'text',
    tagName: 'INPUT',
    children: [],
    appendChild(c) { this.children.push(c); return c; },
    addEventListener(type, fn) { (listeners[type] ??= []).push(fn); },
    // Test seam: invoke handlers registered for `type`; `this` is the element
    // (page handlers use `this.value` / `this.checked`).
    dispatch(type, ev = {}) {
      for (const fn of listeners[type] ?? []) fn.call(this, ev);
    },
    closest() { return null; },
    classList: {
      add: (...cs) => cs.forEach((c) => classSet.add(c)),
      remove: (...cs) => cs.forEach((c) => classSet.delete(c)),
      contains: (c) => classSet.has(c),
      toggle: (c, force) => {
        const on = force === undefined ? !classSet.has(c) : force;
        if (on) classSet.add(c); else classSet.delete(c);
        return on;
      },
    },
    querySelector() { return null; },
    querySelectorAll() { return []; },
    scrollIntoView() {},
    ...overrides,
  };
  return el;
}

// Matches the selector shapes the page scripts actually use:
//   '[data-key]', '.field[data-field]', and '[data-<attr>="<value>"]'
function matches(el, sel) {
  if (!el.dataset) return false;
  if (sel === '[data-key]') return el.dataset.key !== undefined;
  if (sel === '.field[data-field]') return el.dataset.field !== undefined;
  const m = sel.match(/^\[data-([a-zA-Z]+)="(.*)"\]$/);
  if (m) return el.dataset[m[1]] !== undefined && String(el.dataset[m[1]]) === m[2];
  return false;
}

export function makeDocument() {
  const elements = new Map();
  const docListeners = {};
  const doc = {
    hidden: false,
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, makeEl({ id }));
      return elements.get(id);
    },
    createElement(tag) { return makeEl({ tagName: String(tag).toUpperCase() }); },
    addEventListener(type, fn) { (docListeners[type] ??= []).push(fn); },
    dispatch(type, ev = {}) {
      for (const fn of docListeners[type] ?? []) fn(ev);
    },
    querySelectorAll(sel) {
      return [...elements.values()].filter((el) => matches(el, sel));
    },
    querySelector(sel) {
      return this.querySelectorAll(sel)[0] ?? null;
    },
  };
  return { doc, elements };
}

// Controllable timers: setTimeout captures; flush(dueMs) fires every pending
// timer with delay <= dueMs and returns a promise that settles when all the
// (possibly async) callbacks have run.
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

/* exported apiFetch, apiJson, esc, fmtBytes, fmtEta, fmtNum, snippetHtml, toast, zimControls */
'use strict';

// ── HTML escape ──────────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// ── Auth token (password mode) ─────────────────────────────────────────────
// sessionStorage (not localStorage): the token is cleared when the browser
// tab closes, which is the correct lifetime for a shared-service admin token.
// localStorage would persist across sessions and risk the token lingering on a
// shared machine. sessionStorage also isolates per-tab, so two users on the
// same machine don't interfere.
function _getToken() { return sessionStorage.getItem('zimservice_token') || ''; }
function _setToken(t) {
  if (t) sessionStorage.setItem('zimservice_token', t);
  else sessionStorage.removeItem('zimservice_token');
}
function _promptToken() {
  const t = prompt('Enter admin password:');
  if (t) _setToken(t);
  return !!t;
}
async function apiFetch(url, opts) {
  // Build a fresh request object so the caller's opts is never mutated:
  // headers are added here, and the retry flag is internal to this call.
  const merged = { ...opts };
  const headers = { ...(opts?.headers || {}) };
  const token = _getToken();
  if (token) headers['Authorization'] = 'Bearer ' + token;
  merged.headers = headers;
  let r = await fetch(url, merged);
  if (r.status === 401 && !merged._retried) {
    if (_promptToken()) {
      merged._retried = true;
      merged.headers['Authorization'] = 'Bearer ' + _getToken();
      r = await fetch(url, merged);
    }
  }
  return r;
}

function fmtBytes(n) {
  if (n == null || isNaN(n)) return '–';
  const u = ['B','KB','MB','GB','TB']; let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return n.toFixed(n >= 100 || i === 0 ? 0 : 1) + ' ' + u[i];
}
function fmtNum(n) { return n == null ? '–' : n.toLocaleString(); }
function fmtEta(s) {
  if (s == null || !isFinite(s) || s < 0) return '';
  s = Math.round(s);
  if (s < 60) return s + 's';
  if (s < 3600) return Math.floor(s/60) + 'm ' + (s%60) + 's';
  return Math.floor(s/3600) + 'h ' + Math.floor(s%3600/60) + 'm';
}

// ── Toast notifications ──────────────────────────────────────────────────
let _toastTimer;
function toast(msg, ok = true) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show ' + (ok ? 'ok' : 'err');
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => t.className = 'toast', 3000);
}

// ── JSON fetch helper ────────────────────────────────────────────────────
// Wraps apiFetch + r.json() + error extraction. Use in place of the repeated
//   const r = await apiFetch(url, opts);
//   const body = await r.json();
//   if (!r.ok) throw new Error(body.error || 'HTTP ' + r.status);
// pattern found across all pages.
async function apiJson(url, opts) {
  const r = await apiFetch(url, opts);
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || 'HTTP ' + r.status);
  return body;
}

// ── Search snippet highlighting ──────────────────────────────────────────
// The API wraps matched terms in <b>…</b> (ts_headline) when highlight=true.
// Everything else is plain text, so: find the literal <b>/</b> tag pairs,
// escape everything, and re-emit the matched pairs as <mark>…</mark>.
function snippetHtml(raw) {
  if (!raw) return '';
  const re = /<\/b>|<b>/g;
  let out = '';
  let i = 0;
  let inMark = false;
  let m;
  while ((m = re.exec(raw)) !== null) {
    out += esc(raw.slice(i, m.index));
    if (m[0] === '<b>' && !inMark) {
      inMark = true;
      out += '<mark>';
    } else if (m[0] === '</b>' && inMark) {
      inMark = false;
      out += '</mark>';
    } else {
      out += esc(m[0]); // unbalanced tag → render as literal text
    }
    i = m.index + m[0].length;
  }
  out += esc(raw.slice(i));
  return out;
}

// ── Per-ZIM controls (embed toggle + category input + saved marker) ─────
// Shared by the library page (zim-card) and the settings page (zim-row),
// which render the same controls with page-local classes and data-* attribute
// names (data-embed vs data-zembed, …). The per-page parts are passed in via
// `p`: { toggleTitle, embedAttr, catAttr, msgClass, msgAttr, msgText, labelHtml }
function zimControls(z, p) {
  return `
      <label class="toggle" title="${p.toggleTitle}">
        <input type="checkbox" data-${p.embedAttr}="${esc(z.name)}" ${z.embed_enabled ? 'checked' : ''}>
        <span class="slider"></span>
      </label>
      ${p.labelHtml || ''}
      <input type="text" data-${p.catAttr}="${esc(z.name)}" value="${esc(z.category || '')}" placeholder="category (default)">
      <span class="${p.msgClass}" data-${p.msgAttr}="${esc(z.name)}">${p.msgText}</span>`;
}

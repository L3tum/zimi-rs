/* exported apiFetch, apiJson, esc, fmtBytes, fmtEta, fmtNum, snippetHtml, toast */
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
  opts = opts || {};
  const headers = Object.assign({}, opts.headers);
  const token = _getToken();
  if (token) headers['Authorization'] = 'Bearer ' + token;
  opts.headers = headers;
  let r = await fetch(url, opts);
  if (r.status === 401 && !opts._retried) {
    if (_promptToken()) {
      opts._retried = true;
      opts.headers['Authorization'] = 'Bearer ' + _getToken();
      r = await fetch(url, opts);
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

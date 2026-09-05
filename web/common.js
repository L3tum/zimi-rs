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

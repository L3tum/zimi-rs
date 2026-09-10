'use strict';

let zims = [];
const params = new URLSearchParams(location.search);
const initialQ = params.get('q') || '';
const initialZim = params.get('zim') || '';
const initialLang = params.get('lang') || '';
const mode = params.get('mode');
const initialMode = ['hybrid','fts','trgm','vector'].includes(mode) ? mode : 'hybrid';
const qInput = document.getElementById('q');
qInput.value = initialQ;
document.getElementById('mode').value = initialMode;

async function loadZims() {
  try {
    const data = await apiJson('/list');
    zims = data.zims || [];
    const zimSel = document.getElementById('zim');
    const langSel = document.getElementById('lang');
    const langs = new Set();
    zims.forEach(z => {
      const opt = document.createElement('option');
      opt.value = z.name;
      opt.textContent = z.display_title;
      if (z.name === initialZim) opt.selected = true;
      zimSel.appendChild(opt);
      if (z.language) langs.add(z.language);
    });
    [...langs].sort().forEach(l => {
      const opt = document.createElement('option');
      opt.value = l;
      opt.textContent = l;
      if (l === initialLang) opt.selected = true;
      langSel.appendChild(opt);
    });
    if (initialQ) doSearch();
  } catch (e) {
    showState('error', 'Could not load ZIM list: ' + esc(e.message));
  }
}

function showState(which, msg) {
  ['loading', 'error', 'empty'].forEach(id => {
    document.getElementById(id).style.display = 'none';
  });
  if (which && which !== 'results') {
    const el = document.getElementById(which);
    if (msg != null) el.textContent = msg;
    el.style.display = 'block';
  }
}

let searchSeq = 0;
async function doSearch() {
  const q = qInput.value.trim();
  if (!q) return;
  closeSuggestions();
  const zim = document.getElementById('zim').value;
  const lang = document.getElementById('lang').value;
  const mode = document.getElementById('mode').value;
  const url = `/search?q=${encodeURIComponent(q)}&highlight=true` +
    (zim ? '&zim=' + encodeURIComponent(zim) : '') +
    (lang ? '&language=' + encodeURIComponent(lang) : '') +
    '&mode=' + encodeURIComponent(mode);
  const seq = ++searchSeq;
  showState('loading');
  const t0 = performance.now();
  try {
    const data = await apiJson(url);
    if (seq !== searchSeq) return; // stale response
    const ms = (performance.now() - t0).toFixed(0);
    document.getElementById('stats').textContent = `${data.results.length} results in ${ms}ms` +
      (data.total != null && data.total !== data.results.length ? ` (of ${data.total} total)` : '');
    renderResults(data.results || []);
  } catch (e) {
    if (seq !== searchSeq) return;
    showState('error', 'Search failed: ' + esc(e.message));
    document.getElementById('stats').textContent = '';
  }
  history.replaceState(null, '', url);
}

function renderResults(results) {
  document.getElementById('results').innerHTML = results.map(res => {
    const href = `/w/${encodeURIComponent(res.zim_name)}/${encodeURIComponent(res.path)}`;
    const score = Number(res.score).toFixed(3);
    return `
    <div class="result">
      <h3><a href="${href}">${esc(res.title)}</a></h3>
      <div class="snippet">${snippetHtml(res.snippet || res.content_preview || '')}</div>
      <div class="meta">${esc(res.zim_name)} · ${esc(res.language)} · score ${score}</div>
    </div>
  `;
  }).join('');
  showState(results.length ? 'results' : 'empty');
}

// ── Autocomplete (no inline handlers; safe with arbitrary titles) ──────────
let suggestTimer = null;
let suggestItems = [];
let suggestSel = -1;

function closeSuggestions() {
  document.getElementById('suggestions').style.display = 'none';
  suggestItems = [];
  suggestSel = -1;
}

qInput.addEventListener('input', function() {
  clearTimeout(suggestTimer);
  const q = this.value.trim();
  if (q.length < 2) { closeSuggestions(); return; }
  suggestTimer = setTimeout(async () => {
    try {
      const data = await apiJson(`/suggest?q=${encodeURIComponent(q)}&limit=8`);
      if (qInput.value.trim() !== q) return; // user kept typing
      renderSuggestions(data.suggestions || []);
    } catch { /* non-fatal */ }
  }, 200);
});

function renderSuggestions(items) {
  const list = document.getElementById('suggestions');
  list.innerHTML = '';
  suggestItems = items;
  suggestSel = -1;
  if (!items.length) { list.style.display = 'none'; return; }
  items.forEach((s, i) => {
    const div = document.createElement('div');
    div.textContent = s;
    div.addEventListener('mousedown', (e) => { e.preventDefault(); pickSuggestion(s); });
    div.addEventListener('mouseenter', () => setSuggestSel(i));
    list.appendChild(div);
  });
  list.style.display = 'block';
}

function setSuggestSel(i) {
  const list = document.getElementById('suggestions');
  suggestSel = i;
  [...list.children].forEach((el, j) => el.classList.toggle('sel', j === i));
  if (suggestSel >= 0) qInput.value = suggestItems[suggestSel];
}

function pickSuggestion(title) {
  qInput.value = title;
  closeSuggestions();
  doSearch();
}

qInput.addEventListener('keydown', function(e) {
  const open = document.getElementById('suggestions').style.display === 'block';
  if (e.key === 'Enter') {
    if (open && suggestSel >= 0) pickSuggestion(suggestItems[suggestSel]);
    else doSearch();
  } else if (open && e.key === 'ArrowDown') {
    e.preventDefault();
    setSuggestSel((suggestSel + 1) % suggestItems.length);
  } else if (open && e.key === 'ArrowUp') {
    e.preventDefault();
    setSuggestSel(suggestSel <= 0 ? suggestItems.length - 1 : suggestSel - 1);
  } else if (e.key === 'Escape' && open) {
    closeSuggestions();
  }
});
document.addEventListener('click', (e) => {
  if (!e.target.closest('.search-box')) closeSuggestions();
});
document.getElementById('go').addEventListener('click', doSearch);

loadZims();

'use strict';

// ── Magic constants (timing) ─────────────────────────────────────
// How long the save-bar success/error message lingers before clearing (ms).
const SAVE_MESSAGE_CLEAR_MS = 6000;
// How long a ZIM row's flash highlight stays on after scrolling to it (ms).
const ZIM_FLASH_DURATION_MS = 1600;
// Delay before flashing the row targeted by a #zim-<name> anchor (ms), so the
// layout settles first.
const ANCHOR_FLASH_DELAY_MS = 60;

// Field metadata: label, description, input type. `int`/`float`/`password`/
// `select` default to text. `restart` marks settings read only at startup.
const FIELDS = {
  'general.zim_dir': {
    label: 'ZIM directory',
    desc: 'Directory scanned for .zim files (watched for changes)',
    restart: true,
  },
  'general.host': { label: 'Bind host', restart: true },
  'general.port': { label: 'Port', type: 'int', restart: true },
  'general.log_level': {
    label: 'Log level',
    type: 'select',
    options: ['trace', 'debug', 'info', 'warn', 'error'],
    restart: true,
  },
  'general.cors_origins': {
    label: 'CORS origins',
    desc: 'Comma-separated allowed origins (applied at startup)',
    restart: true,
  },
  'general.trusted_proxy_cidrs': {
    label: 'Trusted proxy CIDRs',
    desc: 'Comma-separated reverse-proxy source CIDRs (applied at startup)',
    restart: true,
  },

  'downloads.max_bytes': {
    label: 'Max download size',
    desc: 'Largest .zim accepted by direct downloads (bytes)',
    type: 'int',
  },
  'downloads.allow_private_networks': {
    label: 'Allow private networks',
    desc: 'Permit direct downloads from private/LAN addresses (SSRF opt-in)',
  },

  'search.fts_weight': {
    label: 'Full-text weight',
    desc: 'Hybrid: weight of FTS matches (0–1)',
    type: 'float',
  },
  'search.trgm_weight': {
    label: 'Trigram weight',
    desc: 'Hybrid: weight of fuzzy matches (0–1)',
    type: 'float',
  },
  'search.vector_weight': {
    label: 'Vector weight',
    desc: 'Hybrid: weight of semantic matches (0–1); requires embeddings',
    type: 'float',
  },
  'search.trgm_threshold': {
    label: 'Trigram threshold',
    desc: 'Minimum similarity for fuzzy matches (0–1)',
    type: 'float',
  },
  'search.default_limit':  { label: 'Default result limit', type: 'int' },
  'search.max_limit':      { label: 'Max result limit', type: 'int' },

  'torrent.enabled': {
    label: 'Enable qBittorrent',
    desc: 'Manage ZIM downloads via the qBittorrent Web API',
  },
  'torrent.url': {
    label: 'qBittorrent URL',
    desc: 'Base URL of the Web UI, e.g. http://localhost:8080',
  },
  'torrent.username':       { label: 'qBittorrent username' },
  'torrent.password':       { label: 'qBittorrent password', type: 'password' },
  'torrent.save_path': {
    label: 'Save path',
    desc: 'Directory inside qBittorrent where ZIM torrents are stored',
  },
  'torrent.category':       { label: 'Torrent category' },
  'torrent.max_active':     { label: 'Max active downloads', type: 'int' },
  'torrent.poll_secs':      { label: 'Poll interval (seconds)', type: 'int' },
  'torrent.file_strategy': {
    label: 'File strategy',
    desc: 'hardlink = no extra disk space (same filesystem); copy = always duplicate',
    type: 'select',
    options: ['hardlink', 'copy'],
  },
  'torrent.seed_ratio': {
    label: 'Seed ratio cap',
    desc: 'Stop seeding after this upload/download ratio',
    type: 'float',
  },
  'torrent.keep_completed': {
    label: 'Keep completed torrents',
    desc: 'Leave finished torrents in qBittorrent (recommended for seeding)',
  },
  'torrent.opds_url': {
    label: 'OPDS catalog URL',
    desc: 'Kiwix catalog polled for new ZIM versions',
  },
  'torrent.allow_private_networks': {
    label: 'Allow LAN qBittorrent',
    desc: 'Permit the qB endpoint on private/LAN addresses (SSRF opt-in)',
  },
  'torrent.auto_update': {
    label: 'Auto-update ZIMs',
    desc: 'Queue newer catalog versions as downloads automatically. ' +
      'Trust model: downloaded ZIMs are structurally verified only — the ' +
      'OPDS source is trusted, so use a catalog you control',
  },

  'embedding.enabled': {
    label: 'Enable embeddings',
    desc: 'Semantic (vector) search over article text',
  },
  'embedding.endpoint': {
    label: 'Embeddings endpoint',
    desc: 'OpenAI-compatible base URL, e.g. http://localhost:11434/v1 (Ollama)',
  },
  'embedding.api_key': {
    label: 'API key',
    desc: 'Leave empty for local servers',
    type: 'password',
  },
  'embedding.model':            { label: 'Model' },
  'embedding.dimension': {
    label: 'Vector dimension',
    desc: 'Must match the model output size',
    type: 'int',
  },
  'embedding.batch_size':       { label: 'Batch size', type: 'int' },
  'embedding.hnsw_threshold': {
    label: 'HNSW below (rows)',
    desc: 'Use HNSW index when row count is below this',
    type: 'int',
  },
  'embedding.ivfflat_threshold': {
    label: 'IVFFlat above (rows)',
    desc: 'Use IVFFlat index above this (less RAM)',
    type: 'int',
  },
  'embedding.max_concurrency': {
    label: 'Max concurrent embeds',
    desc: 'Parallel embedding HTTP calls per batch',
    type: 'int',
  },
  'embedding.timeout_secs': {
    label: 'Embed timeout (secs)',
    desc: 'Per-request timeout for the embedding HTTP call',
    type: 'int',
  },

  'access.mode': {
    label: 'Access mode',
    desc: 'open = no auth · password = shared token required for mutating API '
      + 'calls (POST/PUT/DELETE) via Bearer header or ?access_token=',
    type: 'select',
    options: ['open', 'password'],
  },
  'access.admin_password': {
    label: 'Admin password',
    desc: 'Required when mode = password',
    type: 'password',
  },
  'access.read_only_token': {
    label: 'Read-only API token',
    desc: 'Optional: grants read-only API access (search/suggest/content) only',
    type: 'password',
  },
  'access.rate_limit_rps': {
    label: 'Rate limit (req/s)',
    desc: 'Sustained requests per second allowed (token bucket refill rate)',
    type: 'int',
  },
  'access.rate_limit_burst': {
    label: 'Rate limit (burst)',
    desc: 'Max burst of requests before throttling to the rate above',
    type: 'int',
  },
  'access.require_auth_for_reads': {
    label: 'Require auth for reads',
    desc: 'Gate GET/HEAD/OPTIONS (incl. /w/ raw content) behind the token',
  },
};
const CAT_ORDER = ['general', 'search', 'downloads', 'torrent', 'embedding', 'access'];
const CAT_NAMES = {
  general: 'General',
  search: 'Search',
  downloads: 'Downloads',
  torrent: 'Torrent (qBittorrent)',
  embedding: 'Embedding',
  access: 'Access',
};

let settings = {};   // raw server response: {cat: {key: {value, locked?, locked_by?}}}
let originals = {};  // full key -> original value (for change detection + typing)

async function load() {
  settings = await apiJson('/settings');
  originals = {};
  for (const [cat, fields] of Object.entries(settings))
    for (const [key, info] of Object.entries(fields))
      originals[`${cat}.${key}`] = info.value;
  render();
  loadZimRows();
}

function inputFor(fullKey, info) {
  const val = info.value;
  const locked = !!info.locked;
  const meta = FIELDS[fullKey] || {};
  const type = meta.type
    || (typeof val === 'number' ? (Number.isInteger(val) ? 'int' : 'float') : 'text');

  if (typeof val === 'boolean') {
    const checked = val ? 'checked' : '';
    const disabled = locked ? 'disabled' : '';
    return `<label class="toggle">`
      + `<input type="checkbox" data-key="${esc(fullKey)}" ${checked} ${disabled}>`
      + `<span class="slider"></span></label>`;
  }
  if (type === 'select') {
    const opts = (meta.options || [String(val)])
      .map(o => `<option ${o === String(val) ? 'selected' : ''}>${esc(o)}</option>`)
      .join('');
    return `<select data-key="${esc(fullKey)}" ${locked ? 'disabled' : ''}>${opts}</select>`;
  }
  if (type === 'password') {
    // A locked secret is set elsewhere (env); never echo its value into the DOM.
    const shown = locked ? '' : val;
    const ph = locked ? ' placeholder="locked (set via env)"' : '';
    const disabled = locked ? 'disabled' : '';
    return `<input type="password" data-key="${esc(fullKey)}" value="${esc(shown)}"${ph}`
      + ` ${disabled} autocomplete="new-password">`
      + `\n            <button class="show-pass" ${disabled}>show</button>`;
  }
  const itype = type === 'int' || type === 'float' ? 'number' : 'text';
  const step = type === 'float' ? 'step="any"' : '';
  return `<input type="${itype}" ${step} data-key="${esc(fullKey)}"`
    + ` value="${esc(val)}" ${locked ? 'disabled' : ''}>`;
}

function render() {
  const cats = [
    ...CAT_ORDER.filter(c => settings[c]),
    ...Object.keys(settings).filter(c => !CAT_ORDER.includes(c)),
  ];
  document.getElementById('sections').innerHTML = cats.map(cat => {
    const fields = Object.entries(settings[cat]).map(([key, info]) => {
      const fullKey = `${cat}.${key}`;
      const meta = FIELDS[fullKey] || {};
      const tags = [];
      const lockedBy = info.locked_by || 'env';
      if (info.locked) {
        const title = `Set via ${esc(info.locked_by || 'environment variable')}`;
        tags.push(`<span class="tag lock" title="${title}">🔒 ${esc(lockedBy)}</span>`);
      }
      const restartTag = '<span class="tag restart"'
        + ' title="Read at startup — restart zimservice to apply">⚠ restart</span>';
      if (meta.restart) tags.push(restartTag);
      return `<div class="field" data-field="${esc(fullKey)}">
        <div>
          <div class="flabel">${esc(meta.label || key)}</div>
          ${meta.desc ? `<div class="desc">${meta.desc}</div>` : ''}
        </div>
        <div class="fcontrol">${tags.join('')}${inputFor(fullKey, info)}</div>
      </div>`;
    }).join('');
    return `<div class="section"><h2>${esc(CAT_NAMES[cat] || cat)}</h2>${fields}</div>`;
  }).join('');
  bindChanges();
  updateSaveBtn();
}

function togglePass(btn) {
  const inp = btn.previousElementSibling;
  const show = inp.type === 'password';
  inp.type = show ? 'text' : 'password';
  btn.textContent = show ? 'hide' : 'show';
}

function currentValue(el) {
  if (el.type === 'checkbox') return el.checked;
  if (el.tagName === 'SELECT') return el.value;
  const orig = originals[el.dataset.key];
  if (typeof orig === 'number' || el.type === 'number') {
    const n = Number(el.value);
    return el.value.trim() === '' ? null : n;
  }
  return el.value;
}

function bindChanges() {
  document.querySelectorAll('[data-key]').forEach(el => {
    if (el.disabled) return;
    const evt = (el.tagName === 'SELECT' || el.type === 'checkbox') ? 'change' : 'input';
    el.addEventListener(evt, () => {
      el.closest('.field').classList.add('changed');
      updateSaveBtn();
    });
  });
}

function changedEntries() {
  const out = {};
  document.querySelectorAll('.field[data-field]').forEach(f => {
    const key = f.dataset.field;
    const el = f.querySelector('[data-key]');
    if (!el || el.disabled) return;
    const v = currentValue(el);
    if (v === originals[key]) return;
    out[key] = v;
  });
  return out;
}

function updateSaveBtn() {
  const n = Object.keys(changedEntries()).length;
  const btn = document.getElementById('save');
  btn.disabled = n === 0 || btn.dataset.saving === '1';
  btn.textContent = n ? `Save ${n} change${n > 1 ? 's' : ''}` : 'Save';
}

async function saveAll() {
  const updates = changedEntries();
  if (!Object.keys(updates).length) return;
  const btn = document.getElementById('save');
  const msg = document.getElementById('msg');
  btn.dataset.saving = '1';
  btn.disabled = true;
  try {
    const body = await apiJson('/settings', {
      method: 'PUT',
      body: JSON.stringify(updates),
    });
    if (body.errors && body.errors.length) {
      msg.className = 'msg error';
      msg.textContent = body.errors.join('\n');
    } else {
      msg.className = 'msg success';
      msg.textContent = `Saved ${body.updated} setting${body.updated === 1 ? '' : 's'}.`;
      await load();
    }
  } catch (e) {
    msg.className = 'msg error';
    msg.textContent = 'Error: ' + e.message;
  } finally {
    delete btn.dataset.saving;
    updateSaveBtn();
    setTimeout(() => { msg.textContent = ''; msg.className = 'msg'; }, SAVE_MESSAGE_CLEAR_MS);
  }
}

// ── Per-ZIM section ────────────────────────────────────────────────────
// Per-page parts of the shared zimControls() markup (see web/common.js).
const ZIM_ROW_CONTROLS = {
  toggleTitle: 'Embedding for this ZIM',
  embedAttr: 'zembed',
  catAttr: 'zcat',
  msgClass: 'zmsg',
  msgAttr: 'zmsg',
  msgText: 'saved ✓',
};
let zimsData = [];
let zimTimers = {};

async function loadZimRows() {
  const wrap = document.getElementById('zimRows');
  try {
    const data = await apiJson('/list');
    zimsData = data.zims || [];
  } catch (e) {
    wrap.innerHTML = `<div class="sub">Could not load ZIM list: ${esc(e.message)}</div>`;
    return;
  }
  if (!zimsData.length) {
    wrap.innerHTML = '<div class="sub">No ZIMs in the library.</div>';
    return;
  }
  wrap.innerHTML = zimsData.map(z => `
    <div class="zim-row" id="zim-${encodeURIComponent(z.name)}">
      <span class="zname" title="${esc(z.name)}">${esc(z.display_title)}</span>
      ${zimControls(z, ZIM_ROW_CONTROLS)}
    </div>
  `).join('');
  handleAnchor();
}

function flashZim(name) {
  // Row ids are built from encodeURIComponent(z.name) at render time (the
  // index page's Settings links use #zim-${encodeURIComponent(...)}, decoded
  // by handleAnchor), so the lookup must use the same construction.
  const row = document.getElementById('zim-' + encodeURIComponent(name));
  if (!row) return;
  row.classList.add('flash');
  row.scrollIntoView({ behavior: 'smooth', block: 'center' });
  setTimeout(() => row.classList.remove('flash'), ZIM_FLASH_DURATION_MS);
}
function handleAnchor() {
  if (!location.hash) return;
  const name = location.hash.slice(1);
  if (name.startsWith('zim-')) {
    setTimeout(() => flashZim(decodeURIComponent(name.slice(4))), ANCHOR_FLASH_DELAY_MS);
  }
}

async function putZim(name, patch) {
  await apiJson(`/settings/zim/${encodeURIComponent(name)}`, {
    method: 'PUT',
    body: JSON.stringify(patch),
  });
  const msg = document.querySelector(`[data-zmsg="${CSS.escape(name)}"]`);
  if (msg) { msg.style.opacity = 1; setTimeout(() => msg.style.opacity = 0, SAVED_MARKER_FADE_MS); }
}

document.getElementById('zimRows').addEventListener('change', async (e) => {
  const name = e.target.dataset.zembed;
  if (!name) return;
  try {
    await putZim(name, { embed_enabled: e.target.checked });
    const z = zimsData.find(x => x.name === name);
    if (z) z.embed_enabled = e.target.checked;
    toast(`Embedding ${e.target.checked ? 'enabled' : 'disabled'} for ${name}`);
  } catch (err) {
    toast('Save failed: ' + err.message, false);
    e.target.checked = !e.target.checked;
  }
});

document.getElementById('zimRows').addEventListener('input', (e) => {
  const name = e.target.dataset.zcat;
  if (!name) return;
  clearTimeout(zimTimers[name]);
  zimTimers[name] = setTimeout(async () => {
    const z = zimsData.find(x => x.name === name);
    const val = e.target.value.trim();
    if (!z || val === (z.category || '')) return;
    try {
      await putZim(name, { category: val || null });
      z.category = val || null;
    } catch (err) { toast('Category save failed: ' + err.message, false); }
  }, CATEGORY_SAVE_DEBOUNCE_MS);
});

// No inline handlers: the save button is static, the show/hide password
// buttons are re-rendered by render(), so delegate.
document.getElementById('save').addEventListener('click', saveAll);
document.getElementById('sections').addEventListener('click', (e) => {
  const btn = e.target.closest('.show-pass');
  if (btn) togglePass(btn);
});

load().catch(e => {
  const msg = `<div class="sub">Failed to load settings: ${esc(e.message)}</div>`;
  document.getElementById('sections').innerHTML = msg;
});
window.addEventListener('hashchange', handleAnchor);

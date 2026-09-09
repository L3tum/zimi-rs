'use strict';

// ── Library ─────────────────────────────────────────────────────────────
// Per-page parts of the shared zimControls() markup (see web/common.js).
const ZIM_CARD_CONTROLS = {
  toggleTitle: 'Enable semantic (embedding) search for this ZIM',
  embedAttr: 'embed',
  labelHtml: '<span title="Embedding">embed</span>',
  catAttr: 'cat',
  msgClass: 'cat-saved',
  msgAttr: 'catmsg',
  msgText: '✓',
};
let zimsData = [];

async function loadLibrary() {
  const bar = document.getElementById('statusBar');
  try {
    const [health, list] = await Promise.all([
      apiJson('/health'),
      apiJson('/list'),
    ]);
    zimsData = list.zims || [];
    bar.innerHTML =
      `<span><span class="dot ok"></span>v${esc(health.version)}</span>` +
      `<span>${zimsData.length} ZIMs</span>` +
      `<span>${fmtNum(health.articles_count)} articles indexed</span>` +
      `<span><span class="dot ${health.qbit_connected ? 'ok' : 'bad'}"></span>qBittorrent ${health.qbit_connected ? 'connected' : 'not configured'}</span>` +
      `<a class="btn" style="padding:2px 10px" href="/openapi.json">API</a>`;
    renderZims();
  } catch (e) {
    bar.innerHTML = `<span><span class="dot bad"></span>Error loading library: ${esc(e.message)}</span>`;
  }
}

function renderZims() {
  const grid = document.getElementById('zims');
  if (!zimsData.length) {
    grid.innerHTML = '<div class="empty">No ZIM files found. Add one via Downloads below, or place a .zim file in the configured ZIM directory.</div>';
    return;
  }
  grid.innerHTML = zimsData.map(z => {
    const pct = Math.min(100, Math.round((z.index_progress || 0) * 100));
    const bar = (z.index_status === 'indexing' || z.index_status === 'pending')
      ? `<div class="prog"><div style="width:${pct}%"></div></div>` : '';
    return `
    <div class="zim-card">
      <h3>${esc(z.display_title)}</h3>
      <div class="meta">
        <div><span class="badge">${esc(z.language)}</span> ${z.category ? `<span class="badge">${esc(z.category)}</span>` : ''} ${z.date ? `<span class="badge">${esc(z.date)}</span>` : ''}</div>
        <div>${fmtNum(z.entry_count)} entries · ${fmtBytes(z.file_size)}</div>
        <div>${fmtNum(z.indexed_entries)} indexed · ${pct}%</div>
        ${bar}
        <div style="margin-top:8px">
          <span class="status status-${esc(z.index_status)}">${esc(z.index_status)}</span>
          ${z.embed_enabled ? '<span class="badge" style="color:var(--green)">embedding on</span>' : ''}
        </div>
      </div>
      <div class="zim-settings">
        ${zimControls(z, ZIM_CARD_CONTROLS)}
      </div>
      <div style="margin-top:12px;display:flex;gap:8px">
        <a class="btn" href="/search.html?q=&zim=${encodeURIComponent(z.name)}">Search</a>
        <a class="btn" href="/settings.html#zim-${encodeURIComponent(z.name)}">Settings</a>
      </div>
    </div>`;
  }).join('');
}

// Per-ZIM settings (immediate save)
document.getElementById('zims').addEventListener('change', async (e) => {
  const name = e.target.dataset.embed;
  if (!name) return;
  try {
    await apiJson(`/settings/zim/${encodeURIComponent(name)}`, {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ embed_enabled: e.target.checked }),
    });
    toast(`Embedding ${e.target.checked ? 'enabled' : 'disabled'} for ${name}`);
  } catch (err) { toast('Save failed: ' + err.message, false); e.target.checked = !e.target.checked; }
});

let catTimers = {};
document.getElementById('zims').addEventListener('input', (e) => {
  const name = e.target.dataset.cat;
  if (!name) return;
  clearTimeout(catTimers[name]);
  catTimers[name] = setTimeout(() => saveCategory(name, e.target), 800);
});
async function saveCategory(name, input) {
  const z = zimsData.find(x => x.name === name);
  if (!z) return;
  const val = input.value.trim();
  if (val === (z.category || '')) return; // unchanged
  try {
    await apiJson(`/settings/zim/${encodeURIComponent(name)}`, {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ category: val || null }),
    });
    z.category = val || null;
    const msg = document.querySelector(`[data-catmsg="${CSS.escape(name)}"]`);
    if (msg) { msg.style.opacity = 1; setTimeout(() => msg.style.opacity = 0, 1200); }
  } catch (err) { toast('Category save failed: ' + err.message, false); }
}

// ── Downloads ───────────────────────────────────────────────────────────
let dlPoll = null;

async function loadDownloads() {
  let data;
  try {
    data = await apiJson('/downloads');
  } catch { return; }
  const dl = data.downloads || [];
  renderDownloads(dl);
  // Keep polling only while something is in flight.
  const active = dl.some(d => d.status === 'queued' || d.status === 'downloading' || d.status === 'seeding');
  if (active && !dlPoll) dlPoll = setInterval(loadDownloads, 3000);
  if (!active && dlPoll) { clearInterval(dlPoll); dlPoll = null; }
}

function renderDownloads(dl) {
  const list = document.getElementById('dlList');
  if (!dl.length) { list.innerHTML = '<div class="empty">No downloads yet.</div>'; return; }
  list.innerHTML = dl.map(d => {
    const pct = Math.min(100, Math.round((d.progress || 0) * 100));
    const inFlight = d.status === 'queued' || d.status === 'downloading';
    const speed = d.speed_bps ? ` · ${fmtBytes(d.speed_bps)}/s` : '';
    const upSpeed = d.up_speed_bps ? ` · ▲ ${fmtBytes(d.up_speed_bps)}/s` : '';
    const eta = d.eta_secs ? ` · ETA ${fmtEta(d.eta_secs)}` : '';
    const prog = inFlight ? `<div class="prog"><div style="width:${pct}%;background:${d.status === 'queued' ? 'var(--muted)' : 'var(--accent)'}"></div></div>` : '';
    const statusColor = { queued: 'var(--muted)', downloading: 'var(--accent)', complete: 'var(--green)', seeding: 'var(--purple)', error: 'var(--red)', cancelled: 'var(--muted)' }[d.status] || 'var(--muted)';
    const seedDetail = d.status === 'seeding'
      ? `<div class="detail">${d.up_speed_bps != null ? `▲ ${fmtBytes(d.up_speed_bps)}/s` : '▲ –'}${d.ratio != null ? ` · ratio ${d.ratio.toFixed(2)}` : ''}${d.num_seeds != null ? ` · ${d.num_seeds} seeders` : ''}${d.speed_bps ? ` · ▼ ${fmtBytes(d.speed_bps)}/s` : ''}</div>`
      : '';
    return `
    <div class="dl-item">
      <div class="row1">
        <span class="status" style="background:#21262d;color:${statusColor}">${esc(d.status)}</span>
        <span class="name">${esc(d.name)}</span>
        ${inFlight ? `<button class="btn btn-danger" style="padding:3px 10px" data-id="${d.id}">Cancel</button>` : ''}
      </div>
      <div class="row1" style="margin-top:4px"><span class="url">${esc(d.url)}</span></div>
      ${prog}
      ${inFlight ? `<div class="detail">${pct}%${speed}${upSpeed}${eta}</div>` : ''}
      ${seedDetail}
      ${d.error ? `<div class="detail"><span class="err">${esc(d.error)}</span></div>` : ''}
    </div>`;
  }).join('');
}

async function addDownload() {
  const url = document.getElementById('dlUrl').value.trim();
  const name = document.getElementById('dlName').value.trim();
  if (!url) { toast('Enter a URL or magnet link', false); return; }
  const btn = document.getElementById('dlAdd');
  btn.disabled = true;
  try {
    await apiJson('/downloads', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, name: name || undefined }),
    });
    document.getElementById('dlUrl').value = '';
    document.getElementById('dlName').value = '';
    toast('Download queued');
    loadDownloads();
  } catch (e) { toast('Failed: ' + e.message, false); }
  btn.disabled = false;
}
document.getElementById('dlUrl').addEventListener('keydown', e => { if (e.key === 'Enter') addDownload(); });
document.getElementById('dlAdd').addEventListener('click', addDownload);
document.getElementById('dlList').addEventListener('click', e => {
  const btn = e.target.closest('[data-id]');
  if (btn) cancelDownload(Number(btn.dataset.id));
});

async function cancelDownload(id) {
  try {
    const r = await apiFetch(`/downloads/${id}`, { method: 'DELETE' });
    if (!r.ok) throw new Error(r.status);
    loadDownloads();
  } catch (e) { toast('Cancel failed: ' + e.message, false); }
}

loadLibrary();
loadDownloads();
setInterval(() => { if (!document.hidden) loadLibrary(); }, 15000); // keep index progress / new files fresh (paused while the tab is hidden)

const ACTIVE = new Set(['RUNNING', 'PAUSING', 'PENDING', 'QUEUED']);
const MODAL_IDS = ['submitModal', 'errorsModal'];
const syncFormatter = new Intl.DateTimeFormat(undefined, {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});
const integerFormatter = new Intl.NumberFormat(undefined);
const initialUrl = new URL(window.location.href);

let currentErrorsJobId = null;
let currentGraphJobId = initialUrl.searchParams.get('tab') === 'job'
  ? (initialUrl.searchParams.get('id') || null)
  : null;
let currentLogFile = initialUrl.searchParams.get('log') || '';
let followLatestLog = initialUrl.searchParams.get('follow') === 'latest' || !currentLogFile;
let currentLogLineLimit = Number(initialUrl.searchParams.get('lines') || 500);
let latestLogLines = [];
let latestLogTailLoaded = false;
let latestLogTailError = '';
let currentTab = initialUrl.searchParams.get('tab') === 'job' && currentGraphJobId
  ? 'job'
  : initialUrl.searchParams.get('tab') === 'iceberg'
  ? 'iceberg'
  : initialUrl.searchParams.get('tab') === 'logs'
    ? 'logs'
    : 'doris';
let refreshInFlight = false;
let authEnabled = false;
let currentAuthUsername = '';
let previousJobListTab = currentTab === 'job' ? 'doris' : currentTab;
let latestDorisArchiveJobs = [];
let latestIcebergArchiveJobs = [];
let bulkDeleteInFlight = false;

if (!Number.isFinite(currentLogLineLimit) || currentLogLineLimit <= 0) {
  currentLogLineLimit = 500;
}
currentLogLineLimit = Math.min(5000, Math.trunc(currentLogLineLimit));

if (currentTab !== 'job') {
  currentGraphJobId = null;
}

function escapeHtml(s) {
  return (s ?? '').toString()
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function quoteJsString(s) {
  return JSON.stringify((s ?? '').toString());
}

function fmtWholeNumber(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  return integerFormatter.format(Math.trunc(num));
}

function formatDuration(seconds) {
  const raw = Number(seconds);
  if (!Number.isFinite(raw) || raw < 0) return '-';
  let remaining = Math.trunc(raw);
  const days = Math.floor(remaining / 86400);
  remaining %= 86400;
  const hours = Math.floor(remaining / 3600);
  remaining %= 3600;
  const minutes = Math.floor(remaining / 60);
  const secs = remaining % 60;
  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0 || parts.length > 0) parts.push(`${hours}h`);
  if (minutes > 0 || parts.length > 0) parts.push(`${minutes}m`);
  parts.push(`${secs}s`);
  return parts.join(' ');
}

function currentLocationForRedirect() {
  return window.location.pathname + window.location.search;
}

function redirectToLogin() {
  const next = currentLocationForRedirect() || '/';
  window.location.href = '/login?next=' + encodeURIComponent(next);
}

async function apiFetch(input, init) {
  return fetch(input, { credentials: 'same-origin', ...(init || {}) });
}

async function loadAuthStatus() {
  const menu = document.getElementById('profileMenu');
  const name = document.getElementById('profileName');
  const initials = document.getElementById('profileInitials');

  try {
    const res = await fetch('/auth/status', { credentials: 'same-origin' });
    if (!res.ok) throw new Error(`auth ${res.status}`);

    const data = await res.json();
    authEnabled = !!data.enabled;
    currentAuthUsername = data.username || '';

    if (authEnabled && !data.authenticated) {
      redirectToLogin();
      return;
    }

    menu.classList.toggle('hidden', !authEnabled);

    if (authEnabled) {
      const displayName = currentAuthUsername || 'User';
      name.textContent = displayName;
      initials.textContent = profileInitials(displayName);
    }
  } catch (err) {
    authEnabled = false;
    currentAuthUsername = '';
    setProfileMenu(false);
    menu.classList.add('hidden');
  }
}

async function loadAppVersion() {
  const el = document.getElementById('appVersion');
  if (!el) return;

  try {
    const res = await fetch('/api/version', { credentials: 'same-origin' });
    if (!res.ok) throw new Error(`version ${res.status}`);
    const data = await res.json();
    const version = data.version || 'dev';
    const commit = data.commit ? ` ${String(data.commit).slice(0, 7)}` : '';
    el.textContent = `${version}${commit}`;
    if (data.build_date) {
      el.title = `Built ${data.build_date}`;
    }
  } catch (err) {
    el.textContent = 'dev';
  }
}

function profileInitials(username) {
  const name = (username || 'User').trim();
  if (!name) return 'U';

  return name
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part.charAt(0))
    .join('')
    .toUpperCase();
}

function setProfileMenu(open) {
  const dropdown = document.getElementById('profileDropdown');
  const trigger = document.getElementById('profileButton');
  if (!dropdown || !trigger) return;

  dropdown.classList.toggle('hidden', !open);
  trigger.setAttribute('aria-expanded', open ? 'true' : 'false');
}

function toggleProfileMenu() {
  const dropdown = document.getElementById('profileDropdown');
  if (!dropdown) return;

  setProfileMenu(dropdown.classList.contains('hidden'));
}

function fmtBytes(n) {
  if (!n && n !== 0) return '-';
  const mb = n / (1024 * 1024);
  if (mb < 1024) return mb.toFixed(1) + ' MB';
  return (mb / 1024).toFixed(2) + ' GB';
}

function setBoundText(name, value) {
  document.querySelectorAll(`[data-bind="${name}"]`).forEach((el) => {
    el.textContent = value;
  });
}

function setInnerHTMLIfChanged(el, html) {
  if (!el || el.innerHTML === html) return;
  el.innerHTML = html;
}

function setJobsNotice(message, tone = 'info') {
  const el = document.getElementById('jobsNotice');
  if (!message) {
    el.className = 'mt-4 hidden rounded-lg border px-4 py-3 text-sm';
    el.textContent = '';
    return;
  }

  const toneClass = tone === 'error'
    ? 'border-rose-200 bg-rose-50 text-rose-700'
    : 'border-slate-200 bg-white/70 text-slate-600';

  el.className = `mt-4 rounded-lg border px-4 py-3 text-sm ${toneClass}`;
  el.textContent = message;
}

function setLogsNotice(message, tone = 'info') {
  const el = document.getElementById('logsNotice');
  if (!el) return;
  if (!message) {
    el.className = 'hidden rounded-xl border px-4 py-3 text-sm';
    el.textContent = '';
    return;
  }

  const toneClass = tone === 'error'
    ? 'border-rose-200 bg-rose-50 text-rose-700'
    : 'border-slate-200 bg-white/70 text-slate-600';

  el.className = `rounded-xl border px-4 py-3 text-sm ${toneClass}`;
  el.textContent = message;
}

function setSubmitStatus(message, tone = 'muted') {
  const el = document.getElementById('submitStatus');
  const toneClass = tone === 'error'
    ? 'text-rose-600'
    : tone === 'success'
      ? 'text-blue-700'
      : 'text-slate-500';
  el.className = `text-sm ${toneClass}`;
  el.textContent = message;
}

function markSynced(prefix = 'Synced') {
  setBoundText('sync-stamp', `${prefix} ${syncFormatter.format(new Date())}`);
}

function setHealthState(connected) {
  const dot = document.getElementById('healthDot');
  const badge = document.getElementById('healthBadge');

  if (connected) {
    dot.className = 'brand-dot h-2.5 w-2.5 rounded-full';
    badge.className = 'brand-badge inline-flex items-center rounded-md px-2.5 py-1 text-xs font-medium';
    badge.textContent = 'Connected';
    return;
  }

  dot.className = 'h-2.5 w-2.5 rounded-full bg-amber-500 shadow-[0_0_0_4px_rgba(245,158,11,0.12)]';
  badge.className = 'inline-flex items-center rounded-md border border-amber-200 bg-white px-2.5 py-1 text-xs font-medium text-amber-900';
  badge.textContent = 'Degraded';
}

function syncBodyLock() {
  const hasOpenModal = MODAL_IDS.some((id) => !document.getElementById(id).classList.contains('hidden'));
  document.body.classList.toggle('modal-open', hasOpenModal);
}

function isAnyModalOpen() {
  return MODAL_IDS.some((id) => !document.getElementById(id).classList.contains('hidden'));
}

function setModalState(id, open) {
  document.getElementById(id).classList.toggle('hidden', !open);
  syncBodyLock();
}

function updateTabUrl() {
  const url = new URL(window.location.href);
  if (currentTab === 'job') {
    url.searchParams.set('tab', 'job');
    if (currentGraphJobId) {
      url.searchParams.set('id', currentGraphJobId);
    } else {
      url.searchParams.delete('id');
    }
    url.searchParams.delete('log');
    url.searchParams.delete('lines');
    url.searchParams.delete('follow');
  } else if (currentTab === 'logs') {
    url.searchParams.set('tab', 'logs');
    url.searchParams.delete('id');
    if (currentLogFile) {
      url.searchParams.set('log', currentLogFile);
    } else {
      url.searchParams.delete('log');
    }
    if (followLatestLog) {
      url.searchParams.set('follow', 'latest');
    } else {
      url.searchParams.delete('follow');
    }
    const lines = selectedLogLineLimit();
    if (lines !== 500) {
      url.searchParams.set('lines', String(lines));
    } else {
      url.searchParams.delete('lines');
    }
  } else {
    url.searchParams.set('tab', currentTab === 'iceberg' ? 'iceberg' : 'doris');
    url.searchParams.delete('id');
    url.searchParams.delete('log');
    url.searchParams.delete('lines');
    url.searchParams.delete('follow');
  }
  history.replaceState({}, '', url);
}

function switchTab(tab, options = {}) {
  currentTab = tab === 'job'
    ? 'job'
    : tab === 'logs'
      ? 'logs'
      : tab === 'iceberg'
        ? 'iceberg'
        : 'doris';

  const logsActive = currentTab === 'logs';
  const jobActive = currentTab === 'job';
  const dorisActive = currentTab === 'doris';
  const icebergActive = currentTab === 'iceberg';

  const toggleHidden = (id, hidden) => {
    const el = document.getElementById(id);
    if (el) el.classList.toggle('hidden', hidden);
  };
  const setTabClass = (id, active) => {
    const el = document.getElementById(id);
    if (el) el.className = active ? 'tab-button tab-button-active' : 'tab-button';
  };

  toggleHidden('jobsView', logsActive || jobActive);
  toggleHidden('jobDetailView', !jobActive);
  toggleHidden('logsView', !logsActive);
  toggleHidden('dorisJobsView', !dorisActive);
  toggleHidden('icebergJobsView', !icebergActive);

  setTabClass('tabDorisJobs', dorisActive);
  setTabClass('tabIcebergJobs', icebergActive);
  setTabClass('tabLogs', logsActive);

  if (options.updateUrl !== false) {
    if (currentTab !== 'job') {
      currentGraphJobId = null;
    }
    updateTabUrl();
  }

  if (logsActive && options.load !== false) {
    loadLogs();
  }
}

function statusPill(status) {
  const value = (status ?? '-').toString();
  const base = 'inline-flex items-center gap-2 rounded-md border px-2.5 py-1 text-xs font-semibold uppercase tracking-normal';
  const map = {
    RUNNING: ['border-emerald-200 bg-emerald-50 text-emerald-800', 'bg-emerald-500'],
    PAUSING: ['border-amber-200 bg-amber-50 text-amber-800', 'bg-amber-500'],
    PAUSED: ['border-violet-200 bg-violet-50 text-violet-800', 'bg-violet-500'],
    QUEUED: ['border-sky-200 bg-sky-50 text-sky-800', 'bg-sky-500'],
    PENDING: ['border-amber-200 bg-amber-50 text-amber-800', 'bg-amber-500'],
    FAILED: ['border-rose-200 bg-rose-50 text-rose-800', 'bg-rose-500'],
    STOPPED: ['border-slate-200 bg-slate-100 text-slate-700', 'bg-slate-400'],
    DONE: ['border-blue-200 bg-blue-50 text-blue-800', 'bg-blue-500'],
  };
  const [pillClass, dotClass] = map[value] || ['border-slate-200 bg-slate-100 text-slate-700', 'bg-slate-400'];
  return `<span class="${base} ${pillClass}"><span class="h-2 w-2 rounded-full ${dotClass}"></span>${escapeHtml(value)}</span>`;
}

function actionButtons(job) {
  const errorCount = Number(job.error_count || 0);
  const isActive = ACTIVE.has(job.status);
  const showErr = !isActive && (job.status === 'FAILED' || errorCount > 0);
  const canPause = job.status === 'RUNNING';
  const canCancel = isActive;
  const canResubmit = job.status === 'FAILED' || job.status === 'STOPPED' || job.status === 'PAUSED';
  const idAttr = escapeHtml(job.id ?? '');

  const btnBase = 'whitespace-nowrap rounded-md border px-3 py-2 text-xs font-semibold transition';
  const btnCancel = `${btnBase} border-amber-200 text-amber-800 hover:bg-amber-50`;
  const btnPause = `${btnBase} border-violet-200 text-violet-800 hover:bg-violet-50`;
  const btnDelete = `${btnBase} border-rose-200 text-rose-700 hover:bg-rose-50`;
  const btnError = `${btnBase} border-rose-200 bg-rose-50 text-rose-700 hover:bg-rose-100`;
  const btnDetail = `${btnBase} border-blue-300 text-blue-800 hover:bg-blue-50`;
  const btnLogs = `${btnBase} border-slate-300 text-slate-700 hover:bg-slate-50`;
  const btnResubmit = `${btnBase} border-blue-200 bg-blue-50 text-blue-800 hover:bg-blue-100`;

  return `
    <div class="flex min-w-max flex-nowrap items-center justify-end gap-2">
      <button type="button" class="${btnDetail}" data-job-action="details" data-job-id="${idAttr}">Details</button>
      <button type="button" class="${btnLogs}" data-job-action="logs" data-job-id="${idAttr}">Logs</button>
      ${canPause ? `<button type="button" class="${btnPause}" data-job-action="pause" data-job-id="${idAttr}">Pause</button>` : ''}
      ${canCancel ? `<button type="button" class="${btnCancel}" data-job-action="cancel" data-job-id="${idAttr}">Cancel</button>` : ''}
      ${canResubmit ? `<button type="button" class="${btnResubmit}" data-job-action="resubmit" data-job-id="${idAttr}">Resubmit</button>` : ''}
      <button type="button" class="${btnDelete}" data-job-action="delete" data-job-id="${idAttr}">Delete</button>
      ${showErr ? `<button type="button" class="${btnError}" data-job-action="errors" data-job-id="${idAttr}">Errors${errorCount > 0 ? ` (${errorCount})` : ''}</button>` : ''}
    </div>
  `;
}

function tableDateCell(value) {
  const raw = (value ?? '-').toString().trim();
  if (!raw || raw === '-') {
    return `<span class="mono text-xs text-slate-500">-</span>`;
  }

  const normalized = raw.replace('T', ' ');
  const parts = normalized.split(/\s+/);
  const datePart = parts[0] || normalized;
  const timePart = parts.slice(1).join(' ');

  return `
    <div class="flex flex-col gap-0.5">
      <span class="mono text-xs font-medium text-slate-700">${escapeHtml(datePart)}</span>
      ${timePart ? `<span class="mono text-[11px] text-slate-500">${escapeHtml(timePart)}</span>` : ''}
    </div>
  `;
}

function formatDateTime(value) {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString();
}

function normalizeCommaSpacing(value) {
  const raw = (value ?? '-').toString().trim();
  if (!raw) return '-';
  return raw.replace(/,([^\s])/g, ', $1');
}

function jobDisplay(value) {
  const normalized = normalizeCommaSpacing(value);
  const match = normalized.match(/^(.*?)\s*\((.+)\)$/);

  if (match) {
    const title = match[1].trim() || normalized;
    const detail = match[2].trim();
    return { title, detail };
  }

  return { title: normalized, detail: '' };
}

function idChip(value) {
  const safeValue = escapeHtml(value ?? '-');
  return `
    <div class="id-chip" title="${safeValue}">
      <span class="mono text-xs font-semibold text-slate-700">${safeValue}</span>
    </div>
  `;
}

function progressPill(progress) {
  const phase = String(progress?.phase || '').trim().toLowerCase();
  const labelMap = {
    preflight: 'PRE-FLIGHT',
    snapshot: 'SNAPSHOT',
    snapshot_complete: 'SNAPSHOT DONE',
    streaming: 'CDC',
    done: 'DONE',
    stopped: 'STOPPED',
    failed: 'FAILED',
  };
  const classMap = {
    preflight: 'border-amber-200 bg-amber-50 text-amber-800',
    snapshot: 'border-blue-200 bg-blue-50 text-blue-800',
    snapshot_complete: 'border-blue-200 bg-blue-50 text-blue-800',
    streaming: 'border-emerald-200 bg-emerald-50 text-emerald-800',
    done: 'border-blue-200 bg-blue-50 text-blue-800',
    stopped: 'border-slate-200 bg-slate-100 text-slate-700',
    failed: 'border-rose-200 bg-rose-50 text-rose-800',
  };
  const label = labelMap[phase] || 'PROGRESS';
  const cls = classMap[phase] || 'border-slate-200 bg-slate-100 text-slate-700';
  return `<span class="inline-flex items-center rounded-md border px-2.5 py-1 text-xs font-semibold uppercase tracking-normal ${cls}">${escapeHtml(label)}</span>`;
}

function progressMetaParts(progress) {
  const currentTableIndex = Number(progress?.current_table_index);
  const totalTables = Number(progress?.total_tables);
  const currentTableRows = Number(progress?.current_table_rows);
  const metaParts = [];

  if (Number.isFinite(currentTableIndex) && currentTableIndex > 0 && Number.isFinite(totalTables) && totalTables > 0) {
    metaParts.push(`Table ${fmtWholeNumber(currentTableIndex)}/${fmtWholeNumber(totalTables)}`);
  }
  if (Number.isFinite(currentTableRows) && currentTableRows >= 0) {
    metaParts.push(`${fmtWholeNumber(currentTableRows)} rows`);
  }

  return metaParts;
}

function renderJobProgress(job) {
  const progress = job?.progress;
  if (!progress) {
    if (!ACTIVE.has(job?.status)) return '';
    return `
      <div class="mt-3 text-xs text-slate-500">
        Waiting for runtime progress...
      </div>
    `;
  }

  const summary = String(progress.summary || '').trim() || 'Waiting for update';
  const metaParts = progressMetaParts(progress);

  return `
    <div class="mt-3 flex flex-wrap items-center gap-2 text-xs">
      ${progressPill(progress)}
      <div class="font-semibold text-slate-700">${escapeHtml(summary)}</div>
      ${metaParts.length > 0 ? `<div class="mono text-[11px] text-slate-500">${escapeHtml(metaParts.join(' | '))}</div>` : ''}
    </div>
  `;
}

function renderGraphProgress(graph) {
  const progress = graph?.progress;
  if (!progress) return '';

  const summary = String(progress.summary || '').trim() || 'Waiting for update';
  const detail = String(progress.detail || '').trim();
  const sinkSummary = String(progress.sink_summary || '').trim();
  const sinkDetail = String(progress.sink_detail || '').trim();
  const sinkRows = Number(progress.sink_rows);
  const checkpointPending = !!progress.checkpoint_pending;
  const checkpointReason = String(progress.checkpoint_reason || '').trim();
  const checkpointPosition = String(progress.checkpoint_position || '').trim();
  const checkpointPendingTables = String(progress.checkpoint_pending_tables || '').trim();
  const currentTable = String(progress.current_table || '').trim();
  const currentTableIndex = Number(progress.current_table_index);
  const completedTables = Number(progress.completed_tables);
  const totalTables = Number(progress.total_tables);
  const currentTableRows = Number(progress.current_table_rows);

  const currentPosition = Number.isFinite(currentTableIndex) && currentTableIndex > 0 && Number.isFinite(totalTables) && totalTables > 0
    ? `${fmtWholeNumber(currentTableIndex)} / ${fmtWholeNumber(totalTables)}`
    : '-';
  const tableProgress = Number.isFinite(totalTables) && totalTables > 0
    ? `${fmtWholeNumber(Math.max(0, completedTables))}/${fmtWholeNumber(totalTables)}`
    : '-';
  const rowsValue = Number.isFinite(currentTableRows) && currentTableRows >= 0
    ? fmtWholeNumber(currentTableRows)
    : '-';
  const detailLower = detail.toLowerCase();
  const currentTableLower = currentTable.toLowerCase();
  const showDetail = !!detail && (
    !currentTable ||
    !detailLower.includes(currentTableLower) ||
    detailLower.includes('waiting') ||
    detailLower.includes('flush') ||
    detailLower.includes('slower') ||
    detailLower.includes('blocked') ||
    detailLower.includes('failed') ||
    detailLower.includes('error')
  );

  return `
    <div class="rounded-[20px] border border-slate-200 bg-white p-5">
      <div class="flex flex-wrap items-center gap-2">
        ${progressPill(progress)}
        ${statusPill(graph?.status || '-')}
      </div>
      <div class="mt-4 text-lg font-semibold tracking-tight text-slate-900">${escapeHtml(summary)}</div>
      ${showDetail ? `<div class="mt-1 text-sm leading-6 text-slate-500 break-words">${escapeHtml(detail)}</div>` : ''}
      ${sinkSummary ? `
        <div class="mt-3 rounded-[14px] border border-indigo-100 bg-indigo-50 px-4 py-3 text-sm leading-6 text-indigo-900">
          <span class="font-semibold">Sink:</span>
          <span class="ml-1">${escapeHtml(sinkSummary)}</span>
          ${Number.isFinite(sinkRows) && sinkRows > 0 ? `<span class="mono ml-2 text-xs text-indigo-700">${escapeHtml(fmtWholeNumber(sinkRows))} rows</span>` : ''}
          ${sinkDetail ? `<div class="mono mt-1 text-xs text-indigo-700 break-words">${escapeHtml(sinkDetail)}</div>` : ''}
        </div>
      ` : ''}
      ${checkpointPending ? `
        <div class="mt-3 rounded-[14px] border border-amber-200 bg-amber-50 px-4 py-3 text-sm leading-6 text-amber-900">
          <div class="font-semibold">Checkpoint pending</div>
          <div class="mt-1">Reason: <span class="mono">${escapeHtml(checkpointReason || 'pending_events')}</span>${checkpointPosition ? ` | Position: <span class="mono">${escapeHtml(checkpointPosition)}</span>` : ''}</div>
          ${checkpointPendingTables ? `<div class="mono mt-1 text-xs text-amber-800 break-words">${escapeHtml(checkpointPendingTables)}</div>` : ''}
        </div>
      ` : ''}
      ${renderGraphAlert(graph)}
      <div class="mt-5 grid gap-3 sm:grid-cols-3">
        <div class="rounded-[18px] border border-slate-200 bg-slate-50 px-4 py-3">
          <div class="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Current Table</div>
          <div class="mono mt-2 text-xs font-semibold text-slate-800 break-all">${escapeHtml(currentTable || '-')}</div>
        </div>
        <div class="rounded-[18px] border border-slate-200 bg-slate-50 px-4 py-3">
          <div class="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Progress</div>
          <div class="mono mt-2 text-sm font-semibold text-slate-900">${escapeHtml(currentPosition)}</div>
          <div class="mt-2 text-xs text-slate-500">Done <span class="mono font-semibold text-slate-700">${escapeHtml(tableProgress)}</span></div>
        </div>
        <div class="rounded-[18px] border border-slate-200 bg-slate-50 px-4 py-3">
          <div class="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Rows on Current Table</div>
          <div class="mono mt-2 text-sm font-semibold text-slate-900">${escapeHtml(rowsValue)}</div>
        </div>
      </div>
    </div>
  `;
}

function rowTemplate(job) {
  const id = job.id ?? '-';
  const display = jobDisplay(job.name ?? '-');
  const detail = 'Open details for pipeline, checkpoint, and lifecycle actions.';

  return `
    <tr class="align-top transition hover:bg-white/70">
      <td data-label="ID" class="px-6 py-4">
        ${idChip(id)}
      </td>
      <td data-label="Job" class="px-6 py-4">
        <div class="max-w-[34rem] md:min-w-[22rem]">
          <div class="font-semibold leading-6 text-slate-900 break-words">${escapeHtml(display.title)}</div>
          <div class="mt-1 text-xs leading-5 text-slate-500 break-words">
            ${escapeHtml(display.detail || detail)}
          </div>
          ${renderJobProgress(job)}
        </div>
      </td>
      <td data-label="Status" class="px-6 py-4">${statusPill(job.status)}</td>
      <td data-label="Created" class="px-6 py-4 text-slate-600">${tableDateCell(job.created)}</td>
      <td data-label="Updated" class="px-6 py-4 text-slate-600">${tableDateCell(job.updated)}</td>
      <td data-label="Actions" class="cell-actions px-6 py-4 md:min-w-[24rem]">${actionButtons(job)}</td>
    </tr>
  `;
}

async function loadMetrics() {
  try {
    const res = await apiFetch('/api/metrics');
    if (!res.ok) throw new Error(`metrics ${res.status}`);

    const m = await res.json();
    const cpu = (m.cpu_percent ?? 0).toFixed(1) + '%';
    const mem = fmtBytes(m.rss_bytes);

    document.getElementById('m_pid').textContent = m.pid ?? '-';
    document.getElementById('m_cpu').textContent = cpu;
    document.getElementById('m_mem').textContent = mem;
    setHealthState(true);
  } catch (err) {
    document.getElementById('m_cpu').textContent = '-';
    document.getElementById('m_mem').textContent = '-';
    setHealthState(false);
  }
}

function partitionJobsByDomain(jobs) {
  const standardActive = [];
  const standardArchive = [];

  for (const job of jobs) {
    const target = ACTIVE.has(job.status) ? standardActive : standardArchive;
    target.push(job);
  }

  return {
    standardActive,
    standardArchive,
  };
}

function normalizedSinkType(job) {
  return String(job?.sink_type || '').trim().toLowerCase();
}

function isDorisJob(job) {
  return normalizedSinkType(job) === 'doris';
}

function isIcebergJob(job) {
  return normalizedSinkType(job).startsWith('iceberg');
}

function compareJobsForDisplay(a, b) {
  const updatedA = String(a?.updated || '');
  const updatedB = String(b?.updated || '');
  if (updatedA !== updatedB) return updatedB.localeCompare(updatedA);

  const createdA = String(a?.created || '');
  const createdB = String(b?.created || '');
  if (createdA !== createdB) return createdB.localeCompare(createdA);

  return String(a?.id || '').localeCompare(String(b?.id || ''));
}

function updateArchiveBulkButton(scope, jobs) {
  const button = document.querySelector(`[data-bulk-delete-archive="${scope}"]`);
  if (!button) return;

  const count = jobs.length;
  button.disabled = count === 0 || bulkDeleteInFlight;
  button.textContent = count > 0 ? `Delete archive (${count})` : 'Delete archive';
}

async function loadJobs() {
  try {
    const res = await apiFetch('/api/jobs');
    if (!res.ok) throw new Error(`jobs ${res.status}`);

    const jobs = (await res.json()).slice().sort(compareJobsForDisplay);
    const {
      standardActive,
      standardArchive,
    } = partitionJobsByDomain(jobs);
    const dorisActive = standardActive.filter(isDorisJob);
    const icebergActive = standardActive.filter(isIcebergJob);
    const dorisArchive = standardArchive.filter(isDorisJob);
    const icebergArchive = standardArchive.filter(isIcebergJob);
    const otherStandardJobs = jobs.filter((job) => !isDorisJob(job) && !isIcebergJob(job));

    latestDorisArchiveJobs = dorisArchive.slice();
    latestIcebergArchiveJobs = icebergArchive.slice();

    setInnerHTMLIfChanged(document.getElementById('dorisActiveBody'), dorisActive.map(rowTemplate).join(''));
    setInnerHTMLIfChanged(document.getElementById('icebergActiveBody'), icebergActive.map(rowTemplate).join(''));
    setInnerHTMLIfChanged(document.getElementById('dorisArchiveBody'), dorisArchive.map(rowTemplate).join(''));
    setInnerHTMLIfChanged(document.getElementById('icebergArchiveBody'), icebergArchive.map(rowTemplate).join(''));
    document.getElementById('dorisActiveEmpty').classList.toggle('hidden', dorisActive.length !== 0);
    document.getElementById('icebergActiveEmpty').classList.toggle('hidden', icebergActive.length !== 0);
    document.getElementById('dorisArchiveEmpty').classList.toggle('hidden', dorisArchive.length !== 0);
    document.getElementById('icebergArchiveEmpty').classList.toggle('hidden', icebergArchive.length !== 0);
    updateArchiveBulkButton('doris', dorisArchive);
    updateArchiveBulkButton('iceberg', icebergArchive);

    setBoundText('active-doris-count', String(dorisActive.length));
    setBoundText('active-iceberg-count', String(icebergActive.length));
    setBoundText('archive-count', String(standardArchive.length));
    setBoundText('total-count', String(standardActive.length + standardArchive.length));
    const otherStandardJobsNotice = document.getElementById('otherStandardJobsNotice');
    if (otherStandardJobs.length > 0) {
      otherStandardJobsNotice.textContent = `${otherStandardJobs.length} standard job(s) use a sink other than doris or iceberg and are not shown in these tabs.`;
      otherStandardJobsNotice.classList.remove('hidden');
    } else {
      otherStandardJobsNotice.textContent = '';
      otherStandardJobsNotice.classList.add('hidden');
    }
    setJobsNotice('');
    markSynced('Synced');
  } catch (err) {
    const otherStandardJobsNotice = document.getElementById('otherStandardJobsNotice');
    otherStandardJobsNotice.textContent = '';
    otherStandardJobsNotice.classList.add('hidden');
    setJobsNotice('Failed to load jobs. Check the API status and try again.', 'error');
  }
}

function selectedLogLineLimit() {
  const select = document.getElementById('logLineSelect');
  const value = Number(select?.value || currentLogLineLimit || 500);
  if (!Number.isFinite(value) || value <= 0) return 500;
  return Math.min(5000, Math.trunc(value));
}

function setInitialLogLineLimit() {
  const select = document.getElementById('logLineSelect');
  if (!select) return;
  const requested = String(currentLogLineLimit || 500);
  const hasOption = Array.from(select.options).some((option) => option.value === requested);
  if (hasOption) {
    select.value = requested;
  }
}

function formatLogTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return syncFormatter.format(date);
}

function logFileRowTemplate(file) {
  const name = String(file?.name || '');
  const active = name === currentLogFile;
  return `
    <tr class="cursor-pointer ${active ? 'log-file-row-active' : ''}" data-log-file="${escapeHtml(name)}">
      <td class="px-4 py-3">
        <div class="mono max-w-[13rem] truncate text-xs font-semibold text-slate-800" title="${escapeHtml(name)}">${escapeHtml(name)}</div>
        <div class="mt-1 text-[11px] text-slate-500">${escapeHtml(formatLogTime(file?.mod_time))}</div>
      </td>
      <td class="px-4 py-3 text-right text-xs font-semibold text-slate-600">${escapeHtml(fmtBytes(file?.size || 0))}</td>
    </tr>
  `;
}

async function loadLogs(options = {}) {
  try {
    setInitialLogLineLimit();
    if (options.preferLatest === true) {
      setFollowLatestLog(true);
    }
    const res = await apiFetch('/api/logs');
    if (!res.ok) throw new Error(await operationErrorMessage(res, 'Load log files'));

    const files = await res.json();
    const logFiles = Array.isArray(files) ? files : [];
    const select = document.getElementById('logFileSelect');
    select.innerHTML = logFiles.map((file) => {
      const name = String(file?.name || '');
      return `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`;
    }).join('');

    if (logFiles.length === 0) {
      currentLogFile = '';
      latestLogLines = [];
      latestLogTailLoaded = true;
      latestLogTailError = '';
      renderLogOutput();
      document.getElementById('logsFileBody').innerHTML = '';
      document.getElementById('logsFileEmpty').classList.remove('hidden');
      setLogMeta({});
      setLogsNotice('No Rivus log files are available yet.');
      updateTabUrl();
      return;
    }

    const names = new Set(logFiles.map((file) => String(file?.name || '')));
    if (followLatestLog || !currentLogFile || !names.has(currentLogFile)) {
      currentLogFile = String(logFiles[0]?.name || '');
    }
    select.value = currentLogFile;
    document.getElementById('logsFileBody').innerHTML = logFiles.map(logFileRowTemplate).join('');
    document.getElementById('logsFileEmpty').classList.toggle('hidden', logFiles.length !== 0);

    setLogsNotice('');
    updateTabUrl();
    if (currentLogFile && options.skipTail !== true) {
      await loadLogTail(options);
    } else if (!currentLogFile) {
      latestLogLines = [];
      latestLogTailLoaded = true;
      latestLogTailError = '';
      renderLogOutput();
    }
  } catch (err) {
    const message = err?.message || 'Failed to load log files. Check RIVUS_LOG_DIR and API status.';
    setLogsNotice(message, 'error');
  }
}

function setLogMeta(payload) {
  const files = Array.isArray(payload.files) ? payload.files : [];
  const fileLabel = payload.file || (files.length ? `${files.length} matching files` : '-');
  document.getElementById('logCurrentFile').textContent = fileLabel;
  document.getElementById('logCurrentSize').textContent = payload.total_size || payload.total_size === 0 ? fmtBytes(payload.total_size) : '-';
  document.getElementById('logCurrentUpdated').textContent = payload.mod_time ? formatLogTime(payload.mod_time) : '-';
  document.getElementById('logConsoleTitle').textContent = fileLabel;
  document.getElementById('logTruncatedBadge').classList.toggle('hidden', !payload.truncated);
}

async function loadLogTail(options = {}) {
  if (!currentLogFile) {
    latestLogLines = [];
    latestLogTailLoaded = true;
    latestLogTailError = '';
    renderLogOutput();
    return;
  }

  try {
    latestLogTailLoaded = false;
    latestLogTailError = '';
    const url = new URL('/api/logs/tail', window.location.origin);
    const filter = String(document.getElementById('logFilter')?.value || '').trim();
    if (!(followLatestLog && filter)) {
      url.searchParams.set('file', currentLogFile);
    }
    url.searchParams.set('lines', String(selectedLogLineLimit()));
    if (filter) {
      url.searchParams.set('filter', filter);
    }
    const res = await apiFetch(url.pathname + url.search);
    if (!res.ok) throw new Error(await operationErrorMessage(res, 'Load log tail'));

    const payload = await res.json();
    latestLogLines = Array.isArray(payload.lines) ? payload.lines : [];
    latestLogTailLoaded = true;
    setLogMeta(payload);
    renderLogOutput();
    if (!options.silent) setLogsNotice('');
  } catch (err) {
    latestLogLines = [];
    latestLogTailLoaded = true;
    latestLogTailError = err?.message || 'Failed to load log tail for the selected file.';
    setLogMeta({ file: currentLogFile });
    renderLogOutput();
    if (!options.silent) {
      setLogsNotice(latestLogTailError, 'error');
    }
  }
}

function renderLogOutput() {
  const output = document.getElementById('logOutput');
  if (!output) return;

  const filter = String(document.getElementById('logFilter')?.value || '').trim().toLowerCase();
  const lines = filter
    ? latestLogLines.filter((line) => String(line).toLowerCase().includes(filter))
    : latestLogLines;

  let emptyMessage = 'No log file selected.';
  if (latestLogTailError) {
    emptyMessage = latestLogTailError;
  } else if (currentLogFile && !latestLogTailLoaded) {
    emptyMessage = 'Loading log tail...';
  } else if (currentLogFile && latestLogLines.length === 0) {
    emptyMessage = 'The selected log file is empty.';
  } else if (currentLogFile && filter) {
    emptyMessage = `No log lines match filter "${filter}". Clear or widen the filter to see the file contents.`;
  } else if (currentLogFile) {
    emptyMessage = 'No log lines are available for the selected file.';
  }

  output.textContent = lines.length > 0 ? lines.join('\n') : emptyMessage;
  document.getElementById('logVisibleLines').textContent = String(lines.length);
  document.getElementById('logTotalLines').textContent = String(latestLogLines.length);

  if (document.getElementById('logAutoRefresh')?.checked) {
    output.scrollTop = output.scrollHeight;
  }
}

function selectLogFile(name) {
  setFollowLatestLog(false);
  currentLogFile = String(name || '');
  updateTabUrl();
  loadLogs();
}

function setFollowLatestLog(enabled) {
  followLatestLog = !!enabled;
  const checkbox = document.getElementById('logAutoRefresh');
  if (checkbox) checkbox.checked = followLatestLog;
}

function changeLogFollowLatest(enabled) {
  setFollowLatestLog(enabled);
  if (followLatestLog) {
    currentLogFile = '';
    latestLogLines = [];
    loadLogs({ preferLatest: true });
    return;
  }
  updateTabUrl();
}

function changeLogLineLimit(value) {
  const select = document.getElementById('logLineSelect');
  if (select) select.value = String(value || 500);
  currentLogLineLimit = selectedLogLineLimit();
  updateTabUrl();
  loadLogTail();
}

function downloadSelectedLog() {
  if (!currentLogFile) return;
  window.location.href = '/api/logs/download?file=' + encodeURIComponent(currentLogFile);
}

function showLogsForJob(jobId) {
  const filter = document.getElementById('logFilter');
  if (filter) filter.value = jobId || '';
  setFollowLatestLog(true);
  currentLogFile = '';
  latestLogLines = [];
  latestLogTailLoaded = false;
  latestLogTailError = '';
  switchTab('logs');
  renderLogOutput();
}

async function operationErrorMessage(res, action) {
  const fallback = `${action} failed: ${res.status}`;
  const contentType = String(res.headers.get('content-type') || '').toLowerCase();
  if (contentType.includes('application/json')) {
    const data = await res.json().catch(() => ({}));
    return data.error || fallback;
  }

  const text = String(await res.text().catch(() => '')).trim();
  if (res.status === 502 || res.status === 503 || res.status === 504 || /<html[\s>]/i.test(text)) {
    return `${action} could not reach Rivus through the gateway (${res.status}). Refresh and retry after the service is available.`;
  }
  return text ? `${fallback}\n${text.slice(0, 300)}` : fallback;
}

async function cancelJob(id) {
  if (!confirm('Cancel job ' + id + '?')) return;

  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id) + '/cancel', { method: 'POST' });
  if (!res.ok) {
    alert(await operationErrorMessage(res, 'Cancel'));
    return;
  }

  await refreshDashboard();
}

async function pauseJob(id) {
  if (!confirm('Pause job ' + id + ' after draining and committing pending events?')) return;

  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id) + '/pause', { method: 'POST' });
  if (!res.ok) {
    alert(await operationErrorMessage(res, 'Pause'));
    return;
  }

  await refreshDashboard();
}

async function deleteJob(id) {
  if (!confirm('Delete job ' + id + '? (This removes it from the list)')) return;

  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id), { method: 'DELETE' });
  if (!res.ok) {
    alert(await operationErrorMessage(res, 'Delete'));
  }

  await refreshDashboard();
}

async function deleteJobWithoutPrompt(id) {
  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id), { method: 'DELETE' });
  if (res.status === 404) return null;
  if (res.ok) return null;
  return await operationErrorMessage(res, 'Delete');
}

async function deleteJobsWithLimit(ids, limit = 6) {
  const failures = [];
  let next = 0;
  const workerCount = Math.min(limit, ids.length);
  const workers = Array.from({ length: workerCount }, async () => {
    while (next < ids.length) {
      const id = ids[next];
      next += 1;
      const error = await deleteJobWithoutPrompt(id);
      if (error) failures.push({ id, error });
    }
  });
  await Promise.all(workers);
  return failures;
}

async function bulkDeleteArchive(scope) {
  const jobs = scope === 'iceberg' ? latestIcebergArchiveJobs : latestDorisArchiveJobs;
  const ids = jobs.map((job) => job?.id).filter(Boolean);
  const label = scope === 'iceberg' ? 'Iceberg' : 'Doris';

  if (ids.length === 0) return;
  if (!confirm(`Delete ${ids.length} archived ${label} job(s)? This removes them from the list.`)) return;

  bulkDeleteInFlight = true;
  updateArchiveBulkButton('doris', latestDorisArchiveJobs);
  updateArchiveBulkButton('iceberg', latestIcebergArchiveJobs);
  setJobsNotice(`Deleting ${ids.length} archived ${label} job(s)...`);

  try {
    const failures = await deleteJobsWithLimit(ids);
    await refreshDashboard();
    if (failures.length > 0) {
      const sample = failures.slice(0, 3).map((item) => item.id).join(', ');
      setJobsNotice(`Deleted ${ids.length - failures.length} job(s). Failed to delete ${failures.length}: ${sample}.`, 'error');
      return;
    }
    setJobsNotice(`Deleted ${ids.length} archived ${label} job(s).`);
  } finally {
    bulkDeleteInFlight = false;
    updateArchiveBulkButton('doris', latestDorisArchiveJobs);
    updateArchiveBulkButton('iceberg', latestIcebergArchiveJobs);
  }
}

async function resubmitJob(id) {
  if (!confirm('Resubmit job ' + id + ' in resume mode from the latest saved checkpoint?')) return;

  const res = await apiFetch('/api/jobs/' + encodeURIComponent(id) + '/resubmit', { method: 'POST' });
  if (!res.ok) {
    alert(await operationErrorMessage(res, 'Resubmit'));
    return;
  }

  await refreshDashboard();
}

async function refreshDashboard(options = {}) {
  if (options.auto && isAnyModalOpen()) return;
  if (options.auto && bulkDeleteInFlight) return;
  if (refreshInFlight) return;
  refreshInFlight = true;

  document.querySelectorAll('[data-refresh-button]').forEach((button) => {
    button.disabled = true;
    button.classList.add('cursor-wait', 'opacity-80');
  });

  try {
    const tasks = [loadJobs(), loadMetrics()];
        if (currentTab === 'logs') {
          if (options.auto) {
            if (document.getElementById('logAutoRefresh')?.checked) {
              tasks.push(followLatestLog ? loadLogs({ silent: true }) : (currentLogFile ? loadLogTail({ silent: true }) : loadLogs({ silent: true })));
            }
          } else {
            tasks.push(loadLogs());
          }
        }
        if (currentTab === 'job' && currentGraphJobId) {
          tasks.push(refreshGraph({ silent: options.auto }));
        }
        await Promise.allSettled(tasks);
  } finally {
    document.querySelectorAll('[data-refresh-button]').forEach((button) => {
      button.disabled = false;
      button.classList.remove('cursor-wait', 'opacity-80');
    });
    refreshInFlight = false;
  }
}

async function logout() {
  try {
    await fetch('/auth/logout', { method: 'POST', credentials: 'same-origin' });
  } finally {
    redirectToLogin();
  }
}

function openSubmitModal() {
  setSubmitStatus('Ready for a new YAML definition.');
  setModalState('submitModal', true);
  setTimeout(() => document.getElementById('yamlInput').focus(), 0);
}

function closeSubmitModal() {
  setModalState('submitModal', false);
}

function clearYaml() {
  document.getElementById('yamlInput').value = '';
  document.getElementById('yamlFileInput').value = '';
  setSubmitStatus('Editor cleared.');
}

async function loadYamlFile(event) {
  const file = event.target.files && event.target.files[0];
  if (!file) return;
  try {
    const text = await file.text();
    document.getElementById('yamlInput').value = text;
    setSubmitStatus('Loaded file: ' + file.name);
  } catch (error) {
    setSubmitStatus('Failed to read file: ' + error.message, 'error');
  }
}

async function submitJob() {
  const yaml = document.getElementById('yamlInput').value;
  if (!yaml.trim()) {
    setSubmitStatus('YAML input is empty.', 'error');
    return;
  }

  const button = document.getElementById('submitButton');
  button.disabled = true;
  button.classList.add('opacity-80', 'cursor-wait');
  setSubmitStatus('Submitting job...', 'muted');

  try {
    const res = await apiFetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-yaml' },
      body: yaml,
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      setSubmitStatus('Submit failed: ' + (data.error || res.status), 'error');
      return;
    }

    if (String(data.status || '').toUpperCase() === 'FAILED') {
      setSubmitStatus('Submitted but job failed: ' + (data.error || 'open the failed job details for the error.'), 'error');
      await refreshDashboard();
      return;
    }

    if (data.batch) {
      const counts = data.counts || {};
      const failed = Number(counts.failed || 0);
      const message = `Jobs file submitted: ${counts.submitted || 0} submitted (${counts.queued || 0} queued), ${counts.skipped || 0} skipped, ${failed} failed.`;
      setSubmitStatus(message, failed > 0 ? 'error' : 'success');
      await refreshDashboard();
      if (failed === 0) closeSubmitModal();
      return;
    }

    if (data.action === 'skipped') {
      setSubmitStatus('Skipped existing job: ' + (data.id || '-'), 'success');
      await refreshDashboard();
      closeSubmitModal();
      return;
    }

    setSubmitStatus('Submitted: ' + JSON.stringify(data), 'success');
    await refreshDashboard();
    closeSubmitModal();
  } finally {
    button.disabled = false;
    button.classList.remove('opacity-80', 'cursor-wait');
  }
}

function openErrorsModal() {
  setModalState('errorsModal', true);
}

function closeErrorsModal() {
  setModalState('errorsModal', false);
  currentErrorsJobId = null;
  document.getElementById('errorsMeta').textContent = 'Waiting for data';
}

function toggleStack(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle('hidden');
}

async function refreshErrors() {
  const jobId = currentErrorsJobId;
  if (!jobId) return;

  const list = document.getElementById('errorsList');
  const meta = document.getElementById('errorsMeta');
  list.innerHTML = `<div class="rounded-[18px] border border-slate-200 bg-white p-5 text-sm text-slate-500">Loading errors...</div>`;

  const res = await apiFetch('/api/jobs/' + encodeURIComponent(jobId));
  if (!res.ok) {
    meta.textContent = 'Unavailable';
    list.innerHTML = `<div class="rounded-[18px] border border-rose-200 bg-rose-50 p-5 text-sm text-rose-700">Failed to load errors: ${res.status}</div>`;
    return;
  }

  const job = await res.json();
  const errs = Array.isArray(job.errors) ? [...job.errors].reverse() : [];
  meta.textContent = errs.length === 1 ? '1 recorded event' : `${errs.length} recorded events`;

  if (errs.length === 0) {
    list.innerHTML = `<div class="rounded-[18px] border border-slate-200 bg-white p-5 text-sm text-slate-500">(no errors recorded)</div>`;
    return;
  }

  list.innerHTML = errs.map((e, idx) => {
    const comp = (e.component || 'system').toUpperCase();
    const time = escapeHtml(e.time || '');
    const msg = escapeHtml(e.message || '');
    const stack = escapeHtml(e.stack || '');
    const stackId = `stack_${idx}`;

    const badgeClass =
      comp === 'SOURCE' ? 'bg-sky-100 text-sky-800 border-sky-200' :
      comp === 'SINK' ? 'bg-amber-100 text-amber-800 border-amber-200' :
      'bg-rose-100 text-rose-800 border-rose-200';

    return `
      <div class="overflow-hidden rounded-[20px] border border-slate-200 bg-white shadow-[0_10px_24px_rgba(15,23,42,0.04)]">
        <div class="border-b border-slate-200/80 px-5 py-4">
          <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div class="flex flex-wrap items-center gap-2">
              <span class="rounded-full border px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] ${badgeClass}">
                ${escapeHtml(comp)}
              </span>
              <span class="mono text-xs text-slate-500">${time}</span>
            </div>
            <button
              class="rounded-full border border-slate-200 bg-white px-4 py-2 text-xs font-semibold text-slate-600 transition hover:bg-slate-50"
              onclick="toggleStack('${stackId}')"
            >
              Toggle Stack
            </button>
          </div>
        </div>

        <div class="px-5 py-4">
          <div class="text-sm font-semibold text-slate-900">${msg}</div>
          <pre id="${stackId}" class="mono mt-4 hidden rounded-[18px] border border-slate-200 bg-slate-50 p-4 text-xs leading-6 text-slate-600 whitespace-pre-wrap">${stack}</pre>
        </div>
      </div>
    `;
  }).join('');
}

async function showErrors(jobId) {
  currentErrorsJobId = jobId;
  document.getElementById('errorsJobId').textContent = jobId;
  openErrorsModal();
  await refreshErrors();
}

function nodeBadge(node) {
  const t = (node.type || '').toLowerCase();
  const base = 'inline-flex items-center rounded-md border px-2.5 py-1 text-xs font-semibold uppercase tracking-normal';
  const map = {
    source: 'border-blue-200 bg-blue-50 text-blue-800',
    buffer: 'border-amber-200 bg-amber-50 text-amber-800',
    transform: 'border-amber-200 bg-amber-50 text-amber-800',
    sink: 'border-blue-200 bg-blue-50 text-blue-800',
  };
  const cls = map[t] || 'border-slate-200 bg-slate-100 text-slate-700';
  return `<span class="${base} ${cls}">${escapeHtml(node.type || 'node')}</span>`;
}

function graphStatePill(state) {
  const value = String(state || '').trim();
  if (!value) return '';

  const upper = value.toUpperCase();
  const base = 'inline-flex shrink-0 items-center gap-2 whitespace-nowrap rounded-md border px-2.5 py-1 text-xs font-semibold uppercase tracking-normal';

  let tone = ['border-slate-200 bg-slate-100 text-slate-700', 'bg-slate-400'];
  if (upper.includes('BACKPRESSURE') || upper.includes('WAITING') || upper.includes('PAUSED')) {
    tone = ['border-amber-200 bg-amber-50 text-amber-800', 'bg-amber-500'];
  } else if (upper.includes('BLOCKED') || upper.includes('FAILED') || upper.includes('ERROR')) {
    tone = ['border-rose-200 bg-rose-50 text-rose-800', 'bg-rose-500'];
  } else if (upper.includes('COMPLETED') || upper.includes('DONE') || upper.includes('EMPTY')) {
    tone = ['border-blue-200 bg-blue-50 text-blue-800', 'bg-blue-500'];
  } else if (upper.includes('FLOW') || upper.includes('READING') || upper.includes('WRITING') || upper.includes('APPLYING') || upper.includes('RUNNING') || upper.includes('READY')) {
    tone = ['border-emerald-200 bg-emerald-50 text-emerald-800', 'bg-emerald-500'];
  } else if (upper.includes('PREPAR') || upper.includes('DISCOVER') || upper.includes('CREATING') || upper.includes('IDLE')) {
    tone = ['border-amber-200 bg-amber-50 text-amber-800', 'bg-amber-500'];
  }

  return `<span class="${base} ${tone[0]}"><span class="h-2 w-2 rounded-full ${tone[1]}"></span>${escapeHtml(value)}</span>`;
}

function graphMetricValueClass(tone) {
  switch (String(tone || '').trim().toLowerCase()) {
    case 'blue':
      return 'text-blue-900';
    case 'amber':
      return 'text-amber-900';
    case 'rose':
      return 'text-rose-800';
    case 'emerald':
      return 'text-emerald-800';
    default:
      return 'text-slate-900';
  }
}

function graphMetricChipClass(tone) {
  switch (String(tone || '').trim().toLowerCase()) {
    case 'blue':
      return 'border-blue-200 bg-blue-50 text-blue-800';
    case 'amber':
      return 'border-amber-200 bg-amber-50 text-amber-800';
    case 'rose':
      return 'border-rose-200 bg-rose-50 text-rose-800';
    case 'emerald':
      return 'border-emerald-200 bg-emerald-50 text-emerald-800';
    default:
      return 'border-slate-200 bg-slate-50 text-slate-700';
  }
}

function graphStateKind(state) {
  const upper = String(state || '').trim().toUpperCase();
  if (!upper) return 'neutral';
  if (upper.includes('BACKPRESSURE') || upper.includes('WAITING') || upper.includes('PAUSED') || upper.includes('SLOWLY')) return 'warning';
  if (upper.includes('BLOCKED') || upper.includes('FAILED') || upper.includes('ERROR')) return 'error';
  if (upper.includes('COMPLETED') || upper.includes('DONE') || upper.includes('EMPTY')) return 'done';
  if (upper.includes('FLOW') || upper.includes('READING') || upper.includes('WRITING') || upper.includes('APPLYING') || upper.includes('RUNNING') || upper.includes('READY')) return 'active';
  return 'neutral';
}

function graphCardToneClass(kind) {
  switch (kind) {
    case 'warning':
      return 'border-amber-200 bg-amber-50/35';
    case 'error':
      return 'border-rose-200 bg-rose-50/40';
    default:
      return 'border-slate-200 bg-white';
  }
}

function truncateGraphDetail(detail, limit = 280) {
  const text = String(detail || '').replace(/\s+/g, ' ').trim();
  if (!text || text.length <= limit) return text;
  return `${text.slice(0, limit).trimEnd()}...`;
}

function renderGraphNodeDetail(node, detail) {
  const text = String(detail || '').trim();
  if (!text) return '';

  const toneKind = graphStateKind(node?.state);
  const isLong = text.length > 320 || text.split(/\r?\n/).length > 4;
  const panelTone = toneKind === 'error'
    ? 'border-rose-200 bg-white/70 text-rose-900'
    : toneKind === 'warning'
      ? 'border-amber-200 bg-white/70 text-amber-900'
      : 'border-slate-200 bg-white/70 text-slate-700';
  const preview = isLong ? truncateGraphDetail(text) : text;

  return `
    <div class="graph-node-detail mt-4 rounded-[12px] border px-3 py-2.5 text-sm leading-6 ${panelTone}">
      <div class="graph-node-detail-preview">${escapeHtml(preview)}</div>
      ${isLong ? `
        <details class="graph-node-detail-full mt-2">
          <summary class="cursor-pointer text-xs font-semibold text-current">Full error</summary>
          <pre class="mono mt-2 max-h-56 overflow-auto whitespace-pre-wrap rounded-md border border-current/10 bg-white/80 p-3 text-xs leading-5">${escapeHtml(text)}</pre>
        </details>
      ` : ''}
    </div>
  `;
}

function graphBannerClass(kind) {
  switch (kind) {
    case 'warning':
      return 'border-amber-200 bg-amber-50 text-amber-900';
    case 'error':
      return 'border-rose-200 bg-rose-50 text-rose-900';
    default:
      return 'border-slate-200 bg-slate-50 text-slate-700';
  }
}

function findGraphNode(graph, type) {
  if (!graph || !Array.isArray(graph.nodes)) return null;
  return graph.nodes.find((node) => String(node?.type || '').toLowerCase() === String(type || '').toLowerCase()) || null;
}

function renderGraphAlert(graph) {
  const sourceNode = findGraphNode(graph, 'source');
  const bufferNode = findGraphNode(graph, 'buffer');
  const sinkNode = findGraphNode(graph, 'sink');
  const sourceState = String(sourceNode?.state || '').trim().toUpperCase();
  const bufferState = String(bufferNode?.state || '').trim().toUpperCase();
  const sinkState = String(sinkNode?.state || '').trim().toUpperCase();

  if (sinkState.includes('BLOCKED') || sinkState.includes('FAILED')) {
    return `
      <div class="mt-4 rounded-[16px] border px-4 py-3 ${graphBannerClass('error')}">
        <div class="text-sm font-semibold">Sink error</div>
        <div class="mt-1 text-sm">Sink failed to write into the target. Check Doris and the sink error log.</div>
      </div>
    `;
  }

  if (bufferState.includes('BACKPRESSURE') || sinkState.includes('FLUSHING SLOWLY') || sourceState.includes('PAUSED ON BUFFER')) {
    return `
      <div class="mt-4 rounded-[16px] border px-4 py-3 ${graphBannerClass('warning')}">
        <div class="text-sm font-semibold">Sink slowdown detected</div>
        <div class="mt-1 text-sm">Source is waiting for sink flush. This means sink throughput is lower than snapshot read throughput.</div>
      </div>
    `;
  }

  return '';
}

function renderGraphMetric(metric) {
  const label = String(metric?.label || '').trim();
  const value = String(metric?.value || '').trim();
  if (!label && !value) return '';

  return `
    <div class="rounded-[16px] border border-slate-200 bg-slate-50 px-4 py-3">
      <div class="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">${escapeHtml(label || 'Metric')}</div>
      <div class="mono mt-2 text-sm font-semibold ${graphMetricValueClass(metric?.tone)} break-all">${escapeHtml(value || '-')}</div>
    </div>
  `;
}

function renderGraphMetricChips(metrics) {
  const items = Array.isArray(metrics) ? metrics.filter((metric) => String(metric?.label || metric?.value || '').trim()) : [];
  if (items.length === 0) return '';

  return `
    <div class="mt-4 flex flex-wrap gap-2">
      ${items.map((metric) => `
        <div class="rounded-full border px-3 py-1.5 text-xs ${graphMetricChipClass(metric?.tone)}">
          <span class="font-semibold uppercase tracking-[0.14em]">${escapeHtml(metric?.label || 'Metric')}</span>
          <span class="mono ml-2 font-semibold">${escapeHtml(metric?.value || '-')}</span>
        </div>
      `).join('')}
    </div>
  `;
}

function compactGraphMetrics(node, limit = 3) {
  const metrics = Array.isArray(node?.metrics) ? node.metrics : [];
  const nodeType = String(node?.type || '').toLowerCase();
  const preferredLabels = {
    source: ['CDC current', 'CDC start', 'Mode', 'Tables', 'Chunk', 'Resume strategy'],
    buffer: ['Capacity', 'Phase', 'Guard'],
    sink: ['Runtime', 'Batch', 'Flush', 'Routes', 'Type'],
  };
  const order = preferredLabels[nodeType] || [];
  const picked = [];

  for (const label of order) {
    const metric = metrics.find((item) => String(item?.label || '').trim().toLowerCase() === label.toLowerCase());
    if (metric) picked.push(metric);
    if (picked.length >= limit) return picked;
  }

  for (const metric of metrics) {
    if (!String(metric?.label || metric?.value || '').trim()) continue;
    if (picked.includes(metric)) continue;
    picked.push(metric);
    if (picked.length >= limit) break;
  }

  return picked;
}

function shouldRenderNodeDetail(node, detail) {
  const value = String(detail || '').trim().toLowerCase();
  if (!value) return false;
  const nodeType = String(node?.type || '').toLowerCase();
  if (nodeType === 'source') {
    return value.includes('waiting') || value.includes('flush') || value.includes('slower') || value.includes('blocked') || value.includes('failed') || value.includes('error');
  }
  if (nodeType === 'buffer') {
    return value.includes('waiting') || value.includes('drain') || value.includes('blocked') || value.includes('failed');
  }
  return true;
}

function renderGraphNodeCard(node, idx) {
  const label = escapeHtml(node?.label || node?.id || 'Node');
  const subtitle = String(node?.subtitle || '').trim();
  const detail = String(node?.detail || '').trim();
  const metrics = compactGraphMetrics(node, 3);
  const showDetail = shouldRenderNodeDetail(node, detail);
  const toneKind = graphStateKind(node?.state);

  return `
    <div class="rounded-[20px] border p-5 shadow-[0_10px_24px_rgba(15,23,42,0.04)] ${graphCardToneClass(toneKind)}">
      <div class="flex items-start justify-between gap-3">
        <div>
          <div class="text-lg font-semibold tracking-tight text-slate-900">${label}</div>
          ${subtitle ? `<div class="mono mt-2 text-xs text-slate-500 break-all">${escapeHtml(subtitle)}</div>` : ''}
        </div>
        <div class="flex flex-wrap items-center justify-end gap-2">
          ${nodeBadge(node)}
          ${graphStatePill(node?.state)}
        </div>
      </div>
      ${showDetail ? renderGraphNodeDetail(node, detail) : ''}
      ${metrics.length > 0 ? `<div class="mt-5 grid gap-3 sm:grid-cols-2">${metrics.map(renderGraphMetric).join('')}</div>` : ''}
    </div>
  `;
}

function renderGraphFlowSummary(nodes) {
  if (!Array.isArray(nodes) || nodes.length === 0) return '';

  return `
    <div class="mt-6 rounded-[20px] border border-slate-200 bg-white p-5">
      <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Pipeline Flow</div>
      <div class="mt-4 flex flex-wrap items-center gap-3">
        ${nodes.map((node, idx) => `
          <div class="flex items-center gap-3">
            <div class="rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-xs">
              <span class="font-semibold text-slate-900">${escapeHtml(node?.label || node?.id || 'Node')}</span>
            </div>
            ${graphStatePill(node?.state)}
            ${idx < nodes.length - 1 ? '<span class="text-slate-300">&rarr;</span>' : ''}
          </div>
        `).join('')}
      </div>
    </div>
  `;
}

function renderGraph(graph) {
  const canvas = document.getElementById('graphCanvas');
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph.edges) ? graph.edges : [];
  const progressBlock = renderGraphProgress(graph);

  if (nodes.length === 0) {
    setInnerHTMLIfChanged(canvas, `
      ${progressBlock}
      <div class="${progressBlock ? 'mt-6 ' : ''}rounded-[18px] border border-slate-200 bg-white p-5 text-sm text-slate-500">(no graph nodes)</div>
    `);
    return;
  }

  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  const toSet = new Set(edges.map((e) => e.to));
  let start = nodes.find((n) => !toSet.has(n.id)) || nodes[0];

  const ordered = [];
  const seen = new Set();
  let cur = start;

  while (cur && !seen.has(cur.id)) {
    ordered.push(cur);
    seen.add(cur.id);
    const nextEdge = edges.find((e) => e.from === cur.id);
    cur = nextEdge ? nodeById.get(nextEdge.to) : null;
  }

  for (const node of nodes) {
    if (!seen.has(node.id)) ordered.push(node);
  }

  const nodeCards = ordered.map(renderGraphNodeCard).join('');
  const flowSummary = renderGraphFlowSummary(ordered);

  setInnerHTMLIfChanged(canvas, `
    ${progressBlock}
    <div class="${progressBlock ? 'mt-6 ' : ''}grid gap-4 xl:grid-cols-3">
      ${nodeCards}
    </div>
    ${flowSummary}
  `);
}

function checkpointPosition(file, pos) {
  const cleanFile = String(file || '').trim();
  const numericPos = Number(pos);
  if (!cleanFile || !Number.isFinite(numericPos) || numericPos <= 0) return '-';
  return `${cleanFile}:${Math.trunc(numericPos)}`;
}

function parseBinlogOrdinal(file) {
  const match = String(file || '').trim().match(/^(.*?)(\d+)$/);
  if (!match) return null;
  return {
    prefix: match[1],
    ordinal: Number(match[2]),
  };
}

function binlogFileDistance(fromFile, toFile) {
  const from = parseBinlogOrdinal(fromFile);
  const to = parseBinlogOrdinal(toFile);
  if (!from || !to || from.prefix !== to.prefix) return null;
  const distance = to.ordinal - from.ordinal;
  return Number.isFinite(distance) ? distance : null;
}

function checkpointTime(value) {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString();
}

function checkpointCard(title, value, updatedAt, detail = '') {
  return `
    <div class="rounded-[16px] border border-slate-200 bg-slate-50 px-4 py-3">
      <div class="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">${escapeHtml(title)}</div>
      <div class="mono mt-2 text-sm font-semibold text-slate-900 break-all">${escapeHtml(value || '-')}</div>
      ${detail ? `<div class="mt-2 text-xs text-slate-500">${escapeHtml(detail)}</div>` : ''}
      <div class="mt-2 text-[11px] text-slate-500">Updated <span class="mono text-slate-700">${escapeHtml(checkpointTime(updatedAt))}</span></div>
    </div>
  `;
}

function binlogDiagnosticTone(status) {
  switch (String(status || '').toLowerCase()) {
    case 'available':
      return 'border-emerald-200 bg-emerald-50 text-emerald-800';
    case 'purged':
    case 'missing':
      return 'border-rose-200 bg-rose-50 text-rose-800';
    default:
      return 'border-amber-200 bg-amber-50 text-amber-800';
  }
}

function renderBinlogDiagnostics(diag, hasCdcOffset = false) {
  if (!diag) {
    if (!hasCdcOffset) return '';
    return `
      <div class="mt-4 rounded-[16px] border border-amber-200 bg-amber-50 px-4 py-3 text-amber-800">
        <div class="text-sm font-semibold">Binlog diagnostics unavailable</div>
        <div class="mt-1 text-sm">CDC offset exists, but the API response did not include binlog diagnostics yet. Restart/reload the Rivus server after deploying this UI/API build.</div>
      </div>
    `;
  }

  const status = String(diag.status || 'unknown').toUpperCase();
  const range = diag.earliest_file || diag.latest_file
    ? `${diag.earliest_file || '-'} -> ${diag.latest_file || '-'}`
    : '-';
  const checkpoint = checkpointPosition(diag.checkpoint_file, diag.checkpoint_pos);
  const checkpointToLatestFiles = binlogFileDistance(diag.checkpoint_file, diag.latest_file);
  const checkpointToEarliestFiles = binlogFileDistance(diag.checkpoint_file, diag.earliest_file);
  let cdcLagLabel = '-';
  let cdcLagDetail = '';
  if (Number.isFinite(checkpointToLatestFiles)) {
    if (checkpointToLatestFiles > 0) {
      cdcLagLabel = `${fmtWholeNumber(checkpointToLatestFiles)} binlog file${checkpointToLatestFiles === 1 ? '' : 's'} behind latest`;
      cdcLagDetail = 'Rivus has not checkpointed into the latest binlog file yet.';
    } else if (checkpointToLatestFiles === 0) {
      cdcLagLabel = 'checkpoint is on latest file';
      cdcLagDetail = 'Rivus checkpoint is in the current latest binlog file.';
    } else {
      cdcLagLabel = 'checkpoint is ahead of latest';
      cdcLagDetail = 'Check binlog naming or source server mismatch.';
    }
  }
  if (String(diag.status || '').toLowerCase() === 'purged' && Number.isFinite(checkpointToEarliestFiles)) {
    cdcLagLabel = `${fmtWholeNumber(Math.abs(checkpointToEarliestFiles))} binlog file${Math.abs(checkpointToEarliestFiles) === 1 ? '' : 's'} before earliest`;
    cdcLagDetail = 'Saved checkpoint file has already been purged by MySQL.';
  }
  const checkpointAge = Number(diag.checkpoint_age_sec);
  const checkpointAgeLabel = Number.isFinite(checkpointAge) && checkpointAge >= 0
    ? formatDuration(checkpointAge)
    : '-';
  const earliestFirstEventAt = String(diag.earliest_first_event_at || '').trim();
  const earliestFirstEventType = String(diag.earliest_first_event || '').trim();
  const earliestFirstEventError = String(diag.earliest_first_event_error || '').trim();
  const earliestFirstEventGap = Number(diag.checkpoint_to_earliest_first_event_sec);
  const earliestFirstEventDetail = [];
  if (earliestFirstEventType) {
    earliestFirstEventDetail.push(`event=${earliestFirstEventType}`);
  }
  if (Number.isFinite(earliestFirstEventGap)) {
    const gap = formatDuration(Math.abs(earliestFirstEventGap));
    earliestFirstEventDetail.push(earliestFirstEventGap >= 0
      ? `history starts ${gap} after checkpoint save`
      : `checkpoint saved ${gap} after history starts`);
  }
  if (earliestFirstEventError) {
    earliestFirstEventDetail.push(`unavailable: ${earliestFirstEventError}`);
  }
  const latestFirstEventAt = String(diag.latest_first_event_at || diag.latest_created_at || '').trim();
  const latestFirstEventType = String(diag.latest_first_event || '').trim();
  const latestFirstEventError = String(diag.latest_first_event_error || '').trim();
  const latestFirstEventGap = Number(diag.checkpoint_to_latest_first_event_sec);
  const latestFirstEventDetail = [];
  if (latestFirstEventType) {
    latestFirstEventDetail.push(`event=${latestFirstEventType}`);
  }
  if (Number.isFinite(latestFirstEventGap)) {
    const gap = formatDuration(Math.abs(latestFirstEventGap));
    latestFirstEventDetail.push(latestFirstEventGap >= 0
      ? `latest starts ${gap} after checkpoint save`
      : `checkpoint saved ${gap} after latest starts`);
  }
  if (latestFirstEventError) {
    latestFirstEventDetail.push(`unavailable: ${latestFirstEventError}`);
  }
  const detail = diag.error
    ? `Diagnostic error: ${diag.error}`
    : diag.status === 'available'
      ? 'Checkpoint binlog is still present on the MySQL server.'
      : diag.status === 'purged'
        ? 'Checkpoint binlog is older than the earliest binlog currently available on the MySQL server.'
        : 'Checkpoint binlog was not found in the currently available MySQL binlog range.';

  return `
    <div class="mt-4 rounded-[16px] border px-4 py-3 ${binlogDiagnosticTone(diag.status)}">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div class="text-sm font-semibold">Binlog checkpoint ${escapeHtml(status)}</div>
          <div class="mt-1 text-sm">${escapeHtml(detail)}</div>
        </div>
        <div class="mono rounded-md border border-current/20 bg-white/60 px-3 py-1.5 text-xs">${escapeHtml(checkpoint)}</div>
      </div>
      <div class="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <div>
          <div class="text-[11px] font-semibold uppercase">Saved At</div>
          <div class="mono mt-1 text-xs">${escapeHtml(checkpointTime(diag.checkpoint_updated_at))}</div>
          <div class="mt-1 text-xs">Age <span class="mono">${escapeHtml(checkpointAgeLabel)}</span></div>
        </div>
        <div>
          <div class="text-[11px] font-semibold uppercase">CDC Lag</div>
          <div class="mono mt-1 text-xs">${escapeHtml(cdcLagLabel)}</div>
          ${cdcLagDetail ? `<div class="mt-1 text-xs">${escapeHtml(cdcLagDetail)}</div>` : ''}
        </div>
        <div>
          <div class="text-[11px] font-semibold uppercase">Available Binlogs</div>
          <div class="mono mt-1 text-xs break-all">${escapeHtml(range)}</div>
        </div>
        <div>
          <div class="text-[11px] font-semibold uppercase">History First Event</div>
          <div class="mono mt-1 text-xs">${escapeHtml(earliestFirstEventAt ? checkpointTime(earliestFirstEventAt) : 'Not available')}</div>
          ${earliestFirstEventDetail.length ? `<div class="mt-1 text-xs">${escapeHtml(earliestFirstEventDetail.join(' | '))}</div>` : ''}
        </div>
        <div>
          <div class="text-[11px] font-semibold uppercase">Latest First Event</div>
          <div class="mono mt-1 text-xs">${escapeHtml(latestFirstEventAt ? checkpointTime(latestFirstEventAt) : 'Not available')}</div>
          ${latestFirstEventDetail.length ? `<div class="mt-1 text-xs">${escapeHtml(latestFirstEventDetail.join(' | '))}</div>` : ''}
          <div class="mt-1 text-xs">Observed <span class="mono">${escapeHtml(checkpointTime(diag.source_server_time || diag.observed_at))}</span></div>
        </div>
      </div>
      <div class="mt-3 text-xs">Available binlog count: <span class="mono font-semibold">${escapeHtml(diag.available_count ?? '-')}</span></div>
    </div>
  `;
}

function renderCheckpoint(job) {
  const panel = document.getElementById('checkpointPanel');
  if (!panel) return;

  const checkpoint = job?.checkpoint;
  const metaKey = String(job?.meta_key || checkpoint?.meta_key || '').trim();

  if (!checkpoint) {
    setInnerHTMLIfChanged(panel, `
      <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Checkpoint</div>
          <div class="mt-2 text-sm text-slate-500">No saved checkpoint yet.</div>
        </div>
        ${metaKey ? `<div class="mono max-w-full rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-[11px] text-slate-600 break-all">${escapeHtml(metaKey)}</div>` : ''}
      </div>
    `);
    return;
  }

  if (checkpoint.error) {
    setInnerHTMLIfChanged(panel, `
      <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Checkpoint</div>
      <div class="mt-3 rounded-[16px] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
        Failed to load checkpoint: ${escapeHtml(checkpoint.error)}
      </div>
      ${metaKey ? `<div class="mono mt-3 text-[11px] text-slate-500 break-all">${escapeHtml(metaKey)}</div>` : ''}
    `);
    return;
  }

  const cards = [];
  const cdc = checkpoint.cdc_offset;
  if (cdc) {
    cards.push(checkpointCard('CDC Offset', checkpointPosition(cdc.binlog_file, cdc.binlog_pos), cdc.updated_at, 'Last sink-committed binlog position.'));
  }

  const snapshot = checkpoint.snapshot_state;
  if (snapshot) {
    cards.push(checkpointCard(
      'Snapshot Start',
      checkpointPosition(snapshot.start_file, snapshot.start_pos),
      snapshot.updated_at,
      snapshot.done ? 'Snapshot marked done.' : 'Snapshot not marked done yet.'
    ));
  }

  const progress = checkpoint.snapshot_progress;
  if (progress) {
    cards.push(checkpointCard(
      'Snapshot Progress',
      String(progress.table_name || '-'),
      progress.updated_at,
      `Next offset ${fmtWholeNumber(progress.next_offset || 0)}`
    ));
  }

  setInnerHTMLIfChanged(panel, `
    <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
      <div>
        <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Checkpoint</div>
        <div class="mt-2 text-sm text-slate-500">Saved metadata for resume decisions.</div>
      </div>
      ${metaKey ? `<div class="mono max-w-full rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-[11px] text-slate-600 break-all">${escapeHtml(metaKey)}</div>` : ''}
    </div>
    <div class="mt-4 grid gap-3 lg:grid-cols-3">
      ${cards.length > 0 ? cards.join('') : '<div class="rounded-[16px] border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-500">No checkpoint rows found.</div>'}
    </div>
    ${renderBinlogDiagnostics(checkpoint.binlog_diagnostics, !!cdc)}
  `);
}

async function refreshGraph(options = {}) {
  const jobId = currentGraphJobId;
  if (!jobId) return;
  const silent = !!options.silent;

  const statusEl = document.getElementById('graphStatus');
  const rawEl = document.getElementById('graphRaw');
  const canvas = document.getElementById('graphCanvas');
  const metaEl = document.getElementById('graphMeta');
  const checkpointPanel = document.getElementById('checkpointPanel');

  if (!silent) {
    metaEl.textContent = 'Loading graph';
    setInnerHTMLIfChanged(canvas, `<div class="rounded-[18px] border border-slate-200 bg-white p-5 text-sm text-slate-500">(loading...)</div>`);
    if (checkpointPanel) {
      setInnerHTMLIfChanged(checkpointPanel, `<div class="text-sm text-slate-500">Loading checkpoint...</div>`);
    }
  }

  const [res, jobRes] = await Promise.all([
    apiFetch('/api/jobs/' + encodeURIComponent(jobId) + '/graph'),
    apiFetch('/api/jobs/' + encodeURIComponent(jobId)),
  ]);

  let job = null;
  if (jobRes.ok) {
    job = await jobRes.json();
    renderJobDetailHeader(job);
    renderCheckpoint(job);
  } else {
    renderJobDetailHeader({ id: jobId });
    if (checkpointPanel) {
      setInnerHTMLIfChanged(checkpointPanel, `<div class="rounded-[16px] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">Failed to load checkpoint: ${jobRes.status}</div>`);
    }
  }

  if (!res.ok) {
    metaEl.textContent = 'Unavailable';
    setInnerHTMLIfChanged(canvas, `<div class="rounded-[18px] border border-rose-200 bg-rose-50 p-5 text-sm text-rose-700">Failed to load graph: ${res.status}</div>`);
    return;
  }

  const graph = await res.json();
  const progressSummary = String(graph?.progress?.summary || '').trim();
  const runtimeState = String((Array.isArray(graph?.nodes) ? graph.nodes.find((node) => String(node?.type || '').toLowerCase() === 'buffer')?.state : '') || '').trim();

  statusEl.textContent = graph.status || job?.status || '-';
  rawEl.textContent = JSON.stringify(graph, null, 2);
  metaEl.textContent = progressSummary || runtimeState || 'Graph ready';

  renderGraph(graph);
}

function renderJobDetailHeader(job) {
  const display = jobDisplay(job?.name || job?.id || '-');
  const errorCount = Array.isArray(job?.errors) ? job.errors.length : Number(job?.error_count || 0);
  const sinkType = String(job?.sink_type || job?.config?.sink?.type || job?.config?.Sink?.type || '').trim();

  document.getElementById('jobDetailTitle').textContent = display.title || '-';
  document.getElementById('jobDetailSubtitle').textContent = display.detail || String(job?.name || '').trim();
  document.getElementById('jobDetailId').textContent = job?.id || currentGraphJobId || '-';
  document.getElementById('jobDetailSink').textContent = sinkType || '-';
  document.getElementById('jobDetailCreated').textContent = formatDateTime(job?.created);
  document.getElementById('jobDetailUpdated').textContent = formatDateTime(job?.updated);
  document.getElementById('jobDetailErrors').textContent = Number.isFinite(errorCount) ? `${fmtWholeNumber(errorCount)} recorded` : '-';
}

async function showJobDetails(jobId) {
  currentGraphJobId = jobId;
  if (currentTab !== 'job') {
    previousJobListTab = currentTab;
  }
  switchTab('job', { updateUrl: false });
  updateTabUrl();
  document.getElementById('jobDetailId').textContent = jobId;
  document.getElementById('jobDetailTitle').textContent = 'Loading job...';
  document.getElementById('jobDetailSubtitle').textContent = '';
  document.getElementById('graphStatus').textContent = '-';
  document.getElementById('graphRaw').textContent = '';
  await refreshGraph();
}

function backToJobs() {
  const tab = previousJobListTab === 'iceberg' || previousJobListTab === 'logs'
    ? previousJobListTab
    : 'doris';
  switchTab(tab);
}

document.addEventListener('keydown', (event) => {
  if (event.key !== 'Escape') return;

  if (!document.getElementById('profileDropdown').classList.contains('hidden')) {
    setProfileMenu(false);
    return;
  }
  if (!document.getElementById('errorsModal').classList.contains('hidden')) {
    closeErrorsModal();
    return;
  }
  if (!document.getElementById('submitModal').classList.contains('hidden')) {
    closeSubmitModal();
  }
});

document.addEventListener('click', (event) => {
  const profileMenu = document.getElementById('profileMenu');
  if (profileMenu && !profileMenu.contains(event.target)) {
    setProfileMenu(false);
  }

  const bulkArchiveButton = event.target.closest('[data-bulk-delete-archive]');
  if (bulkArchiveButton) {
    bulkDeleteArchive(bulkArchiveButton.dataset.bulkDeleteArchive);
    return;
  }

  const jobButton = event.target.closest('[data-job-action]');
  if (jobButton) {
    const jobId = jobButton.dataset.jobId;
    const action = jobButton.dataset.jobAction;

    if (!jobId || !action) return;

    if (action === 'details') showJobDetails(jobId);
    if (action === 'logs') showLogsForJob(jobId);
    if (action === 'pause') pauseJob(jobId);
    if (action === 'cancel') cancelJob(jobId);
    if (action === 'resubmit') resubmitJob(jobId);
    if (action === 'delete') deleteJob(jobId);
    if (action === 'errors') showErrors(jobId);
    return;
  }

  const logRow = event.target.closest('[data-log-file]');
  if (logRow) {
    selectLogFile(logRow.dataset.logFile);
    return;
  }

});

function startApp() {
  setFollowLatestLog(followLatestLog);
  switchTab(currentTab, { updateUrl: false, load: false });
  loadAppVersion();
  loadAuthStatus().finally(() => refreshDashboard());
  setInterval(() => refreshDashboard({ auto: true }), 5000);
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', startApp, { once: true });
} else {
  startApp();
}

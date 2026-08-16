import {
  apiFetch,
  escapeHtml,
  fmtBytes,
  operationErrorMessage,
  setLogsNotice,
  syncFormatter,
} from './core/api.js';

const initialUrl = new URL(window.location.href);
let currentLogFile = initialUrl.searchParams.get('log') || '';
let followLatestLog = initialUrl.searchParams.get('follow') === 'latest' || !currentLogFile;
let currentLogLineLimit = Number(initialUrl.searchParams.get('lines') || 500);
let latestLogLines = [];
let latestLogTailLoaded = false;
let latestLogTailError = '';
let routeChanged = () => {};
let switchTab = () => {};
let initialized = false;

if (!Number.isFinite(currentLogLineLimit) || currentLogLineLimit <= 0) currentLogLineLimit = 500;
currentLogLineLimit = Math.min(5000, Math.trunc(currentLogLineLimit));

export function initLogs(handlers = {}) {
  routeChanged = handlers.routeChanged || routeChanged;
  switchTab = handlers.switchTab || switchTab;
  setFollowLatestLog(followLatestLog);

  if (initialized) return;
  initialized = true;
  document.addEventListener('click', (event) => {
    const logRow = event.target.closest('[data-log-file]');
    if (logRow) selectLogFile(logRow.dataset.logFile);
  });
}

export function getLogRouteState() {
  return {
    currentLogFile,
    followLatestLog,
    lineLimit: selectedLogLineLimit(),
  };
}

export function selectedLogLineLimit() {
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
  if (hasOption) select.value = requested;
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

export async function loadLogs(options = {}) {
  try {
    setInitialLogLineLimit();
    if (options.preferLatest === true) setFollowLatestLog(true);
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
      routeChanged();
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
    routeChanged();
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

export async function loadLogTail(options = {}) {
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
    if (!(followLatestLog && filter)) url.searchParams.set('file', currentLogFile);
    url.searchParams.set('lines', String(selectedLogLineLimit()));
    if (filter) url.searchParams.set('filter', filter);
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
    if (!options.silent) setLogsNotice(latestLogTailError, 'error');
  }
}

export function renderLogOutput() {
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
  if (document.getElementById('logAutoRefresh')?.checked) output.scrollTop = output.scrollHeight;
}

export function selectLogFile(name) {
  setFollowLatestLog(false);
  currentLogFile = String(name || '');
  routeChanged();
  loadLogs();
}

export function setFollowLatestLog(enabled) {
  followLatestLog = !!enabled;
  const checkbox = document.getElementById('logAutoRefresh');
  if (checkbox) checkbox.checked = followLatestLog;
}

export function changeLogFollowLatest(enabled) {
  setFollowLatestLog(enabled);
  if (followLatestLog) {
    currentLogFile = '';
    latestLogLines = [];
    loadLogs({ preferLatest: true });
    return;
  }
  routeChanged();
}

export function changeLogLineLimit(value) {
  const select = document.getElementById('logLineSelect');
  if (select) select.value = String(value || 500);
  currentLogLineLimit = selectedLogLineLimit();
  routeChanged();
  loadLogTail();
}

export function downloadSelectedLog() {
  if (!currentLogFile) return;
  window.location.href = '/api/logs/download?file=' + encodeURIComponent(currentLogFile);
}

export function showLogsForJob(jobId) {
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

export async function refreshLogs(options = {}) {
  if (options.auto) {
    if (!document.getElementById('logAutoRefresh')?.checked) return;
    if (followLatestLog) {
      await loadLogs({ silent: true });
    } else if (currentLogFile) {
      await loadLogTail({ silent: true });
    } else {
      await loadLogs({ silent: true });
    }
    return;
  }
  await loadLogs();
}

const fmt = new Intl.NumberFormat();
const initialQuery = new URLSearchParams(window.location.search);

let jobID = String(initialQuery.get('job_id') || '').trim();
let runOffset = Math.max(0, Number.parseInt(initialQuery.get('offset') || '0', 10) || 0);
let currentResults = [];

const esc = (value) => String(value ?? '')
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')
  .replaceAll("'", '&#39;');

const dateText = (value) => {
  if (!value) return '-';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
};

const bytes = (value) => {
  let size = Number(value || 0);
  if (!Number.isFinite(size)) return '-';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
};

const duration = (milliseconds) => {
  const value = Number(milliseconds || 0);
  if (!Number.isFinite(value)) return '-';
  if (value < 1000) return `${Math.trunc(value)} ms`;
  return `${(value / 1000).toFixed(1)} s`;
};

function selected(id) {
  return String(document.getElementById(id)?.value || '').trim();
}

function scopedPath(path) {
  const url = new URL(path, window.location.origin);
  if (jobID) url.searchParams.set('job_id', jobID);
  return url.pathname + url.search;
}

function configureScope() {
  const badge = document.getElementById('scopeBadge');
  const description = document.getElementById('scopeDescription');
  const backLink = document.getElementById('backLink');
  document.getElementById('jobFilter').value = jobID;
  if (jobID) {
    badge.textContent = `Job ${jobID}`;
    description.textContent = 'Queue health and execution history for this Rivus job, read from durable metadata MySQL. Other jobs are excluded from this view.';
    backLink.href = `/?tab=job&id=${encodeURIComponent(jobID)}`;
    backLink.textContent = 'Back to job';
    return;
  }
  badge.textContent = 'All jobs';
  description.textContent = 'Queue health and execution history from durable metadata MySQL. Compaction results show the actual native or Spark engine and routing reason.';
  backLink.href = '/';
  backLink.textContent = 'Back to jobs';
}

function notice(message, error = false) {
  const element = document.getElementById('notice');
  if (!message) {
    element.classList.add('hidden');
    return;
  }
  element.className = `mb-4 rounded-xl border px-4 py-3 text-sm ${error ? 'border-rose-200 bg-rose-50 text-rose-700' : 'border-slate-200 bg-white text-slate-600'}`;
  element.textContent = message;
}

async function api(path) {
  const response = await fetch(path, { credentials: 'same-origin' });
  if (response.status === 401) {
    window.location.href = '/login?next=' + encodeURIComponent(window.location.pathname + window.location.search);
    throw new Error('authentication required');
  }
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

function metric(label, value, detail) {
  return `<div class="rounded-[18px] border border-slate-200 bg-white p-4 shadow-sm"><div class="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">${esc(label)}</div><div class="mt-2 font-mono text-xl font-semibold text-slate-950">${esc(value)}</div><div class="mt-1 text-xs text-slate-500">${esc(detail || '')}</div></div>`;
}

function statusBadge(value) {
  const status = String(value || '-').toLowerCase();
  const classes = status.includes('error') || status === 'failed'
    ? 'border-rose-200 bg-rose-50 text-rose-700'
    : status === 'running' || status === 'succeeded' || status === 'finished'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
      : status === 'skipped'
        ? 'border-slate-200 bg-slate-50 text-slate-600'
        : 'border-amber-200 bg-amber-50 text-amber-700';
  return `<span class="inline-flex rounded-full border px-2.5 py-1 text-[11px] font-semibold ${classes}">${esc(value || '-')}</span>`;
}

function runsPath() {
  const url = new URL('/api/iceberg/maintenance/runs', window.location.origin);
  if (jobID) url.searchParams.set('job_id', jobID);
  const mappings = [
    ['q', 'runSearch'],
    ['status', 'runStatus'],
    ['operation', 'runOperation'],
    ['engine', 'runEngine'],
  ];
  mappings.forEach(([parameter, id]) => {
    const value = selected(id);
    if (value) url.searchParams.set(parameter, value);
  });
  url.searchParams.set('limit', selected('pageSize') || '50');
  url.searchParams.set('offset', String(runOffset));
  return url.pathname + url.search;
}

function syncURL() {
  const url = new URL(window.location.href);
  ['job_id', 'q', 'status', 'operation', 'engine', 'limit', 'offset'].forEach((key) => url.searchParams.delete(key));
  if (jobID) url.searchParams.set('job_id', jobID);
  const mappings = [
    ['q', 'runSearch'],
    ['status', 'runStatus'],
    ['operation', 'runOperation'],
    ['engine', 'runEngine'],
  ];
  mappings.forEach(([parameter, id]) => {
    const value = selected(id);
    if (value) url.searchParams.set(parameter, value);
  });
  const pageSize = selected('pageSize') || '50';
  if (pageSize !== '50') url.searchParams.set('limit', pageSize);
  if (runOffset > 0) url.searchParams.set('offset', String(runOffset));
  window.history.replaceState({}, '', url);
}

function renderRuns(payload) {
  const runs = Array.isArray(payload.runs) ? payload.runs : [];
  const total = Number(payload.total || 0);
  const limit = Number(payload.limit || 50);
  const offset = Number(payload.offset || 0);
  const first = total === 0 ? 0 : offset + 1;
  const last = Math.min(offset + runs.length, total);

  document.getElementById('runCount').textContent = `${fmt.format(total)} run${total === 1 ? '' : 's'}`;
  document.getElementById('pageSummary').textContent = total === 0
    ? 'No matching runs'
    : `Showing ${fmt.format(first)}–${fmt.format(last)} of ${fmt.format(total)}`;
  document.getElementById('previousPage').disabled = offset <= 0;
  document.getElementById('nextPage').disabled = offset + limit >= total;

  const emptyText = jobID
    ? 'No maintenance runs match these filters for this job.'
    : 'No maintenance runs match these filters.';
  document.getElementById('runsBody').innerHTML = runs.map((run) => `
    <tr class="cursor-pointer hover:bg-slate-50" data-run-id="${esc(run.id)}">
      <td class="px-4 py-3 font-mono text-xs font-semibold text-blue-700">#${esc(run.id)}</td>
      <td class="px-4 py-3 font-mono text-xs text-slate-700">${esc(run.worker_id || '-')}</td>
      <td class="px-4 py-3">${statusBadge(run.status)}</td>
      <td class="px-4 py-3 text-right font-mono text-xs">${fmt.format(run.task_count || 0)}</td>
      <td class="px-4 py-3 text-right font-mono text-xs text-emerald-700">${fmt.format(run.success_count || 0)}</td>
      <td class="px-4 py-3 text-right font-mono text-xs text-slate-600">${fmt.format(run.skipped_count || 0)}</td>
      <td class="px-4 py-3 text-right font-mono text-xs text-rose-700">${fmt.format(run.failed_count || 0)}</td>
      <td class="px-4 py-3 text-xs text-slate-600">${esc(dateText(run.started_at))}</td>
    </tr>`).join('') || `<tr><td colspan="8" class="px-4 py-10 text-center text-sm text-slate-500">${esc(emptyText)}</td></tr>`;
  document.querySelectorAll('[data-run-id]').forEach((row) => row.addEventListener('click', () => loadRun(row.dataset.runId)));
}

async function load() {
  const button = document.getElementById('refreshButton');
  button.disabled = true;
  notice('');
  try {
    const [summary, payload] = await Promise.all([
      api(scopedPath('/api/iceberg/maintenance/summary')),
      api(runsPath()),
    ]);
    document.getElementById('summary').innerHTML = [
      metric('Tracked tables', fmt.format(summary.tables || 0), `${fmt.format(summary.snapshot_blocked || 0)} snapshot blocked`),
      metric('Queued', fmt.format(summary.queued_tasks || 0), 'Waiting for a worker lease'),
      metric('Retrying', fmt.format(summary.retry_tasks || 0), 'Retryable worker tasks'),
      metric('Running', fmt.format(summary.active_leases || 0), 'Active durable leases'),
      metric('Failed', fmt.format(summary.failed_tasks || 0), 'Terminal task failures'),
      metric('Oldest queue age', summary.oldest_queued_age_seconds ? `${fmt.format(summary.oldest_queued_age_seconds)} s` : '0 s', summary.oldest_queued_at ? dateText(summary.oldest_queued_at) : 'Queue is clear'),
    ].join('');
    renderRuns(payload);
  } catch (error) {
    notice(`Failed to load maintenance worker state: ${error.message}`, true);
  } finally {
    button.disabled = false;
  }
}

function renderResults() {
  const search = selected('resultSearch').toLowerCase();
  const operation = selected('resultOperation');
  const engine = selected('resultEngine');
  const status = selected('resultStatus');
  const results = currentResults.filter((result) => (
    (!search || String(result.table_key || '').toLowerCase().includes(search))
    && (!operation || result.operation === operation)
    && (!engine || result.engine === engine)
    && (!status || result.status === status)
  ));

  document.getElementById('resultsBody').innerHTML = results.map((result) => {
    const route = result.routing_reason ? `<div class="mt-1 max-w-sm text-[11px] leading-4 text-slate-500">${esc(result.routing_reason)}</div>` : '';
    const error = result.error ? `<div class="mt-1 max-w-lg break-words text-[11px] leading-4 text-rose-600">${esc(result.error)}</div>` : '';
    return `<tr class="align-top">
      <td class="px-4 py-3"><div class="font-mono text-xs font-semibold text-slate-800">${esc(result.table_key || '-')}</div>${error}</td>
      <td class="px-4 py-3 font-mono text-xs text-slate-700">${esc(result.operation || '-')}</td>
      <td class="px-4 py-3"><div class="font-mono text-xs font-semibold text-blue-700">${esc(result.engine || '-')}</div>${route}</td>
      <td class="px-4 py-3">${statusBadge(result.status)}</td>
      <td class="px-4 py-3 text-right"><div class="font-mono text-xs">${fmt.format(result.input_files || 0)} files</div><div class="mt-1 text-[11px] text-slate-500">${esc(bytes(result.input_bytes))}</div></td>
      <td class="px-4 py-3 text-right"><div class="font-mono text-xs">${fmt.format(result.output_files || 0)} files</div><div class="mt-1 text-[11px] text-slate-500">${esc(bytes(result.output_bytes))}</div></td>
      <td class="px-4 py-3 text-right font-mono text-xs">${fmt.format(result.attempt || 0)}</td>
      <td class="px-4 py-3 text-right font-mono text-xs">${esc(duration(result.duration_ms))}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="8" class="px-4 py-8 text-center text-sm text-slate-500">No task results match these filters.</td></tr>';
}

async function loadRun(id) {
  const panel = document.getElementById('detailsPanel');
  panel.classList.remove('hidden');
  document.getElementById('detailsTitle').textContent = `Run #${id}`;
  document.getElementById('resultsBody').innerHTML = '<tr><td colspan="8" class="px-4 py-8 text-center text-sm text-slate-500">Loading results...</td></tr>';
  try {
    const payload = await api(scopedPath(`/api/iceberg/maintenance/runs/${encodeURIComponent(id)}?limit=500`));
    currentResults = Array.isArray(payload.results) ? payload.results : [];
    renderResults();
  } catch (error) {
    currentResults = [];
    document.getElementById('resultsBody').innerHTML = `<tr><td colspan="8" class="px-4 py-8 text-center text-sm text-rose-600">${esc(error.message)}</td></tr>`;
  }
  panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function initializeFilters() {
  document.getElementById('runSearch').value = initialQuery.get('q') || '';
  document.getElementById('runStatus').value = initialQuery.get('status') || '';
  document.getElementById('runOperation').value = initialQuery.get('operation') || '';
  document.getElementById('runEngine').value = initialQuery.get('engine') || '';
  const initialLimit = initialQuery.get('limit') || '50';
  document.getElementById('pageSize').value = ['25', '50', '100'].includes(initialLimit) ? initialLimit : '50';
  configureScope();
}

document.getElementById('runFilters').addEventListener('submit', (event) => {
  event.preventDefault();
  jobID = selected('jobFilter');
  runOffset = 0;
  configureScope();
  syncURL();
  load();
});
document.getElementById('clearFilters').addEventListener('click', () => {
  ['runSearch', 'runStatus', 'runOperation', 'runEngine'].forEach((id) => { document.getElementById(id).value = ''; });
  runOffset = 0;
  configureScope();
  syncURL();
  load();
});
document.getElementById('pageSize').addEventListener('change', () => {
  runOffset = 0;
  syncURL();
  load();
});
document.getElementById('previousPage').addEventListener('click', () => {
  runOffset = Math.max(0, runOffset - Number(selected('pageSize') || 50));
  syncURL();
  load();
});
document.getElementById('nextPage').addEventListener('click', () => {
  runOffset += Number(selected('pageSize') || 50);
  syncURL();
  load();
});
document.getElementById('refreshButton').addEventListener('click', load);
document.getElementById('closeDetails').addEventListener('click', () => document.getElementById('detailsPanel').classList.add('hidden'));
['resultSearch', 'resultOperation', 'resultEngine', 'resultStatus'].forEach((id) => document.getElementById(id).addEventListener('input', renderResults));

initializeFilters();
load();
setInterval(load, 30000);

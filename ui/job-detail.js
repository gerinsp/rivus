import {
  apiFetch,
  escapeHtml,
  fmtWholeNumber,
  formatDateTime,
  formatDuration,
  jobDisplay,
  progressPill,
  setInnerHTMLIfChanged,
  statusPill,
} from './core/api.js';
import { renderIcebergMaintenance } from './iceberg-maintenance.js';

let currentJobId = null;
let previousJobListTab = 'doris';
let getCurrentTab = () => 'doris';
let switchTab = () => {};
let routeChanged = () => {};
let inventoryRefreshPoll = null;

export function initJobDetail(handlers = {}) {
  currentJobId = handlers.initialJobId || currentJobId;
  previousJobListTab = handlers.initialPreviousTab || previousJobListTab;
  getCurrentTab = handlers.getCurrentTab || getCurrentTab;
  switchTab = handlers.switchTab || switchTab;
  routeChanged = handlers.routeChanged || routeChanged;
}

export function getCurrentJobId() {
  return currentJobId;
}

function isNativeIcebergJob(job) {
  return String(job?.sink_type || job?.config?.sink?.type || '').trim().toLowerCase() === 'iceberg_native'
    && !!job?.iceberg_maintenance?.enabled;
}

async function queueInventoryRefresh(jobId, force = false) {
  const response = await apiFetch(`/api/jobs/${encodeURIComponent(jobId)}/iceberg/inventory/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ force }),
  });
  if (!response.ok) {
    throw new Error(`Unable to queue inventory scan: ${response.status}`);
  }
  const result = await response.json();
  if (Number(result?.requested || 0) > 0) {
    scheduleInventoryRefreshPoll(jobId);
  }
  return result;
}

function scheduleInventoryRefreshPoll(jobId) {
  if (inventoryRefreshPoll) clearTimeout(inventoryRefreshPoll);
  let attempts = 0;
  const poll = async () => {
    if (currentJobId !== jobId || getCurrentTab() !== 'job') return;
    attempts += 1;
    await refreshGraph({ silent: true });
    if (attempts < 8) {
      inventoryRefreshPoll = setTimeout(poll, 5000);
    }
  };
  inventoryRefreshPoll = setTimeout(poll, 5000);
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
  const checkpointPositionText = String(progress.checkpoint_position || '').trim();
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
          <div class="mt-1">Reason: <span class="mono">${escapeHtml(checkpointReason || 'pending_events')}</span>${checkpointPositionText ? ` | Position: <span class="mono">${escapeHtml(checkpointPositionText)}</span>` : ''}</div>
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
    case 'blue': return 'text-blue-900';
    case 'amber': return 'text-amber-900';
    case 'rose': return 'text-rose-800';
    case 'emerald': return 'text-emerald-800';
    default: return 'text-slate-900';
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
    case 'warning': return 'border-amber-200 bg-amber-50/35';
    case 'error': return 'border-rose-200 bg-rose-50/40';
    default: return 'border-slate-200 bg-white';
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
    case 'warning': return 'border-amber-200 bg-amber-50 text-amber-900';
    case 'error': return 'border-rose-200 bg-rose-50 text-rose-900';
    default: return 'border-slate-200 bg-slate-50 text-slate-700';
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

function renderGraphNodeCard(node) {
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
  const start = nodes.find((n) => !toSet.has(n.id)) || nodes[0];
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

  setInnerHTMLIfChanged(canvas, `
    ${progressBlock}
    <div class="${progressBlock ? 'mt-6 ' : ''}grid gap-4 xl:grid-cols-3">
      ${ordered.map(renderGraphNodeCard).join('')}
    </div>
    ${renderGraphFlowSummary(ordered)}
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
  return { prefix: match[1], ordinal: Number(match[2]) };
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
    case 'available': return 'border-emerald-200 bg-emerald-50 text-emerald-800';
    case 'purged':
    case 'missing': return 'border-rose-200 bg-rose-50 text-rose-800';
    default: return 'border-amber-200 bg-amber-50 text-amber-800';
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
  const range = diag.earliest_file || diag.latest_file ? `${diag.earliest_file || '-'} -> ${diag.latest_file || '-'}` : '-';
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
  const checkpointAgeLabel = Number.isFinite(checkpointAge) && checkpointAge >= 0 ? formatDuration(checkpointAge) : '-';
  const earliestFirstEventAt = String(diag.earliest_first_event_at || '').trim();
  const earliestFirstEventType = String(diag.earliest_first_event || '').trim();
  const earliestFirstEventError = String(diag.earliest_first_event_error || '').trim();
  const earliestFirstEventGap = Number(diag.checkpoint_to_earliest_first_event_sec);
  const earliestFirstEventDetail = [];
  if (earliestFirstEventType) earliestFirstEventDetail.push(`event=${earliestFirstEventType}`);
  if (Number.isFinite(earliestFirstEventGap)) {
    const gap = formatDuration(Math.abs(earliestFirstEventGap));
    earliestFirstEventDetail.push(earliestFirstEventGap >= 0 ? `history starts ${gap} after checkpoint save` : `checkpoint saved ${gap} after history starts`);
  }
  if (earliestFirstEventError) earliestFirstEventDetail.push(`unavailable: ${earliestFirstEventError}`);

  const latestFirstEventAt = String(diag.latest_first_event_at || diag.latest_created_at || '').trim();
  const latestFirstEventType = String(diag.latest_first_event || '').trim();
  const latestFirstEventError = String(diag.latest_first_event_error || '').trim();
  const latestFirstEventGap = Number(diag.checkpoint_to_latest_first_event_sec);
  const latestFirstEventDetail = [];
  if (latestFirstEventType) latestFirstEventDetail.push(`event=${latestFirstEventType}`);
  if (Number.isFinite(latestFirstEventGap)) {
    const gap = formatDuration(Math.abs(latestFirstEventGap));
    latestFirstEventDetail.push(latestFirstEventGap >= 0 ? `latest starts ${gap} after checkpoint save` : `checkpoint saved ${gap} after latest starts`);
  }
  if (latestFirstEventError) latestFirstEventDetail.push(`unavailable: ${latestFirstEventError}`);

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
        <div><div class="text-[11px] font-semibold uppercase">Saved At</div><div class="mono mt-1 text-xs">${escapeHtml(checkpointTime(diag.checkpoint_updated_at))}</div><div class="mt-1 text-xs">Age <span class="mono">${escapeHtml(checkpointAgeLabel)}</span></div></div>
        <div><div class="text-[11px] font-semibold uppercase">CDC Lag</div><div class="mono mt-1 text-xs">${escapeHtml(cdcLagLabel)}</div>${cdcLagDetail ? `<div class="mt-1 text-xs">${escapeHtml(cdcLagDetail)}</div>` : ''}</div>
        <div><div class="text-[11px] font-semibold uppercase">Available Binlogs</div><div class="mono mt-1 text-xs break-all">${escapeHtml(range)}</div></div>
        <div><div class="text-[11px] font-semibold uppercase">History First Event</div><div class="mono mt-1 text-xs">${escapeHtml(earliestFirstEventAt ? checkpointTime(earliestFirstEventAt) : 'Not available')}</div>${earliestFirstEventDetail.length ? `<div class="mt-1 text-xs">${escapeHtml(earliestFirstEventDetail.join(' | '))}</div>` : ''}</div>
        <div><div class="text-[11px] font-semibold uppercase">Latest First Event</div><div class="mono mt-1 text-xs">${escapeHtml(latestFirstEventAt ? checkpointTime(latestFirstEventAt) : 'Not available')}</div>${latestFirstEventDetail.length ? `<div class="mt-1 text-xs">${escapeHtml(latestFirstEventDetail.join(' | '))}</div>` : ''}<div class="mt-1 text-xs">Observed <span class="mono">${escapeHtml(checkpointTime(diag.source_server_time || diag.observed_at))}</span></div></div>
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
        <div><div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Checkpoint</div><div class="mt-2 text-sm text-slate-500">No saved checkpoint yet.</div></div>
        ${metaKey ? `<div class="mono max-w-full rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-[11px] text-slate-600 break-all">${escapeHtml(metaKey)}</div>` : ''}
      </div>
    `);
    return;
  }

  if (checkpoint.error) {
    setInnerHTMLIfChanged(panel, `
      <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Checkpoint</div>
      <div class="mt-3 rounded-[16px] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">Failed to load checkpoint: ${escapeHtml(checkpoint.error)}</div>
      ${metaKey ? `<div class="mono mt-3 text-[11px] text-slate-500 break-all">${escapeHtml(metaKey)}</div>` : ''}
    `);
    return;
  }

  const cards = [];
  const cdc = checkpoint.cdc_offset;
  if (cdc) cards.push(checkpointCard('CDC Offset', checkpointPosition(cdc.binlog_file, cdc.binlog_pos), cdc.updated_at, 'Last sink-committed binlog position.'));
  const snapshot = checkpoint.snapshot_state;
  if (snapshot) {
    cards.push(checkpointCard('Snapshot Start', checkpointPosition(snapshot.start_file, snapshot.start_pos), snapshot.updated_at, snapshot.done ? 'Snapshot marked done.' : 'Snapshot not marked done yet.'));
  }
  const progress = checkpoint.snapshot_progress;
  if (progress) cards.push(checkpointCard('Snapshot Progress', String(progress.table_name || '-'), progress.updated_at, `Next offset ${fmtWholeNumber(progress.next_offset || 0)}`));

  setInnerHTMLIfChanged(panel, `
    <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
      <div><div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Checkpoint</div><div class="mt-2 text-sm text-slate-500">Saved metadata for resume decisions.</div></div>
      ${metaKey ? `<div class="mono max-w-full rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-[11px] text-slate-600 break-all">${escapeHtml(metaKey)}</div>` : ''}
    </div>
    <div class="mt-4 grid gap-3 lg:grid-cols-3">${cards.length > 0 ? cards.join('') : '<div class="rounded-[16px] border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-500">No checkpoint rows found.</div>'}</div>
    ${renderBinlogDiagnostics(checkpoint.binlog_diagnostics, !!cdc)}
  `);
}

function renderJobDetailHeader(job) {
  const display = jobDisplay(job?.name || job?.id || '-');
  const errorCount = Array.isArray(job?.errors) ? job.errors.length : Number(job?.error_count || 0);
  const sinkType = String(job?.sink_type || job?.config?.sink?.type || job?.config?.Sink?.type || '').trim();

  document.getElementById('jobDetailTitle').textContent = display.title || '-';
  document.getElementById('jobDetailSubtitle').textContent = display.detail || String(job?.name || '').trim();
  document.getElementById('jobDetailId').textContent = job?.id || currentJobId || '-';
  document.getElementById('jobDetailSink').textContent = sinkType || '-';
  document.getElementById('jobDetailCreated').textContent = formatDateTime(job?.created);
  document.getElementById('jobDetailUpdated').textContent = formatDateTime(job?.updated);
  document.getElementById('jobDetailErrors').textContent = Number.isFinite(errorCount) ? `${fmtWholeNumber(errorCount)} recorded` : '-';
}

export async function refreshGraph(options = {}) {
  const jobId = currentJobId;
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
    if (checkpointPanel) setInnerHTMLIfChanged(checkpointPanel, `<div class="text-sm text-slate-500">Loading checkpoint...</div>`);
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
    renderIcebergMaintenance(job, {
      onRefreshInventory: async () => {
        await queueInventoryRefresh(jobId, true);
        await refreshGraph({ silent: true });
      },
    });
  } else {
    renderJobDetailHeader({ id: jobId });
    renderIcebergMaintenance({ id: jobId });
    if (checkpointPanel) {
      setInnerHTMLIfChanged(checkpointPanel, `<div class="rounded-[16px] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">Failed to load checkpoint: ${jobRes.status}</div>`);
    }
  }

  if (!res.ok) {
    metaEl.textContent = 'Unavailable';
    setInnerHTMLIfChanged(canvas, `<div class="rounded-[18px] border border-rose-200 bg-rose-50 p-5 text-sm text-rose-700">Failed to load graph: ${res.status}</div>`);
    return null;
  }

  const graph = await res.json();
  const progressSummary = String(graph?.progress?.summary || '').trim();
  const runtimeState = String((Array.isArray(graph?.nodes) ? graph.nodes.find((node) => String(node?.type || '').toLowerCase() === 'buffer')?.state : '') || '').trim();
  statusEl.textContent = graph.status || job?.status || '-';
  rawEl.textContent = JSON.stringify(graph, null, 2);
  metaEl.textContent = progressSummary || runtimeState || 'Graph ready';
  renderGraph(graph);
  return job;
}

export async function showJobDetails(jobId) {
  currentJobId = jobId;
  const currentTab = getCurrentTab();
  if (currentTab !== 'job') previousJobListTab = currentTab;
  switchTab('job', { updateUrl: false });
  routeChanged();
  document.getElementById('jobDetailId').textContent = jobId;
  document.getElementById('jobDetailTitle').textContent = 'Loading job...';
  document.getElementById('jobDetailSubtitle').textContent = '';
  document.getElementById('graphStatus').textContent = '-';
  document.getElementById('graphRaw').textContent = '';
  const job = await refreshGraph();
  if (isNativeIcebergJob(job)) {
    try {
      await queueInventoryRefresh(jobId);
      await refreshGraph({ silent: true });
    } catch (error) {
      console.warn('Unable to queue Iceberg inventory refresh', error);
    }
  }
}

export function backToJobs() {
  const tab = previousJobListTab === 'iceberg' || previousJobListTab === 'logs' ? previousJobListTab : 'doris';
  switchTab(tab);
}

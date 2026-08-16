import { apiFetch, escapeHtml, setModalState } from './core/api.js';

let currentErrorsJobId = null;
let initialized = false;

export function initErrors() {
  if (initialized) return;
  initialized = true;
  document.addEventListener('click', (event) => {
    const button = event.target.closest('[data-error-stack]');
    if (!button) return;
    document.getElementById(button.dataset.errorStack)?.classList.toggle('hidden');
  });
}

function openErrorsModal() {
  setModalState('errorsModal', true);
}

export function closeErrorsModal() {
  setModalState('errorsModal', false);
  currentErrorsJobId = null;
  const meta = document.getElementById('errorsMeta');
  if (meta) meta.textContent = 'Waiting for data';
}

export async function refreshErrors() {
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
              <span class="rounded-full border px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] ${badgeClass}">${escapeHtml(comp)}</span>
              <span class="mono text-xs text-slate-500">${time}</span>
            </div>
            <button type="button" data-error-stack="${stackId}" class="rounded-full border border-slate-200 bg-white px-4 py-2 text-xs font-semibold text-slate-600 transition hover:bg-slate-50">Toggle Stack</button>
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

export async function showErrors(jobId) {
  currentErrorsJobId = jobId;
  document.getElementById('errorsJobId').textContent = jobId;
  openErrorsModal();
  await refreshErrors();
}

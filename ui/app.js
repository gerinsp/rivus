import {
  isAnyModalOpen,
  loadAppVersion,
  loadAuthStatus,
  loadMetrics,
  logout,
  setProfileMenu,
  toggleProfileMenu,
} from './core/api.js';
import {
  clearYaml,
  closeSubmitModal,
  initJobs,
  isBulkBusy,
  loadJobs,
  loadYamlFile,
  openSubmitModal,
  submitJob,
} from './jobs.js';
import {
  changeLogFollowLatest,
  changeLogLineLimit,
  downloadSelectedLog,
  getLogRouteState,
  initLogs,
  loadLogs,
  refreshLogs,
  renderLogOutput,
  selectLogFile,
  showLogsForJob,
} from './logs.js';
import {
  backToJobs,
  getCurrentJobId,
  initJobDetail,
  refreshGraph,
  showJobDetails,
} from './job-detail.js';
import { closeErrorsModal, initErrors, refreshErrors, showErrors } from './errors.js';

const initialUrl = new URL(window.location.href);
const initialJobId = initialUrl.searchParams.get('tab') === 'job'
  ? (initialUrl.searchParams.get('id') || null)
  : null;

let currentTab = initialJobId
  ? 'job'
  : initialUrl.searchParams.get('tab') === 'iceberg'
    ? 'iceberg'
    : initialUrl.searchParams.get('tab') === 'logs'
      ? 'logs'
      : 'doris';
let refreshInFlight = false;

function updateTabUrl() {
  const url = new URL(window.location.href);

  if (currentTab === 'job') {
    url.searchParams.set('tab', 'job');
    const jobId = getCurrentJobId();
    if (jobId) url.searchParams.set('id', jobId);
    else url.searchParams.delete('id');
    url.searchParams.delete('log');
    url.searchParams.delete('lines');
    url.searchParams.delete('follow');
  } else if (currentTab === 'logs') {
    const { currentLogFile, followLatestLog, lineLimit } = getLogRouteState();
    url.searchParams.set('tab', 'logs');
    url.searchParams.delete('id');
    if (currentLogFile) url.searchParams.set('log', currentLogFile);
    else url.searchParams.delete('log');
    if (followLatestLog) url.searchParams.set('follow', 'latest');
    else url.searchParams.delete('follow');
    if (lineLimit !== 500) url.searchParams.set('lines', String(lineLimit));
    else url.searchParams.delete('lines');
  } else {
    url.searchParams.set('tab', currentTab === 'iceberg' ? 'iceberg' : 'doris');
    url.searchParams.delete('id');
    url.searchParams.delete('log');
    url.searchParams.delete('lines');
    url.searchParams.delete('follow');
  }

  history.replaceState({}, '', url);
}

export function switchTab(tab, options = {}) {
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
    document.getElementById(id)?.classList.toggle('hidden', hidden);
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

  if (options.updateUrl !== false) updateTabUrl();
  if (logsActive && options.load !== false) loadLogs();
}

export async function refreshDashboard(options = {}) {
  if (options.auto && isAnyModalOpen()) return;
  if (options.auto && isBulkBusy()) return;
  if (refreshInFlight) return;
  refreshInFlight = true;

  document.querySelectorAll('[data-refresh-button]').forEach((button) => {
    button.disabled = true;
    button.classList.add('cursor-wait', 'opacity-80');
  });

  try {
    const tasks = [loadJobs(), loadMetrics()];
    if (currentTab === 'logs') tasks.push(refreshLogs({ auto: !!options.auto }));
    if (currentTab === 'job' && getCurrentJobId()) tasks.push(refreshGraph({ silent: !!options.auto }));
    await Promise.allSettled(tasks);
  } finally {
    document.querySelectorAll('[data-refresh-button]').forEach((button) => {
      button.disabled = false;
      button.classList.remove('cursor-wait', 'opacity-80');
    });
    refreshInFlight = false;
  }
}

function installCompatibilityGlobals() {
  // index.html still uses a small number of inline handlers. Keep those stable
  // while feature implementation lives in ES modules.
  Object.assign(window, {
    switchTab,
    refreshDashboard,
    toggleProfileMenu,
    logout,
    openSubmitModal,
    closeSubmitModal,
    clearYaml,
    loadYamlFile,
    submitJob,
    closeErrorsModal,
    refreshErrors,
    refreshGraph,
    backToJobs,
    loadLogs,
    downloadSelectedLog,
    selectLogFile,
    changeLogLineLimit,
    renderLogOutput,
    changeLogFollowLatest,
  });
}

function bindShellEvents() {
  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') return;

    const profileDropdown = document.getElementById('profileDropdown');
    if (profileDropdown && !profileDropdown.classList.contains('hidden')) {
      setProfileMenu(false);
      return;
    }
    const errorsModal = document.getElementById('errorsModal');
    if (errorsModal && !errorsModal.classList.contains('hidden')) {
      closeErrorsModal();
      return;
    }
    const submitModal = document.getElementById('submitModal');
    if (submitModal && !submitModal.classList.contains('hidden')) closeSubmitModal();
  });

  document.addEventListener('click', (event) => {
    const profileMenu = document.getElementById('profileMenu');
    if (profileMenu && !profileMenu.contains(event.target)) setProfileMenu(false);
  });
}

function startApp() {
  initLogs({ routeChanged: updateTabUrl, switchTab });
  initJobDetail({
    initialJobId,
    initialPreviousTab: currentTab === 'job' ? 'doris' : currentTab,
    getCurrentTab: () => currentTab,
    switchTab,
    routeChanged: updateTabUrl,
  });
  initErrors();
  initJobs({ refreshDashboard, showJobDetails, showLogsForJob, showErrors });
  installCompatibilityGlobals();
  bindShellEvents();

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

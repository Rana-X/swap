const runsList = document.getElementById('runsList');
const refreshButton = document.getElementById('refreshButton');
const emptyState = document.getElementById('emptyState');
const runDetail = document.getElementById('runDetail');
const runTitle = document.getElementById('runTitle');
const runMeta = document.getElementById('runMeta');
const runStatus = document.getElementById('runStatus');
const outputVideo = document.getElementById('outputVideo');
const noOutput = document.getElementById('noOutput');
const preparedImage = document.getElementById('preparedImage');
const noImage = document.getElementById('noImage');
const preparedVideo = document.getElementById('preparedVideo');
const noPreparedVideo = document.getElementById('noPreparedVideo');
const summaryList = document.getElementById('summaryList');
const warningsList = document.getElementById('warningsList');
const apiDetails = document.getElementById('apiDetails');
const rawPreflight = document.getElementById('rawPreflight');

let runs = [];
let selectedRunId = null;

refreshButton.addEventListener('click', () => {
  void loadRuns(selectedRunId);
});

void loadRuns();

async function loadRuns(preferredRunId) {
  emptyState.textContent = 'Loading runs…';
  emptyState.classList.remove('hidden');
  runDetail.classList.add('hidden');

  const response = await fetch('/api/runs');
  const payload = await response.json();
  runs = payload.runs ?? [];

  if (runs.length === 0) {
    runsList.innerHTML = '';
    emptyState.textContent = 'No runs found yet.';
    return;
  }

  selectedRunId = preferredRunId && runs.some((run) => run.id === preferredRunId)
    ? preferredRunId
    : runs[0].id;

  renderRunList();
  renderRunDetail(runs.find((run) => run.id === selectedRunId));
}

function renderRunList() {
  runsList.innerHTML = '';

  for (const run of runs) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `run-item ${run.id === selectedRunId ? 'active' : ''}`;
    button.innerHTML = `
      <div class="run-item-title">${escapeHtml(run.id)}</div>
      <div class="run-item-meta">${escapeHtml(formatStatus(run.status))}${run.createdAt ? ` • ${escapeHtml(new Date(run.createdAt).toLocaleString())}` : ''}</div>
    `;
    button.addEventListener('click', () => {
      selectedRunId = run.id;
      renderRunList();
      renderRunDetail(run);
    });
    runsList.appendChild(button);
  }
}

function renderRunDetail(run) {
  if (!run) {
    emptyState.textContent = 'Run not found.';
    emptyState.classList.remove('hidden');
    runDetail.classList.add('hidden');
    return;
  }

  emptyState.classList.add('hidden');
  runDetail.classList.remove('hidden');

  runTitle.textContent = run.id;
  runMeta.textContent = run.createdAt ? new Date(run.createdAt).toLocaleString() : 'Unknown timestamp';
  runStatus.textContent = formatStatus(run.status);
  runStatus.className = `status-pill status-${run.status}`;

  setMedia(outputVideo, noOutput, run.files.outputVideo);
  setMedia(preparedVideo, noPreparedVideo, run.files.preparedVideo);
  setImage(preparedImage, noImage, run.files.preparedImage);

  summaryList.innerHTML = '';
  const summaryItems = [
    ['Ratio', run.ratio ?? '—'],
    ['Source video', run.sourceVideo ?? '—'],
    ['Source image', run.sourceImage ?? '—'],
    ['Prepared trim', run.preparation ? `${run.preparation.trimStartSeconds}s for ${run.preparation.trimDurationSeconds}s` : '—'],
    ['Auto crop', run.preparation?.autoCropApplied ? 'true' : 'false'],
    ['Auto trim', run.preparation?.autoTrimApplied ? 'true' : 'false']
  ];

  for (const [label, value] of summaryItems) {
    const dt = document.createElement('dt');
    dt.textContent = label;
    const dd = document.createElement('dd');
    dd.textContent = String(value);
    summaryList.appendChild(dt);
    summaryList.appendChild(dd);
  }

  warningsList.innerHTML = '';
  if ((run.warnings ?? []).length === 0) {
    const item = document.createElement('li');
    item.textContent = 'No warnings.';
    warningsList.appendChild(item);
  } else {
    for (const warning of run.warnings) {
      const item = document.createElement('li');
      item.textContent = warning;
      warningsList.appendChild(item);
    }
  }

  const apiPayload = run.runwayTask ?? run.requestFailed ?? run.taskFailed ?? { note: 'No API payload for this run.' };
  apiDetails.textContent = JSON.stringify(apiPayload, null, 2);
  rawPreflight.textContent = JSON.stringify(run, null, 2);
}

function setMedia(videoEl, fallbackEl, src) {
  if (!src) {
    videoEl.classList.add('hidden');
    videoEl.removeAttribute('src');
    videoEl.load();
    fallbackEl.classList.remove('hidden');
    return;
  }

  videoEl.src = src;
  videoEl.classList.remove('hidden');
  fallbackEl.classList.add('hidden');
}

function setImage(imgEl, fallbackEl, src) {
  if (!src) {
    imgEl.classList.add('hidden');
    imgEl.removeAttribute('src');
    fallbackEl.classList.remove('hidden');
    return;
  }

  imgEl.src = src;
  imgEl.classList.remove('hidden');
  fallbackEl.classList.add('hidden');
}

function formatStatus(status) {
  switch (status) {
    case 'succeeded':
      return 'Succeeded';
    case 'failed':
      return 'Failed';
    case 'preflight_only':
      return 'Preflight only';
    default:
      return status ?? 'Unknown';
  }
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

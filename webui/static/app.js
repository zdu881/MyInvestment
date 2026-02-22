const LOCALE_STORAGE_KEY = 'myinvestment_locale';
const DEFAULT_LOCALE = 'zh-CN';
const LOCALE_BASE_PATH = '/static/locales';
const localeDicts = {};

const state = {
  activeView: 'action-center',
  locale: DEFAULT_LOCALE,
  proposals: [],
  executions: [],
  runs: [],
};

function byId(id) {
  return document.getElementById(id);
}

function getLocalePath(locale) {
  return `${LOCALE_BASE_PATH}/${encodeURIComponent(locale)}.json`;
}

function getDict(locale) {
  return localeDicts[normalizeLocale(locale)] || {};
}

async function loadLocaleDict(locale) {
  const normalized = normalizeLocale(locale);
  if (localeDicts[normalized]) return;

  const resp = await fetch(getLocalePath(normalized), { cache: 'no-cache' });
  if (!resp.ok) {
    throw new Error(`failed to load locale ${normalized}: HTTP ${resp.status}`);
  }

  const dict = await resp.json();
  localeDicts[normalized] = dict && typeof dict === 'object' ? dict : {};
}

async function ensureLocaleDicts(locale) {
  await loadLocaleDict(DEFAULT_LOCALE);
  const normalized = normalizeLocale(locale);
  if (normalized !== DEFAULT_LOCALE) {
    await loadLocaleDict(normalized);
  }
}

function hasTranslationKey(key) {
  return Boolean(
    Object.prototype.hasOwnProperty.call(getDict(state.locale), key)
      || Object.prototype.hasOwnProperty.call(getDict(DEFAULT_LOCALE), key)
  );
}

function t(key, vars = {}) {
  const dict = getDict(state.locale);
  const fallback = getDict(DEFAULT_LOCALE);
  const template = dict[key] ?? fallback[key] ?? key;
  return String(template).replaceAll(/\{([A-Za-z0-9_]+)\}/g, (match, name) => {
    if (!Object.prototype.hasOwnProperty.call(vars, name)) return match;
    return String(vars[name]);
  });
}

function normalizeLocale(locale) {
  const value = String(locale || '').toLowerCase();
  if (value.startsWith('en')) return 'en-US';
  return 'zh-CN';
}

function detectInitialLocale() {
  const stored = localStorage.getItem(LOCALE_STORAGE_KEY);
  if (stored) return normalizeLocale(stored);
  return normalizeLocale(navigator.language || DEFAULT_LOCALE);
}

function escapeHtml(text) {
  return String(text || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function notify(message, isError = false) {
  const toast = byId('toast');
  toast.textContent = message;
  toast.style.borderColor = isError ? 'rgba(240,93,94,0.7)' : 'rgba(145,188,224,0.4)';
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 2800);
}

function applyI18nToDom() {
  document.title = t('app.title');
  document.documentElement.lang = state.locale;

  document.querySelectorAll('[data-i18n]').forEach((node) => {
    node.textContent = t(node.dataset.i18n);
  });

  document.querySelectorAll('[data-i18n-placeholder]').forEach((node) => {
    node.placeholder = t(node.dataset.i18nPlaceholder);
  });

  const langSelect = byId('languageSelect');
  if (langSelect) {
    langSelect.value = state.locale;
  }
}

async function setLocale(locale, options = {}) {
  const { persist = true, rerender = true } = options;
  state.locale = normalizeLocale(locale);

  await ensureLocaleDicts(state.locale);

  if (persist) {
    localStorage.setItem(LOCALE_STORAGE_KEY, state.locale);
  }

  applyI18nToDom();

  if (rerender) {
    await loadCurrentView();
  }
}

function displayLevel(level) {
  const normalized = String(level || '').toLowerCase();
  const key = `level.${normalized}`;
  if (!hasTranslationKey(key)) return String(level || '-');
  return t(key);
}

function displayEvent(event) {
  const normalized = String(event || '').toLowerCase();
  const key = `event.${normalized}`;
  if (!hasTranslationKey(key)) return String(event || '-');
  return t(key);
}

function displayStatus(status) {
  const normalized = String(status || '').toLowerCase();
  const statusKey = `status.${normalized}`;
  if (hasTranslationKey(statusKey)) return t(statusKey);
  const levelKey = `level.${normalized}`;
  if (hasTranslationKey(levelKey)) return t(levelKey);
  return String(status || '-');
}

function getToken() {
  return localStorage.getItem('myinvestment_api_token') || '';
}

async function request(path, options = {}) {
  const token = getToken();
  const headers = { ...(options.headers || {}) };
  if (token) headers['X-API-Token'] = token;
  if (options.body && !headers['Content-Type']) headers['Content-Type'] = 'application/json';

  const resp = await fetch(path, { ...options, headers });
  let body;
  try {
    body = await resp.json();
  } catch {
    body = {};
  }
  if (!resp.ok) {
    const msg = body.message || body.detail?.message || `HTTP ${resp.status}`;
    throw new Error(msg);
  }
  return body;
}

function setView(view) {
  state.activeView = view;
  document.querySelectorAll('.tab').forEach((tab) => {
    tab.classList.toggle('active', tab.dataset.view === view);
  });
  document.querySelectorAll('.view').forEach((v) => {
    v.classList.toggle('active', v.id === `view-${view}`);
  });
  loadCurrentView();
}

function renderList(containerId, items, renderItem) {
  const container = byId(containerId);
  if (!items || items.length === 0) {
    container.innerHTML = `<div class="item mono">${escapeHtml(t('common.none'))}</div>`;
    return;
  }
  container.innerHTML = `<div class="list">${items.map(renderItem).join('')}</div>`;
}

function toTag(level) {
  if (level === 'critical') return 'tag critical';
  if (level === 'warn') return 'tag warn';
  return 'tag ok';
}

async function loadActionCenter() {
  const data = await request('/api/action-center');
  const o = data.overview || {};
  const kpis = [
    [t('kpi.health'), `${o.health_score ?? '-'} (${o.health_label ?? '-'})`],
    [t('kpi.alerts'), `${o.alert_status ?? '-'} / ${o.active_alert_count ?? 0}`],
    [t('kpi.pendingReview'), String(o.pending_review_count ?? 0)],
    [t('kpi.pendingExecution'), String(o.pending_execution_count ?? 0)],
    [t('kpi.qualitySample'), String(o.quality_sample_size ?? 0)],
    [t('kpi.qualityAvg'), String(o.quality_avg_score ?? 0)],
  ];
  byId('kpiGrid').innerHTML = kpis
    .map(([k, v]) => `<div class="kpi"><div class="label">${escapeHtml(k)}</div><div class="value">${escapeHtml(v)}</div></div>`)
    .join('');

  renderList('acAlerts', data.active_alerts || [], (row) => `
    <div class="item mono">
      <span class="${toTag(row.level)}">${escapeHtml(displayLevel(row.level))}</span>
      <div><b>${escapeHtml(row.check_id)}</b></div>
      <div>${escapeHtml(row.message)}</div>
    </div>
  `);

  renderList('acReviews', data.pending_review || [], (row) => `
    <div class="item mono">
      <div><b>${escapeHtml(row.run_id)}</b></div>
      <div>${escapeHtml(row.proposal_id)} | ${escapeHtml(row.suggested_decision)}</div>
      <button class="btn" onclick="setView('proposal-review'); selectProposal('${escapeHtml(row.run_id)}')">${escapeHtml(t('common.review'))}</button>
    </div>
  `);

  renderList('acExecutions', data.pending_execution || [], (row) => `
    <div class="item mono">
      <div><b>${escapeHtml(row.queue_id)}</b></div>
      <div>${escapeHtml(t('actionCenter.executionRow', { runId: row.run_id, orderCount: row.order_count }))}</div>
      <button class="btn" onclick="setView('execution-queue'); selectExecution('${escapeHtml(row.run_id)}')">${escapeHtml(t('common.execute'))}</button>
    </div>
  `);
}

async function loadProposals() {
  const data = await request('/api/proposals/pending');
  state.proposals = data.items || [];
  renderList('proposalList', state.proposals, (row) => `
    <div class="item mono">
      <div><b>${escapeHtml(row.run_id)}</b></div>
      <div>${escapeHtml(row.proposal_id)} | ${escapeHtml(row.suggested_decision)}</div>
      <button class="btn" onclick="selectProposal('${escapeHtml(row.run_id)}')">${escapeHtml(t('common.viewDetail'))}</button>
    </div>
  `);

  if (state.proposals.length > 0) {
    await selectProposal(state.proposals[0].run_id);
  } else {
    byId('proposalAdvice').textContent = '';
    byId('proposalDetail').innerHTML = `<div class="mono">${escapeHtml(t('common.none'))}</div>`;
  }
}

async function selectProposal(runId) {
  const data = await request(`/api/proposals/${runId}`);
  byId('reviewRunId').value = runId;
  byId('proposalAdvice').textContent = data.advice_report || '';

  const p = data.proposal || {};
  const detailRows = [
    [t('field.proposalId'), p.proposal_id],
    [t('field.decision'), p.decision],
    [t('field.reviewStatus'), p.review_status],
    [t('field.evidence'), p.evidence_completeness],
    [t('field.turnoverEst'), p.turnover_est],
    [t('field.costEst'), p.transaction_cost_est],
    [t('field.gateFailures'), (p.gate_failures || []).join(', ')],
    [t('field.constraintViolations'), (p.constraint_violations || []).join(', ')],
  ];
  const actions = (data.rebalance_actions || []).filter((x) => x.action && x.action !== 'HOLD');
  byId('proposalDetail').innerHTML = `
    <div class="item mono">${detailRows
      .map(([k, v]) => `<div><b>${escapeHtml(k)}</b>: ${escapeHtml(v)}</div>`)
      .join('')}</div>
    <div class="item mono">
      <div><b>${escapeHtml(t('proposal.actions'))}</b></div>
      ${actions.length === 0 ? `<div>${escapeHtml(t('common.none'))}</div>` : actions
        .map((a) => `<div>${escapeHtml(a.action)} ${escapeHtml(a.ticker)} ${escapeHtml(a.current_weight)} -> ${escapeHtml(a.target_weight)}</div>`)
        .join('')}
    </div>
  `;
}

async function loadExecutions() {
  const data = await request('/api/executions/pending');
  state.executions = data.items || [];
  renderList('executionList', state.executions, (row) => `
    <div class="item mono">
      <div><b>${escapeHtml(row.run_id)}</b></div>
      <div>${escapeHtml(t('execution.queueRow', { queueId: row.queue_id }))}</div>
      <div>${escapeHtml(t('execution.orderRow', { count: row.order_count, createdAt: row.created_at }))}</div>
      <button class="btn" onclick="selectExecution('${escapeHtml(row.run_id)}')">${escapeHtml(t('common.detail'))}</button>
    </div>
  `);

  if (state.executions.length > 0) {
    await selectExecution(state.executions[0].run_id);
  } else {
    byId('executionDetail').innerHTML = `<div class="mono">${escapeHtml(t('common.none'))}</div>`;
  }
}

async function selectExecution(runId) {
  byId('executeRunId').value = runId;
  const proposal = await request(`/api/proposals/${runId}`);
  const actions = proposal.rebalance_actions || [];
  byId('executionDetail').innerHTML = `
    <div class="item mono">
      <div><b>${escapeHtml(t('field.runId'))}</b>: ${escapeHtml(runId)}</div>
      <div><b>${escapeHtml(t('field.proposalDecision'))}</b>: ${escapeHtml(proposal.proposal?.decision)}</div>
      <div><b>${escapeHtml(t('field.reviewStatus'))}</b>: ${escapeHtml(proposal.proposal?.review_status)}</div>
      <div><b>${escapeHtml(t('field.orders'))}</b>: ${escapeHtml(actions.filter((x) => x.action !== 'HOLD').length)}</div>
    </div>
    <div class="item mono">
      ${(actions || []).map((a) => `<div>${escapeHtml(a.action)} ${escapeHtml(a.ticker)} ${escapeHtml(a.delta_weight)}</div>`).join('') || escapeHtml(t('common.none'))}
    </div>
  `;
}

async function loadRuns() {
  const data = await request('/api/runs?limit=60');
  state.runs = data.items || [];
  renderList('runsList', state.runs, (row) => `
    <div class="item mono">
      <div><b>${escapeHtml(row.run_id)}</b></div>
      <div>${escapeHtml(row.trading_date)} | ${escapeHtml(row.phase)} | ${escapeHtml(row.status)}</div>
      <button class="btn" onclick="selectRun('${escapeHtml(row.run_id)}')">${escapeHtml(t('common.open'))}</button>
    </div>
  `);

  if (state.runs.length > 0) {
    await selectRun(state.runs[0].run_id);
  } else {
    byId('runManifest').textContent = '';
    byId('artifactList').innerHTML = `<div class="mono">${escapeHtml(t('common.none'))}</div>`;
    byId('artifactContent').textContent = '';
  }
}

async function selectRun(runId) {
  const manifest = await request(`/api/runs/${runId}`);
  byId('runManifest').textContent = JSON.stringify(manifest, null, 2);

  const artifactsResp = await request(`/api/runs/${runId}/artifacts`);
  const artifacts = artifactsResp.items || [];
  renderList('artifactList', artifacts, (a) => `
    <div class="item mono">
      <div><b>${escapeHtml(a.name)}</b> (${escapeHtml(a.kind)})</div>
      <button class="btn" onclick="readArtifact('${escapeHtml(runId)}', '${escapeHtml(a.name)}')">${escapeHtml(t('common.view'))}</button>
    </div>
  `);

  if (artifacts.length > 0) {
    await readArtifact(runId, artifacts[0].name);
  } else {
    byId('artifactContent').textContent = '';
  }
}

async function readArtifact(runId, artifact) {
  const data = await request(`/api/runs/${runId}/artifact-content?artifact=${encodeURIComponent(artifact)}`);
  byId('artifactContent').textContent = data.content || '';
}

async function loadAlertsOps() {
  const alerts = await request('/api/alerts');
  const events = await request('/api/alerts/events?limit=40');
  const ops = await request('/api/ops/report');

  byId('alertsSummary').innerHTML = `
    <div class="item mono"><b>${escapeHtml(t('alerts.status'))}</b>: <span class="${toTag(alerts.status)}">${escapeHtml(displayStatus(alerts.status))}</span></div>
    <div class="item mono"><b>${escapeHtml(t('alerts.active'))}</b>: ${escapeHtml(alerts.active_alert_count)}</div>
    <div class="item mono">${(alerts.active_alerts || []).map((a) => `[${escapeHtml(displayLevel(a.level))}] ${escapeHtml(a.check_id)}`).join('<br/>') || escapeHtml(t('common.none'))}</div>
  `;

  renderList('alertsEvents', events.items || [], (e) => `
    <div class="item mono">
      <div>${escapeHtml(e.timestamp)} <span class="${toTag(e.level)}">${escapeHtml(displayEvent(e.event))}</span></div>
      <div>${escapeHtml(e.check_id)} ${escapeHtml(e.message)}</div>
    </div>
  `);

  byId('opsReport').textContent = JSON.stringify(ops, null, 2);
}

async function loadQuality() {
  const q = await request('/api/quality/latest');
  byId('qualitySummary').textContent = JSON.stringify(
    {
      generated_at: q.generated_at,
      sample_size: q.sample_size,
      avg_quality_score: q.avg_quality_score,
      avg_cost_ratio: q.avg_cost_ratio,
      model_feedback: q.model_feedback,
    },
    null,
    2
  );

  const low = (q.quality_rows || []).filter((r) => Number(r.quality_score || 0) < 0.6);
  renderList('qualityCases', low, (r) => `
    <div class="item mono">
      <div><b>${escapeHtml(r.run_id)}</b> | ${escapeHtml(r.quality_score)}</div>
      <div>${escapeHtml(t('quality.caseMeta', { costRatio: r.cost_ratio, warningCount: r.warning_count }))}</div>
    </div>
  `);
}

async function loadConfig() {
  const cfg = await request('/api/config');
  byId('configView').textContent = JSON.stringify(cfg, null, 2);
}

async function loadCurrentView() {
  try {
    if (state.activeView === 'action-center') await loadActionCenter();
    if (state.activeView === 'proposal-review') await loadProposals();
    if (state.activeView === 'execution-queue') await loadExecutions();
    if (state.activeView === 'runs') await loadRuns();
    if (state.activeView === 'alerts-ops') await loadAlertsOps();
    if (state.activeView === 'quality') await loadQuality();
    if (state.activeView === 'config') await loadConfig();
  } catch (err) {
    notify(err.message || String(err), true);
  }
}

async function bindActions() {
  byId('tabs').addEventListener('click', (e) => {
    const btn = e.target.closest('.tab');
    if (!btn) return;
    setView(btn.dataset.view);
  });

  byId('languageSelect').addEventListener('change', async (e) => {
    try {
      await setLocale(e.target.value, { persist: true, rerender: true });
    } catch (err) {
      notify(err.message || String(err), true);
    }
  });

  byId('saveTokenBtn').addEventListener('click', () => {
    const token = byId('apiTokenInput').value.trim();
    localStorage.setItem('myinvestment_api_token', token);
    notify(token ? t('token.saved') : t('token.cleared'));
  });

  byId('refreshActionCenterBtn').addEventListener('click', loadActionCenter);
  byId('refreshProposalBtn').addEventListener('click', loadProposals);
  byId('refreshExecutionBtn').addEventListener('click', loadExecutions);
  byId('refreshRunsBtn').addEventListener('click', loadRuns);
  byId('refreshOpsBtn').addEventListener('click', loadAlertsOps);
  byId('refreshQualityBtn').addEventListener('click', loadQuality);
  byId('refreshConfigBtn').addEventListener('click', loadConfig);

  byId('reviewForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const runId = byId('reviewRunId').value;
    if (!runId) return notify(t('proposal.selectFirst'), true);
    try {
      await request(`/api/reviews/${runId}`, {
        method: 'POST',
        body: JSON.stringify({
          decision: byId('reviewDecision').value,
          reviewer: byId('reviewerInput').value.trim(),
          note: byId('reviewNote').value.trim(),
        }),
      });
      notify(t('proposal.submitSuccess'));
      await loadProposals();
      await loadActionCenter();
    } catch (err) {
      notify(err.message || String(err), true);
    }
  });

  byId('executeForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const runId = byId('executeRunId').value;
    if (!runId) return notify(t('execution.selectFirst'), true);
    try {
      await request(`/api/executions/${runId}`, {
        method: 'POST',
        body: JSON.stringify({
          executor: byId('executorInput').value.trim(),
          dry_run: byId('executeDryRun').checked,
          force: byId('executeForce').checked,
        }),
      });
      notify(t('execution.submitSuccess'));
      await loadExecutions();
      await loadActionCenter();
    } catch (err) {
      notify(err.message || String(err), true);
    }
  });

  byId('configPatchForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    try {
      const patch = JSON.parse(byId('configPatchInput').value || '{}');
      await request('/api/config', { method: 'PATCH', body: JSON.stringify(patch) });
      notify(t('config.patchSuccess'));
      await loadConfig();
    } catch (err) {
      notify(err.message || String(err), true);
    }
  });

  byId('schedulerForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    try {
      await request('/api/scheduler/once', {
        method: 'POST',
        body: JSON.stringify({
          dry_run: byId('schedulerDryRun').checked,
          skip_maintenance: byId('schedulerSkipMaintenance').checked,
          skip_alerts: byId('schedulerSkipAlerts').checked,
        }),
      });
      notify(t('config.schedulerTriggered'));
      await loadActionCenter();
      await loadAlertsOps();
    } catch (err) {
      notify(err.message || String(err), true);
    }
  });

  const token = getToken();
  if (token) byId('apiTokenInput').value = token;
}

window.setView = setView;
window.selectProposal = selectProposal;
window.selectExecution = selectExecution;
window.selectRun = selectRun;
window.readArtifact = readArtifact;

state.locale = detectInitialLocale();

async function bootstrap() {
  try {
    await ensureLocaleDicts(state.locale);
  } catch (err) {
    notify(err.message || String(err), true);
  }

  applyI18nToDom();
  bindActions();
  loadCurrentView();
  setInterval(() => {
    if (state.activeView === 'action-center') {
      loadActionCenter().catch((err) => notify(err.message || String(err), true));
    }
  }, 10000);
}

bootstrap();

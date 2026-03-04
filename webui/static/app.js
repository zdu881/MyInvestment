const LOCALE_STORAGE_KEY = 'myinvestment_locale';
const AGENT_MODE_STORAGE_KEY = 'myinvestment_agent_mode';
const AGENT_OPERATION_STORAGE_KEY = 'myinvestment_agent_operation_id';
const DEFAULT_LOCALE = 'zh-CN';
const LOCALE_BASE_PATH = '/static/locales';
const localeDicts = {};
const AGENT_HINT_KEYS = {
  ask: 'agent.hint.ask',
  plan: 'agent.hint.plan',
  operation: 'agent.hint.operation',
};
const AGENT_PLACEHOLDER_KEYS = {
  ask: 'agent.prompt.ask',
  plan: 'agent.prompt.plan',
  operation: 'agent.prompt.operation',
};

const state = {
  activeView: 'action-center',
  locale: DEFAULT_LOCALE,
  agentMode: 'ask',
  agentOperationId: '',
  agentOperationSpecs: [],
  agentConfirmation: null,
  agentHistory: [],
  operationHistory: [],
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

function normalizeAgentMode(mode) {
  const value = String(mode || '').toLowerCase();
  if (value === 'plan') return 'plan';
  if (value === 'operation') return 'operation';
  return 'ask';
}

function detectInitialAgentMode() {
  return normalizeAgentMode(localStorage.getItem(AGENT_MODE_STORAGE_KEY));
}

function detectInitialAgentOperationId() {
  return String(localStorage.getItem(AGENT_OPERATION_STORAGE_KEY) || '').trim();
}

function escapeHtml(text) {
  const raw = text === null || text === undefined ? '' : String(text);
  return raw
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

  setAgentMode(state.agentMode, { persist: false });
  renderAgentConfirmation();
  renderAgentConversation();
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

function parseOptionalFloatInput(inputId) {
  const node = byId(inputId);
  if (!node) return null;
  const raw = String(node.value || '').trim();
  if (!raw) return null;
  const parsed = Number.parseFloat(raw);
  if (!Number.isFinite(parsed)) return NaN;
  return parsed;
}

function parseInfoLines(text) {
  const src = String(text || '').trim();
  if (!src) return [];
  return src
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('[INFO] '))
    .map((line) => line.slice('[INFO] '.length).trim())
    .filter(Boolean);
}

function buildOnboardingSummary(resp, payload) {
  const ok = Boolean(resp?.ok);
  const command = Array.isArray(resp?.command) ? resp.command.join(' ') : '';
  const exitCode = Number(resp?.exit_code ?? -1);
  const stdout = String(resp?.stdout_tail || '').trim();
  const stderr = String(resp?.stderr_tail || '').trim();
  const infoLines = parseInfoLines(stdout);

  const lines = [];
  lines.push(`## ${t('onboarding.resultTitle')}`);
  lines.push(`${t('onboarding.result.status')}: ${ok ? t('onboarding.result.success') : t('onboarding.result.failure')}`);
  lines.push(`${t('onboarding.result.exitCode')}: ${exitCode}`);
  if (command) {
    lines.push(`${t('onboarding.result.command')}: ${command}`);
  }

  if (infoLines.length > 0) {
    lines.push('');
    lines.push(`${t('onboarding.result.output')}:`);
    infoLines.forEach((line) => lines.push(`- ${line}`));
  }

  if (stderr) {
    lines.push('');
    lines.push('stderr:');
    lines.push(stderr);
  }

  const nextSteps = [];
  if (payload.dry_run) {
    nextSteps.push(t('onboarding.next.apply'));
  } else {
    nextSteps.push(t('onboarding.next.refreshConfig'));
    nextSteps.push(t('onboarding.next.scheduler'));
    nextSteps.push(t('onboarding.next.actionCenter'));
  }

  lines.push('');
  lines.push(`${t('onboarding.result.next')}:`);
  nextSteps.forEach((step, idx) => {
    lines.push(`${idx + 1}. ${step}`);
  });

  return lines.join('\n');
}

function renderOperatorGuide(overview = {}) {
  const container = byId('operatorGuide');
  if (!container) return;

  const pendingReview = Number(overview.pending_review_count || 0);
  const pendingExecution = Number(overview.pending_execution_count || 0);
  const activeAlerts = Number(overview.active_alert_count || 0);
  const hasRiskAlert = String(overview.alert_status || '').toLowerCase() === 'critical';

  const steps = [];
  if (activeAlerts > 0) {
    const key = hasRiskAlert ? 'operator.guide.step.alertCritical' : 'operator.guide.step.alertWarn';
    steps.push(t(key, { count: activeAlerts }));
  }
  if (pendingReview > 0) {
    steps.push(t('operator.guide.step.review', { count: pendingReview }));
  }
  if (pendingExecution > 0) {
    steps.push(t('operator.guide.step.execution', { count: pendingExecution }));
  }

  if (steps.length === 0) {
    steps.push(t('operator.guide.step.onboarding'));
    steps.push(t('operator.guide.step.scheduler'));
    steps.push(t('operator.guide.step.observe'));
  } else if (steps.length < 3) {
    steps.push(t('operator.guide.step.scheduler'));
    if (steps.length < 3) {
      steps.push(t('operator.guide.step.observe'));
    }
  }

  container.innerHTML = `
    <ol class="operator-guide-list">
      ${steps.slice(0, 3).map((step) => `<li class="operator-guide-item">${escapeHtml(step)}</li>`).join('')}
    </ol>
  `;
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

function formatLocalTimestamp(value) {
  try {
    if (!value) return '';
    const dt = new Date(value);
    if (Number.isNaN(dt.getTime())) return String(value);
    return dt.toLocaleString();
  } catch {
    return String(value || '');
  }
}

function getAgentOperationSpecById(operationId) {
  const opId = String(operationId || '').trim();
  return state.agentOperationSpecs.find((spec) => String(spec.id || '') === opId) || null;
}

function getAgentOperationLabel(spec) {
  if (!spec) return '-';
  const key = String(spec.i18n_key || '').trim();
  if (key && hasTranslationKey(key)) return t(key);
  return String(spec.label || spec.id || '-');
}

function getAgentOperationOptionLabel(option) {
  const key = String(option?.i18n_key || '').trim();
  if (key && hasTranslationKey(key)) return t(key);
  return String(option?.name || '-');
}

function renderAgentOperationSelect() {
  const select = byId('agentOperationSelect');
  if (!select) return;

  if (!state.agentOperationSpecs.length) {
    select.innerHTML = `<option value="">${escapeHtml(t('common.none'))}</option>`;
    select.disabled = true;
    state.agentOperationId = '';
    return;
  }

  if (!getAgentOperationSpecById(state.agentOperationId)) {
    state.agentOperationId = String(state.agentOperationSpecs[0].id || '');
  }

  select.innerHTML = state.agentOperationSpecs
    .map((spec) => `<option value="${escapeHtml(spec.id)}">${escapeHtml(getAgentOperationLabel(spec))}</option>`)
    .join('');
  select.value = state.agentOperationId;
  select.disabled = false;
}

function getAgentOptionInputId(name) {
  return `agentOpOpt_${String(name || '').replaceAll(/[^A-Za-z0-9_]/g, '_')}`;
}

function renderAgentOperationOptions() {
  const container = byId('agentOperationOptions');
  if (!container) return;
  const spec = getAgentOperationSpecById(state.agentOperationId);
  if (!spec) {
    container.innerHTML = '';
    return;
  }

  const options = Array.isArray(spec.options) ? spec.options : [];
  if (!options.length) {
    container.innerHTML = `<div class="mono">${escapeHtml(t('agent.operation.noOptions'))}</div>`;
    return;
  }

  container.innerHTML = options.map((opt) => {
    const name = String(opt.name || '').trim();
    const typ = String(opt.type || '').toLowerCase();
    const label = escapeHtml(getAgentOperationOptionLabel(opt));
    const inputId = getAgentOptionInputId(name);

    if (typ === 'bool') {
      const checked = Boolean(opt.default) ? ' checked' : '';
      return `
        <label class="checkbox-row">
          <input id="${escapeHtml(inputId)}" data-op-name="${escapeHtml(name)}" type="checkbox"${checked} />
          <span>${label}</span>
        </label>
      `;
    }

    if (typ === 'int') {
      const minAttr = Number.isFinite(Number(opt.min)) ? ` min="${escapeHtml(opt.min)}"` : '';
      const maxAttr = Number.isFinite(Number(opt.max)) ? ` max="${escapeHtml(opt.max)}"` : '';
      const val = Number.isFinite(Number(opt.default)) ? Number(opt.default) : 0;
      return `
        <label>
          <span>${label}</span>
          <input
            id="${escapeHtml(inputId)}"
            data-op-name="${escapeHtml(name)}"
            data-op-type="int"
            type="number"
            value="${escapeHtml(val)}"${minAttr}${maxAttr}
          />
        </label>
      `;
    }

    return `
      <label>
        <span>${label}</span>
        <input id="${escapeHtml(inputId)}" data-op-name="${escapeHtml(name)}" type="text" value="${escapeHtml(opt.default || '')}" />
      </label>
    `;
  }).join('');
}

function collectAgentOperationOptions(spec) {
  const options = Array.isArray(spec?.options) ? spec.options : [];
  const out = {};
  options.forEach((opt) => {
    const name = String(opt.name || '').trim();
    if (!name) return;
    const typ = String(opt.type || '').toLowerCase();
    const input = byId(getAgentOptionInputId(name));
    if (!input) return;

    if (typ === 'bool') {
      out[name] = Boolean(input.checked);
      return;
    }

    if (typ === 'int') {
      const fallback = Number.isFinite(Number(opt.default)) ? Number(opt.default) : 0;
      let value = Number.parseInt(String(input.value || ''), 10);
      if (Number.isNaN(value)) value = fallback;
      if (Number.isFinite(Number(opt.min))) value = Math.max(value, Number(opt.min));
      if (Number.isFinite(Number(opt.max))) value = Math.min(value, Number(opt.max));
      out[name] = value;
      return;
    }

    out[name] = String(input.value || '');
  });
  return out;
}

function getAgentOperationPromptFallback(spec, options) {
  const parts = Object.entries(options || {}).map(([k, v]) => `${k}=${v}`);
  const suffix = parts.length ? ` (${parts.join(', ')})` : '';
  return `${getAgentOperationLabel(spec)}${suffix}`;
}

function renderAgentConfirmation() {
  const idNode = byId('agentConfirmationId');
  const expireNode = byId('agentConfirmationExpire');
  if (!idNode || !expireNode) return;

  const payload = state.agentConfirmation || {};
  const confirmationId = String(payload.confirmation_id || '').trim();
  const expiresAt = String(payload.expires_at || '').trim();

  idNode.textContent = confirmationId || '-';
  expireNode.textContent = expiresAt ? formatLocalTimestamp(expiresAt) : '-';
}

async function loadAgentOperationSpecs() {
  const resp = await request('/api/agent/operations');
  const items = Array.isArray(resp.items) ? resp.items : [];
  state.agentOperationSpecs = items;

  if (!getAgentOperationSpecById(state.agentOperationId) && items.length > 0) {
    state.agentOperationId = String(items[0].id || '');
    localStorage.setItem(AGENT_OPERATION_STORAGE_KEY, state.agentOperationId);
  }

  renderAgentOperationSelect();
  renderAgentOperationOptions();
}

function setAgentMode(mode, options = {}) {
  const { persist = true } = options;
  state.agentMode = normalizeAgentMode(mode);

  if (persist) {
    localStorage.setItem(AGENT_MODE_STORAGE_KEY, state.agentMode);
  }

  document.querySelectorAll('.agent-mode').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.agentMode === state.agentMode);
  });

  const hintNode = byId('agentModeHint');
  if (hintNode) {
    hintNode.textContent = t(AGENT_HINT_KEYS[state.agentMode]);
  }

  const promptInput = byId('agentPromptInput');
  if (promptInput) {
    promptInput.placeholder = t(AGENT_PLACEHOLDER_KEYS[state.agentMode]);
  }

  const confirmRow = byId('agentConfirmRow');
  if (confirmRow) {
    confirmRow.classList.toggle('show', state.agentMode === 'operation');
  }

  const operationPanel = byId('agentOperationPanel');
  if (operationPanel) {
    operationPanel.classList.toggle('show', state.agentMode === 'operation');
  }

  if (state.agentMode === 'operation') {
    renderAgentOperationSelect();
    renderAgentOperationOptions();
    renderAgentConfirmation();
  }
}

function renderAgentConversation() {
  const container = byId('agentConversation');
  if (!container) return;

  if (!state.agentHistory.length) {
    container.innerHTML = `<div class="agent-empty mono">${escapeHtml(t('agent.history.empty'))}</div>`;
    return;
  }

  container.innerHTML = state.agentHistory.map((row) => {
    const modeKey = `agent.mode.${row.mode}`;
    const modeLabel = hasTranslationKey(modeKey) ? t(modeKey) : row.mode;
    const statusTag = row.ok ? 'tag ok' : 'tag warn';
    const operation = row.operation || {};
    const opLabel = operation.i18n_key && hasTranslationKey(operation.i18n_key)
      ? t(operation.i18n_key)
      : operation.label || operation.id || '-';
    const opOptions = operation.options ? JSON.stringify(operation.options) : '{}';
    const operationBlock = operation.id ? `
      <div class="agent-turn-op mono">
        <div><b>${escapeHtml(t('agent.turn.operation'))}</b>: ${escapeHtml(opLabel)}</div>
        <div><b>${escapeHtml(t('agent.turn.options'))}</b>: ${escapeHtml(opOptions)}</div>
        <div><b>${escapeHtml(t('agent.turn.command'))}</b>: ${escapeHtml((operation.command || []).join(' '))}</div>
        <div><b>${escapeHtml(t('agent.turn.exitCode'))}</b>: ${escapeHtml(operation.executed ? operation.exit_code : t('agent.turn.preview'))}</div>
      </div>
    ` : '';
    return `
      <article class="agent-turn">
        <div class="agent-turn-head">
          <span class="${statusTag}">${escapeHtml(modeLabel)}</span>
          <span class="mono">${escapeHtml(formatLocalTimestamp(row.created_at))}</span>
        </div>
        <div class="agent-turn-block">
          <div class="agent-turn-role">${escapeHtml(t('agent.turn.you'))}</div>
          <pre class="agent-turn-text">${escapeHtml(row.prompt || '')}</pre>
        </div>
        <div class="agent-turn-block">
          <div class="agent-turn-role">${escapeHtml(t('agent.turn.agent'))}</div>
          <pre class="agent-turn-text">${escapeHtml(row.reply || '')}</pre>
        </div>
        ${operationBlock}
      </article>
    `;
  }).join('');
}

async function submitAgentInteraction() {
  const promptInput = byId('agentPromptInput');
  const sendBtn = byId('agentSendBtn');
  const confirmExec = byId('agentConfirmExec');
  const mode = state.agentMode;
  let message = promptInput.value.trim();
  const confirm = mode === 'operation' && Boolean(confirmExec?.checked);
  const payload = { mode, message, confirm };

  if (mode === 'operation') {
    const spec = getAgentOperationSpecById(state.agentOperationId);
    if (!spec) {
      notify(t('agent.operation.required'), true);
      return;
    }
    const options = collectAgentOperationOptions(spec);
    payload.operation_id = spec.id;
    payload.operation_options = options;
    if (!message) {
      message = getAgentOperationPromptFallback(spec, options);
      payload.message = message;
    }
    if (confirm) {
      const confirmationId = String(state.agentConfirmation?.confirmation_id || '').trim();
      if (!confirmationId) {
        notify(t('agent.confirmation.missing'), true);
        return;
      }
      payload.confirmation_id = confirmationId;
    }
  } else if (!message) {
    notify(t('agent.prompt.required'), true);
    return;
  }

  sendBtn.disabled = true;
  sendBtn.textContent = t('agent.sending');

  try {
    const resp = await request('/api/agent/interact', {
      method: 'POST',
      body: JSON.stringify(payload),
    });

    if (mode === 'operation') {
      if (!confirm && resp.confirmation?.confirmation_id) {
        state.agentConfirmation = resp.confirmation;
      } else if (confirm) {
        state.agentConfirmation = null;
      }
      renderAgentConfirmation();
    }

    state.agentHistory.unshift({
      created_at: new Date().toISOString(),
      mode,
      prompt: message,
      reply: resp.reply || '',
      ok: Boolean(resp.ok),
      operation: resp.operation || null,
    });
    state.agentHistory = state.agentHistory.slice(0, 20);

    promptInput.value = '';
    if (confirmExec) confirmExec.checked = false;
    renderAgentConversation();
    if (mode === 'operation' && confirm) {
      await loadOperationHistory().catch(() => {});
    }
    notify(resp.ok ? t('agent.send.success') : t('agent.send.partial'), !resp.ok);
  } catch (err) {
    notify(err.message || String(err), true);
  } finally {
    sendBtn.disabled = false;
    sendBtn.textContent = t('agent.send');
  }
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

function toOkTag(ok) {
  return ok ? 'tag ok' : 'tag critical';
}

async function loadActionCenter() {
  const data = await request('/api/action-center');
  const o = data.overview || {};
  renderOperatorGuide(o);
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

async function loadOperationHistory() {
  const data = await request('/api/agent/operations/history?limit=120');
  state.operationHistory = Array.isArray(data.items) ? data.items : [];
  const body = byId('operationHistoryBody');
  if (!body) return;

  if (!state.operationHistory.length) {
    body.innerHTML = `
      <tr>
        <td colspan="7" class="mono">${escapeHtml(t('operationHistory.empty'))}</td>
      </tr>
    `;
    return;
  }

  body.innerHTML = state.operationHistory.map((row) => {
    const opLabel = (row.operation_i18n_key && hasTranslationKey(row.operation_i18n_key))
      ? t(row.operation_i18n_key)
      : (row.operation_label || row.operation_id || '-');
    const optionsText = JSON.stringify(row.operation_options || {});
    const commandText = Array.isArray(row.command) ? row.command.join(' ') : String(row.command || '');
    const outputLines = [
      row.stdout_tail ? `stdout:\n${row.stdout_tail}` : '',
      row.stderr_tail ? `stderr:\n${row.stderr_tail}` : '',
    ].filter(Boolean);
    const outputText = outputLines.join('\n\n') || '-';
    const statusText = row.ok ? t('operationHistory.status.success') : t('operationHistory.status.failed');

    return `
      <tr>
        <td class="mono">${escapeHtml(formatLocalTimestamp(row.timestamp))}</td>
        <td>${escapeHtml(opLabel)}</td>
        <td class="mono">${escapeHtml(optionsText)}</td>
        <td class="history-command">${escapeHtml(commandText)}</td>
        <td class="mono">${escapeHtml(row.exit_code)}</td>
        <td><span class="${toOkTag(Boolean(row.ok))}">${escapeHtml(statusText)}</span></td>
        <td><div class="history-output">${escapeHtml(outputText)}</div></td>
      </tr>
    `;
  }).join('');
}

async function loadCurrentView() {
  try {
    if (state.activeView === 'action-center') await loadActionCenter();
    if (state.activeView === 'proposal-review') await loadProposals();
    if (state.activeView === 'execution-queue') await loadExecutions();
    if (state.activeView === 'runs') await loadRuns();
    if (state.activeView === 'alerts-ops') await loadAlertsOps();
    if (state.activeView === 'quality') await loadQuality();
    if (state.activeView === 'operation-history') await loadOperationHistory();
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

  byId('agentModeSwitch').addEventListener('click', (e) => {
    const btn = e.target.closest('.agent-mode');
    if (!btn) return;
    setAgentMode(btn.dataset.agentMode, { persist: true });
  });

  byId('agentOperationSelect').addEventListener('change', (e) => {
    state.agentOperationId = String(e.target.value || '').trim();
    localStorage.setItem(AGENT_OPERATION_STORAGE_KEY, state.agentOperationId);
    state.agentConfirmation = null;
    renderAgentOperationOptions();
    renderAgentConfirmation();
  });

  byId('agentOperationOptions').addEventListener('change', () => {
    state.agentConfirmation = null;
    renderAgentConfirmation();
  });

  byId('agentClearBtn').addEventListener('click', () => {
    state.agentHistory = [];
    renderAgentConversation();
    notify(t('agent.history.cleared'));
  });

  byId('agentForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    await submitAgentInteraction();
  });

  byId('refreshActionCenterBtn').addEventListener('click', loadActionCenter);
  byId('refreshProposalBtn').addEventListener('click', loadProposals);
  byId('refreshExecutionBtn').addEventListener('click', loadExecutions);
  byId('refreshRunsBtn').addEventListener('click', loadRuns);
  byId('refreshOpsBtn').addEventListener('click', loadAlertsOps);
  byId('refreshQualityBtn').addEventListener('click', loadQuality);
  byId('refreshOperationHistoryBtn').addEventListener('click', loadOperationHistory);
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

  byId('onboardingInitForm').addEventListener('submit', async (e) => {
    e.preventDefault();

    const initialCapital = Number.parseFloat(String(byId('onboardingInitialCapital').value || ''));
    if (!Number.isFinite(initialCapital) || initialCapital <= 0) {
      notify(t('onboarding.initialCapitalInvalid'), true);
      return;
    }

    const optionalKeys = [
      {
        inputId: 'onboardingMaxSingleWeight',
        payloadKey: 'max_single_weight',
        min: 0,
        max: 1,
        minInclusive: false,
        maxInclusive: true,
      },
      {
        inputId: 'onboardingMaxIndustryWeight',
        payloadKey: 'max_industry_weight',
        min: 0,
        max: 1,
        minInclusive: false,
        maxInclusive: true,
      },
      {
        inputId: 'onboardingMinCashRatio',
        payloadKey: 'min_cash_ratio',
        min: 0,
        max: 1,
        minInclusive: true,
        maxInclusive: false,
      },
    ];

    const payload = {
      initial_capital: initialCapital,
      risk_profile: byId('onboardingRiskProfile').value.trim() || 'defensive',
      seed_watchlist: byId('onboardingSeedWatchlist').value.trim(),
      dry_run: byId('onboardingDryRun').checked,
      reset_runtime: byId('onboardingResetRuntime').checked,
      reset_knowledge: byId('onboardingResetKnowledge').checked,
      reset_watchlist: byId('onboardingResetWatchlist').checked,
      force: byId('onboardingForce').checked,
    };
    const dangerousReset = (!payload.dry_run)
      && (payload.reset_runtime || payload.reset_knowledge || payload.reset_watchlist);
    if (dangerousReset) {
      const dangerConfirmed = byId('onboardingDangerConfirm').checked;
      const confirmText = String(byId('onboardingConfirmText').value || '').trim().toUpperCase();
      if (!dangerConfirmed || confirmText !== 'INIT') {
        notify(t('onboarding.confirmInvalid'), true);
        return;
      }
    }

    for (const opt of optionalKeys) {
      const { inputId, payloadKey, min, max, minInclusive, maxInclusive } = opt;
      const parsed = parseOptionalFloatInput(inputId);
      if (parsed === null) continue;
      const minOk = minInclusive ? parsed >= min : parsed > min;
      const maxOk = maxInclusive ? parsed <= max : parsed < max;
      if (!Number.isFinite(parsed) || !minOk || !maxOk) {
        notify(
          t('onboarding.optionalRangeInvalid', {
            field: payloadKey,
            min: String(min),
            max: String(max),
          }),
          true
        );
        return;
      }
      payload[payloadKey] = parsed;
    }

    if (
      Number.isFinite(payload.max_single_weight)
      && Number.isFinite(payload.max_industry_weight)
      && payload.max_industry_weight < payload.max_single_weight
    ) {
      notify(t('onboarding.industryLtSingle'), true);
      return;
    }

    try {
      const resp = await request('/api/onboarding/init', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      byId('onboardingResult').textContent = buildOnboardingSummary(resp, payload);
      notify(t('onboarding.submitSuccess'));
      await loadConfig().catch(() => {});
      await loadActionCenter().catch(() => {});
    } catch (err) {
      notify(err.message || String(err), true);
    }
  });

  byId('operatorGuideGoConfigBtn').addEventListener('click', () => {
    setView('config');
  });

  const token = getToken();
  if (token) byId('apiTokenInput').value = token;
  setAgentMode(state.agentMode, { persist: false });
  renderAgentOperationSelect();
  renderAgentOperationOptions();
  renderAgentConfirmation();
  renderAgentConversation();
}

window.setView = setView;
window.selectProposal = selectProposal;
window.selectExecution = selectExecution;
window.selectRun = selectRun;
window.readArtifact = readArtifact;

state.locale = detectInitialLocale();
state.agentMode = detectInitialAgentMode();
state.agentOperationId = detectInitialAgentOperationId();

async function bootstrap() {
  try {
    await ensureLocaleDicts(state.locale);
  } catch (err) {
    notify(err.message || String(err), true);
  }

  try {
    await loadAgentOperationSpecs();
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

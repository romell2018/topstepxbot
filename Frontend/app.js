(function () {
  const els = {
    status: document.getElementById('connection-status'),
    username: document.getElementById('username'),
    accountId: document.getElementById('account-id'),
    symbol: document.getElementById('symbol'),
    apiKey: document.getElementById('api-key'),
    tradingStatus: document.getElementById('trading-status'),
    openOrders: document.getElementById('open-orders'),
    balance: document.getElementById('balance'),
    equity: document.getElementById('equity'),
    maxLoss: document.getElementById('max-loss'),
    lastPrice: document.getElementById('last-price'),
    accountStatus: document.getElementById('account-status'),
    updatedAt: document.getElementById('updated-at'),
    info: document.getElementById('info-msg'),
    toggle: document.getElementById('toggle-btn'),
    refresh: document.getElementById('refresh-btn'),
  };

  let currentState = null;
  let pollTimer = null;

  function numberFormatter(options) {
    return new Intl.NumberFormat('en-US', options);
  }

  const usd = numberFormatter({ style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const num = numberFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });

  function formatCurrency(value) {
    if (value === null || value === undefined) {
      return '—';
    }
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return String(value);
    }
    return usd.format(numeric);
  }

  function formatNumber(value) {
    if (value === null || value === undefined) {
      return '—';
    }
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return String(value);
    }
    return num.format(numeric);
  }

  function formatTimestamp(ts) {
    if (!ts) {
      return '—';
    }
    const date = new Date(ts * 1000);
    return `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;
  }

  function setStatus(message, variant) {
    const validVariant = variant || 'loading';
    els.status.className = `status status--${validVariant}`;
    els.status.textContent = message;
  }

  function render(state) {
    currentState = state;
    els.username.textContent = state.username || '—';
    els.accountId.textContent = state.accountId ?? '—';
    els.symbol.textContent = state.symbol || '—';
    els.apiKey.textContent = state.apiKeyMasked || '—';
    els.openOrders.textContent = state.openOrders ?? '—';
    els.balance.textContent = formatCurrency(state.balance);
    els.equity.textContent = formatCurrency(state.equity);
    els.maxLoss.textContent = state.maximumLoss !== undefined && state.maximumLoss !== null ? formatCurrency(state.maximumLoss) : '—';
    els.lastPrice.textContent = formatNumber(state.lastPrice);
    els.accountStatus.textContent = state.accountStatus || '—';
    els.updatedAt.textContent = formatTimestamp(state.timestamp);

    const tradingDisabled = Boolean(state.tradingDisabled);
    const tokenValid = Boolean(state.tokenValid);

    els.tradingStatus.textContent = tradingDisabled ? 'Disabled' : 'Active';
    els.tradingStatus.classList.toggle('is-disabled', tradingDisabled);

    els.toggle.disabled = !tokenValid;
    if (tokenValid) {
      els.toggle.classList.toggle('btn--danger', !tradingDisabled);
      els.toggle.textContent = tradingDisabled ? 'Enable Trading' : 'Disable Trading';
    } else {
      els.toggle.classList.remove('btn--danger');
      els.toggle.textContent = 'Check Credentials';
    }

    if (!tokenValid) {
      setStatus('Authentication failed. Double-check API credentials.', 'error');
    } else if (tradingDisabled) {
      const reason = state.tradingDisabledReason ? ` — ${state.tradingDisabledReason}` : '';
      setStatus(`Trading paused${reason}`, 'warn');
    } else if (state.connected) {
      setStatus('Bot is connected and trading is enabled.', 'ok');
    } else {
      setStatus('Waiting for market stream…', 'warn');
    }

    if (state.error) {
      els.info.textContent = `Latest API message: ${state.error}`;
    } else {
      els.info.textContent = '';
    }
  }

  async function fetchState(showToast) {
    try {
      const response = await fetch('/ui/state', { cache: 'no-store' });
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      const data = await response.json();
      render(data);
      if (showToast) {
        els.info.textContent = 'Updated.';
        setTimeout(() => {
          if (els.info.textContent === 'Updated.') {
            els.info.textContent = '';
          }
        }, 2000);
      }
    } catch (err) {
      setStatus('Unable to reach the bot server.', 'error');
      els.info.textContent = err instanceof Error ? err.message : String(err);
      els.toggle.disabled = true;
    }
  }

  async function toggleTrading() {
    if (!currentState) {
      return;
    }
    const disabling = !currentState.tradingDisabled;
    els.toggle.disabled = true;
    els.toggle.textContent = disabling ? 'Disabling…' : 'Enabling…';

    try {
      const endpoint = disabling ? '/trading/disable' : '/trading/enable';
      const payload = disabling ? { reason: 'manual toggle via UI' } : undefined;
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: payload ? JSON.stringify(payload) : null,
      });
      if (!response.ok) {
        throw new Error(`Toggle failed with status ${response.status}`);
      }
    } catch (err) {
      els.info.textContent = err instanceof Error ? err.message : String(err);
    } finally {
      await fetchState(true);
      els.toggle.disabled = false;
    }
  }

  function startPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
    }
    pollTimer = setInterval(fetchState, 5000);
  }

  els.toggle.addEventListener('click', toggleTrading);
  els.refresh.addEventListener('click', () => fetchState(true));

  fetchState();
  startPolling();
})();

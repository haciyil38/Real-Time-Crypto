/* ── Config ── */
const WS_URL       = `ws://${location.host}/ws`;
const API_URL      = `http://${location.host}`;
const RECONNECT_MS = 3000;

/* ── Transport state ── */
let ws          = null;
let wsConnected = false;

/* ── App state ── */
let chart        = null;
let candleSeries = null;
let maSeries     = null;
let volChart     = null;
let volSeries    = null;

let activePair = 'BTC/USDT';
let activeEx   = 'binance';

const candles = {};
const MA_WIN  = 20;

let totalTrades    = 0;
let tradeCountFeed = 0;
let lastTpsCalc    = Date.now();
let lastAlertTs    = '';
let feedInit       = false;
let alertsInit     = false;

const lastTradePrices = {}; // key: "pair:exchange" → last price
const seenTradeIds    = new Set(); // dedup between WS stream and snapshot fallback
const chartMarkers    = {}; // key: "pair:exchange" → array of LightweightCharts markers

/* ── Init charts ── */
function initChart() {
  const el = document.getElementById('chart-container');
  chart = LightweightCharts.createChart(el, {
    layout:     { background: { color: '#161b22' }, textColor: '#8b949e' },
    grid:       { vertLines: { color: '#21262d' }, horzLines: { color: '#21262d' } },
    crosshair:  { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: '#30363d' },
    timeScale:  { borderColor: '#30363d', timeVisible: true, secondsVisible: false },
    width: el.clientWidth, height: 300,
  });
  candleSeries = chart.addCandlestickSeries({
    upColor: '#00ff88', downColor: '#ff4466',
    borderUpColor: '#00ff88', borderDownColor: '#ff4466',
    wickUpColor: '#00ff88', wickDownColor: '#ff4466',
  });
  maSeries = chart.addLineSeries({ color: '#58a6ff', lineWidth: 1, priceLineVisible: false });
  window.addEventListener('resize', () => chart.applyOptions({ width: el.clientWidth }));
}

function initVolChart() {
  const el = document.getElementById('vol-chart');
  volChart = LightweightCharts.createChart(el, {
    layout:  { background: { color: '#161b22' }, textColor: '#8b949e' },
    grid:    { vertLines: { color: '#21262d' }, horzLines: { color: '#21262d' } },
    rightPriceScale: { borderColor: '#30363d' },
    timeScale: { borderColor: '#30363d', visible: false },
    width: el.clientWidth, height: 220,
  });
  volSeries = volChart.addHistogramSeries({ color: '#58a6ff88', priceFormat: { type: 'volume' } });
  window.addEventListener('resize', () => volChart.applyOptions({ width: el.clientWidth }));
}

/* ── Candle helpers ── */
function minuteTs(iso) {
  return Math.floor(new Date(iso).getTime() / 60000) * 60;
}

function updateCandle(pair, exchange, price, iso) {
  const k  = `${pair}:${exchange}`;
  const ts = minuteTs(iso);
  if (!candles[k]) candles[k] = {};
  const map = candles[k];
  if (!map[ts]) {
    map[ts] = { time: ts, open: price, high: price, low: price, close: price };
  } else {
    map[ts].high  = Math.max(map[ts].high, price);
    map[ts].low   = Math.min(map[ts].low,  price);
    map[ts].close = price;
  }
  if (pair === activePair && exchange === activeEx) {
    const sorted = Object.values(map).sort((a, b) => a.time - b.time);
    candleSeries.setData(sorted);
    const ma = [];
    for (let i = MA_WIN - 1; i < sorted.length; i++) {
      const avg = sorted.slice(i - MA_WIN + 1, i + 1).reduce((s, c) => s + c.close, 0) / MA_WIN;
      ma.push({ time: sorted[i].time, value: avg });
    }
    maSeries.setData(ma);
  }
}

function switchChart(pair, exchange) {
  activePair = pair; activeEx = exchange;
  const map    = candles[`${pair}:${exchange}`] || {};
  const sorted = Object.values(map).sort((a, b) => a.time - b.time);
  candleSeries.setData(sorted);
  const ma = [];
  for (let i = MA_WIN - 1; i < sorted.length; i++) {
    const avg = sorted.slice(i - MA_WIN + 1, i + 1).reduce((s, c) => s + c.close, 0) / MA_WIN;
    ma.push({ time: sorted[i].time, value: avg });
  }
  maSeries.setData(ma);
  candleSeries.setMarkers(chartMarkers[`${pair}:${exchange}`] || []);
}

/* ── Ticker ── */
const TICKER_IDS = {
  'BTC/USDT': { p: 'tp-BTCUSDT', c: 'tc-BTCUSDT' },
  'ETH/USDT': { p: 'tp-ETHUSDT', c: 'tc-ETHUSDT' },
  'BTC/USD':  { p: 'tp-BTCUSD',  c: 'tc-BTCUSD'  },
};

function updateTicker(pair, price, pct) {
  const ids = TICKER_IDS[pair];
  if (!ids || price == null) return;
  const pEl = document.getElementById(ids.p);
  const cEl = document.getElementById(ids.c);
  pEl.textContent = `$${fmt(price)}`;
  pEl.className   = `ticker-price ${pct >= 0 ? 'up' : 'down'}`;
  cEl.textContent = `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`;
  cEl.className   = `ticker-chg ${pct >= 0 ? 'up' : 'down'}`;
}

/* ── Trades feed ── */
function addTrade(trade) {
  if (trade.trade_id) {
    if (seenTradeIds.has(trade.trade_id)) return;
    seenTradeIds.add(trade.trade_id);
    if (seenTradeIds.size > 1000) seenTradeIds.delete(seenTradeIds.values().next().value);
  }
  tradeCountFeed++;
  totalTrades++;
  setText('trade-counter', `${totalTrades.toLocaleString()} trades`);
  const feed = document.getElementById('feed');
  if (!feedInit) { feed.innerHTML = ''; feedInit = true; }
  const large = trade.quantity > ({ 'BTC/USDT': 1, 'BTC/USD': 1, 'ETH/USDT': 10 }[trade.pair] ?? 5);

  const priceKey  = `${trade.pair}:${trade.exchange}`;
  const prevPrice = lastTradePrices[priceKey];
  const priceDir  = prevPrice == null ? 'neutral' : trade.price >= prevPrice ? 'up' : 'down';
  lastTradePrices[priceKey] = trade.price;

  const row = document.createElement('div');
  row.className = `trade-row${large ? ' large' : ''}`;
  row.innerHTML = `
    <span class="trade-exch exch-${trade.exchange}">${trade.exchange}</span>
    <span class="trade-pair">${trade.pair}</span>
    <span class="trade-price ${priceDir}">$${fmt(trade.price)}</span>
    <span class="trade-qty">${trade.quantity.toFixed(5)}</span>
  `;
  feed.prepend(row);
  while (feed.children.length > 20) feed.removeChild(feed.lastChild);
}

/* ── Alerts panel ── */
function addAlert(alert) {
  const list = document.getElementById('alerts-list');
  if (!alertsInit) { list.innerHTML = ''; alertsInit = true; }
  const row = document.createElement('div');
  row.className = 'alert-row';
  row.innerHTML = `
    <span class="sev sev-${alert.severity}">${alert.severity}</span>
    <div class="alert-body">
      <div class="alert-type">${alert.alert_type}</div>
      <div class="alert-meta">${alert.exchange} · ${alert.pair} · ${alert.actual_value?.toFixed?.(4) ?? alert.actual_value}</div>
    </div>
    <span class="alert-time">${fmtTime(alert.triggered_at)}</span>
  `;
  list.prepend(row);
  while (list.children.length > 50) list.removeChild(list.lastChild);
  addAlertMarker(alert);
}

/* ── Chart anomaly markers ── */
const MARKER_STYLES = {
  PRICE_SPIKE: { color: '#f0c040', shape: 'arrowDown', text: '⚡' },
};

function addAlertMarker(alert) {
  const style = MARKER_STYLES[alert.alert_type];
  if (!style) return;
  const exchange = alert.pair === 'BTC/USD' ? 'coinbase'
                 : alert.exchange === 'binance/coinbase' ? 'binance'
                 : alert.exchange;
  const pair = alert.pair === 'BTC' ? 'BTC/USDT' : alert.pair;
  const key  = `${pair}:${exchange}`;
  const ts   = minuteTs(alert.triggered_at);
  if (!chartMarkers[key]) chartMarkers[key] = [];
  if (!chartMarkers[key].find(m => m.time === ts && m.text === style.text)) {
    chartMarkers[key].push({ time: ts, position: 'aboveBar', ...style });
    chartMarkers[key].sort((a, b) => a.time - b.time);
  }
  if (pair === activePair && exchange === activeEx) {
    candleSeries.setMarkers(chartMarkers[key]);
  }
}

/* ── Volume chart ── */
const WINDOWS = ['1m', '5m', '15m', '1h'];

function updateVolChart(statsAll) {
  const key  = `${activePair}:${activeEx}`;
  const data = statsAll?.[key] ?? {};
  const bars = WINDOWS.map((w, i) => ({
    time:  i + 1,
    value: data[w]?.volume_base ?? 0,
    color: '#58a6ff88',
  }));
  volSeries.setData(bars);
  volChart.applyOptions({
    timeScale: {
      visible: true,
      tickMarkFormatter: (t) => WINDOWS[t - 1] ?? '',
    },
  });
}

/* ── KPIs ── */
function updateKPIs(snap) {
  const btcPrice = snap.prices?.['BTC/USDT']?.binance;
  const btcPct   = snap.prices?.['BTC/USDT']?.last_change_pct ?? 0;
  if (btcPrice != null) {
    setText('kpi-price', `$${fmt(btcPrice)}`);
    const el = document.getElementById('kpi-chg');
    el.textContent = `${btcPct >= 0 ? '+' : ''}${btcPct.toFixed(2)}% (5 min)`;
    el.className   = `kpi-sub ${btcPct >= 0 ? 'up' : 'down'}`;
  }
  const s5 = snap.stats_5m?.['BTC/USDT:binance'];
  if (s5) setText('kpi-vol', s5.volume_base?.toFixed(2) ?? '—');
}

function calcTps() {
  const now = Date.now();
  const el  = now - lastTpsCalc;
  if (el >= 5000) {
    setText('kpi-tps', (tradeCountFeed / (el / 1000)).toFixed(1));
    tradeCountFeed = 0;
    lastTpsCalc    = now;
  }
}

/* ── Snapshot handler (shared by WS and polling) ── */
function handleSnapshot(snap) {
  setText('ts', fmtTime(snap.timestamp));
  Object.entries(snap.prices || {}).forEach(([pair, info]) => {
    const exchange = pair === 'BTC/USD' ? 'coinbase' : 'binance';
    const price    = info[exchange];
    updateTicker(pair, price, info.last_change_pct ?? 0);
    if (price != null) updateCandle(pair, exchange, price, snap.timestamp);
  });
  updateKPIs(snap);
  if (snap.stats_all) updateVolChart(snap.stats_all);
  (snap.recent_trades || []).slice().reverse().forEach(addTrade);
  (snap.recent_alerts || []).slice().reverse().forEach(a => {
    if (a.triggered_at > lastAlertTs) { addAlert(a); lastAlertTs = a.triggered_at; }
  });
  calcTps();
  const latMs = Date.now() - new Date(snap.timestamp).getTime();
  setText('h-lat', `${latMs} ms`);
}

/* ══════════════════════════════════════════
   TRANSPORT — WebSocket uniquement
══════════════════════════════════════════ */
function connectWS() {
  setStatus('h-ws', 'wrn', 'Connecting…');

  try { ws = new WebSocket(WS_URL); }
  catch (_) { setStatus('h-ws', 'err', 'WS unavailable'); return; }

  ws.onopen = () => {
    wsConnected = true;
    setStatus('h-ws', 'ok', 'WebSocket ✓');
  };

  ws.onmessage = (evt) => {
    try {
      const msg = JSON.parse(evt.data);
      if      (msg.type === 'snapshot') handleSnapshot(msg);
      else if (msg.type === 'trade')    addTrade(msg);
      else if (msg.type === 'alert')  { addAlert(msg); lastAlertTs = msg.triggered_at; }
    } catch (_) {}
  };

  ws.onclose = () => {
    wsConnected = false;
    setStatus('h-ws', 'wrn', 'Reconnecting…');
    setTimeout(connectWS, RECONNECT_MS);
  };

  ws.onerror = () => { try { ws.close(); } catch (_) {} };
}

/* ── Health & anomaly (always HTTP) ── */
async function fetchHealth() {
  try {
    const res  = await fetch(`${API_URL}/api/health`);
    const data = await res.json();
    setHealth('h-mongo', data.mongodb, data.mongodb ? 'OK' : 'ERR');
    setHealth('h-kafka', data.kafka,   data.kafka   ? 'OK' : 'ERR');
    setText('h-cons', `${data.consumers_active} / 3`);
    setText('h-rate', `${data.ingest_rate ?? 0} msg/s`);
  } catch (_) {
    setHealth('h-mongo', false, 'ERR');
    setHealth('h-kafka', false, 'ERR');
  }
}
setInterval(fetchHealth, 10_000);

async function fetchAnomalyCount() {
  try {
    const data = await (await fetch(`${API_URL}/api/alerts/count?minutes=10`)).json();
    setText('kpi-anom', data.count);
  } catch (_) {}
}
setInterval(fetchAnomalyCount, 30_000);

/* ── Chart tab switcher ── */
document.getElementById('chart-tabs').addEventListener('click', (e) => {
  const tab = e.target.closest('.tab');
  if (!tab) return;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  tab.classList.add('active');
  switchChart(tab.dataset.pair, tab.dataset.ex);
});

/* ── Helpers ── */
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}
function fmt(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function fmtTime(iso) {
  return iso ? new Date(iso).toLocaleTimeString() : '—';
}
function setHealth(id, ok, label) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = label;
  el.className   = `hv ${ok ? 'ok' : 'err'}`;
}
function setStatus(id, state, label) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = label;
  el.className   = `hv ${state}`;
}

/* ── Boot ── */
initChart();
initVolChart();
fetchHealth();
fetchAnomalyCount();
connectWS();

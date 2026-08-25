import React, { useMemo, useState, useEffect, useRef, useCallback } from 'react';
import FuturesSpotPremium from './FuturesSpotPremium';
import ConstituentPanel from './ConstituentPanel';

const EXCHANGE_LABELS = {
  binance: 'Binance',
  bybit: 'Bybit',
  okx: 'OKX',
  bitget: 'Bitget',
  mexc: 'MEXC',
  gateio: 'Gate.io',
  bingx: 'BingX',
  kucoinfutures: 'KuCoin',
  aster: 'Aster',
  coinw: 'CoinW',
  hyperliquid: 'Hyperliquid',
  tradexyz: 'Trade.xyz',
  ourbit: 'Ourbit',
  deepcoin: 'DeepCoin',
  lbank: 'LBank',
  kraken: 'Kraken',
  deribit: 'Deribit',
  lighter: 'Lighter',
  lighter_rh: 'Lighter RH',
  binance_spot: 'Binance 現貨',
  bybit_spot: 'Bybit 現貨',
  okx_spot: 'OKX 現貨',
  bitget_spot: 'Bitget 現貨',
  mexc_spot: 'MEXC 現貨',
  gateio_spot: 'Gate.io 現貨',
  bingx_spot: 'BingX 現貨',
};

const EXCHANGE_ORDER = [
  'binance', 'bybit', 'okx', 'bitget', 'mexc', 'gateio', 'bingx', 'kucoinfutures', 'aster', 'coinw', 'hyperliquid', 'tradexyz', 'ourbit', 'deepcoin', 'lbank', 'kraken', 'deribit', 'lighter', 'lighter_rh'
];

const SPOT_ORDER = [
  'binance_spot', 'bybit_spot', 'okx_spot', 'bitget_spot', 'mexc_spot', 'gateio_spot', 'bingx_spot',
];

const isSpotExchange = (ex) => typeof ex === 'string' && ex.endsWith('_spot');

// 含現貨腿的配對，兩腿價差超過此% 視為「基準不同」（份額不同的代幣化股票），不配對
const SPOT_BASIS_MAX_DEVIATION_PCT = 20;

function rateColor(rate) {
  if (rate === null || rate === undefined) return 'text-gray-600';
  if (rate > 0.0001) return 'text-green-400';
  if (rate > 0) return 'text-green-600';
  if (rate < -0.0001) return 'text-red-400';
  if (rate < 0) return 'text-red-600';
  return 'text-gray-400';
}

function formatRate(rate) {
  if (rate === null || rate === undefined) return '-';
  return (rate * 100).toFixed(4) + '%';
}

function formatPrice(price) {
  if (!price) return '-';
  if (price >= 1000) return price.toFixed(2);
  if (price >= 1) return price.toFixed(4);
  return price.toPrecision(4);
}

function formatInterval(h) {
  if (!h || h === 8) return '8H';
  if (h >= 1) return `${h}H`;
  return `${Math.round(h * 60)}M`;
}

function shortSymbol(symbol) {
  return symbol.split('/')[0];
}

function formatTime(ts) {
  if (!ts) return '-';
  const d = new Date(ts);
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  return `${mm}-${dd} ${hh}:00`;
}

function formatFundingTime(isoStr) {
  if (!isoStr) return '-';
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return '-';
  // UTC+8
  const utc8 = new Date(d.getTime() + 8 * 3600000);
  const hh = String(utc8.getUTCHours()).padStart(2, '0');
  const mi = String(utc8.getUTCMinutes()).padStart(2, '0');
  return `${hh}:${mi}`;
}

function PremiumTooltip({ chartData, W, H, PAD, yMin, yMax, hover, setHover, shortLabel, longLabel }) {
  // hover 由 PremiumPanel 持有（受控），這樣下面的「資費差」圖能共用同一條十字線
  const ref = React.useRef(null);

  const handleMove = (e) => {
    const rect = ref.current?.getBoundingClientRect();
    if (!rect || chartData.length < 2) return;
    const x = e.clientX - rect.left;
    const pct = (x - rect.width * PAD / W) / (rect.width * (1 - 2 * PAD / W));
    const idx = Math.round(pct * (chartData.length - 1));
    if (idx >= 0 && idx < chartData.length) {
      setHover({ idx, x });
    }
  };

  const r = hover ? chartData[hover.idx] : null;
  const toY = (v) => PAD + (yMax - v) / (yMax - yMin) * (H - PAD * 2);

  return (
    <div
      ref={ref}
      className="absolute inset-0 cursor-crosshair"
      onMouseMove={handleMove}
      onMouseLeave={() => setHover(null)}
    >
      {hover && r && (
        <>
          {/* 垂直十字線 */}
          <div className="absolute top-0 bottom-0 w-px bg-gray-500/50" style={{ left: hover.x }} />
          {/* 水平線 */}
          <div className="absolute left-0 right-0 h-px bg-gray-500/30" style={{ top: toY(r.premium) }} />
          {/* 圓點 */}
          <div
            className="absolute w-2 h-2 rounded-full border border-gray-900"
            style={{
              left: hover.x - 4,
              top: toY(r.premium) - 4,
              backgroundColor: r.premium <= 0 ? '#22c55e' : '#ef4444',
            }}
          />
          {/* tooltip */}
          <div
            className="absolute z-30 pointer-events-none"
            style={{
              left: hover.x < 200 ? hover.x + 12 : hover.x - 160,
              top: 4,
            }}
          >
            <div className="bg-gray-900/95 border border-gray-700 rounded px-2.5 py-1.5 text-xs whitespace-nowrap shadow-lg">
              <div className="text-gray-400">
                {new Date(r.timestamp).toLocaleString('zh-TW', { timeZone: 'Asia/Taipei', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })}
              </div>
              <div className={`font-mono font-bold text-sm ${r.premium <= 0 ? 'text-green-400' : 'text-red-400'}`}>
                價差 {r.premium >= 0 ? '+' : ''}{r.premium.toFixed(4)}%
              </div>
              {r.rateDiff != null && (
                <>
                  {/* 各所當下的資費分開列出——只給「差」看不出是誰高誰低 */}
                  <div className="mt-1 pt-1 border-t border-gray-700/70 space-y-0.5">
                    <div className="flex justify-between gap-3 font-mono">
                      <span className="text-red-400">{shortLabel} 空</span>
                      <span className={r.sRate >= 0 ? 'text-green-400' : 'text-red-400'}>
                        {r.sRate >= 0 ? '+' : ''}{(r.sRate * 100).toFixed(4)}%
                      </span>
                    </div>
                    <div className="flex justify-between gap-3 font-mono">
                      <span className="text-green-400">{longLabel} 多</span>
                      <span className={r.lRate >= 0 ? 'text-green-400' : 'text-red-400'}>
                        {r.lRate >= 0 ? '+' : ''}{(r.lRate * 100).toFixed(4)}%
                      </span>
                    </div>
                  </div>
                  <div className={`font-mono text-sm ${r.rateDiff >= 0 ? 'text-orange-400' : 'text-purple-400'}`}>
                    資費差 {r.rateDiff >= 0 ? '+' : ''}{r.rateDiff.toFixed(4)}%
                  </div>
                </>
              )}
              <div className="text-gray-500 font-mono mt-0.5">
                ${r.price_a.toFixed(r.price_a >= 100 ? 2 : 4)} / ${r.price_b.toFixed(r.price_b >= 100 ? 2 : 4)}
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

function PremiumPanel({ symbol, shortEx, longEx, history }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  // 兩張圖共用同一個 hover：滑鼠在價差圖上移動時，資費差圖也要出現對應位置的十字線
  const [hover, setHover] = useState(null);

  useEffect(() => {
    setLoading(true);
    const params = new URLSearchParams({
      symbol, exchange_a: longEx, exchange_b: shortEx, days: '3',
    });
    fetch(`/api/price-premium?${params}`)
      .then(r => r.json())
      .then(res => {
        const rows = (res.data || []).map(d => ({
          ...d,
          // premium 已經是百分比
        }));
        rows.sort((a, b) => b.timestamp - a.timestamp);
        setData(rows);
      })
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [symbol, shortEx, longEx]);

  if (loading) return <div className="text-center text-gray-500 py-3 text-xs">載入價差資料中...</div>;
  if (!data || data.length === 0) return <div className="text-center text-gray-500 py-3 text-xs">無價差歷史資料</div>;

  const premiums = data.map(r => r.premium);
  const avg = premiums.reduce((s, v) => s + v, 0) / premiums.length;
  const maxP = Math.max(...premiums);
  const minP = Math.min(...premiums);
  const shortLabel = EXCHANGE_LABELS[shortEx] || shortEx;
  const longLabel = EXCHANGE_LABELS[longEx] || longEx;

  // 預先計算資費差（供圖表和表格共用）
  const shortRates = (history?.[shortEx] || []).slice().sort((a, b) => a.timestamp - b.timestamp);
  const longRates = (history?.[longEx] || []).slice().sort((a, b) => a.timestamp - b.timestamp);
  const findRate = (sorted, ts) => {
    let last = null;
    for (const e of sorted) {
      if (e.timestamp <= ts) last = e.rate;
      else break;
    }
    return last;
  };

  // 全部數據用於折線圖（時間正序），注入 rateDiff
  const chartData = data.slice().reverse().map(r => {
    const sRate = findRate(shortRates, r.timestamp);
    const lRate = findRate(longRates, r.timestamp);
    const rateDiff = (sRate != null && lRate != null) ? (sRate - lRate) * 100 : null;
    // sRate / lRate 一起帶著走：只給「差」看不出各所當下各是多少，tooltip 要分開列
    return { ...r, rateDiff, sRate, lRate };
  });
  // 時間軸刻度（取樣 6 個）
  const tickStep = Math.max(1, Math.floor(chartData.length / 6));
  const timeTicks = [];
  for (let i = 0; i < chartData.length; i += tickStep) timeTicks.push({ idx: i, ts: chartData[i].timestamp });

  return (
    <div className="bg-[var(--bg-secondary)] rounded p-3 space-y-2">
      <div className="flex items-center gap-4 text-xs text-gray-400 flex-wrap">
        <span>價差 = (<span className="text-green-400">做多 {longLabel}</span> 賣一 / <span className="text-red-400">做空 {shortLabel}</span> 買一 - 1) × 100%</span>
        <span>平均: <span className="text-white font-mono">{avg.toFixed(4)}%</span></span>
        <span>最高: <span className="text-yellow-400 font-mono">{maxP.toFixed(4)}%</span></span>
        <span>最低: <span className="text-blue-400 font-mono">{minP.toFixed(4)}%</span></span>
        <span className="text-gray-600">共 {data.length} 筆 (5分K)</span>
      </div>

      {/* 價差折線圖 */}
      {(() => {
        const W = 960, H = 120, PAD = 2;
        // Y 軸：以實際 min/max + 5% padding 為範圍，避免對稱浪費畫面高度
        const range = (maxP - minP) || 0.01;
        const pad = range * 0.05;
        const yMin = minP - pad;
        const yMax = maxP + pad;
        const hasZero = yMin <= 0 && yMax >= 0;
        const n = chartData.length;
        const toX = (i) => PAD + (i / (n - 1)) * (W - PAD * 2);
        const toY = (v) => PAD + (yMax - v) / (yMax - yMin) * (H - PAD * 2);
        const zeroY = hasZero ? toY(0) : (yMin > 0 ? H - PAD : PAD);
        const linePath = chartData.map((r, i) => `${i === 0 ? 'M' : 'L'}${toX(i).toFixed(1)},${toY(r.premium).toFixed(1)}`).join(' ');
        const fillPath = linePath + ` L${toX(n - 1).toFixed(1)},${zeroY} L${toX(0).toFixed(1)},${zeroY} Z`;
        // Y 軸刻度：5 個等距
        const step = range / 4;
        const yTicks = [yMax, yMax - pad - step, yMax - pad - step * 2, yMax - pad - step * 3, yMin];
        const maxAbsTick = Math.max(Math.abs(yMax), Math.abs(yMin));
        const fmtTick = (v) => {
          const digits = maxAbsTick >= 1 ? 2 : maxAbsTick >= 0.1 ? 3 : 4;
          return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
        };

        return (
          <div>
            <div className="text-[10px] text-gray-500 mb-1">價差（<span className="text-green-400">做多 {longLabel}</span> / <span className="text-red-400">做空 {shortLabel}</span> - 1，負值 = 進場有利）</div>
            <div className="flex">
              {/* Y 軸刻度 */}
              <div className="relative w-12 shrink-0" style={{ height: '120px' }}>
                {yTicks.map((v, i) => (
                  <span
                    key={i}
                    className="absolute right-1 text-[9px] font-mono text-gray-500 leading-none"
                    style={{ top: `${(toY(v) / H) * 100}%`, transform: 'translateY(-50%)' }}
                  >
                    {fmtTick(v)}
                  </span>
                ))}
              </div>
              <div className="flex-1 min-w-0">
                <div className="relative">
                  <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: '120px' }} preserveAspectRatio="none">
                    {/* Y 軸 grid lines（0 軸用較粗的 dash，其餘較淡） */}
                    {yTicks.map((v, i) => (
                      v === 0 ? null : (
                        <line key={i} x1={PAD} y1={toY(v)} x2={W - PAD} y2={toY(v)}
                          stroke="#374151" strokeWidth="0.3" strokeDasharray="2,3" />
                      )
                    ))}
                    {hasZero && <line x1={PAD} y1={zeroY} x2={W - PAD} y2={zeroY} stroke="#4b5563" strokeWidth="0.5" strokeDasharray="4,3" />}
                    <defs>
                      <linearGradient id="premFill" gradientUnits="userSpaceOnUse" x1="0" y1="0" x2="0" y2={H}>
                        <stop offset="0%" stopColor="#ef4444" stopOpacity="0.3" />
                        <stop offset={`${(zeroY / H) * 100}%`} stopColor="#ef4444" stopOpacity="0.05" />
                        <stop offset={`${(zeroY / H) * 100}%`} stopColor="#22c55e" stopOpacity="0.05" />
                        <stop offset="100%" stopColor="#22c55e" stopOpacity="0.3" />
                      </linearGradient>
                      <clipPath id="premClip"><rect x={PAD} y={0} width={W - PAD * 2} height={H} /></clipPath>
                    </defs>
                    <path d={fillPath} fill="url(#premFill)" clipPath="url(#premClip)" />
                    <path d={linePath} fill="none" stroke="#60a5fa" strokeWidth="1.5" vectorEffect="non-scaling-stroke" clipPath="url(#premClip)" />
                  </svg>
                  <PremiumTooltip chartData={chartData} W={W} H={H} PAD={PAD} yMin={yMin} yMax={yMax}
                    hover={hover} setHover={setHover} shortLabel={shortLabel} longLabel={longLabel} />
                </div>
                {/* X 軸時間刻度（絕對定位對齊資料點） */}
                <div className="relative mt-1" style={{ height: '18px' }}>
                  <div className="absolute left-0 right-0 top-0 h-px bg-gray-700" />
                  {timeTicks.map((t, i) => {
                    const xPct = (toX(t.idx) / W) * 100;
                    const transform = i === 0 ? 'translateX(0)' : i === timeTicks.length - 1 ? 'translateX(-100%)' : 'translateX(-50%)';
                    return (
                      <div key={i} className="absolute top-0" style={{ left: `${xPct}%` }}>
                        <div className="w-px bg-gray-600" style={{ height: '4px' }} />
                        <span className="absolute text-[9px] text-gray-600 font-mono whitespace-nowrap leading-none" style={{ top: '6px', left: 0, transform }}>
                          {new Date(t.ts).toLocaleString('zh-TW', { timeZone: 'Asia/Taipei', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        );
      })()}

      {/* 資費差折線圖 */}
      {(() => {
        const validRateDiffs = chartData.map(r => r.rateDiff).filter(v => v != null);
        if (validRateDiffs.length === 0) return null;

        const W = 960, H = 80, PAD = 2;
        const n = chartData.length;
        const toX = (i) => PAD + (i / (n - 1)) * (W - PAD * 2);
        const maxR = Math.max(...validRateDiffs);
        const minR = Math.min(...validRateDiffs);
        const rRange = (maxR - minR) || 0.01;
        const rPad = rRange * 0.05;
        const yMinR = minR - rPad;
        const yMaxR = maxR + rPad;
        const hasZeroR = yMinR <= 0 && yMaxR >= 0;
        const toYRate = (v) => PAD + (yMaxR - v) / (yMaxR - yMinR) * (H - PAD * 2);
        const zeroY = hasZeroR ? toYRate(0) : (yMinR > 0 ? H - PAD : PAD);

        const rateParts = [];
        let started = false;
        chartData.forEach((r, i) => {
          if (r.rateDiff != null) {
            rateParts.push(`${!started ? 'M' : 'L'}${toX(i).toFixed(1)},${toYRate(r.rateDiff).toFixed(1)}`);
            started = true;
          } else {
            started = false;
          }
        });
        const rateLinePath = rateParts.join(' ');
        const rateFillPath = rateLinePath + ` L${toX(n - 1).toFixed(1)},${zeroY} L${toX(0).toFixed(1)},${zeroY} Z`;

        const rStep = rRange / 4;
        const yTicksRate = [yMaxR, yMaxR - rPad - rStep, yMaxR - rPad - rStep * 2, yMaxR - rPad - rStep * 3, yMinR];
        const maxAbsRateTick = Math.max(Math.abs(yMaxR), Math.abs(yMinR));
        const fmtRateTick = (v) => {
          const digits = maxAbsRateTick >= 1 ? 2 : maxAbsRateTick >= 0.1 ? 3 : 4;
          return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
        };

        return (
          <div>
            <div className="text-[10px] text-gray-500 mb-1">資費差（<span className="text-orange-400">{shortLabel}</span> 資費 - <span className="text-orange-400">{longLabel}</span> 資費，每8H%）</div>
            <div className="flex">
              {/* Y 軸刻度 */}
              <div className="relative w-12 shrink-0" style={{ height: '80px' }}>
                {yTicksRate.map((v, i) => (
                  <span
                    key={i}
                    className="absolute right-1 text-[9px] font-mono text-gray-500 leading-none"
                    style={{ top: `${(toYRate(v) / H) * 100}%`, transform: 'translateY(-50%)' }}
                  >
                    {fmtRateTick(v)}
                  </span>
                ))}
              </div>
              <div className="flex-1 min-w-0 relative">
                <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height: '80px' }} preserveAspectRatio="none">
                  {yTicksRate.map((v, i) => (
                    v === 0 ? null : (
                      <line key={i} x1={PAD} y1={toYRate(v)} x2={W - PAD} y2={toYRate(v)}
                        stroke="#374151" strokeWidth="0.3" strokeDasharray="2,3" />
                    )
                  ))}
                  {hasZeroR && <line x1={PAD} y1={zeroY} x2={W - PAD} y2={zeroY} stroke="#4b5563" strokeWidth="0.5" strokeDasharray="4,3" />}
                  <defs>
                    <linearGradient id="rateFill" gradientUnits="userSpaceOnUse" x1="0" y1="0" x2="0" y2={H}>
                      <stop offset="0%" stopColor="#fb923c" stopOpacity="0.3" />
                      <stop offset={`${(zeroY / H) * 100}%`} stopColor="#fb923c" stopOpacity="0.05" />
                      <stop offset={`${(zeroY / H) * 100}%`} stopColor="#a855f7" stopOpacity="0.05" />
                      <stop offset="100%" stopColor="#a855f7" stopOpacity="0.3" />
                    </linearGradient>
                    <clipPath id="rateClip"><rect x={PAD} y={0} width={W - PAD * 2} height={H} /></clipPath>
                  </defs>
                  <path d={rateFillPath} fill="url(#rateFill)" clipPath="url(#rateClip)" />
                  <path d={rateLinePath} fill="none" stroke="#fb923c" strokeWidth="1.5" vectorEffect="non-scaling-stroke" clipPath="url(#rateClip)" />
                </svg>
                {/* 與上方價差圖共用的十字線：滑鼠在上面移動時，這裡同步標出對應時點。
                    沒有這條線就只看得到「差」的曲線，對不出游標當下落在哪個位置。 */}
                {hover && chartData[hover.idx]?.rateDiff != null && (
                  <>
                    <div className="absolute top-0 bottom-0 w-px bg-gray-500/50 pointer-events-none"
                      style={{ left: hover.x }} />
                    <div className="absolute w-2 h-2 rounded-full border border-gray-900 pointer-events-none"
                      style={{
                        left: hover.x - 4,
                        top: `${(toYRate(chartData[hover.idx].rateDiff) / H) * 100}%`,
                        marginTop: -4,
                        backgroundColor: chartData[hover.idx].rateDiff >= 0 ? '#fb923c' : '#a855f7',
                      }} />
                  </>
                )}
              </div>
            </div>
          </div>
        );
      })()}

      {/* 明細表格（每小時取樣） */}
      {(() => {
        // shortRates / longRates / findRate 已在上方計算
        const hourlyData = data.filter((_, i) => i % 12 === 0);

        return (
          <div className="overflow-auto max-h-[200px]">
            <table className="w-full border-collapse text-xs">
              <thead className="sticky top-0 z-10">
                <tr className="bg-[var(--bg-card)]">
                  <th className="px-2 py-1 text-left text-gray-500 border-b border-[var(--border)]">時間</th>
                  <th className="px-2 py-1 text-right text-gray-500 border-b border-[var(--border)]">{shortLabel}</th>
                  <th className="px-2 py-1 text-right text-gray-500 border-b border-[var(--border)]">{longLabel}</th>
                  <th className="px-2 py-1 text-right text-gray-500 border-b border-[var(--border)]">價差</th>
                  <th className="px-2 py-1 text-right text-gray-500 border-b border-[var(--border)]">資費差</th>
                </tr>
              </thead>
              <tbody>
                {hourlyData.map((r, i) => {
                  const sRate = findRate(shortRates, r.timestamp);
                  const lRate = findRate(longRates, r.timestamp);
                  const rateDiff = (sRate !== null && lRate !== null) ? sRate - lRate : null;
                  return (
                    <tr key={i} className="border-b border-[var(--border)]/20 hover:bg-[var(--bg-hover)]">
                      <td className="px-2 py-0.5 text-gray-400 whitespace-nowrap">
                        {new Date(r.timestamp).toLocaleString('zh-TW', { timeZone: 'Asia/Taipei', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })}
                      </td>
                      <td className="px-2 py-0.5 text-right font-mono text-red-400">${r.price_a.toFixed(r.price_a >= 100 ? 2 : 4)}</td>
                      <td className="px-2 py-0.5 text-right font-mono text-green-400">${r.price_b.toFixed(r.price_b >= 100 ? 2 : 4)}</td>
                      <td className={`px-2 py-0.5 text-right font-mono font-bold ${r.premium >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        {r.premium >= 0 ? '+' : ''}{r.premium.toFixed(4)}%
                      </td>
                      <td className={`px-2 py-0.5 text-right font-mono ${
                        rateDiff === null ? 'text-gray-600' : rateDiff >= 0 ? 'text-green-400' : 'text-red-400'
                      }`}>
                        {rateDiff !== null ? `${(rateDiff * 100).toFixed(4)}%` : '-'}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        );
      })()}
    </div>
  );
}

// ===== 歷史費率框選 hook =====
function useRateSelection() {
  const [selecting, setSelecting] = useState(false);
  const [startCell, setStartCell] = useState(null); // { row, col }
  const [endCell, setEndCell] = useState(null);
  const [selectedCells, setSelectedCells] = useState([]); // [{ row, col, value }]
  const [selectionStats, setSelectionStats] = useState(null);

  const getRange = (start, end) => {
    if (!start || !end) return { minRow: 0, maxRow: 0, minCol: 0, maxCol: 0 };
    return {
      minRow: Math.min(start.row, end.row),
      maxRow: Math.max(start.row, end.row),
      minCol: Math.min(start.col, end.col),
      maxCol: Math.max(start.col, end.col),
    };
  };

  const isCellInRange = (row, col) => {
    if (!startCell || !endCell) return false;
    const { minRow, maxRow, minCol, maxCol } = getRange(startCell, endCell);
    return row >= minRow && row <= maxRow && col >= minCol && col <= maxCol;
  };

  const finishSelection = (rows, exchanges) => {
    if (!startCell || !endCell) return;
    const { minRow, maxRow, minCol, maxCol } = getRange(startCell, endCell);
    const cells = [];
    for (let r = minRow; r <= maxRow; r++) {
      for (let c = minCol; c <= maxCol; c++) {
        if (r < rows.length && c < exchanges.length) {
          const rate = rows[r].rates[exchanges[c]];
          if (rate !== null && rate !== undefined) {
            cells.push({ row: r, col: c, value: rate });
          }
        }
      }
    }
    setSelectedCells(cells);
    if (cells.length > 0) {
      const sum = cells.reduce((s, c) => s + c.value, 0);
      const avg = sum / cells.length;
      // 按交易所欄位分別加總
      const byExchange = {};
      for (const c of cells) {
        const ex = exchanges[c.col];
        if (!byExchange[ex]) byExchange[ex] = { sum: 0, count: 0 };
        byExchange[ex].sum += c.value;
        byExchange[ex].count += 1;
      }
      setSelectionStats({ count: cells.length, sum, avg, byExchange });
    } else {
      setSelectionStats(null);
    }
  };

  const onMouseDown = (row, col) => {
    setSelecting(true);
    setStartCell({ row, col });
    setEndCell({ row, col });
    setSelectionStats(null);
    setSelectedCells([]);
  };

  const onMouseEnter = (row, col) => {
    if (selecting) {
      setEndCell({ row, col });
    }
  };

  const onMouseUp = (rows, exchanges) => {
    if (selecting) {
      setSelecting(false);
      finishSelection(rows, exchanges);
    }
  };

  const clearSelection = () => {
    setSelecting(false);
    setStartCell(null);
    setEndCell(null);
    setSelectedCells([]);
    setSelectionStats(null);
  };

  return { selecting, startCell, endCell, isCellInRange, selectionStats, onMouseDown, onMouseEnter, onMouseUp, clearSelection };
}

export default function SymbolDetail({ symbol, records, onClose, onRealtimeUpdate, defaultExchanges }) {
  const [history, setHistory] = useState(null);
  const [loading, setLoading] = useState(true);
  const [realtimeRecords, setRealtimeRecords] = useState(null);
  const [realtimeLoading, setRealtimeLoading] = useState(true);
  const [currentOpen, setCurrentOpen] = useState(true);
  const [historyOpen, setHistoryOpen] = useState(true);
  // 當前資金費率排序：[{key, dir}, ...]，多欄位排序
  const [currentSortKeys, setCurrentSortKeys] = useState([]);
  const [premiumIdx, setPremiumIdx] = useState(null);
  const [premiumMode, setPremiumMode] = useState(false);
  const [connectStatus, setConnectStatus] = useState({}); // {idx: 'ok'|'err'}
  // 套利篩選條件
  const [arbFilters, setArbFilters] = useState({ longEx: '', shortEx: '', minDiff: '', minSpread: '' });
  // 套利表格排序：{ key: 'diff'|'spread'|'profit', dir: 'asc'|'desc' } | null
  const [arbSort, setArbSort] = useState(null);
  // 納入現貨做多方（套利建議）— localStorage 持久化
  const [includeSpot, setIncludeSpot] = useState(() => {
    try { return localStorage.getItem('symbolDetail_includeSpot') === '1'; } catch { return false; }
  });
  useEffect(() => {
    try { localStorage.setItem('symbolDetail_includeSpot', includeSpot ? '1' : '0'); } catch {}
  }, [includeSpot]);
  // 納入現貨做空方（空現貨/多合約）— localStorage 持久化
  const [includeSpotShort, setIncludeSpotShort] = useState(() => {
    try { return localStorage.getItem('symbolDetail_includeSpotShort') === '1'; } catch { return false; }
  });
  useEffect(() => {
    try { localStorage.setItem('symbolDetail_includeSpotShort', includeSpotShort ? '1' : '0'); } catch {}
  }, [includeSpotShort]);
  // 空方現貨借貸年化(%)：{exchange_id: annual_pct}，僅在納入空方現貨時抓（目前 Bybit/Bitget）
  const [borrowRates, setBorrowRates] = useState({});
  useEffect(() => {
    if (!symbol || !includeSpotShort) { setBorrowRates({}); return; }
    let cancelled = false;
    const base = symbol.split('/')[0].toUpperCase();
    fetch(`/api/spot-borrow-rates?coin=${encodeURIComponent(base)}`)
      .then(r => r.json())
      .then(d => { if (!cancelled) setBorrowRates(d.annual_pct || {}); })
      .catch(() => { if (!cancelled) setBorrowRates({}); });
    return () => { cancelled = true; };
  }, [symbol, includeSpotShort]);
  const rateSelection = useRateSelection();
  const [showConstituent, setShowConstituent] = useState(false);
  const [columnOrder, setColumnOrder] = useState(null); // 自訂交易所欄位排序
  const [dragCol, setDragCol] = useState(null); // 正在拖曳的欄位 index
  const [dragOverCol, setDragOverCol] = useState(null); // 拖曳目標欄位 index
  const [historyHeight, setHistoryHeight] = useState(680); // 歷史表格高度（預設能顯示 24h+1 內容；含「近24h」「今日08:00」兩列 sticky 統計列）
  const resizingRef = useRef(false);
  const resizeStartRef = useRef({ y: 0, h: 0 });

  // 取得歷史費率 + 即時費率
  // 1. 歷史 + 掃描快取（快，毫秒級）先顯示
  // 2. 即時 API（慢，幾秒）完成後替換
  // 3. WebSocket 串流持續更新
  const [cachedRecords, setCachedRecords] = useState(null);
  const [wsData, setWsData] = useState({});  // {exchange: latestUpdate}
  const [wsConnected, setWsConnected] = useState(false);
  const wsRef = useRef(null);

  useEffect(() => {
    if (!symbol) return;
    setLoading(true);
    setRealtimeLoading(true);
    setWsData({});
    setWsConnected(false);

    // 歷史費率（從本地快取，快）
    fetch(`/api/funding-history?symbol=${encodeURIComponent(symbol)}&limit=100`)
      .then(r => r.json())
      .then(data => setHistory(data.history))
      .catch(() => setHistory(null))
      .finally(() => setLoading(false));

    // 掃描快取的當前費率（快，先顯示）
    const base = symbol.split('/')[0].toUpperCase();
    fetch(`/api/funding-rates?symbol=${base}`)
      .then(r => r.json())
      .then(data => {
        const matched = (data.records || []).filter(r =>
          (r.normalized_symbol || r.symbol) === symbol
        );
        if (matched.length > 0) setCachedRecords(matched);
      })
      .catch(() => {});

    // 即時 API（慢，完成後替換快取）
    fetch(`/api/funding-realtime?symbol=${encodeURIComponent(symbol)}`)
      .then(r => r.json())
      .then(data => {
        setRealtimeRecords(data.records);
        if (onRealtimeUpdate) onRealtimeUpdate(data.records);
      })
      .catch(() => setRealtimeRecords(null))
      .finally(() => setRealtimeLoading(false));

    // WebSocket 即時串流
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${proto}//${window.location.host}/ws/symbol?symbol=${encodeURIComponent(symbol)}`);
    ws.onopen = () => setWsConnected(true);
    ws.onclose = () => setWsConnected(false);
    ws.onerror = () => setWsConnected(false);
    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'snapshot' && Array.isArray(msg.data)) {
          const map = {};
          for (const d of msg.data) map[d.exchange] = d;
          setWsData(map);
        } else if (msg.type === 'update' && msg.data) {
          setWsData(prev => ({ ...prev, [msg.data.exchange]: msg.data }));
        }
      } catch {}
    };
    wsRef.current = ws;

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [symbol]);

  // 通知後端啟用/關閉現貨串流
  useEffect(() => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    try {
      ws.send(JSON.stringify({ action: (includeSpot || includeSpotShort) ? 'spot_on' : 'spot_off' }));
    } catch {}
  }, [includeSpot, includeSpotShort, wsConnected, symbol]);

  // 該幣種在各交易所的當前費率
  // 優先順序：WS 即時 > REST 即時 > 掃描快取
  // 分離合約與現貨：現貨僅用於套利建議（做多方），不顯示在當前費率/歷史表
  const { exchangeRates, spotRates } = useMemo(() => {
    const byExchange = {};

    // 1. 掃描快取打底（僅合約）
    const fallback = cachedRecords || records || [];
    for (const r of fallback.filter(r => (r.normalized_symbol || r.symbol) === symbol)) {
      byExchange[r.exchange] = { ...r };
    }

    // 2. REST 即時 API 覆蓋（僅合約）
    if (realtimeRecords && realtimeRecords.length > 0) {
      for (const r of realtimeRecords) {
        byExchange[r.exchange] = { ...byExchange[r.exchange], ...r,
          is_delisting: (byExchange[r.exchange]?.is_delisting || r.is_delisting),
          delisting_time: (byExchange[r.exchange]?.delisting_time || r.delisting_time),
        };
      }
    }

    // 3. WebSocket 即時資料覆蓋（最高優先），同時接收現貨資料
    for (const [ex, update] of Object.entries(wsData)) {
      if (byExchange[ex]) {
        if (update.funding_rate != null) byExchange[ex].funding_rate = update.funding_rate;
        if (update.bid_price != null) byExchange[ex].bid_price = update.bid_price;
        if (update.ask_price != null) byExchange[ex].ask_price = update.ask_price;
        if (update.mark_price != null) byExchange[ex].mark_price = update.mark_price;
        if (update.index_price != null) byExchange[ex].index_price = update.index_price;
        byExchange[ex]._live = update.live;
      } else {
        byExchange[ex] = { ...update, _live: update.live };
      }
    }

    const futures = EXCHANGE_ORDER
      .filter(id => byExchange[id])
      .map(id => byExchange[id]);
    const spots = SPOT_ORDER
      .filter(id => byExchange[id] && byExchange[id].bid_price && byExchange[id].ask_price)
      .map(id => byExchange[id]);

    return { exchangeRates: futures, spotRates: spots };
  }, [records, cachedRecords, realtimeRecords, wsData, symbol]);

  // 歷史費率矩陣
  const { historyRows, historyExchanges, autoStats } = useMemo(() => {
    if (!history) return { historyRows: [], historyExchanges: [], autoStats: {} };

    // 收集所有有資料的交易所
    const exSet = new Set();
    const timeMap = {};

    for (const [exName, entries] of Object.entries(history)) {
      exSet.add(exName);
      for (const entry of entries) {
        // 對齊到整點小時，避免各交易所微小差異導致重複行
        const ts = Math.round(entry.timestamp / 3600000) * 3600000;
        if (!timeMap[ts]) timeMap[ts] = {};
        timeMap[ts][exName] = entry.rate;
      }
    }

    const exchanges = EXCHANGE_ORDER.filter(id => exSet.has(id));
    const rows = Object.keys(timeMap)
      .map(Number)
      .sort((a, b) => b - a) // 最新的在上面
      .filter(ts => ts > Date.now() - 3 * 24 * 3600 * 1000) // 最近 3 天
      .map(ts => ({ ts, rates: timeMap[ts] }));

    // 自動統計：近 24h + 今日 GMT+8 08:00 起（抄 5016 邏輯）
    const now = Date.now();
    const ts24h = now - 24 * 3600 * 1000;
    const gmt8Now = new Date(now + 8 * 3600 * 1000);
    const gmt8Hour = gmt8Now.getUTCHours();
    const dayShift = gmt8Hour < 8 ? -1 : 0;
    const todayStart = Date.UTC(
      gmt8Now.getUTCFullYear(),
      gmt8Now.getUTCMonth(),
      gmt8Now.getUTCDate() + dayShift,
      0, 0, 0
    );
    const stats = {};
    for (const ex of exchanges) {
      stats[ex] = { sum24h: 0, count24h: 0, sumToday: 0, countToday: 0 };
    }
    for (const row of rows) {
      for (const ex of exchanges) {
        const rate = row.rates[ex];
        if (rate == null) continue;
        if (row.ts >= ts24h) {
          stats[ex].sum24h += rate;
          stats[ex].count24h++;
        }
        if (row.ts >= todayStart) {
          stats[ex].sumToday += rate;
          stats[ex].countToday++;
        }
      }
    }

    return { historyRows: rows, historyExchanges: exchanges, autoStats: stats };
  }, [history]);

  // 當 historyExchanges 改變時重置自訂排序
  useEffect(() => {
    if (historyExchanges.length > 0) {
      setColumnOrder(prev => {
        // 保留之前的排序，僅在交易所列表完全不同時重置
        if (prev && prev.length === historyExchanges.length && prev.every(ex => historyExchanges.includes(ex))) {
          return prev;
        }
        return historyExchanges;
      });
    }
  }, [historyExchanges]);

  // 歷史表格高度拖曳調整
  const handleResizeMouseDown = useCallback((e) => {
    e.preventDefault();
    resizingRef.current = true;
    resizeStartRef.current = { y: e.clientY, h: historyHeight };

    const onMouseMove = (ev) => {
      if (!resizingRef.current) return;
      const delta = ev.clientY - resizeStartRef.current.y;
      const newH = Math.max(100, Math.min(800, resizeStartRef.current.h + delta));
      setHistoryHeight(newH);
    };
    const onMouseUp = () => {
      resizingRef.current = false;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, [historyHeight]);

  // 實際使用的交易所排序
  const orderedExchanges = columnOrder && columnOrder.length === historyExchanges.length ? columnOrder : historyExchanges;

  const handleColumnDragStart = (idx) => {
    setDragCol(idx);
  };
  const handleColumnDragOver = (e, idx) => {
    e.preventDefault();
    setDragOverCol(idx);
  };
  const handleColumnDrop = (idx) => {
    if (dragCol === null || dragCol === idx) {
      setDragCol(null);
      setDragOverCol(null);
      return;
    }
    const newOrder = [...orderedExchanges];
    const [moved] = newOrder.splice(dragCol, 1);
    newOrder.splice(idx, 0, moved);
    setColumnOrder(newOrder);
    rateSelection.clearSelection();
    setDragCol(null);
    setDragOverCol(null);
  };
  const handleColumnDragEnd = () => {
    setDragCol(null);
    setDragOverCol(null);
  };

  // 套利配對（統一到較長結算週期比較）
  // 做空（i）：合約 +（勾選時）現貨；做多（j）：合約 +（勾選時）現貨；兩腿皆現貨無意義故略過
  const arbPairs = useMemo(() => {
    const hasSpot = spotRates.length > 0 && exchangeRates.length >= 1;
    const canPair = exchangeRates.length >= 2
      || (includeSpot && hasSpot) || (includeSpotShort && hasSpot);
    if (!canPair) return [];

    const longCandidates = includeSpot ? [...exchangeRates, ...spotRates] : exchangeRates;
    const shortCandidates = includeSpotShort ? [...exchangeRates, ...spotRates] : exchangeRates;

    const pairs = [];
    for (let i = 0; i < shortCandidates.length; i++) {
      for (let j = 0; j < longCandidates.length; j++) {
        const a = shortCandidates[i]; // 做空（合約或現貨）
        const b = longCandidates[j];  // 做多（合約或現貨）
        if (a.exchange === b.exchange) continue;
        const aIsSpot = isSpotExchange(a.exchange);
        const bIsSpot = isSpotExchange(b.exchange);
        if (aIsSpot && bIsSpot) continue; // 兩腿皆現貨：無資費差、無意義

        // 現貨腿無自己的結算週期，沿用對手合約腿的週期
        const aInterval = aIsSpot ? (b.funding_interval_h || 8) : (a.funding_interval_h || 8);
        const bInterval = bIsSpot ? (a.funding_interval_h || 8) : (b.funding_interval_h || 8);

        const normH = Math.max(aInterval, bInterval);
        const aNorm = aIsSpot ? 0 : (a.funding_rate || 0) * (normH / aInterval);
        const bNorm = bIsSpot ? 0 : (b.funding_rate || 0) * (normH / bInterval);
        const diff = aNorm - bNorm;
        const diffPct = diff * 100;

        // 進場成本（與 5005 一致）：(做多ask - 做空bid) / 做空bid * 100%
        // 負數 = 有利（做多買比做空賣便宜）
        let spreadPct = null;
        if (a.bid_price && b.ask_price && a.bid_price > 0) {
          spreadPct = ((b.ask_price - a.bid_price) / a.bid_price) * 100;
        }

        // 現貨腿基準防呆：代幣化股票／商品在各所的「每份額」可能不同
        // （例：TQQQ 分割後 gateio 現貨 127.41 vs binance 現貨 64.13，差 2 倍），
        // 基準不一致的兩腿算出來是假的巨額價差、還會排到最前面。
        // 門檻沿用 rwa_arb 判定「同名不同標的」的 20%，正常現貨/合約價差遠低於此。
        if ((aIsSpot || bIsSpot) && spreadPct !== null
            && Math.abs(spreadPct) > SPOT_BASIS_MAX_DEVIATION_PCT) continue;

        // 空方現貨的借貸成本：借幣賣出要付利息，年化%換算到「每期%」再扣
        let shortBorrowAnnual = null;
        let borrowPerPeriod = 0;
        let borrowKnown = true;
        if (aIsSpot) {
          const exId = a.exchange.replace('_spot', '');
          const ann = borrowRates[exId];
          if (ann != null) {
            shortBorrowAnnual = ann;
            borrowPerPeriod = ann / (8760 / normH); // 年化% ÷ 每年期數
          } else {
            borrowKnown = false; // 該所借貸利率尚未接
          }
        }

        // 預計利潤 = 正規化費率收入 − 進場成本 −（空方現貨）借貸成本
        const estProfit = spreadPct !== null ? diffPct - spreadPct - borrowPerPeriod : null;

        pairs.push({
          shortEx: a.exchange,
          longEx: b.exchange,
          shortSymbol: a.symbol,
          longSymbol: b.symbol,
          shortRate: aIsSpot ? null : a.funding_rate,
          shortNormRate: aNorm,
          longRate: bIsSpot ? null : b.funding_rate,
          longNormRate: bNorm,
          shortInterval: aIsSpot ? null : a.funding_interval_h,
          longInterval: bIsSpot ? null : b.funding_interval_h,
          normInterval: normH,
          shortIsSpot: aIsSpot,
          longIsSpot: bIsSpot,
          shortBorrowAnnual,
          borrowPerPeriod,
          borrowKnown,
          diff,
          spreadPct,
          estProfit,
        });
      }
    }
    pairs.sort((a, b) => (b.estProfit ?? b.diff * 100) - (a.estProfit ?? a.diff * 100));
    return pairs;
  }, [exchangeRates, spotRates, includeSpot, includeSpotShort, borrowRates]);

  // 套利篩選
  const filteredArbPairs = useMemo(() => {
    let list = arbPairs;
    if (arbFilters.longEx) list = list.filter(p => p.longEx === arbFilters.longEx);
    if (arbFilters.shortEx) list = list.filter(p => p.shortEx === arbFilters.shortEx);
    if (arbFilters.minDiff !== '') {
      const v = parseFloat(arbFilters.minDiff);
      if (!isNaN(v)) list = list.filter(p => p.diff * 100 >= v);
    }
    if (arbFilters.minSpread !== '') {
      const v = parseFloat(arbFilters.minSpread);
      if (!isNaN(v)) list = list.filter(p => p.spreadPct != null && p.spreadPct <= v);
    }
    return list;
  }, [arbPairs, arbFilters]);

  // 套利表格最終排序
  const sortedArbPairs = useMemo(() => {
    if (!arbSort) return filteredArbPairs;
    const getter = (p) => {
      if (arbSort.key === 'diff') return p.diff;
      if (arbSort.key === 'spread') return p.spreadPct;
      if (arbSort.key === 'profit') return p.estProfit;
      return null;
    };
    const asc = arbSort.dir === 'asc';
    return [...filteredArbPairs].sort((a, b) => {
      const va = getter(a);
      const vb = getter(b);
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      return asc ? va - vb : vb - va;
    });
  }, [filteredArbPairs, arbSort]);

  // 預設方向：diff/profit 降冪（高在上），spread 升冪（低成本在上）
  const defaultDir = (key) => (key === 'spread' ? 'asc' : 'desc');

  const handleArbSort = (key) => {
    setArbSort(prev => {
      if (!prev || prev.key !== key) return { key, dir: defaultDir(key) };
      if (prev.dir === defaultDir(key)) return { key, dir: defaultDir(key) === 'desc' ? 'asc' : 'desc' };
      return null; // 第三次點擊 → 取消排序
    });
  };

  // 當前資金費率排序後的列表
  const sortedExchangeRates = useMemo(() => {
    if (currentSortKeys.length === 0) return exchangeRates;
    const getter = (r, key) => {
      if (key === 'exchange') return EXCHANGE_ORDER.indexOf(r.exchange);
      if (key === 'funding_rate') return r.funding_rate;
      if (key === 'funding_time') return r.funding_time ? new Date(r.funding_time).getTime() : null;
      if (key === 'funding_interval_h') return r.funding_interval_h;
      if (key === 'bid_price') return r.bid_price;
      if (key === 'ask_price') return r.ask_price;
      if (key === 'max_notional') return r.max_notional;
      return null;
    };
    return [...exchangeRates].sort((a, b) => {
      for (const { key, dir } of currentSortKeys) {
        const va = getter(a, key);
        const vb = getter(b, key);
        if (va == null && vb == null) continue;
        if (va == null) return 1;
        if (vb == null) return -1;
        const diff = va - vb;
        if (diff !== 0) return dir === 'desc' ? -diff : diff;
      }
      return 0;
    });
  }, [exchangeRates, currentSortKeys]);

  const handleCurrentSort = (key) => {
    setCurrentSortKeys(prev => {
      const idx = prev.findIndex(k => k.key === key);
      if (idx >= 0) {
        const cur = prev[idx];
        if (cur.dir === 'desc') {
          return [...prev.slice(0, idx), { key, dir: 'asc' }, ...prev.slice(idx + 1)];
        }
        return [...prev.slice(0, idx), ...prev.slice(idx + 1)];
      }
      return [...prev, { key, dir: 'desc' }];
    });
  };

  if (!symbol) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <div
        className="bg-[var(--bg-card)] border border-[var(--border)] rounded-lg shadow-2xl h-full overflow-auto max-w-full w-fit"
        style={{ minWidth: premiumMode ? 1600 : 1060 }}
      >
        {/* 標題 */}
        <div className="sticky top-0 z-20 flex items-center justify-between px-4 py-3 border-b border-[var(--border)] bg-[var(--bg-card)]">
          <div className="flex items-center gap-3">
            <h2 className="text-base font-bold text-white">
              資金費率比較 - {shortSymbol(symbol)}USDT
            </h2>
            <button
              onClick={() => setPremiumMode(v => !v)}
              className={`px-3 py-1 rounded text-xs font-medium border transition-colors ${
                premiumMode
                  ? 'bg-orange-500/20 text-orange-400 border-orange-500/40'
                  : 'text-gray-400 border-gray-600 hover:text-orange-400 hover:border-orange-500/40'
              }`}
            >
              溢價
            </button>
            <button
              onClick={() => setShowConstituent(true)}
              className="px-3 py-1 rounded text-xs font-medium border text-gray-400 border-gray-600 hover:text-blue-400 hover:border-blue-500/40 transition-colors"
            >
              指數成分
            </button>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white text-xl leading-none px-2">&times;</button>
        </div>

        <div className="p-4 space-y-5">

          {/* ===== 溢價模式 ===== */}
          {premiumMode ? (
            <FuturesSpotPremium symbol={symbol} defaultExchanges={defaultExchanges} />
          ) : (
          <>

          {/* ===== 當前資金費率（可摺疊） ===== */}
          <section>
            <div className="flex items-center gap-2 mb-2">
              <button
                onClick={() => setCurrentOpen(!currentOpen)}
                className="flex items-center gap-1.5 text-xs font-bold text-gray-400 uppercase tracking-wide hover:text-gray-300 transition-colors"
              >
                <span className={`inline-block transition-transform ${currentOpen ? 'rotate-90' : ''}`}>&#9654;</span>
                當前資金費率
              </button>
              {wsConnected ? (
                <span className="text-[10px] text-emerald-400 font-bold normal-case flex items-center gap-1">
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                  LIVE
                </span>
              ) : realtimeLoading ? (
                <span className="text-[10px] text-yellow-500 font-normal normal-case">更新中...</span>
              ) : realtimeRecords ? (
                <span className="text-[10px] text-green-500 font-normal normal-case">已更新</span>
              ) : null}
            </div>
            {currentOpen && <div className="overflow-auto">
              <table className="w-full border-collapse text-xs">
                <thead>
                  <tr className="bg-[var(--bg-secondary)]">
                    {[
                      { key: 'exchange', label: '交易所', align: 'text-left' },
                      { key: 'funding_rate', label: '資金費率', align: 'text-right' },
                      { key: 'funding_time', label: '下次收費', align: 'text-center' },
                      { key: 'funding_interval_h', label: '週期', align: 'text-center' },
                      { key: 'bid_price', label: '買一價', align: 'text-right' },
                      { key: 'ask_price', label: '賣一價', align: 'text-right' },
                      { key: 'max_notional', label: '倉位限制', align: 'text-right' },
                    ].map(col => {
                      const si = currentSortKeys.findIndex(k => k.key === col.key);
                      const info = si >= 0 ? currentSortKeys[si] : null;
                      return (
                        <th
                          key={col.key}
                          className={`px-2 py-1.5 ${col.align} border-b border-[var(--border)] cursor-pointer select-none hover:text-white transition-colors whitespace-nowrap ${info ? 'text-yellow-400' : 'text-[var(--text-secondary)]'}`}
                          onClick={() => handleCurrentSort(col.key)}
                        >
                          {col.label}
                          {info && (
                            <span className="ml-0.5 text-[10px]">
                              {currentSortKeys.length > 1 && <span className="text-yellow-500">{si + 1}</span>}
                              {info.dir === 'desc' ? '▾' : '▴'}
                            </span>
                          )}
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {sortedExchangeRates.map(r => (
                    <tr key={r.exchange} className="border-b border-[var(--border)]/30 hover:bg-[var(--bg-hover)]">
                      <td className="px-2 py-1.5 text-white font-medium">
                        {r._live && <span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-400 mr-1.5 align-middle" title="即時串流" />}
                        {EXCHANGE_LABELS[r.exchange] || r.exchange}
                        {r.is_delisting && <span className="ml-1 text-[9px] text-red-400 font-bold" title="此合約即將下架">⚠下架</span>}
                      </td>
                      <td className={`px-2 py-1.5 text-right font-mono ${rateColor(r.funding_rate)}`}>{formatRate(r.funding_rate)}</td>
                      <td className="px-2 py-1.5 text-center text-gray-400">{formatFundingTime(r.funding_time)}</td>
                      <td className="px-2 py-1.5 text-center text-gray-400">{formatInterval(r.funding_interval_h)}</td>
                      <td className="px-2 py-1.5 text-right font-mono text-gray-400">{formatPrice(r.bid_price)}</td>
                      <td className="px-2 py-1.5 text-right font-mono text-gray-400">{formatPrice(r.ask_price)}</td>
                      <td className={`px-2 py-1.5 text-right font-mono text-xs ${
                        r.max_notional == null ? 'text-gray-600'
                        : r.max_notional <= 500 ? 'text-red-400 font-bold'
                        : r.max_notional <= 5000 ? 'text-orange-400'
                        : r.max_notional <= 50000 ? 'text-yellow-400'
                        : 'text-green-400'
                      }`}>{r.max_notional != null ? (r.max_notional >= 1e6 ? `${(r.max_notional/1e6).toFixed(1)}M` : r.max_notional >= 1e3 ? `${(r.max_notional/1e3).toFixed(0)}K` : `${r.max_notional.toFixed(0)}`) + 'U' : '-'}</td>
                    </tr>
                  ))}
                  {sortedExchangeRates.length === 0 && (
                    <tr><td colSpan={7} className="px-2 py-4 text-center text-gray-500">無資料</td></tr>
                  )}
                </tbody>
              </table>
            </div>}
          </section>

          {/* ===== 歷史資金費率（可摺疊） ===== */}
          <section>
            <div className="flex items-center gap-2 mb-2 flex-wrap">
              <button
                onClick={() => setHistoryOpen(o => !o)}
                className="flex items-center gap-1.5 text-xs font-bold text-gray-400 uppercase tracking-wide hover:text-gray-300 transition-colors"
              >
                <span className={`inline-block transition-transform ${historyOpen ? 'rotate-90' : ''}`}>&#9654;</span>
                歷史資金費率
                {!loading && historyRows.length > 0 && (
                  <span className="font-normal text-gray-600">({historyRows.length})</span>
                )}
              </button>
              {rateSelection.selectionStats && (() => {
                const stats = rateSelection.selectionStats;
                const exKeys = stats.byExchange ? Object.keys(stats.byExchange) : [];
                const exSums = exKeys.map(ex => ({ ex, sum: stats.byExchange[ex].sum, count: stats.byExchange[ex].count }));
                // 差額：如果剛好選了2個交易所，顯示差額
                const showDiff = exSums.length === 2;
                const diff = showDiff ? exSums[0].sum - exSums[1].sum : null;
                return (
                  <span className="text-xs font-mono text-blue-300 flex items-center gap-2 flex-wrap">
                    {exSums.length <= 1 ? (
                      <span>
                        選中{stats.count}筆
                        {' '}總和:<span className={stats.sum >= 0 ? 'text-green-400' : 'text-red-400'}>
                          {stats.sum >= 0 ? '+' : ''}{(stats.sum * 100).toFixed(4)}%
                        </span>
                        {' '}平均:<span className={stats.avg >= 0 ? 'text-green-400' : 'text-red-400'}>
                          {stats.avg >= 0 ? '+' : ''}{(stats.avg * 100).toFixed(4)}%
                        </span>
                      </span>
                    ) : (
                      <span>
                        {exSums.map(({ ex, sum, count }) => (
                          <span key={ex} className="mr-2">
                            {EXCHANGE_LABELS[ex] || ex}:
                            <span className={sum >= 0 ? 'text-green-400' : 'text-red-400'}>
                              {sum >= 0 ? '+' : ''}{(sum * 100).toFixed(4)}%
                            </span>
                            <span className="text-gray-600">({count})</span>
                          </span>
                        ))}
                        {showDiff && (
                          <span className="border-l border-[var(--border)] pl-2">
                            差額:<span className="text-yellow-300">
                              {diff >= 0 ? '+' : ''}{(diff * 100).toFixed(4)}%
                            </span>
                          </span>
                        )}
                      </span>
                    )}
                    <button onClick={rateSelection.clearSelection} className="text-blue-400 hover:text-white ml-1">&times;</button>
                  </span>
                );
              })()}
            </div>
            {historyOpen && (
              loading ? (
                <div className="text-center text-gray-500 py-4 text-xs">載入中...</div>
              ) : historyRows.length === 0 ? (
                <div className="text-center text-gray-500 py-4 text-xs">無歷史資料</div>
              ) : (
                <div className="relative">
                  <div
                    className="overflow-y-auto"
                    style={{ maxHeight: `${historyHeight}px` }}
                    onMouseUp={() => rateSelection.onMouseUp(historyRows, orderedExchanges)}
                    onMouseLeave={() => { if (rateSelection.selecting) rateSelection.onMouseUp(historyRows, orderedExchanges); }}
                  >
                    <table className="w-full border-collapse text-xs select-none">
                      <thead className="sticky top-0 z-10">
                        <tr className="bg-[var(--bg-secondary)]">
                          <th className="px-2 py-1.5 text-left text-[var(--text-secondary)] border-b border-[var(--border)] whitespace-nowrap">時間</th>
                          {orderedExchanges.map((ex, colIdx) => (
                            <th
                              key={ex}
                              draggable
                              onDragStart={() => handleColumnDragStart(colIdx)}
                              onDragOver={(e) => handleColumnDragOver(e, colIdx)}
                              onDrop={() => handleColumnDrop(colIdx)}
                              onDragEnd={handleColumnDragEnd}
                              className={`px-2 py-1.5 text-right text-[var(--text-secondary)] border-b border-[var(--border)] whitespace-nowrap cursor-grab active:cursor-grabbing transition-colors ${
                                dragCol === colIdx ? 'opacity-40' : ''
                              } ${dragOverCol === colIdx && dragCol !== colIdx ? 'bg-blue-500/20 border-l-2 border-l-blue-400' : ''}`}
                            >
                              <span className="inline-flex items-center gap-1">
                                <span className="text-gray-600 text-[9px]">⠿</span>
                                {EXCHANGE_LABELS[ex] || ex}
                              </span>
                            </th>
                          ))}
                        </tr>
                        <tr className="bg-[var(--bg-secondary)]/70 border-b border-[var(--border)]/50">
                          <th className="px-2 py-1 text-left text-xs text-gray-500 font-normal whitespace-nowrap">近 24h</th>
                          {orderedExchanges.map(ex => {
                            const s = autoStats[ex] || { sum24h: 0, count24h: 0 };
                            return (
                              <th key={ex} title={`${s.count24h} 筆`} className={`px-2 py-1 text-right font-mono font-normal text-xs ${rateColor(s.sum24h)}`}>
                                {formatRate(s.sum24h)}
                              </th>
                            );
                          })}
                        </tr>
                        <tr className="bg-[var(--bg-secondary)]/70 border-b border-[var(--border)]">
                          <th className="px-2 py-1 text-left text-xs text-gray-500 font-normal whitespace-nowrap">今日 08:00</th>
                          {orderedExchanges.map(ex => {
                            const s = autoStats[ex] || { sumToday: 0, countToday: 0 };
                            return (
                              <th key={ex} title={`${s.countToday} 筆`} className={`px-2 py-1 text-right font-mono font-normal text-xs ${rateColor(s.sumToday)}`}>
                                {formatRate(s.sumToday)}
                              </th>
                            );
                          })}
                        </tr>
                      </thead>
                      <tbody>
                        {historyRows.map((row, rowIdx) => (
                          <tr key={row.ts} className="border-b border-[var(--border)]/30 hover:bg-[var(--bg-hover)]">
                            <td className="px-2 py-1 text-gray-400 whitespace-nowrap">{formatTime(row.ts)}</td>
                            {orderedExchanges.map((ex, colIdx) => {
                              const rate = row.rates[ex];
                              const inRange = rateSelection.isCellInRange(rowIdx, colIdx);
                              return (
                                <td
                                  key={ex}
                                  className={`px-2 py-1 text-right font-mono cursor-crosshair ${rateColor(rate)} ${inRange ? 'bg-blue-500/30 ring-1 ring-blue-500/50' : ''}`}
                                  onMouseDown={(e) => { e.preventDefault(); rateSelection.onMouseDown(rowIdx, colIdx); }}
                                  onMouseEnter={() => rateSelection.onMouseEnter(rowIdx, colIdx)}
                                >
                                  {formatRate(rate)}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {/* 拖曳調整把手 */}
                  <div
                    onMouseDown={handleResizeMouseDown}
                    className="flex items-center justify-center h-4 cursor-ns-resize group border-t border-[var(--border)]/30 hover:bg-[var(--bg-hover)] transition-colors"
                  >
                    <div className="flex gap-0.5">
                      <span className="block w-6 h-[2px] rounded bg-gray-600 group-hover:bg-gray-400 transition-colors" />
                    </div>
                  </div>
                </div>
              )
            )}
          </section>

          {/* ===== 套利建議 ===== */}
          <section>
              <div className="flex items-center gap-3 mb-2 flex-wrap">
                <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wide">
                  套利建議
                  <span className="font-normal text-gray-600 ml-2">
                    {filteredArbPairs.length !== arbPairs.length
                      ? `${filteredArbPairs.length}/${arbPairs.length}`
                      : arbPairs.length}
                  </span>
                </h3>
                <div className="flex items-center gap-3">
                  <label className="flex items-center gap-1.5 text-xs cursor-pointer select-none">
                    <input
                      type="checkbox"
                      checked={includeSpot}
                      onChange={e => setIncludeSpot(e.target.checked)}
                      className="accent-green-500"
                    />
                    <span className={includeSpot ? 'text-green-300' : 'text-gray-400'}>現貨(多方)</span>
                  </label>
                  <label className="flex items-center gap-1.5 text-xs cursor-pointer select-none">
                    <input
                      type="checkbox"
                      checked={includeSpotShort}
                      onChange={e => setIncludeSpotShort(e.target.checked)}
                      className="accent-red-500"
                    />
                    <span className={includeSpotShort ? 'text-red-300' : 'text-gray-400'}>現貨(空方)</span>
                  </label>
                  {(includeSpot || includeSpotShort) && spotRates.length > 0 && (
                    <span className="text-gray-600 font-mono text-xs">現貨×{spotRates.length}</span>
                  )}
                  {(includeSpot || includeSpotShort) && spotRates.length === 0 && (
                    <span className="text-gray-600 normal-case text-xs">（等待現貨資料...）</span>
                  )}
                </div>
              </div>
              {arbPairs.length === 0 && (
                <div className="text-center text-gray-500 py-4 text-xs">無套利機會</div>
              )}
              {arbPairs.length > 0 && (
              <>

              {/* 篩選條件 */}
              <div className="flex items-center gap-2 mb-2 flex-wrap text-xs">
                <label className="flex items-center gap-1 text-gray-400">
                  <span>多方</span>
                  <select
                    value={arbFilters.longEx}
                    onChange={e => setArbFilters(f => ({ ...f, longEx: e.target.value }))}
                    className="px-1.5 py-0.5 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-xs focus:outline-none focus:border-blue-500"
                  >
                    <option value="">全部</option>
                    {[...EXCHANGE_ORDER, ...SPOT_ORDER].filter(ex => arbPairs.some(p => p.longEx === ex)).map(ex => (
                      <option key={ex} value={ex}>{EXCHANGE_LABELS[ex] || ex}</option>
                    ))}
                  </select>
                </label>
                <label className="flex items-center gap-1 text-gray-400">
                  <span>空方</span>
                  <select
                    value={arbFilters.shortEx}
                    onChange={e => setArbFilters(f => ({ ...f, shortEx: e.target.value }))}
                    className="px-1.5 py-0.5 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-xs focus:outline-none focus:border-blue-500"
                  >
                    <option value="">全部</option>
                    {[...EXCHANGE_ORDER, ...SPOT_ORDER].filter(ex => arbPairs.some(p => p.shortEx === ex)).map(ex => (
                      <option key={ex} value={ex}>{EXCHANGE_LABELS[ex] || ex}</option>
                    ))}
                  </select>
                </label>
                <label className="flex items-center gap-1 text-gray-400">
                  <span>費率差≥</span>
                  <input
                    type="number"
                    step="0.001"
                    value={arbFilters.minDiff}
                    onChange={e => setArbFilters(f => ({ ...f, minDiff: e.target.value }))}
                    className="w-16 px-1.5 py-0.5 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-xs text-center focus:outline-none focus:border-blue-500"
                    placeholder="%"
                  />
                  <span className="text-gray-600">%</span>
                </label>
                <label className="flex items-center gap-1 text-gray-400">
                  <span>成本≤</span>
                  <input
                    type="number"
                    step="0.001"
                    value={arbFilters.minSpread}
                    onChange={e => setArbFilters(f => ({ ...f, minSpread: e.target.value }))}
                    className="w-16 px-1.5 py-0.5 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-xs text-center focus:outline-none focus:border-blue-500"
                    placeholder="%"
                  />
                  <span className="text-gray-600">%</span>
                </label>
                {(arbFilters.longEx || arbFilters.shortEx || arbFilters.minDiff || arbFilters.minSpread) && (
                  <button
                    onClick={() => setArbFilters({ longEx: '', shortEx: '', minDiff: '', minSpread: '' })}
                    className="px-1.5 py-0.5 rounded text-xs text-gray-500 border border-gray-700/50 hover:text-white transition-colors"
                  >
                    清除
                  </button>
                )}
              </div>
              <div className="overflow-auto">
                <table className="w-full border-collapse text-xs">
                  <thead>
                    <tr className="bg-[var(--bg-secondary)]">
                      <th className="px-2 py-1.5 text-left text-[var(--text-secondary)] border-b border-[var(--border)]">A(多)</th>
                      <th className="px-2 py-1.5 text-right text-[var(--text-secondary)] border-b border-[var(--border)]">A費率</th>
                      <th className="px-2 py-1.5 text-left text-[var(--text-secondary)] border-b border-[var(--border)]">B(空)</th>
                      <th className="px-2 py-1.5 text-right text-[var(--text-secondary)] border-b border-[var(--border)]">B費率</th>
                      <th
                        onClick={() => handleArbSort('diff')}
                        className={`px-2 py-1.5 text-right border-b border-[var(--border)] cursor-pointer select-none hover:text-white transition-colors whitespace-nowrap ${arbSort?.key === 'diff' ? 'text-yellow-400' : 'text-[var(--text-secondary)]'}`}
                      >
                        費率差{arbSort?.key === 'diff' && <span className="ml-0.5 text-[10px]">{arbSort.dir === 'desc' ? '▾' : '▴'}</span>}
                      </th>
                      <th
                        onClick={() => handleArbSort('spread')}
                        className={`px-2 py-1.5 text-right border-b border-[var(--border)] cursor-pointer select-none hover:text-white transition-colors whitespace-nowrap ${arbSort?.key === 'spread' ? 'text-yellow-400' : 'text-[var(--text-secondary)]'}`}
                        title="進場成本（與 5005 一致）：負數=有利"
                      >
                        價差{arbSort?.key === 'spread' && <span className="ml-0.5 text-[10px]">{arbSort.dir === 'desc' ? '▾' : '▴'}</span>}
                      </th>
                      <th
                        onClick={() => handleArbSort('profit')}
                        className={`px-2 py-1.5 text-right border-b border-[var(--border)] cursor-pointer select-none hover:text-white transition-colors whitespace-nowrap ${arbSort?.key === 'profit' ? 'text-yellow-400' : 'text-[var(--text-secondary)]'}`}
                      >
                        預計利潤{arbSort?.key === 'profit' && <span className="ml-0.5 text-[10px]">{arbSort.dir === 'desc' ? '▾' : '▴'}</span>}
                      </th>
                      <th className="px-2 py-1.5 text-left text-[var(--text-secondary)] border-b border-[var(--border)]">建議操作</th>
                      <th className="px-2 py-1.5 text-center text-[var(--text-secondary)] border-b border-[var(--border)]"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedArbPairs.map((p) => {
                      const pairKey = `${p.longEx}-${p.shortEx}`;
                      const shortLabel = EXCHANGE_LABELS[p.shortEx] || p.shortEx;
                      const longLabel = EXCHANGE_LABELS[p.longEx] || p.longEx;
                      const si = p.shortIsSpot ? 'Spot' : formatInterval(p.shortInterval);
                      const li = p.longIsSpot ? 'Spot' : formatInterval(p.longInterval);
                      const isOpen = premiumIdx === pairKey;
                      return (
                        <React.Fragment key={pairKey}>
                          <tr className="border-b border-[var(--border)]/30 hover:bg-[var(--bg-hover)]">
                            <td className="px-2 py-1.5 text-green-400">{longLabel} <span className="text-gray-500">{li}</span></td>
                            <td className="px-2 py-1.5 text-right font-mono text-green-400" title={p.longIsSpot ? '現貨無資費' : `原始每期(${li}): ${formatRate(p.longRate)}`}>{p.longIsSpot ? '-' : formatRate(p.longNormRate)}</td>
                            <td className="px-2 py-1.5 text-red-400">{shortLabel} <span className="text-gray-500">{si}</span></td>
                            <td
                              className="px-2 py-1.5 text-right font-mono text-red-400"
                              title={p.shortIsSpot
                                ? (p.shortBorrowAnnual != null
                                    ? `借貸年化 ${p.shortBorrowAnnual.toFixed(2)}%（每期約 ${p.borrowPerPeriod.toFixed(4)}%，已計入預計利潤）`
                                    : '此交易所借貸利率尚未接入，未計入成本')
                                : `原始每期(${si}): ${formatRate(p.shortRate)}`}
                            >
                              {p.shortIsSpot
                                ? (p.shortBorrowAnnual != null
                                    ? <span className="text-orange-400">借{p.shortBorrowAnnual.toFixed(1)}%</span>
                                    : <span className="text-gray-500">借?</span>)
                                : formatRate(p.shortNormRate)}
                            </td>
                            <td className="px-2 py-1.5 text-right font-mono text-white">{formatRate(p.diff)}</td>
                            <td className={`px-2 py-1.5 text-right font-mono ${
                              p.spreadPct !== null
                                ? (p.spreadPct <= 0 ? 'text-green-400' : 'text-red-400')
                                : 'text-gray-600'
                            }`} title="進場成本：(做多ask - 做空bid) / 做空bid，負數有利">
                              {p.spreadPct !== null ? `${p.spreadPct >= 0 ? '+' : ''}${p.spreadPct.toFixed(4)}%` : '-'}
                            </td>
                            <td
                              className={`px-2 py-1.5 text-right font-mono font-bold ${
                                p.estProfit === null ? 'text-gray-600' :
                                p.estProfit > 0 ? 'text-green-400' : 'text-red-400'
                              }`}
                              title={p.shortIsSpot
                                ? (p.shortBorrowAnnual != null
                                    ? `已扣空方現貨借貸成本 ${p.shortBorrowAnnual.toFixed(2)}%/年`
                                    : '注意：空方現貨借貸成本未計入（此所未接）')
                                : ''}
                            >
                              {p.estProfit !== null ? `${p.estProfit >= 0 ? '+' : ''}${p.estProfit.toFixed(4)}%` : '-'}
                              {p.shortIsSpot && !p.borrowKnown && <span className="text-gray-500 ml-0.5" title="借貸成本未計入">*</span>}
                            </td>
                            <td className="px-2 py-1.5 text-gray-300 whitespace-nowrap">
                              多 {longLabel}({li}) / 空 {shortLabel}({si})
                            </td>
                            <td className="px-2 py-1.5 text-center">
                              <button
                                onClick={() => setPremiumIdx(isOpen ? null : pairKey)}
                                className={`px-2 py-0.5 rounded text-xs font-medium border transition-colors ${
                                  isOpen
                                    ? 'bg-yellow-500/20 text-yellow-400 border-yellow-500/40'
                                    : 'text-gray-500 border-gray-700/50 hover:text-yellow-400 hover:border-yellow-500/40'
                                }`}
                              >
                                價差
                              </button>
                              <button
                                onClick={async () => {
                                  // 5005 symbol 格式 BASE/USDT（去掉 :USDT 後綴）
                                  const to5005Sym = (s) => (s || symbol).split(':')[0];
                                  // 5005 exchange id：合約 bitget → bitget；現貨 bitget_spot → bitget-spot；aster → aster-pro
                                  const to5005Ex = (ex) =>
                                    ex === 'aster' ? 'aster-pro' :
                                    ex.endsWith('_spot') ? ex.replace('_spot', '-spot') : ex;
                                  const longSym = to5005Sym(p.longSymbol);
                                  const shortSym = to5005Sym(p.shortSymbol);
                                  let ok = false;
                                  try {
                                    // 選配：把套利機會送到自己的下單服務。
                                    // 設 VITE_EXECUTOR_URL 才會啟用，沒設就是空字串（fetch 會失敗，走下面的 catch）。
                                    const executorUrl = import.meta.env.VITE_EXECUTOR_URL || '';
                                    const res = await fetch(`${executorUrl}/api/pending-strategy`, {
                                      method: 'POST',
                                      headers: { 'Content-Type': 'application/json' },
                                      body: JSON.stringify({
                                        long_ex: to5005Ex(p.longEx),
                                        short_ex: to5005Ex(p.shortEx),
                                        long_symbol: longSym,
                                        short_symbol: shortSym,
                                        symbol: longSym,  // 向後相容：舊版 5005 只讀 symbol
                                      }),
                                    });
                                    ok = res.ok;
                                  } catch (_) {}
                                  setConnectStatus(prev => ({ ...prev, [pairKey]: ok ? 'ok' : 'err' }));
                                  setTimeout(() => setConnectStatus(prev => { const n = {...prev}; delete n[pairKey]; return n; }), 4000);
                                }}
                                className={`ml-1 px-2 py-0.5 rounded text-xs font-medium border transition-colors ${
                                  connectStatus[pairKey] === 'ok' ? 'text-green-400 border-green-500/40' :
                                  connectStatus[pairKey] === 'err' ? 'text-red-400 border-red-500/40' :
                                  'text-blue-400 border-blue-500/40 hover:bg-blue-500/20'
                                }`}
                              >
                                {connectStatus[pairKey] === 'ok' ? '→切換5005' : connectStatus[pairKey] === 'err' ? '✗失敗' : '連線'}
                              </button>
                            </td>
                          </tr>
                          {isOpen && (
                            <tr>
                              <td colSpan={9} className="p-2">
                                <PremiumPanel symbol={symbol} shortEx={p.shortEx} longEx={p.longEx} history={history} />
                              </td>
                            </tr>
                          )}
                        </React.Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              </>
              )}
          </section>

          </>
          )}
        </div>
      </div>
      {showConstituent && (
        <ConstituentPanel symbol={symbol} onClose={() => setShowConstituent(false)} />
      )}
    </div>
  );
}

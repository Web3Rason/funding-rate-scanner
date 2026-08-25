import { useState, useEffect, useRef, useId } from 'react';

const PREMIUM_EXCHANGES = [
  { id: 'binance', label: 'Binance' },
  { id: 'bybit', label: 'Bybit' },
  { id: 'okx', label: 'OKX' },
  { id: 'bitget', label: 'Bitget' },
  { id: 'mexc', label: 'MEXC' },
  { id: 'gateio', label: 'Gate.io' },
  { id: 'bingx', label: 'BingX' },
  { id: 'kucoinfutures', label: 'KuCoin' },
  { id: 'aster', label: 'Aster' },
  { id: 'hyperliquid', label: 'Hyperliquid' },
  { id: 'tradexyz', label: 'Trade.xyz' },
];

const PREMIUM_LABELS = Object.fromEntries(PREMIUM_EXCHANGES.map(e => [e.id, e.label]));

const INTERVALS = [
  { value: '1m', label: '1m', hours: 12 },
  { value: '5m', label: '5m', hours: 24 },
  { value: '15m', label: '15m', hours: 72 },
  { value: '1h', label: '1h', hours: 168 },
];

function shortSymbol(symbol) {
  return symbol.split('/')[0];
}

function formatChartTime(ts) {
  const d = new Date(ts);
  const utc8 = new Date(d.getTime() + 8 * 3600000);
  const mm = String(utc8.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(utc8.getUTCDate()).padStart(2, '0');
  const hh = String(utc8.getUTCHours()).padStart(2, '0');
  const mi = String(utc8.getUTCMinutes()).padStart(2, '0');
  return `${mm}-${dd} ${hh}:${mi}`;
}

function calcTicks(min, max, count = 5) {
  const range = max - min;
  if (range === 0) return [min];
  const rawStep = range / count;
  const mag = Math.pow(10, Math.floor(Math.log10(Math.abs(rawStep))));
  const res = rawStep / mag;
  let step;
  if (res <= 1.5) step = mag;
  else if (res <= 3) step = 2 * mag;
  else if (res <= 7) step = 5 * mag;
  else step = 10 * mag;
  const nMin = Math.floor(min / step) * step;
  const nMax = Math.ceil(max / step) * step;
  const ticks = [];
  for (let v = nMin; v <= nMax + step * 0.01; v += step) {
    ticks.push(Math.round(v * 1e6) / 1e6);
  }
  return ticks;
}

function PremiumChart({ data, title, loading, externalHoverTs, onHoverChange }) {
  const clipId = useId();
  const containerRef = useRef(null);
  const [localHoverIdx, setLocalHoverIdx] = useState(null);

  // Resolve hover index: if external timestamp provided, find closest point in our data
  const hoverIdx = (() => {
    if (externalHoverTs != null && data && data.length > 1) {
      let best = 0, bestDist = Infinity;
      for (let i = 0; i < data.length; i++) {
        const dist = Math.abs(data[i].timestamp - externalHoverTs);
        if (dist < bestDist) { bestDist = dist; best = i; }
      }
      return best;
    }
    return localHoverIdx;
  })();
  if (loading) {
    return (
      <div className="bg-[#0a0a0a] rounded border border-[var(--border)] flex items-center justify-center" style={{ height: 520 }}>
        <div className="flex items-center gap-2 text-gray-500 text-sm">
          <div className="w-4 h-4 border-2 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
          載入中...
        </div>
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <div className="bg-[#0a0a0a] rounded border border-[var(--border)] flex items-center justify-center" style={{ height: 520 }}>
        <span className="text-gray-600 text-sm">無資料（該交易所可能無此幣種現貨）</span>
      </div>
    );
  }

  const W = 800, H = 480;
  const PAD = { left: 60, right: 72, top: 10, bottom: 28 };
  const chartW = W - PAD.left - PAD.right;
  const chartH = H - PAD.top - PAD.bottom;

  const premiums = data.map(d => d.premium);
  const rawMin = Math.min(...premiums);
  const rawMax = Math.max(...premiums);
  const dataMin = Math.min(0, rawMin);
  const dataMax = Math.max(0, rawMax);
  const margin = (dataMax - dataMin) * 0.08 || 0.05;
  const yMin = dataMin - margin;
  const yMax = dataMax + margin;

  const toX = (i) => PAD.left + (i / Math.max(data.length - 1, 1)) * chartW;
  const toY = (v) => PAD.top + (1 - (v - yMin) / (yMax - yMin)) * chartH;

  const zeroY = toY(0);
  const ticks = calcTicks(yMin, yMax, 5).filter(t => t >= yMin && t <= yMax);

  // Time ticks - snap to whole hours
  const timeTicks = (() => {
    if (data.length < 2) return data.length === 1 ? [{ idx: 0, ts: data[0].timestamp }] : [];
    const tsFirst = data[0].timestamp;
    const tsLast = data[data.length - 1].timestamp;
    const spanH = (tsLast - tsFirst) / 3600000;
    // Pick hour step: aim for ~5-7 ticks
    let stepH = 1;
    if (spanH > 36) stepH = 8;
    else if (spanH > 18) stepH = 4;
    else if (spanH > 7) stepH = 2;
    // First whole hour >= tsFirst
    const firstHour = Math.ceil(tsFirst / (stepH * 3600000)) * (stepH * 3600000);
    const ticks = [];
    for (let t = firstHour; t <= tsLast; t += stepH * 3600000) {
      // Find closest data index
      let bestIdx = 0, bestDist = Infinity;
      for (let i = 0; i < data.length; i++) {
        const dist = Math.abs(data[i].timestamp - t);
        if (dist < bestDist) { bestDist = dist; bestIdx = i; }
      }
      ticks.push({ idx: bestIdx, ts: t }); // use the round hour timestamp for label
    }
    return ticks;
  })();

  // Premium line path
  const linePath = data.map((d, i) =>
    `${i === 0 ? 'M' : 'L'}${toX(i).toFixed(1)},${toY(d.premium).toFixed(1)}`
  ).join(' ');

  // Area fill between line and 0%
  const fillPath = linePath +
    ` L${toX(data.length - 1).toFixed(1)},${zeroY.toFixed(1)}` +
    ` L${toX(0).toFixed(1)},${zeroY.toFixed(1)} Z`;

  // Premium zone shading (above 0%)
  const premZoneY = toY(yMax);
  const premZoneH = zeroY - premZoneY;

  // Current value
  const lastP = data[data.length - 1].premium;

  // Hover handler
  const handleMouseMove = (e) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect || data.length < 2) return;
    const svgX = (e.clientX - rect.left) / rect.width * W;
    const chartX = svgX - PAD.left;
    const rawPct = chartX / chartW;
    const clampedPct = Math.max(0, Math.min(1, rawPct));
    const idx = Math.round(clampedPct * (data.length - 1));
    setLocalHoverIdx(idx);
    onHoverChange?.(data[idx].timestamp);
  };

  const handleMouseLeave = () => {
    setLocalHoverIdx(null);
    onHoverChange?.(null);
  };

  // Compute svgX from hoverIdx
  const hoverSvgX = hoverIdx != null ? toX(hoverIdx) : null;
  const hd = hoverIdx != null ? data[hoverIdx] : null;

  return (
    <div className="bg-[#0a0a0a] rounded border border-[var(--border)]">
      <div className="text-sm text-gray-400 px-3 pt-2 pb-1 font-medium">{title}</div>
      <div className="relative" ref={containerRef}>
        <svg
          viewBox={`0 0 ${W} ${H}`}
          className="w-full"
          style={{ height: 480 }}
          preserveAspectRatio="none"
          onMouseMove={handleMouseMove}
          onMouseLeave={handleMouseLeave}
        >
          <defs>
            <clipPath id={clipId}>
              <rect x={PAD.left} y={PAD.top} width={chartW} height={chartH} />
            </clipPath>
          </defs>

          {/* Premium zone shading (above 0%) */}
          {premZoneH > 0 && (
            <rect
              x={PAD.left} y={premZoneY} width={chartW} height={premZoneH}
              fill="rgba(239,68,68,0.08)"
            />
          )}

          {/* Grid lines */}
          {ticks.map((t, i) => (
            <line
              key={i}
              x1={PAD.left} y1={toY(t)} x2={W - PAD.right} y2={toY(t)}
              stroke="#1f2937" strokeWidth="0.5"
            />
          ))}

          {/* 0% reference line */}
          <line
            x1={PAD.left} y1={zeroY} x2={W - PAD.right} y2={zeroY}
            stroke="#22d3ee" strokeWidth="0.8" strokeDasharray="4,3"
          />

          {/* Area fill between line and 0% */}
          <path d={fillPath} fill="rgba(239,68,68,0.18)" clipPath={`url(#${clipId})`} />

          {/* Premium line */}
          <path
            d={linePath} fill="none" stroke="#ef4444" strokeWidth="1.5"
            vectorEffect="non-scaling-stroke" clipPath={`url(#${clipId})`}
          />

          {/* Y-axis labels */}
          <text x={PAD.left - 5} y={zeroY + 4.5} textAnchor="end" fill="#22d3ee" fontSize="12" fontFamily="monospace">0%</text>
          {ticks.filter(t => Math.abs(t) > 1e-8).map((t, i) => (
            <text key={i} x={PAD.left - 5} y={toY(t) + 4.5} textAnchor="end" fill="#e5e7eb" fontSize="11" fontFamily="monospace">
              {t.toFixed(2)}%
            </text>
          ))}

          {/* Time axis labels */}
          {timeTicks.map((t, i) => {
            // Show date prefix only on first tick or when day changes
            const prevDay = i > 0 ? new Date(timeTicks[i - 1].ts + 8 * 3600000).getUTCDate() : -1;
            const curDate = new Date(t.ts + 8 * 3600000);
            const curDay = curDate.getUTCDate();
            const showDate = i === 0 || curDay !== prevDay;
            const hh = String(curDate.getUTCHours()).padStart(2, '0');
            const label = showDate
              ? `${String(curDate.getUTCMonth() + 1).padStart(2, '0')}-${String(curDay).padStart(2, '0')} ${hh}:00`
              : `${hh}:00`;
            return (
              <text key={i} x={toX(t.idx)} y={H - 6} textAnchor="middle" fill="#e5e7eb" fontSize="11" fontFamily="monospace">
                {label}
              </text>
            );
          })}

          {/* Current value label */}
          <rect
            x={W - PAD.right + 4}
            y={toY(lastP) - 10}
            width={64} height={20} rx="3"
            fill={lastP >= 0 ? '#16a34a' : '#dc2626'}
          />
          <text
            x={W - PAD.right + 8}
            y={toY(lastP) + 5}
            fill="white" fontSize="12" fontWeight="bold" fontFamily="monospace"
          >
            {lastP >= 0 ? '+' : ''}{lastP.toFixed(4)}%
          </text>

          {/* Hover crosshair */}
          {hoverSvgX != null && hd && (
            <>
              <line
                x1={hoverSvgX} y1={PAD.top} x2={hoverSvgX} y2={PAD.top + chartH}
                stroke="rgba(255,255,255,0.3)" strokeWidth="0.5"
              />
              <line
                x1={PAD.left} y1={toY(hd.premium)} x2={W - PAD.right} y2={toY(hd.premium)}
                stroke="rgba(255,255,255,0.15)" strokeWidth="0.5"
              />
              <circle
                cx={hoverSvgX} cy={toY(hd.premium)} r="3"
                fill={hd.premium >= 0 ? '#22c55e' : '#ef4444'} stroke="#111" strokeWidth="1"
              />
            </>
          )}
        </svg>

        {/* Hover tooltip (HTML overlay) */}
        {hoverSvgX != null && hd && (() => {
          const pctX = hoverSvgX / W;
          const rect = containerRef.current?.getBoundingClientRect();
          if (!rect) return null;
          const px = pctX * rect.width;
          const flipLeft = px > rect.width * 0.65;
          return (
            <div
              className="absolute z-30 pointer-events-none"
              style={{ left: flipLeft ? px - 155 : px + 12, top: 8 }}
            >
              <div className="bg-gray-900/95 border border-gray-700 rounded px-2.5 py-1.5 text-xs whitespace-nowrap shadow-lg">
                <div className="text-gray-400">{formatChartTime(hd.timestamp)}</div>
                <div className={`font-mono font-bold text-sm ${hd.premium >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {hd.premium >= 0 ? '+' : ''}{hd.premium.toFixed(4)}%
                </div>
                {hd.futures_price != null && hd.spot_price != null && (
                  <div className="text-gray-500 font-mono mt-0.5">
                    合約 ${hd.futures_price >= 100 ? hd.futures_price.toFixed(2) : hd.futures_price.toPrecision(4)}
                    {' / '}
                    現貨 ${hd.spot_price >= 100 ? hd.spot_price.toFixed(2) : hd.spot_price.toPrecision(4)}
                  </div>
                )}
              </div>
            </div>
          );
        })()}
      </div>
    </div>
  );
}

export default function FuturesSpotPremium({ symbol, defaultExchanges }) {
  const validIds = new Set(PREMIUM_EXCHANGES.map(e => e.id));
  const initLeft = (defaultExchanges?.left && validIds.has(defaultExchanges.left)) ? defaultExchanges.left : 'binance';
  const initRight = (defaultExchanges?.right && validIds.has(defaultExchanges.right)) ? defaultExchanges.right : 'bybit';

  const [leftEx, setLeftEx] = useState(initLeft);
  const [rightEx, setRightEx] = useState(initRight);
  const [interval, setIntervalVal] = useState('1m');
  const [leftData, setLeftData] = useState(null);
  const [rightData, setRightData] = useState(null);
  const [leftLoading, setLeftLoading] = useState(true);
  const [rightLoading, setRightLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [syncHoverTs, setSyncHoverTs] = useState(null);

  const hours = INTERVALS.find(i => i.value === interval)?.hours || 12;
  const base = shortSymbol(symbol);

  const fetchData = () => {
    setLeftLoading(true);
    setRightLoading(true);

    const params = (ex) => new URLSearchParams({
      symbol, exchange: ex, interval, hours: String(hours),
    });

    fetch(`/api/futures-spot-premium?${params(leftEx)}`)
      .then(r => r.json())
      .then(res => setLeftData(res.data || []))
      .catch(() => setLeftData([]))
      .finally(() => setLeftLoading(false));

    fetch(`/api/futures-spot-premium?${params(rightEx)}`)
      .then(r => r.json())
      .then(res => setRightData(res.data || []))
      .catch(() => setRightData([]))
      .finally(() => setRightLoading(false));

    setLastUpdate(new Date());
  };

  useEffect(() => { fetchData(); }, [symbol, leftEx, rightEx, interval]);

  const selectClass = "px-2 py-1 bg-[var(--bg-secondary)] border border-[var(--border)] rounded text-xs text-white focus:outline-none focus:border-blue-500 cursor-pointer";

  return (
    <div className="space-y-3">
      {/* Controls bar */}
      <div className="flex items-center gap-3 flex-wrap text-xs">
        <span className="text-gray-400">左側:</span>
        <select value={leftEx} onChange={e => setLeftEx(e.target.value)} className={selectClass}>
          {PREMIUM_EXCHANGES.map(ex => (
            <option key={ex.id} value={ex.id}>{ex.label}</option>
          ))}
        </select>

        <span className="text-gray-400">右側:</span>
        <select value={rightEx} onChange={e => setRightEx(e.target.value)} className={selectClass}>
          {PREMIUM_EXCHANGES.map(ex => (
            <option key={ex.id} value={ex.id}>{ex.label}</option>
          ))}
        </select>

        <span className="text-gray-400">間隔:</span>
        <select value={interval} onChange={e => setIntervalVal(e.target.value)} className={selectClass}>
          {INTERVALS.map(i => (
            <option key={i.value} value={i.value}>{i.label}</option>
          ))}
        </select>

        <button
          onClick={fetchData}
          className="px-3 py-1 bg-[var(--bg-secondary)] border border-[var(--border)] rounded text-xs text-gray-300 hover:text-white hover:border-blue-500 transition-colors"
        >
          重新整理
        </button>

        {lastUpdate && (
          <span className="text-gray-600 ml-auto">
            數據載入完成 - {formatChartTime(lastUpdate.getTime())}
          </span>
        )}
      </div>

      {/* Two charts side by side */}
      <div className="grid grid-cols-2 gap-2">
        <PremiumChart
          data={leftData}
          title={`${PREMIUM_LABELS[leftEx] || leftEx} - ${base}USDT`}
          loading={leftLoading}
          externalHoverTs={syncHoverTs}
          onHoverChange={setSyncHoverTs}
        />
        <PremiumChart
          data={rightData}
          title={`${PREMIUM_LABELS[rightEx] || rightEx} - ${base}USDT`}
          loading={rightLoading}
          externalHoverTs={syncHoverTs}
          onHoverChange={setSyncHoverTs}
        />
      </div>
    </div>
  );
}

import { useState, useMemo, useEffect } from 'react';
import { usePolling } from '../hooks/usePolling';
import SymbolDetail from './SymbolDetail';

const EXCHANGE_LABELS = {
  binance: 'Binance', bybit: 'Bybit', okx: 'OKX', bitget: 'Bitget',
  mexc: 'MEXC', gateio: 'Gate.io', bingx: 'BingX', kucoinfutures: 'KuCoin',
  aster: 'Aster', hyperliquid: 'Hyperliquid', tradexyz: 'Trade.xyz',
  coinw: 'CoinW', ourbit: 'Ourbit', lbank: 'LBank',
};

function fmtPct(v, digits = 4) {
  if (v == null) return '-';
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
}

function fmtPrice(p) {
  if (p == null) return '-';
  if (p >= 1000) return p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (p >= 1) return p.toFixed(4);
  return p.toPrecision(4);
}

// Sparkline: 小型趨勢圖
function Sparkline({ data, width = 60, height = 20, onClick }) {
  if (!data || data.length < 2) return null;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const step = width / (data.length - 1);

  const points = data.map((v, i) => {
    const x = i * step;
    const y = height - ((v - min) / range) * height;
    return `${x},${y}`;
  }).join(' ');

  const last = data[data.length - 1];
  const color = last >= 0 ? '#ef4444' : '#22c55e';

  return (
    <svg
      width={width} height={height}
      className={`inline-block align-middle ${onClick ? 'cursor-zoom-in hover:opacity-80' : ''}`}
      onClick={onClick}
    >
      <polyline points={points} fill="none" stroke={color} strokeWidth="1.5" />
    </svg>
  );
}

// 放大圖 Modal
function ChartModal({ symbol, exchange, label, recentDevs, baseline, onClose }) {
  const [history, setHistory] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(`/api/index-tracking/history?symbol=${encodeURIComponent(symbol)}&exchange=${exchange}`)
      .then(r => r.json())
      .then(d => { setHistory(d.history || []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [symbol, exchange]);

  const devs = history && history.length >= 2
    ? history.map(h => h.dev)
    : recentDevs || [];
  const timestamps = history && history.length >= 2
    ? history.map(h => {
        const d = new Date(h.ts);
        return `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
      })
    : [];

  const W = 600, H = 220;
  const PL = 56, PR = 16, PT = 16, PB = 36;
  const innerW = W - PL - PR;
  const innerH = H - PT - PB;

  let chart = null;
  if (devs.length >= 2) {
    // 確保 y 範圍包含 0 和 baseline，且最小寬度 0.2%
    const dataMin = Math.min(...devs);
    const dataMax = Math.max(...devs);
    const extras = [0, baseline != null ? baseline : null].filter(v => v != null);
    const allVals = [...devs, ...extras];
    const rawMin = Math.min(...allVals);
    const rawMax = Math.max(...allVals);
    const margin = Math.max((rawMax - rawMin) * 0.15, 0.05);
    const lo = rawMin - margin;
    const hi = rawMax + margin;
    const range = hi - lo;

    const sx = (i) => PL + (i / (devs.length - 1)) * innerW;
    const sy = (v) => PT + innerH - ((v - lo) / range) * innerH;

    // Y刻度（4格）
    const yTicks = Array.from({ length: 5 }, (_, i) => lo + (range * i / 4));
    // X刻度（最多 6 個）
    const xCount = Math.min(6, devs.length);
    const xIdxs = Array.from({ length: xCount }, (_, i) =>
      Math.round(i * (devs.length - 1) / (xCount - 1))
    );

    const pts = devs.map((v, i) => `${sx(i)},${sy(v)}`).join(' ');
    const last = devs[devs.length - 1];
    const lineColor = last >= 0 ? '#f87171' : '#4ade80';
    const zeroY = sy(0);
    const inRange = (y) => y >= PT && y <= PT + innerH;

    chart = (
      <>
        {/* 圖表背景 */}
        <rect x={PL} y={PT} width={innerW} height={innerH} fill="#111827" rx="2" />

        {/* Y 格線 */}
        {yTicks.map((v, i) => (
          <line key={i} x1={PL} x2={PL + innerW} y1={sy(v)} y2={sy(v)}
            stroke="#1f2937" strokeWidth="1" />
        ))}

        {/* 零線 */}
        {inRange(zeroY) && (
          <line x1={PL} x2={PL + innerW} y1={zeroY} y2={zeroY}
            stroke="#6b7280" strokeWidth="1" />
        )}

        {/* 基線 */}
        {baseline != null && inRange(sy(baseline)) && (
          <line x1={PL} x2={PL + innerW} y1={sy(baseline)} y2={sy(baseline)}
            stroke="#818cf8" strokeWidth="1.5" strokeDasharray="5,3" />
        )}

        {/* Y 軸標籤 */}
        {yTicks.map((v, i) => (
          <text key={i} x={PL - 6} y={sy(v) + 4} textAnchor="end"
            fontSize="11" fill="#d1d5db" fontFamily="monospace">
            {v >= 0 ? '+' : ''}{v.toFixed(2)}%
          </text>
        ))}

        {/* X 軸標籤 */}
        {xIdxs.map((idx, i) => timestamps[idx] && (
          <text key={i} x={sx(idx)} y={PT + innerH + 22} textAnchor="middle"
            fontSize="10" fill="#9ca3af">
            {timestamps[idx]}
          </text>
        ))}

        {/* 面積填充 */}
        <polygon
          points={`${sx(0)},${zeroY} ${pts} ${sx(devs.length - 1)},${zeroY}`}
          fill={lineColor} opacity="0.12"
        />

        {/* 折線 */}
        <polyline points={pts} fill="none" stroke={lineColor} strokeWidth="2" strokeLinejoin="round" />

        {/* 最後一點 */}
        <circle cx={sx(devs.length - 1)} cy={sy(last)} r="4" fill={lineColor} />
        <text x={sx(devs.length - 1) + 6} y={sy(last) + 4} fontSize="11" fill={lineColor} fontFamily="monospace">
          {last >= 0 ? '+' : ''}{last.toFixed(3)}%
        </text>
      </>
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/75" onClick={onClose}>
      <div
        className="bg-[#0f1117] border border-gray-700 rounded-xl p-5 shadow-2xl"
        style={{ width: 660 }}
        onClick={e => e.stopPropagation()}
      >
        {/* 標題列 */}
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <span className="text-white font-semibold">{label}</span>
            {baseline != null && (
              <span className="text-xs text-indigo-400 bg-indigo-900/30 px-2 py-0.5 rounded">
                基線 {baseline >= 0 ? '+' : ''}{baseline.toFixed(3)}%
              </span>
            )}
          </div>
          <button onClick={onClose} className="text-gray-500 hover:text-white text-xl leading-none px-1">✕</button>
        </div>

        {/* 圖表 */}
        {loading ? (
          <div className="flex items-center justify-center h-[220px] text-gray-500 text-sm">載入歷史資料中...</div>
        ) : devs.length < 2 ? (
          <div className="flex items-center justify-center h-[220px] text-gray-500 text-sm">資料不足</div>
        ) : (
          <svg width={W} height={H} style={{ overflow: 'visible', display: 'block' }}>
            {chart}
          </svg>
        )}

        {/* 圖例 */}
        <div className="mt-3 flex gap-5 text-[11px] text-gray-500">
          <span className="flex items-center gap-1.5">
            <span className="w-5 inline-block border-t border-gray-500"></span>零線
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-5 inline-block border-t border-dashed border-indigo-400"></span>基線（歷史中位數）
          </span>
          <span className="ml-auto text-gray-600">{history ? `${history.length} 筆歷史` : `${devs.length} 筆`}</span>
        </div>
      </div>
    </div>
  );
}

const SORT_KEYS = {
  coin: (o) => o.base_coin,
  exchange: (o) => o.exchange,
  spike: (o) => o.spike_pct != null ? Math.abs(o.spike_pct) : -1,
  deviation: (o) => Math.abs(o.deviation_pct),
  baseline: (o) => o.baseline_pct != null ? Math.abs(o.baseline_pct) : -1,
  bn_price: (o) => o.bn_price || 0,
  ex_price: (o) => o.ex_price || 0,
};

export default function IndexTracking({ searchTerm }) {
  const [sortKey, setSortKey] = useState('spike');
  const [sortDesc, setSortDesc] = useState(true);
  const [detailSymbol, setDetailSymbol] = useState(null);
  const [filterExchange, setFilterExchange] = useState('');
  const [filterConstChanged, setFilterConstChanged] = useState(false);
  const [expandedChart, setExpandedChart] = useState(null); // { symbol, exchange, label, recentDevs, baseline }

  const { data, loading } = usePolling('/api/index-tracking', 10000);

  const filtered = useMemo(() => {
    // 固定只取突發偏離 > 0.3%
    let list = (data?.anomalies || []).filter(d => {
      const val = d.spike_pct != null ? Math.abs(d.spike_pct) : Math.abs(d.deviation_pct);
      return val >= 0.3;
    });

    // 幣種搜尋
    if (searchTerm) {
      list = list.filter(d => d.base_coin.toUpperCase().includes(searchTerm.toUpperCase()));
    }

    // 交易所篩選
    if (filterExchange) {
      list = list.filter(d => d.exchange === filterExchange);
    }

    // 成分有變動篩選
    if (filterConstChanged) {
      list = list.filter(d => d.constituent_changed);
    }

    const keyFn = SORT_KEYS[sortKey] || SORT_KEYS.spike;
    list = [...list].sort((a, b) => {
      const va = keyFn(a), vb = keyFn(b);
      if (typeof va === 'string') return sortDesc ? vb.localeCompare(va) : va.localeCompare(vb);
      return sortDesc ? vb - va : va - vb;
    });
    return list;
  }, [data, searchTerm, sortKey, sortDesc, filterExchange, filterConstChanged]);

  // 收集所有出現的交易所
  const exchanges = useMemo(() => {
    const s = new Set((data?.all_deviations || []).map(d => d.exchange));
    return [...s].sort();
  }, [data]);

  const handleSort = (key) => {
    if (sortKey === key) setSortDesc(!sortDesc);
    else { setSortKey(key); setSortDesc(true); }
  };

  const thClass = "px-3 py-1.5 text-[var(--text-secondary)] border-b border-[var(--border)] cursor-pointer hover:text-white select-none whitespace-nowrap";
  const sortIcon = (key) => sortKey === key ? (sortDesc ? ' ▼' : ' ▲') : '';

  // 突發偏離顏色
  const spikeColor = (pct) => {
    if (pct == null) return 'text-gray-500';
    const abs = Math.abs(pct);
    if (abs >= 2) return 'text-red-400 font-bold';
    if (abs >= 1) return pct > 0 ? 'text-red-400' : 'text-green-400';
    if (abs >= 0.5) return 'text-yellow-400';
    return 'text-gray-400';
  };

  // 當前偏離顏色（淡化）
  const devColor = (pct) => {
    const abs = Math.abs(pct);
    if (abs >= 3) return 'text-red-400/70';
    if (abs >= 1) return 'text-yellow-400/70';
    return 'text-gray-500';
  };

  return (
    <div>
      {/* 工具列 */}
      <div className="px-4 py-2 border-b border-[var(--border)] flex items-center gap-4 flex-wrap">
        <div className="flex items-center gap-2">
          <span className="text-xs text-[var(--text-secondary)]">交易所:</span>
          <select
            value={filterExchange}
            onChange={e => setFilterExchange(e.target.value)}
            className="px-2 py-0.5 rounded text-xs bg-[var(--bg-secondary)] text-white border border-[var(--border)]"
          >
            <option value="">全部</option>
            {exchanges.map(ex => (
              <option key={ex} value={ex}>{EXCHANGE_LABELS[ex] || ex}</option>
            ))}
          </select>
        </div>

        <button
          onClick={() => setFilterConstChanged(!filterConstChanged)}
          className={`px-2 py-0.5 rounded text-xs transition-colors ${
            filterConstChanged
              ? 'bg-orange-600 text-white'
              : 'bg-[var(--bg-secondary)] text-[var(--text-secondary)] hover:text-white'
          }`}
        >
          成分有變動
        </button>

        <div className="ml-auto flex items-center gap-3 text-xs text-[var(--text-secondary)]">
          <span>顯示: {filtered.length}</span>
          {data?.timestamp && (
            <span className="text-[var(--text-secondary)]">
              掃描: {new Date(new Date(data.timestamp).getTime() + 8*3600000).toISOString().slice(5,16).replace('T',' ')}
            </span>
          )}
        </div>
      </div>

      {/* 表格 */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs">
              <th className={thClass} onClick={() => handleSort('coin')}>
                幣種{sortIcon('coin')}
              </th>
              <th className={thClass} onClick={() => handleSort('exchange')}>
                交易所{sortIcon('exchange')}
              </th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('spike')}>
                突發偏離{sortIcon('spike')}
              </th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('deviation')}>
                當前偏離{sortIcon('deviation')}
              </th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('baseline')}>
                基線{sortIcon('baseline')}
              </th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('bn_price')}>
                BN指數{sortIcon('bn_price')}
              </th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('ex_price')}>
                交易所指數{sortIcon('ex_price')}
              </th>
              <th className={`${thClass} text-center`}>趨勢</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={8} className="px-4 py-8 text-center text-[var(--text-secondary)]">
                  {loading ? '載入中...' : '目前無突發偏離'}
                </td>
              </tr>
            ) : (
              filtered.map((d, i) => (
                <tr
                  key={`${d.symbol}-${d.exchange}`}
                  className="border-b border-[var(--border)] hover:bg-[var(--bg-secondary)] cursor-pointer transition-colors"
                  onClick={() => setDetailSymbol(d.symbol)}
                >
                  <td className="px-3 py-1.5 font-medium text-white">
                    <span className="flex items-center gap-1">
                      {d.base_coin}
                      {d.constituent_changed && (
                        <span className="text-orange-400 text-[10px] leading-none" title="指數成分有變動">●</span>
                      )}
                    </span>
                  </td>
                  <td className="px-3 py-1.5 text-[var(--text-secondary)]">
                    {EXCHANGE_LABELS[d.exchange] || d.exchange}
                  </td>
                  <td className={`px-3 py-1.5 text-right font-mono ${spikeColor(d.spike_pct)}`}>
                    {fmtPct(d.spike_pct)}
                  </td>
                  <td className={`px-3 py-1.5 text-right font-mono ${devColor(d.deviation_pct)}`}>
                    {fmtPct(d.deviation_pct)}
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono text-gray-500">
                    {fmtPct(d.baseline_pct)}
                  </td>
                  <td className="px-3 py-1.5 text-right text-[var(--text-secondary)] font-mono">
                    {fmtPrice(d.bn_price)}
                  </td>
                  <td className="px-3 py-1.5 text-right text-[var(--text-secondary)] font-mono">
                    {fmtPrice(d.ex_price)}
                  </td>
                  <td
                    className="px-3 py-1.5 text-center"
                    onClick={e => {
                      e.stopPropagation();
                      setExpandedChart({
                        symbol: d.symbol,
                        exchange: d.exchange,
                        label: `${d.base_coin} — ${EXCHANGE_LABELS[d.exchange] || d.exchange} 偏離趨勢`,
                        recentDevs: d.recent_devs,
                        baseline: d.baseline_pct,
                      });
                    }}
                  >
                    <Sparkline data={d.recent_devs} />
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* 幣種詳情 Modal */}
      {detailSymbol && (
        <SymbolDetail symbol={detailSymbol} onClose={() => setDetailSymbol(null)} />
      )}

      {/* 趨勢放大圖 Modal */}
      {expandedChart && (
        <ChartModal
          {...expandedChart}
          onClose={() => setExpandedChart(null)}
        />
      )}

    </div>
  );
}

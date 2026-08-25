import { useState, useMemo, useEffect } from 'react';
import { usePolling } from '../hooks/usePolling';
import SymbolDetail from './SymbolDetail';

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

function fmtCountdown(nextMs, nowMs) {
  if (!nextMs) return '-';
  let s = Math.max(0, Math.floor((nextMs - nowMs) / 1000));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  s = s % 60;
  const mm = String(m).padStart(2, '0');
  const ss = String(s).padStart(2, '0');
  return h > 0 ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
}

// 溢價走勢迷你圖（官方 1m K 線收盤，最後一根未收盤以空心點標示）
function Sparkline({ data, width = 80, height = 22 }) {
  if (!data || data.length < 2) return null;
  const min = Math.min(...data, 0);
  const max = Math.max(...data, 0);
  const range = max - min || 1;
  const step = width / (data.length - 1);

  const xy = (v, i) => [i * step, height - ((v - min) / range) * height];
  const points = data.map((v, i) => xy(v, i).join(',')).join(' ');
  const last = data[data.length - 1];
  const [lx, ly] = xy(last, data.length - 1);
  const zeroY = height - ((0 - min) / range) * height;
  const color = last >= 0 ? '#4ade80' : '#f87171';

  return (
    <svg width={width} height={height} className="inline-block align-middle">
      <line x1="0" x2={width} y1={zeroY} y2={zeroY} stroke="#374151" strokeWidth="1" strokeDasharray="2,2" />
      <polyline points={points} fill="none" stroke={color} strokeWidth="1.5" />
      <circle cx={lx} cy={ly} r="2" fill="#111827" stroke={color} strokeWidth="1.2" />
    </svg>
  );
}

// 資費使用率進度條（距上/下限）
function CapUsageBar({ usage }) {
  if (usage == null) return <span className="text-gray-500">-</span>;
  const pct = Math.min(100, Math.max(0, usage));
  const color = pct >= 95 ? 'bg-red-500' : pct >= 80 ? 'bg-orange-500' : 'bg-blue-600';
  return (
    <div className="flex items-center gap-1.5">
      <div className="w-16 h-1.5 rounded bg-gray-800 overflow-hidden">
        <div className={`h-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className={pct >= 95 ? 'text-red-400 font-bold' : pct >= 80 ? 'text-orange-400' : 'text-[var(--text-secondary)]'}>
        {usage.toFixed(0)}%
      </span>
    </div>
  );
}

function premiumClass(v) {
  if (v == null) return 'text-gray-500';
  const a = Math.abs(v);
  if (a >= 3) return v >= 0 ? 'text-green-400 font-bold' : 'text-red-400 font-bold';
  if (a >= 1) return v >= 0 ? 'text-green-400' : 'text-red-400';
  return 'text-[var(--text-secondary)]';
}

function fundingClass(v) {
  if (v == null) return 'text-gray-500';
  if (v > 0) return 'text-green-400';
  if (v < 0) return 'text-red-400';
  return 'text-[var(--text-secondary)]';
}

const RENDER_LIMIT = 150;

export default function PremiumIndexPanel({ searchTerm }) {
  const { data, loading } = usePolling('/api/premium-index', 2000);
  const [sortKey, setSortKey] = useState('official_pct');
  const [sortDesc, setSortDesc] = useState(true);
  const [onlyFocus, setOnlyFocus] = useState(false);
  const [detailSymbol, setDetailSymbol] = useState(null);
  const [nowMs, setNowMs] = useState(Date.now());

  useEffect(() => {
    const t = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);

  const toggleSort = (key) => {
    if (sortKey === key) setSortDesc(d => !d);
    else { setSortKey(key); setSortDesc(true); }
  };
  const sortIcon = (key) => sortKey === key ? (sortDesc ? ' ▼' : ' ▲') : '';

  const rows = useMemo(() => {
    let list = data?.data || [];
    if (searchTerm) {
      const kw = searchTerm.toUpperCase();
      list = list.filter(r => r.base.includes(kw));
    }
    if (onlyFocus) list = list.filter(r => r.official_pct != null);
    const absKeys = new Set(['premium_pct', 'official_pct']);
    list = [...list].sort((a, b) => {
      const va = a[sortKey], vb = b[sortKey];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      const xa = absKeys.has(sortKey) ? Math.abs(va) : va;
      const xb = absKeys.has(sortKey) ? Math.abs(vb) : vb;
      return sortDesc ? xb - xa : xa - xb;
    });
    return list;
  }, [data, searchTerm, onlyFocus, sortKey, sortDesc]);

  const thClass = "px-3 py-1.5 text-[var(--text-secondary)] border-b border-[var(--border)] cursor-pointer hover:text-white select-none whitespace-nowrap";

  if (loading && !data) {
    return <div className="p-8 text-center text-[var(--text-secondary)]">載入中...</div>;
  }

  return (
    <div>
      {/* 工具列 */}
      <div className="px-4 py-2 border-b border-[var(--border)] flex items-center gap-4 flex-wrap text-xs">
        <span className="text-[var(--text-secondary)]">
          Bybit {data?.total ?? 0} 個合約
          {data?.ws_connected > 0
            ? <span className="text-green-400 ml-1">● WS 已連線</span>
            : <span className="text-red-400 ml-1">● WS 斷線</span>}
        </span>
        <button
          onClick={() => setOnlyFocus(f => !f)}
          className={`px-2 py-0.5 rounded text-xs transition-colors ${
            onlyFocus ? 'bg-cyan-600 text-white' : 'bg-[var(--bg-secondary)] text-[var(--text-secondary)] hover:text-white'
          }`}
        >
          只看焦點幣
        </button>
        <span className="text-[var(--text-secondary)]">
          即時溢價 = (mark − index) / index（WS 秒級）；官方溢價 = 溢價指數 1m K 線（焦點幣每 5 秒更新，資費 = 溢價 TWAP 夾上下限）
        </span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs text-left">
          <thead>
            <tr>
              <th className={thClass} onClick={() => toggleSort('base')}>幣種{sortIcon('base')}</th>
              <th className={thClass} onClick={() => toggleSort('last')}>最新價{sortIcon('last')}</th>
              <th className={thClass} onClick={() => toggleSort('premium_pct')}>即時溢價{sortIcon('premium_pct')}</th>
              <th className={thClass} onClick={() => toggleSort('official_pct')}>官方溢價{sortIcon('official_pct')}</th>
              <th className={`${thClass} cursor-default`}>走勢 60m</th>
              <th className={thClass} onClick={() => toggleSort('funding_rate_pct')}>當期資費{sortIcon('funding_rate_pct')}</th>
              <th className={`${thClass} cursor-default`}>上限 / 下限</th>
              <th className={thClass} onClick={() => toggleSort('cap_usage_pct')}>距頂/地板{sortIcon('cap_usage_pct')}</th>
              <th className={thClass} onClick={() => toggleSort('interval_h')}>週期{sortIcon('interval_h')}</th>
              <th className={thClass} onClick={() => toggleSort('next_funding_time')}>結算倒數{sortIcon('next_funding_time')}</th>
            </tr>
          </thead>
          <tbody>
            {rows.slice(0, RENDER_LIMIT).map(r => (
              <tr
                key={r.symbol}
                className="border-b border-[var(--border)] hover:bg-[var(--bg-secondary)] cursor-pointer transition-colors"
                onClick={() => setDetailSymbol(r.detail_symbol || `${r.base}/USDT:USDT`)}
              >
                <td className="px-3 py-1.5 font-medium text-white whitespace-nowrap">
                  {r.base}
                  {r.official_pct != null && (
                    <span className="ml-1 text-cyan-400" title="焦點幣：官方溢價每 5 秒更新">◆</span>
                  )}
                </td>
                <td className="px-3 py-1.5">{fmtPrice(r.last)}</td>
                <td className={`px-3 py-1.5 ${premiumClass(r.premium_pct)}`}>{fmtPct(r.premium_pct)}</td>
                <td className={`px-3 py-1.5 ${premiumClass(r.official_pct)}`}>{fmtPct(r.official_pct)}</td>
                <td className="px-3 py-1.5">
                  {r.spark ? <Sparkline data={r.spark} /> : <span className="text-gray-500">-</span>}
                </td>
                <td className={`px-3 py-1.5 ${fundingClass(r.funding_rate_pct)}`}>{fmtPct(r.funding_rate_pct)}</td>
                <td className="px-3 py-1.5 text-[var(--text-secondary)] whitespace-nowrap">
                  {r.upper_pct != null ? `${r.upper_pct}% / ${r.lower_pct}%` : '-'}
                </td>
                <td className="px-3 py-1.5"><CapUsageBar usage={r.cap_usage_pct} /></td>
                <td className="px-3 py-1.5 text-[var(--text-secondary)]">{r.interval_h != null ? `${r.interval_h}h` : '-'}</td>
                <td className="px-3 py-1.5 text-[var(--text-secondary)] font-mono">{fmtCountdown(r.next_funding_time, nowMs)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {rows.length > RENDER_LIMIT && (
          <div className="px-4 py-2 text-xs text-[var(--text-secondary)]">
            僅顯示前 {RENDER_LIMIT} 筆（共 {rows.length} 筆），可用搜尋框縮小範圍
          </div>
        )}
        {rows.length === 0 && (
          <div className="p-8 text-center text-[var(--text-secondary)]">無資料</div>
        )}
      </div>

      {/* 幣種詳情 Modal（與費率總覽相同） */}
      {detailSymbol && (
        <SymbolDetail symbol={detailSymbol} onClose={() => setDetailSymbol(null)} />
      )}
    </div>
  );
}

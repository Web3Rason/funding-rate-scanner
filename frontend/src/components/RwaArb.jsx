import { useEffect, useMemo, useState } from 'react';
import SymbolDetail from './SymbolDetail';

const EX_LABEL = {
  binance: 'Binance', bybit: 'Bybit', okx: 'OKX', gateio: 'Gate.io',
  kraken: 'Kraken', hyperliquid: 'Hyperliquid', deribit: 'Deribit',
};

const MARKET_STATE = {
  regular: { text: '美股盤中', cls: 'text-green-400 bg-green-500/15' },
  extended: { text: '盤前/盤後', cls: 'text-yellow-400 bg-yellow-500/15' },
  closed: { text: '美股休市', cls: 'text-red-400 bg-red-500/15' },
  always: { text: '24H', cls: 'text-blue-400 bg-blue-500/15' },
};

function fmtPrice(v) {
  if (v == null) return '-';
  return v >= 100 ? v.toFixed(2) : v >= 1 ? v.toFixed(4) : v.toPrecision(4);
}

function aprColor(v) {
  if (v == null) return 'text-gray-500';
  if (v >= 50) return 'text-yellow-300 font-bold';
  if (v >= 20) return 'text-green-400 font-semibold';
  if (v > 0) return 'text-green-500';
  return 'text-red-400';
}

function spreadColor(v) {
  if (v == null) return 'text-gray-500';
  if (v >= 0) return 'text-green-400';
  if (v >= -0.3) return 'text-gray-400';
  return 'text-orange-400';
}

export default function RwaArb({ searchTerm }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [onlyHedgeable, setOnlyHedgeable] = useState(true);
  const [category, setCategory] = useState('all');
  const [detailSymbol, setDetailSymbol] = useState(null);
  const [sortField, setSortField] = useState('funding_apr');
  const [sortDir, setSortDir] = useState('desc');

  useEffect(() => {
    let alive = true;
    const load = () => {
      fetch('/api/rwa-arb')
        .then(r => r.json())
        .then(d => { if (alive) { setData(d); setLoading(false); } })
        .catch(() => { if (alive) setLoading(false); });
    };
    load();
    // 後端每小時算一次並快取，前端 5 分鐘取一次即可拿到最新結果
    const t = setInterval(load, 300000);
    return () => { alive = false; clearInterval(t); };
  }, []);

  const rows = useMemo(() => {
    let list = data?.opportunities || [];
    if (onlyHedgeable) list = list.filter(o => o.hedgeable);
    if (category !== 'all') list = list.filter(o => o.category === category);
    const q = (searchTerm || '').trim().toUpperCase();
    if (q) list = list.filter(o => o.base.includes(q) || (o.name || '').toUpperCase().includes(q));
    const dir = sortDir === 'desc' ? -1 : 1;
    return [...list].sort((a, b) => {
      const av = a[sortField] ?? -9e9, bv = b[sortField] ?? -9e9;
      return av === bv ? 0 : (av > bv ? dir : -dir);
    });
  }, [data, onlyHedgeable, category, searchTerm, sortField, sortDir]);

  const sort = (f) => {
    if (sortField === f) setSortDir(d => (d === 'desc' ? 'asc' : 'desc'));
    else { setSortField(f); setSortDir('desc'); }
  };
  const arrow = (f) => (sortField === f ? (sortDir === 'desc' ? ' ↓' : ' ↑') : '');

  const unhedgeable = (data?.opportunities || []).filter(o => !o.hedgeable).length;
  const ms = MARKET_STATE[data?.market_state] || MARKET_STATE.closed;

  if (loading) return <div className="p-8 text-center text-[var(--text-secondary)]">載入中...</div>;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <span className={`px-2 py-0.5 rounded ${ms.cls}`}>{ms.text}</span>
        <span className="text-[var(--text-secondary)]">
          CrossEx 可交易 RWA：{data?.universe_count ?? 0} 個 ／ 顯示 {rows.length} 筆
        </span>
        {data?.computed_at && (
          <span className="text-[var(--text-secondary)]">
            每小時更新｜上次 {new Date(data.computed_at).toLocaleTimeString()}
          </span>
        )}
        <label className="flex items-center gap-1.5 cursor-pointer">
          <input type="checkbox" checked={onlyHedgeable} onChange={e => setOnlyHedgeable(e.target.checked)} />
          <span>只看可對沖{unhedgeable > 0 && `（隱藏 ${unhedgeable} 筆基準不一致）`}</span>
        </label>
        <div className="flex gap-1">
          {[['all', '全部'], ['equity', '股票'], ['commodity', '商品']].map(([v, t]) => (
            <button key={v} onClick={() => setCategory(v)}
              className={`px-2 py-0.5 rounded text-xs ${category === v ? 'bg-indigo-600 text-white' : 'bg-[var(--bg-tertiary)] text-[var(--text-secondary)]'}`}>
              {t}
            </button>
          ))}
        </div>
      </div>

      <div className="text-xs text-[var(--text-secondary)] leading-relaxed">
        買現貨 + 空等量永續 → Delta≈0，收益 = <span className="text-green-400">資費（空方收）</span> +{' '}
        <span className="text-green-400">進場價差</span>。兩腿都限 CrossEx 帳戶（跨所共用保證金池）。
        非美股盤中時段現貨買賣價差會擴大、侵蝕利潤。
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-[var(--text-secondary)] border-b border-[var(--border)]">
            <tr>
              <th className="px-2 py-2 text-left">標的</th>
              <th className="px-2 py-2 text-left">名稱</th>
              <th className="px-2 py-2 text-right cursor-pointer hover:text-white" onClick={() => sort('funding_apr')}>
                資費年化{arrow('funding_apr')}
              </th>
              <th className="px-2 py-2 text-right cursor-pointer hover:text-white" onClick={() => sort('entry_spread_pct')}>
                進場價差{arrow('entry_spread_pct')}
              </th>
              <th className="px-2 py-2 text-left">買現貨（ask）</th>
              <th className="px-2 py-2 text-right">現貨買賣價差</th>
              <th className="px-2 py-2 text-left">空合約（bid）</th>
              <th className="px-2 py-2 text-center">週期</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(o => {
              const sp = o.spot || {}, fu = o.futures || {};
              return (
                <tr key={o.base} className="border-b border-[var(--border)] hover:bg-[var(--bg-tertiary)]">
                  <td className="px-2 py-1.5 font-medium">
                    <button
                      onClick={() => setDetailSymbol(fu.norm_symbol || fu.symbol)}
                      className="text-white font-medium hover:text-blue-400 hover:underline transition-colors"
                    >
                      {o.base}
                    </button>
                    {!o.hedgeable && (
                      <span className="ml-1 px-1 rounded text-[10px] text-red-300 bg-red-500/20" title="現貨與合約價差過大，兩腿基準不一致，不是有效對沖">
                        基準不符
                      </span>
                    )}
                    {o.category === 'commodity' && (
                      <span className="ml-1 px-1 rounded text-[10px] text-blue-300 bg-blue-500/15">24H</span>
                    )}
                  </td>
                  <td className="px-2 py-1.5 text-[var(--text-secondary)] max-w-[180px] truncate" title={o.name}>{o.name}</td>
                  <td className={`px-2 py-1.5 text-right ${aprColor(o.funding_apr)}`}>
                    {o.funding_apr == null ? '-' : `${o.funding_apr.toFixed(2)}%`}
                  </td>
                  <td className={`px-2 py-1.5 text-right ${spreadColor(o.entry_spread_pct)}`}>
                    {o.entry_spread_pct == null ? '-' : `${o.entry_spread_pct.toFixed(3)}%`}
                  </td>
                  <td className="px-2 py-1.5">
                    <span className="text-blue-300">{EX_LABEL[sp.exchange] || sp.exchange}</span>
                    <span className="text-[var(--text-secondary)]"> {sp.raw_base} </span>
                    <span className="text-gray-300">{fmtPrice(sp.ask)}</span>
                  </td>
                  <td className={`px-2 py-1.5 text-right ${(sp.bid_ask_spread_pct ?? 0) > 0.5 ? 'text-orange-400' : 'text-gray-400'}`}>
                    {sp.bid_ask_spread_pct == null ? '-' : `${sp.bid_ask_spread_pct.toFixed(3)}%`}
                  </td>
                  <td className="px-2 py-1.5">
                    <span className="text-purple-300">{EX_LABEL[fu.exchange] || fu.exchange}</span>
                    <span className="text-[var(--text-secondary)]"> {fu.raw_base} </span>
                    <span className="text-gray-300">{fmtPrice(fu.bid)}</span>
                  </td>
                  <td className="px-2 py-1.5 text-center text-[var(--text-secondary)]">
                    {fu.funding_interval_h ? `${fu.funding_interval_h}h` : '-'}
                  </td>
                </tr>
              );
            })}
            {rows.length === 0 && (
              <tr><td colSpan={8} className="px-2 py-6 text-center text-[var(--text-secondary)]">無符合條件的標的</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {detailSymbol && (
        <SymbolDetail
          symbol={detailSymbol}
          records={[]}
          onClose={() => setDetailSymbol(null)}
        />
      )}
    </div>
  );
}

import { useEffect, useMemo, useState } from 'react';
import SymbolDetail from './SymbolDetail';

const EX_LABEL = {
  binance: 'Binance', bybit: 'Bybit', okx: 'OKX', gateio: 'Gate.io', bitget: 'Bitget',
  mexc: 'MEXC', bingx: 'BingX', kucoinfutures: 'KuCoin', kraken: 'Kraken', deribit: 'Deribit',
  hyperliquid: 'Hyperliquid', aster: 'Aster', coinw: 'CoinW', ourbit: 'Ourbit',
  deepcoin: 'DeepCoin', lbank: 'LBank', tradexyz: 'Trade.xyz', lighter: 'Lighter', lighter_rh: 'Lighter RH',
};

const MARKET_STATE = {
  regular: { text: '美股盤中', cls: 'text-green-400 bg-green-500/15' },
  extended: { text: '盤前/盤後', cls: 'text-yellow-400 bg-yellow-500/15' },
  closed: { text: '美股休市', cls: 'text-red-400 bg-red-500/15' },
};

const fmtPrice = (v) => (v == null ? '-' : v >= 100 ? v.toFixed(2) : v >= 1 ? v.toFixed(4) : v.toPrecision(4));

function profitColor(v) {
  if (v == null) return 'text-gray-500';
  if (v >= 2) return 'text-yellow-300 font-bold';
  if (v >= 1) return 'text-green-400 font-semibold';
  return 'text-green-500';
}

export default function RwaSpread({ searchTerm }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [minProfit, setMinProfit] = useState('');
  const [detailSymbol, setDetailSymbol] = useState(null);

  useEffect(() => {
    let alive = true;
    const load = () => {
      fetch('/api/rwa-spread')
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
    const q = (searchTerm || '').trim().toUpperCase();
    if (q) list = list.filter(o => o.base.includes(q) || (o.name || '').toUpperCase().includes(q));
    const mp = parseFloat(minProfit);
    if (!isNaN(mp)) list = list.filter(o => o.est_profit_pct >= mp);
    return list;
  }, [data, searchTerm, minProfit]);

  const ms = MARKET_STATE[data?.market_state] || MARKET_STATE.closed;

  if (loading) return <div className="p-8 text-center text-[var(--text-secondary)]">載入中...</div>;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <span className={`px-2 py-0.5 rounded ${ms.cls}`}>{ms.text}</span>
        <span className="text-[var(--text-secondary)]">機會 {rows.length} 筆</span>
        <label className="flex items-center gap-1.5">
          <span className="text-[var(--text-secondary)]">最低利潤%</span>
          <input value={minProfit} onChange={e => setMinProfit(e.target.value)} placeholder="不限"
            className="w-20 px-2 py-0.5 rounded bg-[var(--bg-tertiary)] border border-[var(--border)]" />
        </label>
        <span className="text-[var(--text-secondary)]">
          TG 通知門檻：利潤 ≥ {data?.notify_threshold ?? 1}%
        </span>
        {data?.computed_at && (
          <span className="text-[var(--text-secondary)]">
            每小時更新｜上次 {new Date(data.computed_at).toLocaleTimeString()}
          </span>
        )}
      </div>

      <div className="text-xs text-[var(--text-secondary)] leading-relaxed">
        同一標的的永續在不同交易所出現價差 → <span className="text-green-400">做多便宜所 + 做空貴的所</span>，等價差收斂。
        成因是各所<span className="text-yellow-400">指數取價時點不同</span>（例：部分交易所的美股指數要到開盤才更新，
        其他所連續更新 → 收盤後到開盤前兩邊會拉開）。
        預計利潤 = 價差 − 資費成本，算法與「幣種詳情」的套利建議一致。
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-[var(--text-secondary)] border-b border-[var(--border)]">
            <tr>
              <th className="px-2 py-2 text-left">標的</th>
              <th className="px-2 py-2 text-left">名稱</th>
              <th className="px-2 py-2 text-right">預計利潤</th>
              <th className="px-2 py-2 text-right">價差</th>
              <th className="px-2 py-2 text-right">資費</th>
              <th className="px-2 py-2 text-right">指數差</th>
              <th className="px-2 py-2 text-left">做多（買 ask）</th>
              <th className="px-2 py-2 text-left">做空（賣 bid）</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(o => (
              <tr key={`${o.base}-${o.long_exchange}-${o.short_exchange}`}
                  className="border-b border-[var(--border)] hover:bg-[var(--bg-tertiary)]">
                <td className="px-2 py-1.5 font-medium">
                  <button
                    onClick={() => setDetailSymbol(o.long_norm_symbol || o.long_symbol)}
                    className="text-white font-medium hover:text-blue-400 hover:underline transition-colors"
                  >
                    {o.base}
                  </button>
                  {o.category === 'commodity' && (
                    <span className="ml-1 px-1 rounded text-[10px] text-blue-300 bg-blue-500/15">24H</span>
                  )}
                </td>
                <td className="px-2 py-1.5 text-[var(--text-secondary)] max-w-[160px] truncate" title={o.name}>{o.name}</td>
                <td className={`px-2 py-1.5 text-right ${profitColor(o.est_profit_pct)}`}>
                  {o.est_profit_pct.toFixed(3)}%
                </td>
                <td className="px-2 py-1.5 text-right text-green-400">{(-o.spread_pct).toFixed(3)}%</td>
                <td className={`px-2 py-1.5 text-right ${o.funding_diff_pct >= 0 ? 'text-green-500' : 'text-orange-400'}`}
                    title={`${o.norm_interval_h}h 基準`}>
                  {o.funding_diff_pct.toFixed(3)}%
                </td>
                <td className="px-2 py-1.5 text-right text-gray-400">
                  {o.index_diff_pct == null ? '-' : `${o.index_diff_pct.toFixed(3)}%`}
                </td>
                <td className="px-2 py-1.5">
                  <span className="text-blue-300">{EX_LABEL[o.long_exchange] || o.long_exchange}</span>
                  <span className="text-gray-300"> {fmtPrice(o.long_ask)}</span>
                </td>
                <td className="px-2 py-1.5">
                  <span className="text-purple-300">{EX_LABEL[o.short_exchange] || o.short_exchange}</span>
                  <span className="text-gray-300"> {fmtPrice(o.short_bid)}</span>
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr><td colSpan={8} className="px-2 py-6 text-center text-[var(--text-secondary)]">目前沒有符合條件的跨所價差</td></tr>
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

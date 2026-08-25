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

const fmt = (v, d = 3) => (v == null ? '-' : Number(v).toFixed(d));

function devColor(v) {
  const a = Math.abs(v);
  if (a >= 5) return 'text-yellow-300 font-bold';
  if (a >= 2) return 'text-green-400 font-semibold';
  return 'text-green-500';
}

export default function RwaLeveraged({ searchTerm }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [minDev, setMinDev] = useState('');
  const [detailSymbol, setDetailSymbol] = useState(null);

  useEffect(() => {
    let alive = true;
    const load = () => {
      fetch('/api/rwa-leveraged')
        .then(r => r.json())
        .then(d => { if (alive) { setData(d); setLoading(false); } })
        .catch(() => { if (alive) setLoading(false); });
    };
    load();
    const t = setInterval(load, 300000);   // 後端每小時算一次
    return () => { alive = false; clearInterval(t); };
  }, []);

  const rows = useMemo(() => {
    let list = data?.opportunities || [];
    const q = (searchTerm || '').trim().toUpperCase();
    if (q) list = list.filter(o => o.leveraged.includes(q) || o.underlying.includes(q) || (o.name || '').toUpperCase().includes(q));
    const m = parseFloat(minDev);
    if (!isNaN(m)) list = list.filter(o => Math.abs(o.deviation_pct) >= m);
    return list;
  }, [data, searchTerm, minDev]);

  const spotRows = useMemo(() => {
    let list = data?.spot_tokens || [];
    const q = (searchTerm || '').trim().toUpperCase();
    if (q) list = list.filter(o => o.token.includes(q) || o.underlying.includes(q));
    const m = parseFloat(minDev);
    if (!isNaN(m)) list = list.filter(o => Math.abs(o.deviation_pct) >= m);
    return list;
  }, [data, searchTerm, minDev]);

  const ms = MARKET_STATE[data?.market_state] || MARKET_STATE.closed;

  if (loading) return <div className="p-8 text-center text-[var(--text-secondary)]">載入中...</div>;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-3 text-sm">
        <span className={`px-2 py-0.5 rounded ${ms.cls}`}>{ms.text}</span>
        <span className="text-[var(--text-secondary)]">機會 {rows.length} 筆</span>
        <label className="flex items-center gap-1.5">
          <span className="text-[var(--text-secondary)]">最低偏離%</span>
          <input value={minDev} onChange={e => setMinDev(e.target.value)} placeholder={data?.min_deviation ?? 1}
            className="w-20 px-2 py-0.5 rounded bg-[var(--bg-tertiary)] border border-[var(--border)]" />
        </label>
        {data?.computed_at && (
          <span className="text-[var(--text-secondary)]">
            每小時更新｜上次 {new Date(data.computed_at).toLocaleTimeString()}
          </span>
        )}
      </div>

      <div className="text-sm font-semibold text-white">槓桿 ETF 永續對（兩腿都可開合約）</div>

      <div className="text-xs text-[var(--text-secondary)] leading-relaxed">
        槓桿 ETF（如 SSPC = SPCX 的 −2 倍）追蹤的是<span className="text-yellow-400">每日重設後的報酬率</span>，
        基準為<span className="text-yellow-400">美股official收盤（16:00 ET）</span>：
        <span className="ml-1 font-mono">理論價 = ETF收盤 × (1 + 倍數 × 標的漲跌%)</span>。
        交易所指數更新有時差 → 實際價偏離理論價，等收斂即獲利。
        <span className="text-orange-400">偏離為負 = ETF 便宜（做多）；為正 = 貴（做空）。</span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-[var(--text-secondary)] border-b border-[var(--border)]">
            <tr>
              <th className="px-2 py-2 text-left">槓桿 ETF</th>
              <th className="px-2 py-2 text-center">倍數</th>
              <th className="px-2 py-2 text-left">標的</th>
              <th className="px-2 py-2 text-right">標的自收盤</th>
              <th className="px-2 py-2 text-right">理論價</th>
              <th className="px-2 py-2 text-right">實際價</th>
              <th className="px-2 py-2 text-right">偏離</th>
              <th className="px-2 py-2 text-left">操作</th>
              <th className="px-2 py-2 text-right">ETF 資費</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(o => (
              <tr key={`${o.leveraged}-${o.underlying}`}
                  className="border-b border-[var(--border)] hover:bg-[var(--bg-tertiary)]">
                <td className="px-2 py-1.5">
                  <button onClick={() => setDetailSymbol(o.lev_norm_symbol || o.lev_symbol)}
                    className="text-white font-medium hover:text-blue-400 hover:underline">
                    {o.leveraged}
                  </button>
                  <span className="ml-1 text-[10px] text-[var(--text-secondary)]">
                    {EX_LABEL[o.lev_exchange] || o.lev_exchange}
                  </span>
                </td>
                <td className={`px-2 py-1.5 text-center font-mono ${o.multiplier < 0 ? 'text-red-400' : 'text-green-400'}`}>
                  {o.multiplier > 0 ? '+' : ''}{o.multiplier}x
                </td>
                <td className="px-2 py-1.5">
                  <button onClick={() => setDetailSymbol(o.und_norm_symbol || o.und_symbol)}
                    className="text-gray-300 hover:text-blue-400 hover:underline">
                    {o.underlying}
                  </button>
                  <span className="ml-1 text-[10px] text-[var(--text-secondary)]">
                    {EX_LABEL[o.und_exchange] || o.und_exchange}
                  </span>
                </td>
                <td className={`px-2 py-1.5 text-right ${o.und_change_pct >= 0 ? 'text-green-400' : 'text-red-400'}`}
                    title={`收盤 ${o.und_close} → 現價 ${o.und_price}`}>
                  {o.und_change_pct >= 0 ? '+' : ''}{fmt(o.und_change_pct, 2)}%
                </td>
                <td className="px-2 py-1.5 text-right text-gray-400">{fmt(o.lev_theoretical)}</td>
                <td className="px-2 py-1.5 text-right text-gray-200">{fmt(o.lev_price)}</td>
                <td className={`px-2 py-1.5 text-right ${devColor(o.deviation_pct)}`}>
                  {o.deviation_pct >= 0 ? '+' : ''}{fmt(o.deviation_pct, 2)}%
                </td>
                <td className={`px-2 py-1.5 ${o.deviation_pct < 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {o.action}
                </td>
                <td className={`px-2 py-1.5 text-right ${(o.lev_funding ?? 0) > 0.003 ? 'text-orange-400' : 'text-gray-400'}`}
                    title={`${o.lev_interval_h}h 一次`}>
                  {o.lev_funding == null ? '-' : `${(o.lev_funding * 100).toFixed(3)}%/${o.lev_interval_h}h`}
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr><td colSpan={9} className="px-2 py-6 text-center text-[var(--text-secondary)]">目前沒有超過門檻的偏離</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* ── Gate 現貨槓桿代幣：只有現貨、借不到 → 只列可買進的負偏離 ── */}
      <div className="pt-4 mt-2 border-t border-[var(--border)] space-y-3">
        <div className="flex flex-wrap items-center gap-3 text-sm">
          <span className="font-semibold text-white">Gate 現貨槓桿代幣（單邊可買進）</span>
          <span className="text-[var(--text-secondary)]">機會 {spotRows.length} 筆</span>
          <span className="px-1.5 py-0.5 rounded text-[10px] text-orange-300 bg-orange-500/15">
            不在 CrossEx，需直接在 Gate 下單
          </span>
        </div>

        <div className="text-xs text-[var(--text-secondary)] leading-relaxed">
          Gate 自家發行的再平衡槓桿代幣（<span className="font-mono">XAU3L = XAU3xLong</span>），
          <span className="text-yellow-400">只有現貨、沒有合約</span>，且不在 Gate 統一杠桿的可借清單裡
          → <span className="text-orange-400">借不到就無法做空，所以只列偏離為負（代幣偏便宜）能買進的</span>。
          基準是<span className="text-yellow-400">上一個 16:00 UTC</span>（Gate 官方定時再平衡時點，非美股收盤）：
          <span className="ml-1 font-mono">理論價 = 代幣錨點價 × (1 + 倍數 × 標的自錨點漲跌%)</span>。
          <span className="block mt-1">
            <span className="text-green-400">可以對沖</span>：買 N U 代幣 + 在標的的 Gate 永續開
            <span className="text-green-400"> |倍數| × N U 名目</span>的反向單 → Delta≈0。
            已濾掉沒有標的永續的代幣（無法對沖）與 24h 成交額 &lt; {(data?.spot_min_volume ?? 5000).toLocaleString()} U 的。
          </span>
          <span className="block mt-1 text-red-400/90">
            注意：官方再平衡是「有條件」的（槓桿跑出 2.25x–4.125x 或標的日變動 &gt;1% 才調回），
            若錨點後發生過再平衡，理論價會失真；距錨點越久誤差越大。
          </span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-[var(--text-secondary)] border-b border-[var(--border)]">
              <tr>
                <th className="px-2 py-2 text-left">代幣（Gate 現貨）</th>
                <th className="px-2 py-2 text-center">倍數</th>
                <th className="px-2 py-2 text-left">標的</th>
                <th className="px-2 py-2 text-right">標的自錨點</th>
                <th className="px-2 py-2 text-right">理論價</th>
                <th className="px-2 py-2 text-right">實際價</th>
                <th className="px-2 py-2 text-right">偏離</th>
                <th className="px-2 py-2 text-left">對沖腿</th>
                <th className="px-2 py-2 text-right">24h 成交額</th>
              </tr>
            </thead>
            <tbody>
              {spotRows.map(o => (
                <tr key={o.token} className="border-b border-[var(--border)] hover:bg-[var(--bg-tertiary)]">
                  <td className="px-2 py-1.5">
                    <span className="text-white font-medium">{o.token}</span>
                    <span className="ml-1 text-[10px] text-[var(--text-secondary)]">Gate 現貨</span>
                  </td>
                  <td className={`px-2 py-1.5 text-center font-mono ${o.multiplier < 0 ? 'text-red-400' : 'text-green-400'}`}>
                    {o.multiplier > 0 ? '+' : ''}{o.multiplier}x
                  </td>
                  <td className="px-2 py-1.5">
                    <button onClick={() => setDetailSymbol(o.und_norm_symbol)}
                      className="text-gray-300 hover:text-blue-400 hover:underline">
                      {o.underlying}
                    </button>
                    {o.category === 'commodity' && (
                      <span className="ml-1 px-1 rounded text-[10px] text-blue-300 bg-blue-500/15">24H</span>
                    )}
                  </td>
                  <td className={`px-2 py-1.5 text-right ${o.und_change_pct >= 0 ? 'text-green-400' : 'text-red-400'}`}
                      title={`錨點 ${o.und_anchor} → 現價 ${o.und_price}（${o.hours_since_anchor} 小時前的 16:00 UTC）`}>
                    {o.und_change_pct >= 0 ? '+' : ''}{fmt(o.und_change_pct, 2)}%
                  </td>
                  <td className="px-2 py-1.5 text-right text-gray-400">{fmt(o.token_theoretical, 5)}</td>
                  <td className="px-2 py-1.5 text-right text-gray-200">{fmt(o.token_price, 5)}</td>
                  <td className={`px-2 py-1.5 text-right ${devColor(o.deviation_pct)}`}>
                    {fmt(o.deviation_pct, 2)}%
                  </td>
                  <td className="px-2 py-1.5 text-xs">
                    <span className={o.hedge_side === '空' ? 'text-purple-300' : 'text-blue-300'}>
                      {o.hedge_side} {o.hedge_notional_x}x
                    </span>
                    <span className="text-[var(--text-secondary)]"> {o.hedge_contract}</span>
                    {o.hedge_funding != null && (
                      <span className="ml-1 text-[10px] text-gray-500">
                        {(o.hedge_funding * 100).toFixed(3)}%/{o.hedge_interval_h}h
                      </span>
                    )}
                  </td>
                  <td className={`px-2 py-1.5 text-right ${o.volume_24h_usdt < 20000 ? 'text-orange-400' : 'text-gray-400'}`}>
                    {o.volume_24h_usdt.toLocaleString()}
                  </td>
                </tr>
              ))}
              {spotRows.length === 0 && (
                <tr><td colSpan={9} className="px-2 py-6 text-center text-[var(--text-secondary)]">
                  目前沒有偏離為負且可買進的槓桿代幣
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {detailSymbol && (
        <SymbolDetail symbol={detailSymbol} records={[]} onClose={() => setDetailSymbol(null)} />
      )}
    </div>
  );
}

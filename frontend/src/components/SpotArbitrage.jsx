import { useState, useMemo } from 'react';
import { usePolling } from '../hooks/usePolling';

const EXCHANGE_LABELS = {
  binance: 'Binance', bybit: 'Bybit', okx: 'OKX', bitget: 'Bitget',
  mexc: 'MEXC', gateio: 'Gate.io', bingx: 'BingX', binance_alpha: 'Binance Alpha',
};

function formatPrice(price) {
  if (!price) return '-';
  if (price >= 1000) return price.toFixed(2);
  if (price >= 1) return price.toFixed(4);
  if (price >= 0.001) return price.toFixed(6);
  return price.toPrecision(4);
}

function formatVolume(vol) {
  if (!vol) return '-';
  if (vol >= 1e9) return (vol / 1e9).toFixed(2) + 'B';
  if (vol >= 1e6) return (vol / 1e6).toFixed(2) + 'M';
  if (vol >= 1e3) return (vol / 1e3).toFixed(1) + 'K';
  return vol.toFixed(0);
}

function formatQty(qty) {
  if (!qty) return '-';
  if (qty >= 1e6) return (qty / 1e6).toFixed(2) + 'M';
  if (qty >= 1e3) return (qty / 1e3).toFixed(1) + 'K';
  if (qty >= 1) return qty.toFixed(1);
  return qty.toPrecision(3);
}

function formatUsd(usd) {
  if (!usd) return '-';
  if (usd >= 1e6) return '$' + (usd / 1e6).toFixed(2) + 'M';
  if (usd >= 1e3) return '$' + (usd / 1e3).toFixed(1) + 'K';
  return '$' + usd.toFixed(0);
}

export default function SpotArbitrage({ searchTerm }) {
  const [minSpread, setMinSpread] = useState(0.5);
  const [expandedSymbol, setExpandedSymbol] = useState(null);

  const { data, loading } = usePolling(`/api/spot-arbitrage?min_spread=${minSpread}`, 5000);

  const opportunities = data?.opportunities || [];

  const filtered = useMemo(() => {
    let list = opportunities;
    if (searchTerm) {
      const term = searchTerm.toUpperCase();
      list = list.filter(o => o.base_coin.toUpperCase().includes(term));
    }
    return list;
  }, [opportunities, searchTerm]);

  return (
    <div>
      {/* 閾值控制列 */}
      <div className="px-4 py-2.5 border-b border-[var(--border)] flex items-center gap-3 text-xs flex-wrap">
        <span className="text-gray-400">最低價差 &ge;</span>
        <input
          type="number"
          value={minSpread}
          onChange={e => setMinSpread(parseFloat(e.target.value) || 0)}
          step="0.1"
          min="0"
          className="w-20 px-2 py-1 bg-[var(--bg-secondary)] border border-[var(--border)] rounded text-white text-xs font-mono"
        />
        <span className="text-gray-500">%</span>

        {[0.5, 1.0, 2.0, 5.0].map(v => (
          <button
            key={v}
            onClick={() => setMinSpread(v)}
            className={`px-2 py-0.5 rounded border transition-colors ${
              minSpread === v
                ? 'bg-blue-500/20 text-blue-400 border-blue-500/40'
                : 'text-gray-500 border-gray-700/50 hover:text-gray-400'
            }`}
          >
            {v}%
          </button>
        ))}

        <span className="text-gray-600 ml-2">
          符合 {filtered.length} 個機會
          {data?.scanned_coins != null && (
            <span className="text-gray-700 ml-1">
              （共掃描 {data.scanned_coins} 個幣種
              {data.scan_duration_ms != null && `，耗時 ${(data.scan_duration_ms / 1000).toFixed(1)}s`}）
            </span>
          )}
        </span>
      </div>

      {/* 說明 */}
      <div className="px-4 py-1.5 border-b border-[var(--border)]/50 text-[10px] text-gray-600">
        低價交易所買入 → 轉帳 → 高價交易所賣出｜價格為 5 檔加權均價｜價差 = (賣出Bid - 買入Ask) / 買入Ask × 100%｜點擊展開查看明細
      </div>

      {/* 主表格 */}
      <div className="overflow-auto max-h-[calc(100vh-280px)]">
        <table className="w-full border-collapse text-xs">
          <thead className="sticky top-0 z-10">
            <tr className="bg-[var(--bg-secondary)]">
              <th className="px-3 py-2 text-left text-[var(--text-secondary)] border-b border-[var(--border)]">幣種</th>
              <th className="px-3 py-2 text-left text-[var(--text-secondary)] border-b border-[var(--border)]">買入交易所</th>
              <th className="px-3 py-2 text-right text-[var(--text-secondary)] border-b border-[var(--border)]">買入價(5檔Ask)</th>
              <th className="px-3 py-2 text-left text-[var(--text-secondary)] border-b border-[var(--border)]">賣出交易所</th>
              <th className="px-3 py-2 text-right text-[var(--text-secondary)] border-b border-[var(--border)]">賣出價(5檔Bid)</th>
              <th className="px-3 py-2 text-right text-[var(--text-secondary)] border-b border-[var(--border)]">價差%</th>
              <th className="px-3 py-2 text-right text-[var(--text-secondary)] border-b border-[var(--border)]">可用量</th>
              <th className="px-3 py-2 text-right text-[var(--text-secondary)] border-b border-[var(--border)]">等值(U)</th>
              <th className="px-3 py-2 text-center text-[var(--text-secondary)] border-b border-[var(--border)]">共同鏈</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(o => {
              const isExpanded = expandedSymbol === o.base_coin;
              return (
                <RowGroup
                  key={o.base_coin}
                  o={o}
                  isExpanded={isExpanded}
                  onToggle={() => setExpandedSymbol(isExpanded ? null : o.base_coin)}
                />
              );
            })}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={9} className="px-3 py-10 text-center text-gray-500">
                  {!data || data.scanned_coins === 0 ? (
                    <div className="flex flex-col items-center gap-2">
                      <div className="w-5 h-5 border-2 border-blue-500/30 border-t-blue-500 rounded-full animate-spin" />
                      <span>掃描中...</span>
                    </div>
                  ) : (
                    `無價差 ≥ ${minSpread}% 的搬磚機會`
                  )}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function RowGroup({ o, isExpanded, onToggle }) {
  const spreadColor = o.spread_pct >= 5 ? 'text-yellow-300' :
                      o.spread_pct >= 2 ? 'text-yellow-400' :
                      o.spread_pct >= 1 ? 'text-green-400' : 'text-green-500';

  return (
    <>
      <tr
        className="hover:bg-[var(--bg-hover)] border-b border-[var(--border)]/30 cursor-pointer"
        onClick={onToggle}
      >
        <td className="px-3 py-2 text-white font-medium">
          <span className="text-blue-400">{o.base_coin}</span>
          <span className="ml-1 text-[10px] text-gray-600">{isExpanded ? '▴' : '▾'}</span>
        </td>
        <td className="px-3 py-2 text-green-400">
          {o.buy_label}
        </td>
        <td className="px-3 py-2 text-right font-mono text-gray-300">
          {formatPrice(o.buy_ask)}
        </td>
        <td className="px-3 py-2 text-red-400">
          {o.sell_label}
        </td>
        <td className="px-3 py-2 text-right font-mono text-gray-300">
          {formatPrice(o.sell_bid)}
        </td>
        <td className={`px-3 py-2 text-right font-mono font-bold ${spreadColor}`}>
          +{o.spread_pct.toFixed(2)}%
        </td>
        <td className="px-3 py-2 text-right font-mono text-gray-400">
          {formatQty(o.tradeable_qty)}
        </td>
        <td className="px-3 py-2 text-right font-mono text-gray-400">
          {formatUsd(o.tradeable_usd)}
        </td>
        <td className="px-3 py-2 text-center">
          {o.common_networks && o.common_networks.length > 0 ? (
            <div className="flex flex-wrap gap-1 justify-center">
              {o.common_networks.slice(0, 3).map(n => (
                <span key={n} className="px-1.5 py-0.5 rounded bg-blue-500/10 text-blue-400 text-[10px]">
                  {n}
                </span>
              ))}
              {o.common_networks.length > 3 && (
                <span className="text-gray-500 text-[10px]">+{o.common_networks.length - 3}</span>
              )}
            </div>
          ) : (
            <span className="text-gray-600 text-[10px]">-</span>
          )}
        </td>
      </tr>

      {/* 展開：所有交易所價格 + 充提鏈明細 */}
      {isExpanded && o.all_prices && (
        <tr className="border-b border-[var(--border)]/30">
          <td colSpan={9} className="px-0 py-0">
            <div className="bg-[var(--bg-secondary)]/50 px-4 py-2">
              <table className="w-full text-xs">
                <thead>
                  <tr className="text-gray-500">
                    <th className="py-1 text-left font-medium">交易所</th>
                    <th className="py-1 text-left font-medium">Bid</th>
                    <th className="py-1 text-left font-medium">Ask</th>
                    <th className="py-1 text-right font-medium">24h量(USDT)</th>
                    <th className="py-1 text-right font-medium">vs最低Ask</th>
                    <th className="py-1 text-left font-medium pl-4">充值鏈</th>
                    <th className="py-1 text-left font-medium">提現鏈</th>
                  </tr>
                </thead>
                <tbody>
                  {[...o.all_prices].sort((a, b) => {
                    const aIsBuy = a.exchange === o.buy_exchange ? -2 : 0;
                    const aIsSell = a.exchange === o.sell_exchange ? -1 : 0;
                    const bIsBuy = b.exchange === o.buy_exchange ? -2 : 0;
                    const bIsSell = b.exchange === o.sell_exchange ? -1 : 0;
                    return (aIsBuy + aIsSell) - (bIsBuy + bIsSell);
                  }).map(p => {
                    const diffPct = o.buy_ask > 0 ? ((p.bid - o.buy_ask) / o.buy_ask * 100) : 0;
                    const isBuyEx = p.exchange === o.buy_exchange;
                    const isSellEx = p.exchange === o.sell_exchange;

                    const allNets = p.networks || [];
                    const depositNets = allNets.map(n => ({ name: n.network, active: n.deposit, contract: n.contract }));
                    const withdrawNets = allNets.map(n => ({ name: n.network, active: n.withdraw, contract: n.contract }));

                    return (
                      <tr key={p.exchange} className={`border-t border-[var(--border)]/20 ${isBuyEx || isSellEx ? 'bg-blue-500/5' : ''}`}>
                        <td className="py-1.5">
                          <span className={isBuyEx ? 'text-green-400' : isSellEx ? 'text-red-400' : 'text-gray-400'}>
                            {p.label}
                          </span>
                          {isBuyEx && <span className="ml-1 text-[9px] text-green-500">買入</span>}
                          {isSellEx && <span className="ml-1 text-[9px] text-red-500">賣出</span>}
                        </td>
                        <td className="py-1.5 text-left font-mono text-gray-300">
                          {formatPrice(p.bid)}
                          {p.bid1 && p.bid1 !== p.bid && <span className="text-yellow-500/70 ml-1">({formatPrice(p.bid1)})</span>}
                        </td>
                        <td className="py-1.5 text-left font-mono text-gray-300">
                          {formatPrice(p.ask)}
                          {p.ask1 && p.ask1 !== p.ask && <span className="text-yellow-500/70 ml-1">({formatPrice(p.ask1)})</span>}
                        </td>
                        <td className="py-1.5 text-right font-mono text-gray-500">{formatVolume(p.volume)}</td>
                        <td className={`py-1.5 text-right font-mono ${diffPct > 0 ? 'text-green-400' : diffPct < -0.1 ? 'text-red-400' : 'text-gray-500'}`}>
                          {diffPct > 0 ? '+' : ''}{diffPct.toFixed(2)}%
                        </td>
                        <td className="py-1.5 text-left pl-4">
                          <div className="flex flex-wrap gap-0.5">
                            {depositNets.length > 0 ? depositNets.map(n => (
                              <span
                                key={n.name}
                                title={n.contract ? `合約: ${n.contract}` : '原生幣'}
                                className={`px-1 py-0.5 rounded text-[9px] cursor-default ${
                                  !n.active
                                    ? 'bg-red-500/10 text-red-400/70 line-through decoration-red-500/50'
                                    : o.common_networks?.includes(n.name)
                                      ? 'bg-green-500/15 text-green-400'
                                      : 'bg-gray-700/30 text-gray-500'
                                }`}>
                                {n.name}{!n.active && <span className="no-underline ml-0.5">(暫停)</span>}
                              </span>
                            )) : <span className="text-gray-600 text-[9px]">-</span>}
                          </div>
                        </td>
                        <td className="py-1.5 text-left">
                          <div className="flex flex-wrap gap-0.5">
                            {withdrawNets.length > 0 ? withdrawNets.map(n => (
                              <span
                                key={n.name}
                                title={n.contract ? `合約: ${n.contract}` : '原生幣'}
                                className={`px-1 py-0.5 rounded text-[9px] cursor-default ${
                                  !n.active
                                    ? 'bg-red-500/10 text-red-400/70 line-through decoration-red-500/50'
                                    : o.common_networks?.includes(n.name)
                                      ? 'bg-green-500/15 text-green-400'
                                      : 'bg-gray-700/30 text-gray-500'
                                }`}>
                                {n.name}{!n.active && <span className="no-underline ml-0.5">(暫停)</span>}
                              </span>
                            )) : <span className="text-gray-600 text-[9px]">-</span>}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

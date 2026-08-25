import { useState, useMemo } from 'react';
import { usePolling } from '../hooks/usePolling';
import SymbolDetail from './SymbolDetail';

const EXCHANGE_LABELS = {
  binance: 'Binance', bybit: 'Bybit', okx: 'OKX', bitget: 'Bitget',
  mexc: 'MEXC', gateio: 'Gate.io', bingx: 'BingX', kucoinfutures: 'KuCoin',
  aster: 'Aster', hyperliquid: 'Hyperliquid', tradexyz: 'Trade.xyz',
  coinw: 'CoinW', ourbit: 'Ourbit', deepcoin: 'DeepCoin',
  lbank: 'LBank', lighter: 'Lighter', lighter_rh: 'Lighter RH',
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

const SORT_KEYS = {
  coin: (o) => o.base_coin,
  mark_price: (o) => o.mark_price || 0,
  high_rate: (o) => o.high_daily_rate_pct,
  low_rate: (o) => o.low_daily_rate_pct ?? -Infinity,
  daily_diff: (o) => Math.abs(o.daily_diff_pct),
  current_diff: (o) => {
    if (o.high_current_rate == null || o.low_current_rate == null) return -Infinity;
    return (o.high_current_rate - o.low_current_rate) * 100;
  },
  expected_profit: (o) => o.expected_profit || 0,
  annual_diff: (o) => Math.abs(o.annual_diff_pct),
  spread: (o) => o.spread_pct ?? Infinity,
};

const EXCHANGE_OPTIONS = [
  'binance', 'bybit', 'okx', 'bitget', 'mexc', 'gateio', 'bingx',
  'kucoinfutures', 'aster', 'hyperliquid', 'tradexyz', 'coinw', 'ourbit', 'deepcoin', 'lbank', 'lighter', 'lighter_rh',
];

export default function CoinwMeat({ searchTerm }) {
  const [sortKey, setSortKey] = useState('current_diff');
  const [sortDesc, setSortDesc] = useState(true);
  const [detailSymbol, setDetailSymbol] = useState(null);
  const [expandedSymbol, setExpandedSymbol] = useState(null);
  const [maxSpread, setMaxSpread] = useState('');
  const [mode, setMode] = useState('custom'); // 'all' | 'coinw' | 'same_interval' | 'custom'
  const [customExA, setCustomExA] = useState('binance');
  const [customExB, setCustomExB] = useState('coinw');
  const pollUrl = mode === 'custom'
    ? `/api/coinw-meat?min_daily_diff=0&mode=custom&ex_a=${customExA}&ex_b=${customExB}`
    : mode === 'coinw'
      ? `/api/coinw-meat?min_daily_diff=0&mode=custom&ex_a=binance&ex_b=coinw`
      : mode === 'deepcoin'
        ? `/api/coinw-meat?min_daily_diff=0&mode=custom&ex_a=binance&ex_b=deepcoin`
        : mode === 'aster'
          ? `/api/coinw-meat?min_daily_diff=0&mode=custom&ex_a=binance&ex_b=aster`
          : mode === 'lbank'
            ? `/api/coinw-meat?min_daily_diff=0&mode=custom&ex_a=binance&ex_b=lbank`
            : `/api/coinw-meat?min_daily_diff=0.5&mode=${mode}`;
  const { data, loading } = usePolling(pollUrl, 10000);
  const [spreadOverrides, setSpreadOverrides] = useState({}); // symbol -> realtime spread

  const opportunities = data?.opportunities || [];

  const filtered = useMemo(() => {
    // 合併即時價差覆蓋
    let list = opportunities.map(o =>
      spreadOverrides[o.symbol] !== undefined
        ? { ...o, spread_pct: spreadOverrides[o.symbol] }
        : o
    );

    if (searchTerm) {
      list = list.filter(o => o.base_coin.toUpperCase().includes(searchTerm.toUpperCase()));
    }

    // 價差篩選
    const spreadLimit = parseFloat(maxSpread);
    if (!isNaN(spreadLimit)) {
      list = list.filter(o => o.spread_pct != null && o.spread_pct <= spreadLimit);
    }

    const keyFn = SORT_KEYS[sortKey] || SORT_KEYS.daily_diff;
    list = [...list].sort((a, b) => {
      const va = keyFn(a), vb = keyFn(b);
      if (typeof va === 'string') return sortDesc ? vb.localeCompare(va) : va.localeCompare(vb);
      return sortDesc ? vb - va : va - vb;
    });
    return list;
  }, [opportunities, searchTerm, sortKey, sortDesc, maxSpread, spreadOverrides]);

  const handleSort = (key) => {
    if (sortKey === key) setSortDesc(!sortDesc);
    else { setSortKey(key); setSortDesc(true); }
  };

  const thClass = "px-3 py-1.5 text-[var(--text-secondary)] border-b border-[var(--border)] cursor-pointer hover:text-white select-none whitespace-nowrap";
  const sortIcon = (key) => sortKey === key ? (sortDesc ? ' ▼' : ' ▲') : '';

  return (
    <div>
      {/* 統計 + 篩選 */}
      <div className="px-4 py-2 border-b border-[var(--border)] text-xs text-gray-600 flex items-center gap-4 flex-wrap">
        <span>
          多交易所幣種共 {data?.total_symbols ?? '-'} 個，篩出 {filtered.length} 個
          {mode === 'custom'
            ? `（${EXCHANGE_LABELS[customExA] || customExA} vs ${EXCHANGE_LABELS[customExB] || customExB}，兩所皆有 24h 資料，已排除單次異常）`
            : mode === 'coinw'
              ? '（Binance vs CoinW，兩所皆有 24h 資料，已排除單次異常）'
              : mode === 'deepcoin'
                ? '（Binance vs DeepCoin，兩所皆有 24h 資料，已排除單次異常）'
                : mode === 'aster'
                  ? '（Binance vs Aster，兩所皆有 24h 資料，已排除單次異常）'
                  : mode === 'lbank'
                    ? '（Binance vs LBank，兩所皆有 24h 資料，已排除單次異常）'
                    : '（日資費差 ≥ 0.5%，已排除單次異常）'}
          {data?.bootstrapping && (
            <span className="ml-2 text-yellow-400 animate-pulse">補齊歷史資料中...</span>
          )}
        </span>
        <span className="border-l border-[var(--border)] pl-4 flex items-center gap-2">
          價差 ≤
          <input
            type="number"
            step="0.1"
            min="0"
            placeholder="不限"
            value={maxSpread}
            onChange={e => setMaxSpread(e.target.value)}
            className="w-16 px-1.5 py-0.5 bg-[var(--bg-secondary)] border border-[var(--border)] rounded text-xs text-white text-center focus:outline-none focus:border-blue-500"
          />
          <span className="text-gray-500">%</span>
        </span>
        <span className="border-l border-[var(--border)] pl-4 flex items-center gap-1">
          {[
            ['all', '全配對', 'text-gray-300 bg-gray-500/20'],
            ['coinw', 'CoinW', 'text-orange-400 bg-orange-500/20'],
            ['deepcoin', 'DeepCoin', 'text-cyan-400 bg-cyan-500/20'],
            ['aster', 'Aster', 'text-pink-400 bg-pink-500/20'],
            ['lbank', 'LBank', 'text-teal-400 bg-teal-500/20'],
            ['same_interval', '同週期', 'text-blue-400 bg-blue-500/20'],
            ['custom', '自選', 'text-purple-400 bg-purple-500/20'],
          ].map(([m, label, activeClass]) => (
            <button
              key={m}
              onClick={() => {
                setMode(m);
                if (m === 'deepcoin' || m === 'aster' || m === 'lbank') {
                  setSortKey('daily_diff');
                  setSortDesc(true);
                }
              }}
              className={`px-2 py-0.5 rounded text-xs transition-colors font-bold ${
                mode === m ? activeClass : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              {label}
            </button>
          ))}
        </span>
        {mode === 'custom' && (
          <span className="border-l border-[var(--border)] pl-4 flex items-center gap-2">
            <select
              value={customExA}
              onChange={e => setCustomExA(e.target.value)}
              className="px-1.5 py-0.5 bg-[var(--bg-secondary)] border border-[var(--border)] rounded text-xs text-white focus:outline-none focus:border-purple-500"
            >
              {EXCHANGE_OPTIONS.map(ex => (
                <option key={ex} value={ex} disabled={ex === customExB}>{EXCHANGE_LABELS[ex] || ex}</option>
              ))}
            </select>
            <span className="text-gray-500">vs</span>
            <select
              value={customExB}
              onChange={e => setCustomExB(e.target.value)}
              className="px-1.5 py-0.5 bg-[var(--bg-secondary)] border border-[var(--border)] rounded text-xs text-white focus:outline-none focus:border-purple-500"
            >
              {EXCHANGE_OPTIONS.map(ex => (
                <option key={ex} value={ex} disabled={ex === customExA}>{EXCHANGE_LABELS[ex] || ex}</option>
              ))}
            </select>
          </span>
        )}
      </div>

      {/* 說明 */}
      <div className="px-4 py-1.5 border-b border-[var(--border)]/50 text-[10px] text-gray-600 space-y-0.5">
        <div>每日資費差 = 過去24h 高費率所實際資費加總 - 低費率所實際資費加總。自動找出差距最大的交易所配對。</div>
        <div>價差 = (空方bid / 多方ask - 1)，即進場成本。已過濾單次資費異常。點幣種左側箭頭展開各交易所明細。</div>
      </div>

      {/* 表格 */}
      <div className="overflow-auto">
        <table className="w-full border-collapse text-xs">
          <thead className="sticky top-0 z-10">
            <tr className="bg-[var(--bg-secondary)]">
              <th className="px-2 py-1.5 text-center text-[var(--text-secondary)] border-b border-[var(--border)] w-8">#</th>
              <th className={`${thClass} text-left`} onClick={() => handleSort('coin')}>幣種{sortIcon('coin')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('mark_price')}>價格{sortIcon('mark_price')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('high_rate')}>高費率所 24h{sortIcon('high_rate')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('low_rate')}>低費率所 24h{sortIcon('low_rate')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('daily_diff')}>每日資費差{sortIcon('daily_diff')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('current_diff')}>即時費率差{sortIcon('current_diff')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('spread')}>價差{sortIcon('spread')}</th>
              <th className={`${thClass} text-right`}>預計利潤</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('expected_profit')}>預期收益{sortIcon('expected_profit')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('annual_diff')}>年化差{sortIcon('annual_diff')}</th>
              <th className={`${thClass} text-right`} title="CoinW 檔位1 最大持倉張數（= 每幣最大總倉位）">CW張數上限</th>
              <th className={`${thClass} text-right`} title="CoinW 檔位1 最大持倉 × 每張幣量 × 現價（USDT）">持倉上限(U)</th>
              <th className="px-3 py-1.5 text-center text-[var(--text-secondary)] border-b border-[var(--border)]">策略</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((o, idx) => {
              const isExpanded = expandedSymbol === o.symbol;
              const exDetail = o.exchanges || {};
              const exKeys = Object.keys(exDetail).sort((a, b) => (exDetail[b].daily_rate_pct) - (exDetail[a].daily_rate_pct));
              const highLabel = EXCHANGE_LABELS[o.high_exchange] || o.high_exchange;
              const lowLabel = EXCHANGE_LABELS[o.low_exchange] || o.low_exchange;

              return (
                <>
                  <tr key={o.symbol} className="border-b border-[var(--border)]/30 hover:bg-[var(--bg-hover)]">
                    <td className="px-2 py-1.5 text-center text-gray-600 font-mono">{idx + 1}</td>

                    <td className="px-3 py-1.5">
                      <div className="flex items-center gap-1">
                        {exKeys.length > 0 && (
                          <button
                            onClick={() => setExpandedSymbol(isExpanded ? null : o.symbol)}
                            className="text-gray-500 hover:text-white text-[10px] w-3"
                          >
                            {isExpanded ? '▼' : '▶'}
                          </button>
                        )}
                        <button
                          onClick={() => setDetailSymbol(o.symbol)}
                          className="text-white font-medium hover:text-blue-400 hover:underline transition-colors"
                        >
                          {o.base_coin}
                        </button>
                      </div>
                    </td>

                    <td className="px-3 py-1.5 text-right font-mono text-gray-300">{fmtPrice(o.mark_price)}</td>

                    <td className="px-3 py-1.5 text-right">
                      <div className="flex flex-col items-end">
                        <span className={`font-mono ${o.high_daily_rate_pct >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                          {fmtPct(o.high_daily_rate_pct, 3)}
                          <span className="text-gray-600 text-[10px] ml-1">({o.high_periods}期)</span>
                        </span>
                        <span className="text-gray-500 text-[10px]">
                          {highLabel}
                          {o.high_current_rate != null && (
                            <span className={`ml-1 ${o.high_current_rate >= 0 ? 'text-green-400/60' : 'text-red-400/60'}`}>
                              now {fmtPct(o.high_current_rate * 100)}
                            </span>
                          )}
                        </span>
                      </div>
                    </td>

                    <td className="px-3 py-1.5 text-right">
                      <div className="flex flex-col items-end">
                        <span className={`font-mono ${
                          o.low_daily_rate_pct == null ? 'text-gray-600'
                            : o.low_daily_rate_pct >= 0 ? 'text-green-400' : 'text-red-400'
                        }`}>
                          {fmtPct(o.low_daily_rate_pct, 3)}
                          <span className="text-gray-600 text-[10px] ml-1">({o.low_periods}期)</span>
                        </span>
                        <span className="text-gray-500 text-[10px]">
                          {lowLabel}
                          {o.low_current_rate != null && (
                            <span className={`ml-1 ${o.low_current_rate >= 0 ? 'text-green-400/60' : 'text-red-400/60'}`}>
                              now {fmtPct(o.low_current_rate * 100)}
                            </span>
                          )}
                        </span>
                      </div>
                    </td>

                    <td className="px-3 py-1.5 text-right font-mono font-bold text-orange-400">
                      {fmtPct(o.daily_diff_pct, 3)}
                    </td>

                    {(() => {
                      const cd = o.high_current_rate != null && o.low_current_rate != null
                        ? (o.high_current_rate - o.low_current_rate) * 100
                        : null;
                      return (
                        <td className={`px-3 py-1.5 text-right font-mono font-bold ${
                          cd == null ? 'text-gray-600' : cd > 0 ? 'text-green-400' : cd < 0 ? 'text-red-400' : 'text-gray-600'
                        }`}>
                          {cd != null ? fmtPct(cd, 3) : '-'}
                        </td>
                      );
                    })()}

                    <td className={`px-3 py-1.5 text-right font-mono ${
                      o.spread_pct == null ? 'text-gray-600'
                        : o.spread_pct < 0 ? 'text-green-400'
                        : 'text-red-400'
                    }`}>
                      {o.spread_pct != null ? fmtPct(o.spread_pct, 2) : '-'}
                    </td>

                    {(() => {
                      const profit = o.daily_diff_pct != null && o.spread_pct != null
                        ? o.daily_diff_pct - o.spread_pct
                        : null;
                      return (
                        <td className={`px-3 py-1.5 text-right font-mono font-bold ${
                          profit == null ? 'text-gray-600'
                            : profit > 0 ? 'text-green-400'
                            : 'text-red-400'
                        }`}>
                          {profit != null ? fmtPct(profit, 2) : '-'}
                        </td>
                      );
                    })()}

                    <td className={`px-3 py-1.5 text-right font-mono font-bold ${
                      (o.expected_profit ?? 0) > 0 ? 'text-green-400' : (o.expected_profit ?? 0) < 0 ? 'text-red-400' : 'text-gray-600'
                    }`}>
                      {o.expected_profit != null && o.expected_profit !== 0 ? `${o.expected_profit.toFixed(1)}U` : '-'}
                    </td>

                    <td className={`px-3 py-1.5 text-right font-mono font-bold ${
                      Math.abs(o.annual_diff_pct) >= 500 ? 'text-yellow-300' : 'text-orange-400'
                    }`}>
                      {fmtPct(o.annual_diff_pct, 1)}
                    </td>

                    <td className="px-3 py-1.5 text-right font-mono text-[11px] whitespace-nowrap">
                      {o.coinw_max_piece != null ? (
                        <span className="text-cyan-400">{o.coinw_max_piece.toLocaleString()} 張</span>
                      ) : (
                        <span className="text-gray-700">-</span>
                      )}
                    </td>

                    <td className="px-3 py-1.5 text-right font-mono text-[11px] whitespace-nowrap">
                      {o.coinw_max_piece != null && o.coinw_lot_size != null && o.mark_price ? (
                        <span className="text-white">
                          {(o.coinw_max_piece * o.coinw_lot_size * o.mark_price).toLocaleString('en-US', { maximumFractionDigits: 0 })}U
                        </span>
                      ) : (
                        <span className="text-gray-700">-</span>
                      )}
                    </td>

                    <td className="px-3 py-1.5 text-center whitespace-nowrap text-[11px]">
                      <span className="text-orange-400">{highLabel}空 + {lowLabel}多</span>
                    </td>
                  </tr>
                  {isExpanded && exKeys.map(ex => {
                    const d = exDetail[ex];
                    return (
                      <tr key={`${o.symbol}-${ex}`} className="bg-[var(--bg-secondary)]/50 border-b border-[var(--border)]/20">
                        <td></td>
                        <td className="px-3 py-1 text-gray-500 text-[11px] pl-8">
                          {EXCHANGE_LABELS[ex] || ex}
                        </td>
                        <td></td>
                        <td colSpan={2} className={`px-3 py-1 text-right font-mono text-[11px] ${d.daily_rate_pct >= 0 ? 'text-green-400/70' : 'text-red-400/70'}`}>
                          {fmtPct(d.daily_rate_pct, 3)}
                          <span className="text-gray-600 text-[10px] ml-1">({d.periods}期)</span>
                        </td>
                        <td className={`px-3 py-1 text-right font-mono text-[11px] font-bold ${d.diff_pct > 0 ? 'text-orange-400/70' : 'text-gray-600'}`}>
                          {d.diff_pct > 0 ? fmtPct(d.diff_pct, 3) : '-'}
                        </td>
                        <td></td>
                        <td></td>
                        <td></td>
                        <td></td>
                        <td className={`px-3 py-1 text-right font-mono text-[11px] ${d.diff_pct > 0 ? 'text-orange-400/70' : 'text-gray-600'}`}>
                          {d.diff_pct > 0 ? fmtPct(d.diff_pct * 365, 1) : '-'}
                        </td>
                        <td></td>
                        <td></td>
                        <td></td>
                      </tr>
                    );
                  })}
                </>
              );
            })}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={14} className="px-3 py-10 text-center text-gray-500">
                  {loading ? '載入中...' : '無符合條件的碎肉流機會'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {detailSymbol && (
        <SymbolDetail
          symbol={detailSymbol}
          records={[]}
          onClose={() => setDetailSymbol(null)}
          onRealtimeUpdate={(records) => {
            // 用即時 bid/ask 更新價差
            const opp = opportunities.find(o => o.symbol === detailSymbol);
            if (!opp || !records?.length) return;
            const highRec = records.find(r => r.exchange === opp.high_exchange);
            const lowRec = records.find(r => r.exchange === opp.low_exchange);
            if (lowRec?.ask_price && highRec?.bid_price && highRec.bid_price > 0) {
              const spread = ((lowRec.ask_price / highRec.bid_price - 1) * 100).toFixed(4);
              setSpreadOverrides(prev => ({ ...prev, [detailSymbol]: parseFloat(spread) }));
            }
          }}
        />
      )}
    </div>
  );
}

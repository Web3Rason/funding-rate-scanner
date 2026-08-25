import { useMemo, useState } from 'react';
import SymbolDetail from './SymbolDetail';

const EXCHANGES = [
  { id: 'binance', label: 'Binance' },
  { id: 'bybit', label: 'Bybit' },
  { id: 'okx', label: 'OKX' },
  { id: 'bitget', label: 'Bitget' },
  { id: 'mexc', label: 'MEXC' },
  { id: 'gateio', label: 'Gate.io' },
  { id: 'bingx', label: 'BingX' },
  { id: 'kucoinfutures', label: 'KuCoin' },
  { id: 'aster', label: 'Aster' },
  { id: 'coinw', label: 'CoinW' },
  { id: 'hyperliquid', label: 'Hyperliquid' },
  { id: 'tradexyz', label: 'Trade.xyz' },
  { id: 'ourbit', label: 'Ourbit' },
  { id: 'deepcoin', label: 'DeepCoin' },
  { id: 'lbank', label: 'LBank' },
  { id: 'kraken', label: 'Kraken' },
  { id: 'deribit', label: 'Deribit' },
  { id: 'lighter', label: 'Lighter' },
  { id: 'lighter_rh', label: 'Lighter RH' },
];

const EXCHANGE_LABELS = Object.fromEntries(EXCHANGES.map(e => [e.id, e.label]));

function shortSymbol(symbol) {
  return symbol.split('/')[0];
}

function formatInterval(h) {
  if (!h || h === 8) return '8h';
  if (h >= 1) return `${h}h`;
  return `${Math.round(h * 60)}m`;
}

function profitColor(profit) {
  if (profit >= 1.0) return 'text-yellow-400';
  if (profit >= 0.5) return 'text-green-400';
  return 'text-green-500';
}

// 正規化費率：統一到配對中較長的結算週期
function normRate(rate, myInterval, normInterval) {
  return rate * ((normInterval || 8) / (myInterval || 8));
}

function spreadColor(spread) {
  if (spread >= 0.05) return 'text-green-400';
  if (spread >= 0) return 'text-gray-400';
  if (spread <= -0.1) return 'text-red-400';
  return 'text-orange-400';
}

function formatNotional(val) {
  if (val == null) return '-';
  if (val >= 1e6) return `${(val / 1e6).toFixed(1)}M`;
  if (val >= 1e3) return `${(val / 1e3).toFixed(0)}K`;
  return `${val.toFixed(0)}`;
}

function notionalColor(val) {
  if (val == null) return 'text-gray-600';
  if (val <= 500) return 'text-red-400 font-bold';
  if (val <= 5000) return 'text-orange-400';
  if (val <= 50000) return 'text-yellow-400';
  return 'text-green-400';
}

export default function ArbitragePanel({ arbitrage, records, searchTerm }) {
  const [detailSymbol, setDetailSymbol] = useState(null);
  const [detailArb, setDetailArb] = useState(null);

  // 套利專用篩選狀態：0=預設, 1=必須包含(綠), 2=隱藏(紅)
  // 預設隱藏 MEXC、KuCoin、CoinW、DeepCoin（可由使用者在當前 session 重新啟用）
  const [exStates, setExStates] = useState({ mexc: 2, kucoinfutures: 2, coinw: 2, deepcoin: 2 }); // { [id]: 0|1|2 }
  const [minRate, setMinRate] = useState('');
  const [maxRate, setMaxRate] = useState('');
  const [minSpread, setMinSpread] = useState('');
  const [maxSpread, setMaxSpread] = useState('');
  const [excludeCrossInterval, setExcludeCrossInterval] = useState(false);

  // 排序狀態：預設按預計利潤降序
  const [sortField, setSortField] = useState('estimated_profit');
  const [sortDir, setSortDir] = useState('desc');

  const handleSort = (field) => {
    if (sortField === field) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    } else {
      setSortField(field);
      setSortDir('desc');
    }
  };

  const cycleExchange = (id) => {
    if (id === '__reset__') { setExStates({}); return; }
    setExStates(prev => {
      const cur = prev[id] || 0;
      const next = cur === 0 ? 2 : cur === 2 ? 1 : 0;
      if (next === 0) { const { [id]: _, ...rest } = prev; return rest; }
      return { ...prev, [id]: next };
    });
  };

  const filtered = useMemo(() => {
    if (!arbitrage) return [];
    let list = arbitrage;

    // 交易所篩選
    const mustInclude = Object.entries(exStates).filter(([, s]) => s === 1).map(([id]) => id);
    const hiddenSet = new Set(Object.entries(exStates).filter(([, s]) => s === 2).map(([id]) => id));
    if (mustInclude.length > 0) {
      list = list.filter(a => mustInclude.some(ex => a.long_exchange === ex || a.short_exchange === ex));
    }
    if (hiddenSet.size > 0) {
      list = list.filter(a => !hiddenSet.has(a.long_exchange) && !hiddenSet.has(a.short_exchange));
    }

    // 幣種搜尋
    if (searchTerm) {
      const term = searchTerm.toUpperCase();
      list = list.filter(a => a.symbol.toUpperCase().includes(term));
    }

    // 資金費率範圍（百分比，用正規化費率篩選）
    const getNormRate = (a, side) => {
      const ni = a.norm_interval_h || Math.max(a.long_interval_h || 8, a.short_interval_h || 8);
      if (side === 'long') return normRate(a.long_rate, a.long_interval_h, ni);
      return normRate(a.short_rate, a.short_interval_h, ni);
    };
    const minVal = minRate !== '' ? Number(minRate) / 100 : null;
    const maxVal = maxRate !== '' ? Number(maxRate) / 100 : null;
    if (minVal !== null && !isNaN(minVal)) {
      list = list.filter(a => getNormRate(a, 'long') >= minVal || getNormRate(a, 'short') >= minVal);
    }
    if (maxVal !== null && !isNaN(maxVal)) {
      list = list.filter(a => getNormRate(a, 'long') <= maxVal || getNormRate(a, 'short') <= maxVal);
    }

    // 價差範圍篩選
    const minSpreadVal = minSpread !== '' ? Number(minSpread) : null;
    const maxSpreadVal = maxSpread !== '' ? Number(maxSpread) : null;
    if (minSpreadVal !== null && !isNaN(minSpreadVal)) {
      list = list.filter(a => (a.spread_pct ?? 0) >= minSpreadVal);
    }
    if (maxSpreadVal !== null && !isNaN(maxSpreadVal)) {
      list = list.filter(a => (a.spread_pct ?? 0) <= maxSpreadVal);
    }

    // 排除跨週期
    if (excludeCrossInterval) {
      list = list.filter(a => !a.cross_interval);
    }

    // 排序
    list = [...list].sort((a, b) => {
      let va, vb;
      switch (sortField) {
        case 'symbol':
          va = a.symbol; vb = b.symbol;
          return sortDir === 'desc' ? vb.localeCompare(va) : va.localeCompare(vb);
        case 'long_rate': va = getNormRate(a, 'long'); vb = getNormRate(b, 'long'); break;
        case 'short_rate': va = getNormRate(a, 'short'); vb = getNormRate(b, 'short'); break;
        case 'rate_diff': va = a.rate_diff; vb = b.rate_diff; break;
        case 'spread_pct': va = a.spread_pct ?? 0; vb = b.spread_pct ?? 0; break;
        case 'estimated_profit': va = a.estimated_profit ?? 0; vb = b.estimated_profit ?? 0; break;
        case 'expected_profit': va = a.expected_profit ?? 0; vb = b.expected_profit ?? 0; break;
        case 'max_limit': {
          const la = Math.min(a.long_max_notional ?? Infinity, a.short_max_notional ?? Infinity);
          const lb = Math.min(b.long_max_notional ?? Infinity, b.short_max_notional ?? Infinity);
          va = la === Infinity ? -1 : la;
          vb = lb === Infinity ? -1 : lb;
          break;
        }
        default: return 0;
      }
      return sortDir === 'desc' ? vb - va : va - vb;
    });

    return list;
  }, [arbitrage, searchTerm, exStates, minRate, maxRate, minSpread, maxSpread, excludeCrossInterval, sortField, sortDir]);

  if (!arbitrage || arbitrage.length === 0) {
    return (
      <div className="flex items-center justify-center h-64 text-[var(--text-secondary)]">
        暫無套利機會（閾值: 預計利潤 &gt; 0.3%）
      </div>
    );
  }

  return (
    <>
      {/* 套利專用篩選區 */}
      <div className="px-3 py-2.5 border-b border-[var(--border)] bg-[var(--bg-secondary)] space-y-2">
        {/* 第一行：交易所篩選 */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-[var(--text-secondary)] font-medium shrink-0">交易所</span>
          {EXCHANGES.map(ex => {
            const state = exStates[ex.id] || 0;
            const cls = state === 1
              ? 'bg-green-500/20 text-green-400 border-green-500/40'
              : state === 2
              ? 'bg-red-500/20 text-red-400 border-red-500/40 line-through'
              : 'bg-gray-800/50 text-gray-500 border-gray-700/50 hover:text-gray-400';
            const prefix = state === 1 ? '✓ ' : state === 2 ? '' : '';
            return (
              <button
                key={ex.id}
                onClick={() => cycleExchange(ex.id)}
                title={state === 0 ? '點一下：排除' : state === 2 ? '點一下：必須包含' : '點一下：重置'}
                className={`px-2 py-0.5 rounded text-xs font-medium transition-colors border ${cls}`}
              >
                {prefix}{ex.label}
              </button>
            );
          })}
          {Object.keys(exStates).length > 0 && (
            <button
              onClick={() => cycleExchange('__reset__')}
              className="px-2 py-0.5 rounded text-xs text-gray-400 hover:text-white border border-gray-700/50"
            >
              清除
            </button>
          )}
        </div>

        {/* 第二行：資金費率範圍 + 價差範圍 + 計數 */}
        <div className="flex items-center gap-3 flex-wrap text-xs">
          <div className="flex items-center gap-1.5">
            <span className="text-[var(--text-secondary)] font-medium">資費</span>
            <input
              type="number"
              step="0.01"
              value={minRate}
              onChange={e => setMinRate(e.target.value)}
              className="w-20 px-1.5 py-1 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-center focus:outline-none focus:border-blue-500"
              placeholder="min %"
            />
            <span className="text-gray-500">~</span>
            <input
              type="number"
              step="0.01"
              value={maxRate}
              onChange={e => setMaxRate(e.target.value)}
              className="w-20 px-1.5 py-1 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-center focus:outline-none focus:border-blue-500"
              placeholder="max %"
            />
          </div>

          <span className="text-gray-700">|</span>

          <div className="flex items-center gap-1.5">
            <span className="text-[var(--text-secondary)] font-medium">價差</span>
            <input
              type="number"
              step="0.01"
              value={minSpread}
              onChange={e => setMinSpread(e.target.value)}
              className="w-20 px-1.5 py-1 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-center focus:outline-none focus:border-blue-500"
              placeholder="min %"
            />
            <span className="text-gray-500">~</span>
            <input
              type="number"
              step="0.01"
              value={maxSpread}
              onChange={e => setMaxSpread(e.target.value)}
              className="w-20 px-1.5 py-1 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-center focus:outline-none focus:border-blue-500"
              placeholder="max %"
            />
          </div>

          <span className="text-gray-700">|</span>

          <button
            onClick={() => setExcludeCrossInterval(v => !v)}
            className={`px-2 py-0.5 rounded border transition-colors ${
              excludeCrossInterval
                ? 'bg-orange-500/20 text-orange-400 border-orange-500/40'
                : 'text-gray-500 border-gray-700/50 hover:text-gray-400'
            }`}
            title="跨週期：兩交易所結算週期不同（如 1h vs 8h），費率差可能只在當期有效"
          >
            {excludeCrossInterval ? '✓ 排除跨週期' : '排除跨週期'}
          </button>

          <div className="ml-auto text-[var(--text-secondary)]">
            {filtered.length} / {arbitrage.length} 個機會
          </div>
        </div>
      </div>

      {/* 表格 */}
      <div className="overflow-auto max-h-[calc(100vh-280px)]">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-[var(--bg-secondary)]">
              {[
                { field: 'symbol', label: '幣種', align: 'text-left' },
                { field: null, label: 'A (多)', align: 'text-left' },
                { field: 'long_rate', label: 'A 費率', align: 'text-right' },
                { field: null, label: 'B (空)', align: 'text-left' },
                { field: 'short_rate', label: 'B 費率', align: 'text-right' },
                { field: 'rate_diff', label: '費率差', align: 'text-right' },
                { field: 'spread_pct', label: '價差', align: 'text-right' },
                { field: 'estimated_profit', label: '預計利潤', align: 'text-right' },
                { field: 'expected_profit', label: '預期收益', align: 'text-right' },
                { field: 'max_limit', label: '倉位限制', align: 'text-right' },
              ].map((col, idx) => (
                <th
                  key={idx}
                  className={`px-3 py-2 ${col.align} font-medium border-b border-[var(--border)] ${
                    col.field
                      ? 'cursor-pointer hover:text-white select-none ' + (sortField === col.field ? 'text-blue-400' : 'text-[var(--text-secondary)]')
                      : 'text-[var(--text-secondary)]'
                  }`}
                  onClick={col.field ? () => handleSort(col.field) : undefined}
                >
                  {col.label}
                  {col.field && sortField === col.field && (sortDir === 'desc' ? ' ▾' : ' ▴')}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((a, i) => (
              <tr key={`${a.symbol}-${i}`} className="hover:bg-[var(--bg-hover)] border-b border-[var(--border)]/30">
                <td
                  className="px-3 py-2 font-mono font-medium text-blue-400 cursor-pointer hover:underline"
                  onClick={() => { setDetailSymbol(a.symbol); setDetailArb(a); }}
                >
                  {shortSymbol(a.symbol)}
                  {a.cross_interval && (
                    <span className="ml-1.5 text-[9px] font-bold text-orange-400 border border-orange-400/50 rounded px-0.5" title="跨週期配對：本小時有效費率差">跨週期</span>
                  )}
                </td>
                <td className="px-3 py-2 text-green-400">
                  {EXCHANGE_LABELS[a.long_exchange] || a.long_exchange}
                  {a.long_is_delisting && <span className="ml-1 text-[9px] text-red-400 font-bold" title="此交易所合約即將下架">⚠下架</span>}
                  {a.cross_interval && (
                    <span className="ml-1 text-[9px]" title="本小時是否結算">
                      {a.long_settling_soon ? <span className="text-green-400">●</span> : <span className="text-gray-500">○</span>}
                    </span>
                  )}
                  {a.long_symbol && shortSymbol(a.long_symbol) !== shortSymbol(a.symbol) && (
                    <span className="ml-1 text-[10px] text-yellow-500">({shortSymbol(a.long_symbol)})</span>
                  )}
                  <span className="ml-1.5 text-[10px] text-gray-500">{formatInterval(a.long_interval_h)}</span>
                </td>
                <td className="px-3 py-2 text-right font-mono text-green-400" title={`原始每期(${formatInterval(a.long_interval_h)}): ${(a.long_rate * 100).toFixed(4)}%`}>
                  {(normRate(a.long_rate, a.long_interval_h, a.norm_interval_h || Math.max(a.long_interval_h || 8, a.short_interval_h || 8)) * 100).toFixed(4)}%
                </td>
                <td className="px-3 py-2 text-red-400">
                  {EXCHANGE_LABELS[a.short_exchange] || a.short_exchange}
                  {a.short_is_delisting && <span className="ml-1 text-[9px] text-red-400 font-bold" title="此交易所合約即將下架">⚠下架</span>}
                  {a.cross_interval && (
                    <span className="ml-1 text-[9px]" title="本小時是否結算">
                      {a.short_settling_soon ? <span className="text-green-400">●</span> : <span className="text-gray-500">○</span>}
                    </span>
                  )}
                  {a.short_symbol && shortSymbol(a.short_symbol) !== shortSymbol(a.symbol) && (
                    <span className="ml-1 text-[10px] text-yellow-500">({shortSymbol(a.short_symbol)})</span>
                  )}
                  <span className="ml-1.5 text-[10px] text-gray-500">{formatInterval(a.short_interval_h)}</span>
                </td>
                <td className="px-3 py-2 text-right font-mono text-red-400" title={`原始每期(${formatInterval(a.short_interval_h)}): ${(a.short_rate * 100).toFixed(4)}%`}>
                  {(normRate(a.short_rate, a.short_interval_h, a.norm_interval_h || Math.max(a.long_interval_h || 8, a.short_interval_h || 8)) * 100).toFixed(4)}%
                </td>
                <td className="px-3 py-2 text-right font-mono text-white">{(a.rate_diff * 100).toFixed(4)}%</td>
                <td className={`px-3 py-2 text-right font-mono ${spreadColor(a.spread_pct)}`}>
                  {a.spread_pct.toFixed(4)}%
                </td>
                <td className={`px-3 py-2 text-right font-mono font-bold ${profitColor(a.estimated_profit)}`}>
                  {a.estimated_profit.toFixed(4)}%
                </td>
                <td className={`px-3 py-2 text-right font-mono font-bold ${profitColor(a.estimated_profit)}`}>
                  {a.expected_profit != null && a.expected_profit !== 0 ? `${a.expected_profit.toFixed(1)}U` : '-'}
                </td>
                {(() => {
                  const longN = a.long_max_notional;
                  const shortN = a.short_max_notional;
                  const minN = Math.min(longN ?? Infinity, shortN ?? Infinity);
                  const effective = minN === Infinity ? null : minN;
                  return (
                    <td
                      className={`px-3 py-2 text-right font-mono text-xs ${notionalColor(effective)}`}
                      title={`做多(${EXCHANGE_LABELS[a.long_exchange] || a.long_exchange}): ${longN != null ? formatNotional(longN) + 'U' : '未知'}\n做空(${EXCHANGE_LABELS[a.short_exchange] || a.short_exchange}): ${shortN != null ? formatNotional(shortN) + 'U' : '未知'}`}
                    >
                      {effective != null ? formatNotional(effective) + 'U' : '-'}
                    </td>
                  );
                })()}
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <div className="text-center text-[var(--text-secondary)] py-8 text-sm">
            無符合條件的套利機會
          </div>
        )}
      </div>

      {detailSymbol && (
        <SymbolDetail
          symbol={detailSymbol}
          records={records}
          onClose={() => { setDetailSymbol(null); setDetailArb(null); }}
          defaultExchanges={detailArb ? { left: detailArb.long_exchange, right: detailArb.short_exchange } : undefined}
        />
      )}
    </>
  );
}

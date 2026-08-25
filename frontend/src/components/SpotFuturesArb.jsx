import { useState, useMemo } from 'react';
import { usePolling } from '../hooks/usePolling';
import SymbolDetail from './SymbolDetail';

const EX_META = {
  bybit: { label: 'Bybit', color: 'text-yellow-400' },
  bitget: { label: 'Bitget', color: 'text-sky-400' },
  okx: { label: 'OKX', color: 'text-blue-400' },
  binance: { label: 'Binance', color: 'text-amber-400' },
  gateio: { label: 'Gate', color: 'text-red-400' },
  mexc: { label: 'MEXC', color: 'text-emerald-400' },
  bingx: { label: 'BingX', color: 'text-cyan-400' },
};
const exLabel = (ex) => EX_META[ex]?.label || ex;
const exColor = (ex) => EX_META[ex]?.color || 'text-gray-300';

function fmtPrice(p) {
  if (p == null) return '-';
  if (p >= 1000) return p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (p >= 1) return p.toFixed(4);
  return p.toPrecision(4);
}

function fmtAmount(v) {
  if (v == null || v === 0) return '-';
  if (v >= 1e6) return v.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  if (v >= 1000) return v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (v >= 1) return v.toFixed(3);
  return v.toPrecision(4);
}

function fmtPct(v, digits = 4) {
  if (v == null) return '-';
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
}

function fmtFundingTime(isoStr) {
  if (!isoStr) return '-';
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return '-';
  const utc8 = new Date(d.getTime() + 8 * 3600000);
  const mm = String(utc8.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(utc8.getUTCDate()).padStart(2, '0');
  const hh = String(utc8.getUTCHours()).padStart(2, '0');
  const mi = String(utc8.getUTCMinutes()).padStart(2, '0');
  return `${mm}-${dd} ${hh}:${mi}`;
}

const SORT_KEYS = {
  exchange: (o) => o.exchange,
  contract: (o) => o.contract_name,
  mark_price: (o) => o.mark_price || 0,
  entry_spread: (o) => o.entry_spread_pct ?? -Infinity,
  exit_spread: (o) => o.exit_spread_pct ?? -Infinity,
  funding_rate: (o) => Math.abs(o.funding_rate),
  annual: (o) => o.annual_income_pct,
  borrowable: (o) => o.margin_borrowable ? 1 : 0,
  max_borrow: (o) => o.margin_max_loan != null ? (o.margin_max_loan + (o.margin_borrowed || 0)) : -Infinity,
  max_loan: (o) => {
    if (o.margin_max_loan == null || !o.mark_price) return -Infinity;
    return (o.margin_max_loan + (o.margin_borrowed || 0)) * o.mark_price;
  },
  borrow_annual: (o) => o.margin_hourly_rate != null ? o.margin_hourly_rate * 24 * 365 * 100 : -Infinity,
  daily_interest: (o) => {
    if (o.margin_hourly_rate == null || o.margin_max_loan == null) return -Infinity;
    const total = o.margin_max_loan + (o.margin_borrowed || 0);
    return o.margin_hourly_rate * 24 * total;
  },
  net_annual: (o) => {
    const isBuy = o.direction === 'buy_spot_short_futures';
    if (isBuy) return o.annual_income_pct; // 多現貨不需借貸，總結=資費年化
    if (o.margin_hourly_rate == null) return -Infinity;
    const ba = o.margin_hourly_rate * 24 * 365 * 100;
    return o.annual_income_pct - ba;
  },
  daily_income: (o) => {
    const isBuy = o.direction === 'buy_spot_short_futures';
    if (isBuy) return !o.mark_price ? -Infinity : o.annual_income_pct / 365 / 100 * o.mark_price;
    if (o.margin_hourly_rate == null || o.margin_max_loan == null || !o.mark_price) return -Infinity;
    const total = o.margin_max_loan + (o.margin_borrowed || 0);
    const ba = o.margin_hourly_rate * 24 * 365 * 100;
    const net = o.annual_income_pct - ba;
    return net / 365 / 100 * total * o.mark_price;
  },
};

export default function SpotFuturesArb({ searchTerm }) {
  const [sortKey, setSortKey] = useState('net_annual');
  const [sortDesc, setSortDesc] = useState(true);
  const [detailSymbol, setDetailSymbol] = useState(null);
  const [dirFilter, setDirFilter] = useState('all'); // 'all' | 'buy_spot' | 'short_spot'
  const [exFilter, setExFilter] = useState('all'); // 'all' | exchange id
  const [onlyProfitable, setOnlyProfitable] = useState(true);
  const { data, loading } = usePolling('/api/spot-futures?min_rate=0', 10000);

  const opportunities = data?.opportunities || [];
  const totals = data?.totals || {};
  const exchanges = Object.keys(totals);

  const filtered = useMemo(() => {
    let list = searchTerm
      ? opportunities.filter(o => o.base_coin.toUpperCase().includes(searchTerm.toUpperCase()))
      : opportunities;

    // 交易所篩選
    if (exFilter !== 'all') list = list.filter(o => o.exchange === exFilter);

    // 方向篩選
    if (dirFilter === 'buy_spot') {
      list = list.filter(o => o.direction === 'buy_spot_short_futures');
    } else if (dirFilter === 'short_spot') {
      list = list.filter(o => o.direction === 'short_spot_long_futures');
    }

    // 只顯示賺錢：有現貨 + 年化收益 > 成本
    if (onlyProfitable) {
      list = list.filter(o => {
        if (!o.has_spot) return false;
        if (o.entry_spread_pct != null && o.entry_spread_pct > 1) return false;
        const isBuy = o.direction === 'buy_spot_short_futures';
        if (isBuy) {
          return o.annual_income_pct > 0;
        } else {
          if (o.margin_hourly_rate != null) {
            const borrowAnnual = o.margin_hourly_rate * 24 * 365 * 100;
            return (o.annual_income_pct - borrowAnnual) > 0;
          }
          return o.annual_income_pct > 0;
        }
      });
    }

    const keyFn = SORT_KEYS[sortKey] || SORT_KEYS.annual;
    list = [...list].sort((a, b) => {
      const va = keyFn(a), vb = keyFn(b);
      if (typeof va === 'string') return sortDesc ? vb.localeCompare(va) : va.localeCompare(vb);
      return sortDesc ? vb - va : va - vb;
    });
    return list;
  }, [opportunities, searchTerm, sortKey, sortDesc, dirFilter, exFilter, onlyProfitable]);

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDesc(!sortDesc);
    } else {
      setSortKey(key);
      setSortDesc(true);
    }
  };

  const thClass = "px-2 py-1.5 text-[var(--text-secondary)] border-b border-[var(--border)] cursor-pointer hover:text-white select-none whitespace-nowrap";
  const sortIcon = (key) => sortKey === key ? (sortDesc ? ' ▼' : ' ▲') : '';

  return (
    <div>
      {/* 統計列 + 篩選 */}
      <div className="px-4 py-2 border-b border-[var(--border)] text-xs text-gray-600 flex items-center gap-4 flex-wrap">
        <span>顯示 {filtered.length} 個</span>
        <span className="flex items-center gap-1.5">
          交易所：
          <button
            onClick={() => setExFilter('all')}
            className={`px-2 py-0.5 rounded text-xs transition-colors ${exFilter === 'all' ? 'text-white bg-white/10 font-bold' : 'text-gray-500 hover:text-gray-300'}`}
          >全部</button>
          {exchanges.map(ex => (
            <button
              key={ex}
              onClick={() => setExFilter(ex)}
              className={`px-2 py-0.5 rounded text-xs transition-colors ${exFilter === ex ? `${exColor(ex)} bg-white/10 font-bold` : 'text-gray-500 hover:text-gray-300'}`}
            >{exLabel(ex)} {totals[ex] ?? '-'}</button>
          ))}
        </span>
        <span className="border-l border-[var(--border)] pl-4 flex items-center gap-2">
          方向：
          {[
            ['buy_spot', '多現貨/空合約', 'text-green-400'],
            ['short_spot', '空現貨/多合約', 'text-red-400'],
            ['all', '全部', 'text-gray-300'],
          ].map(([val, label, color]) => (
            <button
              key={val}
              onClick={() => setDirFilter(val)}
              className={`px-2 py-0.5 rounded text-xs transition-colors ${
                dirFilter === val ? `${color} bg-white/10 font-bold` : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              {label}
            </button>
          ))}
        </span>
        <label className="border-l border-[var(--border)] pl-4 flex items-center gap-1 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={onlyProfitable}
            onChange={(e) => setOnlyProfitable(e.target.checked)}
            className="accent-green-500"
          />
          <span className={onlyProfitable ? 'text-green-400' : 'text-gray-500'}>只顯示賺錢</span>
        </label>
      </div>

      {/* 說明 */}
      <div className="px-4 py-1.5 border-b border-[var(--border)]/50 text-[10px] text-gray-600 space-y-0.5">
        <div>註：多現貨/空合約：建倉價差%=現貨ask/合約bid-1；關倉價差%=合約ask/現貨bid-1。</div>
        <div>註：借貸額度=實際可借（Binance=min(資金池剩餘,借貸上限)，不受抵押品限制；OKX/Gate/Bybit/Bitget=帳戶即時可借+已借，依你抵押品）；借貸年化=每小時利率×24×365。</div>
      </div>

      {/* 主表格 */}
      <div className="overflow-auto">
        <table className="w-full border-collapse text-xs">
          <thead className="sticky top-0 z-10">
            <tr className="bg-[var(--bg-secondary)]">
              <th className="px-2 py-1.5 text-center text-[var(--text-secondary)] border-b border-[var(--border)] w-8">#</th>
              <th className={`${thClass} text-left`} onClick={() => handleSort('exchange')}>交易所{sortIcon('exchange')}</th>
              <th className={`${thClass} text-left`} onClick={() => handleSort('contract')}>合約{sortIcon('contract')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('mark_price')}>合約價{sortIcon('mark_price')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('entry_spread')}>建倉價差%{sortIcon('entry_spread')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('exit_spread')}>關倉價差%{sortIcon('exit_spread')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('funding_rate')}>資費%/期{sortIcon('funding_rate')}</th>
              <th className="px-2 py-1.5 text-center text-[var(--text-secondary)] border-b border-[var(--border)]">週期</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('annual')}>年化收益{sortIcon('annual')}</th>
              <th className="px-2 py-1.5 text-center text-[var(--text-secondary)] border-b border-[var(--border)] whitespace-nowrap">下次資費</th>
              <th className={`${thClass} text-center`} onClick={() => handleSort('borrowable')}>現貨可空{sortIcon('borrowable')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('max_borrow')}>借貸額度{sortIcon('max_borrow')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('max_loan')}>借貸額度(U){sortIcon('max_loan')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('borrow_annual')}>借貸年化{sortIcon('borrow_annual')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('daily_interest')}>日利息{sortIcon('daily_interest')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('net_annual')}>總結年化{sortIcon('net_annual')}</th>
              <th className={`${thClass} text-right`} onClick={() => handleSort('daily_income')}>日收益(U){sortIcon('daily_income')}</th>
              <th className="px-2 py-1.5 text-center text-[var(--text-secondary)] border-b border-[var(--border)]">說明</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((o, idx) => {
              const isBuy = o.direction === 'buy_spot_short_futures';
              const borrowAnnualPct = !isBuy && o.margin_hourly_rate != null ? o.margin_hourly_rate * 24 * 365 * 100 : null;
              const netAnnualPct = isBuy ? o.annual_income_pct : (borrowAnnualPct != null ? o.annual_income_pct - borrowAnnualPct : null);
              const needsBorrow = !isBuy;
              const notViable = needsBorrow && o.margin_max_loan != null && o.margin_max_loan < 1;

              return (
                <tr
                  key={`${o.exchange}-${o.symbol}`}
                  className={`border-b border-[var(--border)]/30 hover:bg-[var(--bg-hover)] ${!o.has_spot || notViable ? 'opacity-40' : ''}`}
                >
                  <td className="px-2 py-1.5 text-center text-gray-600 font-mono">{idx + 1}</td>

                  {/* 交易所 */}
                  <td className={`px-2 py-1.5 font-medium ${exColor(o.exchange)}`}>{exLabel(o.exchange)}</td>

                  {/* 合約 */}
                  <td className="px-2 py-1.5">
                    <button
                      onClick={() => setDetailSymbol(o.symbol)}
                      className="text-white font-medium hover:text-blue-400 hover:underline transition-colors"
                    >
                      {o.contract_name || `${o.base_coin}USDT`}
                    </button>
                  </td>

                  <td className="px-2 py-1.5 text-right font-mono text-gray-300">{fmtPrice(o.mark_price)}</td>

                  <td className={`px-2 py-1.5 text-right font-mono ${
                    o.entry_spread_pct == null ? 'text-gray-600'
                      : o.entry_spread_pct >= 0 ? 'text-green-400' : 'text-red-400'
                  }`}>
                    {o.entry_spread_pct != null ? fmtPct(o.entry_spread_pct, 3) : '-'}
                  </td>

                  <td className={`px-2 py-1.5 text-right font-mono ${
                    o.exit_spread_pct == null ? 'text-gray-600'
                      : o.exit_spread_pct >= 0 ? 'text-green-400' : 'text-red-400'
                  }`}>
                    {o.exit_spread_pct != null ? fmtPct(o.exit_spread_pct, 3) : '-'}
                  </td>

                  <td className={`px-2 py-1.5 text-right font-mono font-bold ${o.funding_rate > 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {fmtPct(o.funding_rate * 100, 4)}
                  </td>

                  <td className="px-2 py-1.5 text-center text-gray-400">
                    {(o.funding_interval_h ?? 8).toFixed(1)}h
                  </td>

                  <td className={`px-2 py-1.5 text-right font-mono font-bold ${o.annual_income_pct >= 100 ? 'text-yellow-300' : 'text-yellow-400'}`}>
                    {fmtPct(o.annual_income_pct, 2)}
                  </td>

                  <td className="px-2 py-1.5 text-center text-gray-400 whitespace-nowrap">
                    {fmtFundingTime(o.funding_time)}
                  </td>

                  <td className="px-2 py-1.5 text-center">
                    {isBuy ? (
                      <span className="text-gray-600">-</span>
                    ) : o.margin_borrowable ? (
                      <span className="text-green-400">可</span>
                    ) : o.has_spot ? (
                      <span className="text-red-400">否</span>
                    ) : (
                      <span className="text-gray-600">-</span>
                    )}
                  </td>

                  {/* 借貸額度：Binance=min(池剩餘,上限)，其餘=帳戶即時可借+已借 */}
                  <td className="px-2 py-1.5 text-right font-mono text-cyan-400" title={!isBuy && o.margin_max_loan != null ? (o.margin_pool_available != null ? `min(資金池剩餘 ${fmtAmount(o.margin_pool_available)}, 借貸上限 ${fmtAmount(o.margin_max_borrowing_amount)})` : `帳戶即時可借: ${fmtAmount(o.margin_max_loan)} / 已借: ${fmtAmount(o.margin_borrowed || 0)}`) : ''}>
                    {!isBuy && o.margin_max_loan != null ? fmtAmount(o.margin_max_loan + (o.margin_borrowed || 0)) : '-'}
                  </td>

                  <td className="px-2 py-1.5 text-right font-mono text-cyan-400" title={!isBuy && o.margin_max_loan != null && o.mark_price ? `可借額度 ≈ ${fmtAmount((o.margin_max_loan + (o.margin_borrowed || 0)) * o.mark_price)} U` : ''}>
                    {!isBuy && o.margin_max_loan != null && o.mark_price ? fmtAmount((o.margin_max_loan + (o.margin_borrowed || 0)) * o.mark_price) : '-'}
                  </td>

                  <td className="px-2 py-1.5 text-right font-mono text-orange-400">
                    {borrowAnnualPct != null ? `${borrowAnnualPct.toFixed(2)}%` : '-'}
                  </td>

                  <td className="px-2 py-1.5 text-right font-mono text-orange-400">
                    {(() => {
                      if (isBuy || o.margin_hourly_rate == null || o.margin_max_loan == null) return '-';
                      const total = o.margin_max_loan + (o.margin_borrowed || 0);
                      return fmtAmount(o.margin_hourly_rate * 24 * total);
                    })()}
                  </td>

                  <td className={`px-2 py-1.5 text-right font-mono font-bold ${
                    netAnnualPct == null ? 'text-gray-600'
                      : netAnnualPct >= 0 ? 'text-green-400' : 'text-red-400'
                  }`}>
                    {netAnnualPct != null ? fmtPct(netAnnualPct, 2) : '-'}
                  </td>

                  <td className={`px-2 py-1.5 text-right font-mono ${
                    (() => {
                      if (netAnnualPct == null || o.margin_max_loan == null || !o.mark_price) return 'text-gray-600';
                      const total = o.margin_max_loan + (o.margin_borrowed || 0);
                      const daily = netAnnualPct / 365 / 100 * total * o.mark_price;
                      return daily >= 0 ? 'text-green-400' : 'text-red-400';
                    })()
                  }`}>
                    {(() => {
                      if (netAnnualPct == null || o.margin_max_loan == null || !o.mark_price) return '-';
                      const total = o.margin_max_loan + (o.margin_borrowed || 0);
                      const daily = netAnnualPct / 365 / 100 * total * o.mark_price;
                      return `${daily >= 0 ? '+' : ''}${daily.toFixed(2)}`;
                    })()}
                  </td>

                  <td className="px-2 py-1.5 text-center whitespace-nowrap">
                    {notViable ? (
                      <span className="text-gray-600">不可行</span>
                    ) : isBuy ? (
                      <span className="text-green-400">多現貨/空合約</span>
                    ) : (
                      <span className="text-red-400">空現貨/多合約</span>
                    )}
                  </td>
                </tr>
              );
            })}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={18} className="px-3 py-10 text-center text-gray-500">
                  {loading ? '載入中...' : '無期現套利資料'}
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
        />
      )}
    </div>
  );
}

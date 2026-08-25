import { useState, useMemo, useCallback, useEffect } from 'react';
import SymbolDetail from './SymbolDetail';

const EXCHANGE_ORDER = [
  'binance', 'bybit', 'okx', 'bitget', 'mexc', 'gateio', 'bingx', 'kucoinfutures', 'aster', 'coinw', 'hyperliquid', 'tradexyz', 'ourbit', 'deepcoin', 'lbank', 'kraken', 'deribit', 'lighter', 'lighter_rh'
];

const EXCHANGES = EXCHANGE_ORDER.map(id => ({
  id,
  label: { binance: 'Binance', bybit: 'Bybit', okx: 'OKX', bitget: 'Bitget', mexc: 'MEXC', gateio: 'Gate.io', bingx: 'BingX', kucoinfutures: 'KuCoin', aster: 'Aster', coinw: 'CoinW', hyperliquid: 'Hyperliquid', tradexyz: 'Trade.xyz', ourbit: 'Ourbit', deepcoin: 'DeepCoin', lbank: 'LBank', kraken: 'Kraken', deribit: 'Deribit', lighter: 'Lighter', lighter_rh: 'Lighter RH' }[id],
}));

const EXCHANGE_LABELS = Object.fromEntries(EXCHANGES.map(e => [e.id, e.label]));

function rateColor(rate) {
  if (rate === null || rate === undefined) return 'text-gray-600';
  if (rate > 0.0001) return 'text-green-400';
  if (rate > 0.00001) return 'text-green-600';
  if (rate < -0.0001) return 'text-red-400';
  if (rate < -0.00001) return 'text-red-600';
  return 'text-gray-400';
}

function rateBg(rate) {
  if (rate === null || rate === undefined) return '';
  if (rate > 0.0005) return 'bg-green-500/10';
  if (rate < -0.0005) return 'bg-red-500/10';
  return '';
}

function formatRate(rate) {
  if (rate === null || rate === undefined) return '-';
  return (rate * 100).toFixed(4) + '%';
}

function shortSymbol(symbol) {
  return symbol.split('/')[0];
}

function cleanAliasBase(base) {
  // BingX NC 系列：NCSKCOIN2USD → COIN, NCCOGOLD2USD → GOLD
  const m = base.match(/^NC(?:SK|CO|FX|SI)\d*(.+?)2USD$/);
  if (m) return m[1];
  return base;
}

const FAVORITES_KEY = 'funding-scanner-favorites';
const PRESETS_KEY = 'funding-scanner-presets';

function loadFavorites() {
  try {
    return new Set(JSON.parse(localStorage.getItem(FAVORITES_KEY) || '[]'));
  } catch { return new Set(); }
}

function saveFavorites(favs) {
  localStorage.setItem(FAVORITES_KEY, JSON.stringify([...favs]));
}

function loadPresets() {
  try {
    return JSON.parse(localStorage.getItem(PRESETS_KEY) || '[]');
  } catch { return []; }
}

function savePresets(presets) {
  localStorage.setItem(PRESETS_KEY, JSON.stringify(presets));
}

export default function FundingTable({ records, searchTerm, onRealtimeUpdate }) {
  const [sortExchange, setSortExchange] = useState('binance');
  const [sortDir, setSortDir] = useState('asc');

  // 別名反向對照：canonical -> [alias, ...]（如 WTI -> [XTI, USOIL, CL]）
  const [aliasReverse, setAliasReverse] = useState({});
  useEffect(() => {
    fetch('/api/aliases').then(r => r.json()).then(d => {
      const reverse = d.reverse || {};
      setAliasReverse(reverse);
      // 建立正向對照：alias -> canonical（用於遷移舊 favorites）
      const forward = {};
      for (const [canonical, aliases] of Object.entries(reverse)) {
        for (const alias of aliases) forward[alias.toUpperCase()] = canonical;
      }
      // 遷移 localStorage 裡用舊名存的 favorites
      setFavorites(prev => {
        const migrated = new Set([...prev].map(sym => {
          const base = sym.split('/')[0].toUpperCase();
          return forward[base] ? `${forward[base]}/USDT:USDT` : sym;
        }));
        saveFavorites(migrated);
        return migrated;
      });
    }).catch(() => {});
  }, []);
  const [detailSymbol, setDetailSymbol] = useState(null);

  // 篩選狀態：交易所條件 + 費率範圍 + 結算週期
  const [condExchanges, setCondExchanges] = useState(new Set());
  const [minRate, setMinRate] = useState('');
  const [maxRate, setMaxRate] = useState('');
  const [intervalFilter, setIntervalFilter] = useState(null); // null=全部, 1/4/8/24
  const [rwaOnly, setRwaOnly] = useState(false);

  // 自選清單
  const [favorites, setFavorites] = useState(loadFavorites);
  const [showFavOnly, setShowFavOnly] = useState(false);

  // 快取篩選條件（presets）
  const [presets, setPresets] = useState(loadPresets);
  const [activePresetId, setActivePresetId] = useState(null);
  const [showPresetSave, setShowPresetSave] = useState(false);
  const [newPresetName, setNewPresetName] = useState('');

  const toggleFavorite = useCallback((symbol) => {
    setFavorites(prev => {
      const next = new Set(prev);
      if (next.has(symbol)) next.delete(symbol); else next.add(symbol);
      saveFavorites(next);
      return next;
    });
  }, []);

  const applyPreset = useCallback((preset) => {
    setCondExchanges(new Set(preset.condExchanges || []));
    setMinRate(preset.minRate || '');
    setMaxRate(preset.maxRate || '');
    setIntervalFilter(preset.intervalFilter ?? null);
    setSortExchange(preset.sortExchange || null);
    setSortDir(preset.sortDir || 'desc');
    setRwaOnly(preset.rwaOnly || false);
    setActivePresetId(preset.id);
  }, []);

  const saveCurrentAsPreset = useCallback((name) => {
    if (!name.trim()) return;
    const preset = {
      id: `user_${Date.now()}`,
      name: name.trim(),
      condExchanges: [...condExchanges],
      minRate,
      maxRate,
      intervalFilter,
      sortExchange,
      sortDir,
      rwaOnly,
    };
    const next = [...presets, preset];
    setPresets(next);
    savePresets(next);
    setActivePresetId(preset.id);
    setShowPresetSave(false);
    setNewPresetName('');
  }, [condExchanges, minRate, maxRate, intervalFilter, sortExchange, sortDir, rwaOnly, presets]);

  const deletePreset = useCallback((id) => {
    const next = presets.filter(p => p.id !== id);
    setPresets(next);
    savePresets(next);
    if (activePresetId === id) setActivePresetId(null);
  }, [presets, activePresetId]);

  const toggleExchange = useCallback((id) => {
    setActivePresetId(null);
    if (id === '__reset__') { setCondExchanges(new Set()); return; }
    setCondExchanges(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }, []);

  // 建立完整矩陣，然後依條件篩選「行」（幣種），欄位永遠顯示全部交易所
  const { matrix, symbols } = useMemo(() => {
    if (!records || records.length === 0) return { matrix: {}, symbols: [] };

    // 建立完整矩陣（用 normalized_symbol 分組，讓別名幣種合併）
    const m = {};
    for (const r of records) {
      const key = r.normalized_symbol || r.symbol;
      if (!m[key]) m[key] = {};
      m[key][r.exchange] = r;
    }

    let syms = Object.keys(m);

    // 自選篩選
    if (showFavOnly) {
      syms = syms.filter(s => favorites.has(s));
    }

    // RWA 篩選：大宗商品 + 股票映射合約
    if (rwaOnly) {
      syms = syms.filter(sym => {
        const row = m[sym];
        // Trade.xyz 有的 = RWA
        if (row['tradexyz']) return true;
        for (const ex of EXCHANGE_ORDER) {
          const r = row[ex];
          if (!r) continue;
          const base = r.symbol.split('/')[0].toUpperCase();
          // BingX NC 股票/商品
          if (/^NC(?:SK|CO)\d*/.test(base)) return true;
          // MEXC STOCK 後綴
          if (base.endsWith('STOCK')) return true;
        }
        return false;
      });
    }

    // 搜尋篩選（含別名，如搜 XTI 可找到 WTI）
    if (searchTerm) {
      const term = searchTerm.toUpperCase();
      syms = syms.filter(s => {
        const base = s.split('/')[0].toUpperCase();
        if (s.toUpperCase().includes(term)) return true;
        const aliases = aliasReverse[base] || [];
        return aliases.some(a => a.includes(term));
      });
    }

    // 結算週期篩選：該幣種至少有一個交易所的結算週期符合
    if (intervalFilter !== null) {
      syms = syms.filter(sym => {
        const row = m[sym];
        return EXCHANGE_ORDER.some(ex => {
          const r = row[ex];
          return r && r.funding_interval_h === intervalFilter;
        });
      });
    }

    // 條件篩選：選定交易所的費率必須在範圍內
    const minVal = minRate !== '' ? Number(minRate) / 100 : null;
    const maxVal = maxRate !== '' ? Number(maxRate) / 100 : null;
    const hasRateFilter = (minVal !== null && !isNaN(minVal)) || (maxVal !== null && !isNaN(maxVal));
    const hasExFilter = condExchanges.size > 0;

    if (hasExFilter || hasRateFilter) {
      syms = syms.filter(sym => {
        const row = m[sym];
        // 決定要檢查哪些交易所
        const checkExchanges = hasExFilter
          ? [...condExchanges]
          : EXCHANGE_ORDER;

        // 該幣種在條件交易所中，至少有一個費率符合範圍
        return checkExchanges.some(ex => {
          const r = row[ex];
          if (!r) return false;
          if (intervalFilter !== null && r.funding_interval_h !== intervalFilter) return false;
          const rate = r.funding_rate;
          if (rate === null || rate === undefined) return false;
          if (minVal !== null && !isNaN(minVal) && rate < minVal) return false;
          if (maxVal !== null && !isNaN(maxVal) && rate > maxVal) return false;
          return true;
        });
      });
    }

    // 排序
    if (sortExchange) {
      syms.sort((a, b) => {
        const ra = m[a]?.[sortExchange]?.funding_rate ?? 0;
        const rb = m[b]?.[sortExchange]?.funding_rate ?? 0;
        return sortDir === 'desc' ? rb - ra : ra - rb;
      });
    } else {
      syms.sort();
    }

    return { matrix: m, symbols: syms };
  }, [records, searchTerm, sortExchange, sortDir, condExchanges, minRate, maxRate, intervalFilter, favorites, showFavOnly, aliasReverse, rwaOnly]);

  const handleSort = (exchange) => {
    setActivePresetId(null);
    if (sortExchange === exchange) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    } else {
      setSortExchange(exchange);
      setSortDir('desc');
    }
  };

  const clearSort = () => {
    setSortExchange(null);
    setSortDir('desc');
  };

  if (!records || records.length === 0) {
    return (
      <div className="flex items-center justify-center h-64 text-[var(--text-secondary)]">
        等待第一次掃描完成...
      </div>
    );
  }

  return (
    <>
      {/* 篩選區 */}
      <div className="px-3 py-2.5 border-b border-[var(--border)] bg-[var(--bg-secondary)] space-y-2">
        {/* 第一行：條件交易所 */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-[var(--text-secondary)] font-medium shrink-0">條件交易所</span>
          {EXCHANGES.map(ex => (
            <button
              key={ex.id}
              onClick={() => toggleExchange(ex.id)}
              className={`px-2 py-0.5 rounded text-xs font-medium transition-colors border ${
                condExchanges.has(ex.id)
                  ? 'bg-blue-500/20 text-blue-400 border-blue-500/40'
                  : 'bg-gray-800/50 text-gray-500 border-gray-700/50 hover:text-gray-400'
              }`}
            >
              {ex.label}
            </button>
          ))}
          {condExchanges.size > 0 && (
            <button
              onClick={() => toggleExchange('__reset__')}
              className="px-2 py-0.5 rounded text-xs text-gray-400 hover:text-white border border-gray-700/50"
            >
              清除
            </button>
          )}
        </div>

        {/* 第二行：資金費率範圍 + 快速按鈕 + 計數 */}
        <div className="flex items-center gap-3 flex-wrap text-xs">
          <div className="flex items-center gap-1.5">
            <span className="text-[var(--text-secondary)] font-medium">資金費率</span>
            <input
              type="number"
              step="0.01"
              value={minRate}
              onChange={e => { setMinRate(e.target.value); setActivePresetId(null); }}
              className="w-20 px-1.5 py-1 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-center focus:outline-none focus:border-blue-500"
              placeholder="min %"
            />
            <span className="text-gray-500">~</span>
            <input
              type="number"
              step="0.01"
              value={maxRate}
              onChange={e => { setMaxRate(e.target.value); setActivePresetId(null); }}
              className="w-20 px-1.5 py-1 bg-[var(--bg-card)] border border-[var(--border)] rounded text-white text-center focus:outline-none focus:border-blue-500"
              placeholder="max %"
            />
          </div>

          <span className="text-gray-700">|</span>

          <div className="flex items-center gap-1.5">
            <span className="text-[var(--text-secondary)] font-medium">結算週期</span>
            {[
              { label: '全部', value: null },
              { label: '1h', value: 1 },
              { label: '4h', value: 4 },
              { label: '8h', value: 8 },
            ].map(opt => (
              <button
                key={String(opt.value)}
                onClick={() => { setIntervalFilter(opt.value); setActivePresetId(null); }}
                className={`px-2 py-0.5 rounded border transition-colors ${
                  intervalFilter === opt.value
                    ? 'bg-blue-500/20 text-blue-400 border-blue-500/40'
                    : 'text-gray-500 border-gray-700/50 hover:text-gray-400'
                }`}
              >
                {opt.label}
              </button>
            ))}
          </div>

          <span className="text-gray-700">|</span>

          <button
            onClick={() => { setRwaOnly(v => !v); setActivePresetId(null); }}
            className={`px-2 py-0.5 rounded border transition-colors ${
              rwaOnly
                ? 'bg-amber-500/20 text-amber-400 border-amber-500/40'
                : 'text-gray-500 border-gray-700/50 hover:text-gray-400'
            }`}
          >
            RWA
          </button>

          <span className="text-gray-700">|</span>

          {[
            { label: '負費率', min: '', max: '-0.0001' },
            { label: '正費率', min: '0.0001', max: '' },
            { label: '<-0.1%', min: '', max: '-0.1' },
            { label: '>0.01%', min: '0.01', max: '' },
            { label: '>0.1%', min: '0.1', max: '' },
          ].map(p => (
            <button
              key={p.label}
              onClick={() => { setMinRate(p.min); setMaxRate(p.max); setActivePresetId(null); }}
              className={`px-2 py-0.5 rounded border transition-colors ${
                minRate === p.min && maxRate === p.max
                  ? 'bg-blue-500/20 text-blue-400 border-blue-500/40'
                  : 'text-gray-500 border-gray-700/50 hover:text-gray-400'
              }`}
            >
              {p.label}
            </button>
          ))}

          <button
            onClick={() => { setMinRate(''); setMaxRate(''); setIntervalFilter(null); setCondExchanges(new Set()); setSortExchange(null); setSortDir('desc'); setActivePresetId(null); setRwaOnly(false); }}
            className={`px-2 py-0.5 rounded border transition-colors ${
              minRate === '' && maxRate === '' && intervalFilter === null && condExchanges.size === 0 && !sortExchange && !rwaOnly
                ? 'bg-blue-500/20 text-blue-400 border-blue-500/40'
                : 'text-gray-500 border-gray-700/50 hover:text-gray-400'
            }`}
          >
            重置全部
          </button>

          <span className="text-gray-700">|</span>

          <button
            onClick={() => setShowFavOnly(v => !v)}
            className={`px-2 py-0.5 rounded border transition-colors ${
              showFavOnly
                ? 'bg-yellow-500/20 text-yellow-400 border-yellow-500/40'
                : 'text-gray-500 border-gray-700/50 hover:text-gray-400'
            }`}
          >
            {showFavOnly ? '★' : '☆'} 自選 {favorites.size > 0 && `(${favorites.size})`}
          </button>

          <div className="ml-auto text-[var(--text-secondary)]">
            {symbols.length} 個幣種
          </div>
        </div>

        {/* 第三行：快取篩選條件 */}
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-[var(--text-secondary)] font-medium shrink-0">快捷篩選</span>
          {presets.map(p => (
            <div key={p.id} className="group relative inline-flex">
              <button
                onClick={() => activePresetId === p.id ? (setActivePresetId(null), setCondExchanges(new Set()), setMinRate(''), setMaxRate(''), setIntervalFilter(null), setSortExchange(null), setSortDir('desc'), setRwaOnly(false)) : applyPreset(p)}
                className={`px-2.5 py-0.5 rounded text-xs font-medium transition-colors border ${
                  activePresetId === p.id
                    ? 'bg-purple-500/20 text-purple-400 border-purple-500/40'
                    : 'bg-gray-800/50 text-gray-500 border-gray-700/50 hover:text-gray-400'
                }`}
                title={`${(p.condExchanges || []).map(e => EXCHANGE_LABELS[e] || e).join(', ')}${p.minRate ? ` min:${p.minRate}%` : ''}${p.maxRate ? ` max:${p.maxRate}%` : ''}${p.sortExchange ? ` 排序:${EXCHANGE_LABELS[p.sortExchange] || p.sortExchange} ${p.sortDir === 'asc' ? '↑' : '↓'}` : ''}`}
              >
                {p.name}
              </button>
              <button
                onClick={(e) => { e.stopPropagation(); deletePreset(p.id); }}
                className="absolute -top-1.5 -right-1.5 w-4 h-4 rounded-full bg-gray-700 text-gray-400 hover:bg-red-600 hover:text-white text-[10px] leading-none items-center justify-center hidden group-hover:flex"
              >
                x
              </button>
            </div>
          ))}

          <span className="text-gray-700">|</span>

          {showPresetSave ? (
            <div className="flex items-center gap-1.5">
              <input
                type="text"
                value={newPresetName}
                onChange={e => setNewPresetName(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter') saveCurrentAsPreset(newPresetName); if (e.key === 'Escape') setShowPresetSave(false); }}
                className="w-32 px-1.5 py-0.5 bg-[var(--bg-card)] border border-purple-500/40 rounded text-xs text-white focus:outline-none"
                placeholder="條件名稱..."
                autoFocus
              />
              <button
                onClick={() => saveCurrentAsPreset(newPresetName)}
                className="px-2 py-0.5 rounded text-xs bg-purple-500/20 text-purple-400 border border-purple-500/40 hover:bg-purple-500/30"
              >
                儲存
              </button>
              <button
                onClick={() => { setShowPresetSave(false); setNewPresetName(''); }}
                className="px-1.5 py-0.5 rounded text-xs text-gray-500 hover:text-gray-400"
              >
                取消
              </button>
            </div>
          ) : (
            <button
              onClick={() => setShowPresetSave(true)}
              className="px-2 py-0.5 rounded text-xs text-gray-500 border border-dashed border-gray-700/50 hover:text-purple-400 hover:border-purple-500/40 transition-colors"
            >
              + 儲存目前條件
            </button>
          )}
        </div>
      </div>

      {/* 表格：永遠顯示全部交易所 */}
      <div className="overflow-auto max-h-[calc(100vh-280px)]">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-[var(--bg-secondary)]">
              <th
                className="px-2 py-2 text-left font-medium text-[var(--text-secondary)] border-b border-[var(--border)] cursor-pointer hover:text-white whitespace-nowrap"
                onClick={clearSort}
              >
                幣種 {!sortExchange && '▾'}
              </th>
              {EXCHANGE_ORDER.map(ex => (
                <th
                  key={ex}
                  className={`px-0 py-2 text-center font-medium border-b border-[var(--border)] cursor-pointer hover:text-white whitespace-nowrap text-[11px] ${
                    condExchanges.has(ex) ? 'text-blue-400' : 'text-[var(--text-secondary)]'
                  }`}
                  onClick={() => handleSort(ex)}
                >
                  {EXCHANGE_LABELS[ex]}
                  {sortExchange === ex && (sortDir === 'desc' ? ' ▾' : ' ▴')}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {symbols.map(symbol => (
              <tr key={symbol} className="hover:bg-[var(--bg-hover)] border-b border-[var(--border)]/30">
                <td className="px-2 py-1.5 font-mono font-medium flex items-center gap-1">
                  <span
                    className={`cursor-pointer select-none text-sm ${favorites.has(symbol) ? 'text-yellow-400' : 'text-gray-600 hover:text-gray-400'}`}
                    onClick={(e) => { e.stopPropagation(); toggleFavorite(symbol); }}
                  >
                    {favorites.has(symbol) ? '★' : '☆'}
                  </span>
                  <span
                    className="text-blue-400 cursor-pointer hover:underline"
                    onClick={() => setDetailSymbol(symbol)}
                  >
                    {shortSymbol(symbol)}
                  </span>
                </td>
                {EXCHANGE_ORDER.map(ex => {
                  const r = matrix[symbol]?.[ex];
                  const rate = r?.funding_rate;
                  const origBase = r ? r.symbol.split('/')[0] : null;
                  const normBase = symbol.split('/')[0];
                  const isAlias = origBase && origBase !== normBase;
                  return (
                    <td
                      key={ex}
                      className={`px-0 py-1.5 text-right font-mono text-[10px] ${rateColor(rate)} ${rateBg(rate)}`}
                      title={rate != null ? `${isAlias ? `${origBase} → ` : ''}年化: ${(rate * (24 / (r?.funding_interval_h || 8)) * 365 * 100).toFixed(2)}%` : '無資料'}
                    >
                      {r?.is_delisting && <span className="text-[9px] text-red-400 font-bold mr-0.5" title={r?.delisting_time ? `即將下架：${new Date(r.delisting_time).toLocaleString('zh-TW', {month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'})}` : '此合約即將下架'}>⚠</span>}
                      {isAlias && <span className="text-[9px] text-yellow-500 mr-0.5">{cleanAliasBase(origBase)}</span>}
                      {formatRate(rate)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
        {symbols.length === 0 && (
          <div className="text-center text-[var(--text-secondary)] py-8 text-sm">
            無符合條件的資料
          </div>
        )}
      </div>

      {detailSymbol && (
        <SymbolDetail
          symbol={detailSymbol}
          records={records}
          onClose={() => setDetailSymbol(null)}
          onRealtimeUpdate={onRealtimeUpdate}
        />
      )}
    </>
  );
}

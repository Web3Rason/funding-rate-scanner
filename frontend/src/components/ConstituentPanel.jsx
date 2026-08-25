import { useState, useEffect, useMemo } from 'react';

const SOURCE_LABELS = {
  binance: 'Binance', bybit: 'Bybit', okx: 'OKX',
  kucoinfutures: 'KuCoin', gateio: 'Gate.io',
  bitget: 'Bitget', mexc: 'MEXC',
};

const CHANGE_TYPE_LABELS = {
  added: '新增',
  removed: '移除',
  weight_changed: '權重變更',
  source_removed: '指數下架',
};

const CHANGE_TYPE_COLORS = {
  added: 'text-green-400',
  removed: 'text-red-400',
  weight_changed: 'text-yellow-400',
  source_removed: 'text-orange-400',
};

function fmtWeight(w) {
  if (w == null) return '-';
  return (w * 100).toFixed(2) + '%';
}

function fmtPrice(p) {
  if (p == null) return '-';
  if (p >= 1000) return p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (p >= 1) return p.toFixed(4);
  return p.toPrecision(4);
}

function fmtTime(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  return `${(d.getMonth() + 1).toString().padStart(2, '0')}/${d.getDate().toString().padStart(2, '0')} ${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
}

export default function ConstituentPanel({ symbol, onClose }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [scanning, setScanning] = useState(false);
  const [watching, setWatching] = useState(false);
  const [toggling, setToggling] = useState(false);

  const fetchData = (cancelled = { v: false }) => {
    setLoading(true);
    fetch(`/api/index-constituents?symbol=${encodeURIComponent(symbol)}`)
      .then(r => r.json())
      .then(d => { if (!cancelled.v) { setData(d); setWatching(!!d.watching); setLoading(false); } })
      .catch(e => { if (!cancelled.v) { setError(e.message); setLoading(false); } });
  };

  useEffect(() => {
    const cancelled = { v: false };
    fetchData(cancelled);
    return () => { cancelled.v = true; };
  }, [symbol]);

  const handleScan = async () => {
    setScanning(true);
    try {
      const res = await fetch(`/api/index-constituents/scan?symbol=${encodeURIComponent(symbol)}`, { method: 'POST' });
      const d = await res.json();
      setData(d);
      setWatching(!!d.watching);
    } catch (e) {
      setError(e.message);
    } finally {
      setScanning(false);
    }
  };

  const handleWatch = async () => {
    setToggling(true);
    try {
      const res = await fetch(`/api/index-constituents/watch?symbol=${encodeURIComponent(symbol)}&enable=${!watching}`, { method: 'POST' });
      const d = await res.json();
      setWatching(!!d.watching);
    } catch (e) {
      setError(e.message);
    } finally {
      setToggling(false);
    }
  };

  // 收集所有成分交易所名稱（聯集）
  const { allConstituents, sources } = useMemo(() => {
    if (!data?.constituents) return { allConstituents: [], sources: [] };
    const srcs = Object.keys(data.constituents).sort();
    const exchSet = new Set();
    for (const src of srcs) {
      for (const c of data.constituents[src]) {
        exchSet.add(c.exchange);
      }
    }
    return { allConstituents: [...exchSet].sort(), sources: srcs };
  }, [data]);

  // 建立查詢表 {source: {constituent_exchange: {weight, price}}}
  const lookup = useMemo(() => {
    if (!data?.constituents) return {};
    const m = {};
    for (const [src, list] of Object.entries(data.constituents)) {
      m[src] = {};
      for (const c of list) {
        m[src][c.exchange] = c;
      }
    }
    return m;
  }, [data]);

  const baseCoin = symbol.split('/')[0] || symbol;

  return (
    <div
      className="fixed inset-0 bg-black/60 z-50 flex items-center justify-center p-4"
      onClick={onClose}
    >
      <div
        className="bg-[var(--bg-primary)] border border-[var(--border)] rounded-lg shadow-xl max-w-3xl w-full max-h-[80vh] overflow-auto"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="px-4 py-3 border-b border-[var(--border)] flex items-center justify-between">
          <h3 className="text-white font-medium">
            {baseCoin} 指數成分
          </h3>
          <div className="flex items-center gap-2">
            <button
              onClick={handleScan}
              disabled={scanning || loading}
              className="px-2.5 py-1 text-xs rounded border border-blue-500/40 text-blue-400 hover:bg-blue-500/10 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1.5 transition-colors"
            >
              {scanning ? (
                <>
                  <span className="w-3 h-3 border border-blue-400/40 border-t-blue-400 rounded-full animate-spin inline-block" />
                  掃描中...
                </>
              ) : '立即掃描'}
            </button>
            <button
              onClick={handleWatch}
              disabled={toggling}
              title="開著就每 5 秒比對指數成分 + 現貨充提狀態（Gate/Bitget），有變動發 TG（再按一次取消）"
              className={`px-2.5 py-1 text-xs rounded border flex items-center gap-1.5 transition-colors disabled:opacity-40 disabled:cursor-not-allowed ${
                watching
                  ? 'border-green-500/50 text-green-400 bg-green-500/10 hover:bg-green-500/20'
                  : 'border-gray-500/40 text-gray-300 hover:bg-white/5'
              }`}
            >
              {watching && <span className="w-2 h-2 rounded-full bg-green-400 animate-pulse inline-block" />}
              {watching ? '監控中' : '持續監控'}
            </button>
            <button onClick={onClose} className="text-gray-400 hover:text-white text-lg">&times;</button>
          </div>
        </div>

        <div className="p-4">
          {loading ? (
            <div className="text-center text-gray-400 py-8">載入中...</div>
          ) : error ? (
            <div className="text-center text-red-400 py-8">{error}</div>
          ) : (
            <>
              {/* 成分對比表 */}
              {sources.length > 0 && (
                <div className="overflow-x-auto mb-4">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-xs text-[var(--text-secondary)] border-b border-[var(--border)]">
                        <th className="px-3 py-1.5">成分交易所</th>
                        {sources.map(src => (
                          <th key={src} className="px-3 py-1.5 text-center">
                            {SOURCE_LABELS[src] || src}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {allConstituents.map(ex => (
                        <tr key={ex} className="border-b border-[var(--border)] hover:bg-[var(--bg-secondary)]">
                          <td className="px-3 py-1.5 text-white capitalize">{ex}</td>
                          {sources.map(src => {
                            const c = lookup[src]?.[ex];
                            const inIndex = !!c;
                            return (
                              <td key={`${src}-${ex}`} className="px-2 py-1.5 text-center">
                                {inIndex ? (
                                  <div className="flex flex-col items-center">
                                    {c.weight != null ? (
                                      <span className="font-mono text-[var(--text-secondary)]">
                                        {fmtWeight(c.weight)}
                                      </span>
                                    ) : (
                                      <span className="text-green-500 text-xs">✓</span>
                                    )}
                                    {c.price != null && (
                                      <span className="font-mono text-gray-500 text-[10px]">
                                        {fmtPrice(c.price)}
                                      </span>
                                    )}
                                  </div>
                                ) : (
                                  <span className="text-gray-600">-</span>
                                )}
                              </td>
                            );
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {sources.length === 0 && (
                <div className="text-center text-gray-500 py-4">
                  尚無成分資料（等待下次掃描）
                </div>
              )}

              {/* 變更歷史 */}
              {data?.changes?.length > 0 && (
                <div className="mt-4">
                  <h4 className="text-xs text-[var(--text-secondary)] font-medium mb-2">
                    變更歷史（7天內）
                  </h4>
                  <div className="space-y-1">
                    {data.changes.slice().reverse().filter(c =>
                      c.type !== 'weight_changed' ||
                      (c.old_weight != null && c.new_weight != null && Math.abs(c.new_weight - c.old_weight) > 0.05)
                    ).map((c, i) => (
                      <div key={i} className="flex items-center gap-2 text-xs py-1 px-2 rounded bg-[var(--bg-secondary)]">
                        <span className="text-gray-500 shrink-0">{fmtTime(c.ts)}</span>
                        <span className="text-gray-400">{SOURCE_LABELS[c.source] || c.source} 指數</span>
                        <span className={`font-medium ${CHANGE_TYPE_COLORS[c.type] || 'text-gray-400'}`}>
                          {CHANGE_TYPE_LABELS[c.type] || c.type}
                        </span>
                        <span className="text-white capitalize">{c.constituent_exchange}</span>
                        {c.type === 'weight_changed' && (
                          <span className="text-gray-400">
                            {fmtWeight(c.old_weight)} → {fmtWeight(c.new_weight)}
                          </span>
                        )}
                        {c.type === 'added' && c.new_weight != null && (
                          <span className="text-gray-400">權重 {fmtWeight(c.new_weight)}</span>
                        )}
                        {c.type === 'removed' && c.old_weight != null && (
                          <span className="text-gray-400">原權重 {fmtWeight(c.old_weight)}</span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {data?.changes?.length === 0 && (
                <div className="text-center text-gray-500 text-xs py-2">
                  7 天內無成分變更
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

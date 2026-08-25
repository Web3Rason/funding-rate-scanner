import { useState, useCallback } from 'react';
import { usePolling } from './hooks/usePolling';
import FundingTable from './components/FundingTable';
import ArbitragePanel from './components/ArbitragePanel';
import SpotFuturesArb from './components/SpotFuturesArb';
import RwaArb from './components/RwaArb';
import RwaSpread from './components/RwaSpread';
import RwaLeveraged from './components/RwaLeveraged';
import SpotArbitrage from './components/SpotArbitrage';
import CoinwMeat from './components/CoinwMeat';
import IndexTracking from './components/IndexTracking';
import PremiumIndexPanel from './components/PremiumIndexPanel';
import StatusBar from './components/StatusBar';

function App() {
  const [tab, setTab] = useState('rates');
  const [searchTerm, setSearchTerm] = useState('');
  const [scanning, setScanning] = useState(false);

  const { data: ratesData, refetch: refreshRates, patchData: patchRates } = usePolling('/api/funding-rates', 2000);
  const { data: arbData, refetch: refreshArb } = usePolling('/api/arbitrage', 5000);

  // 即時資費回寫到總覽
  const handleRealtimeUpdate = useCallback((realtimeRecords) => {
    if (!realtimeRecords?.length) return;
    patchRates(prev => {
      if (!prev?.records) return prev;
      const updated = [...prev.records];
      for (const rt of realtimeRecords) {
        const idx = updated.findIndex(r =>
          r.exchange === rt.exchange && (r.normalized_symbol || r.symbol) === (rt.normalized_symbol || rt.symbol)
        );
        if (idx >= 0) {
          updated[idx] = {
            ...updated[idx],
            ...rt,
            // 保留掃描時設定的下架標記，不被 realtime 預設值覆蓋
            is_delisting: updated[idx].is_delisting || rt.is_delisting,
            delisting_time: updated[idx].delisting_time || rt.delisting_time,
          };
        }
      }
      return { ...prev, records: updated };
    });
  }, [patchRates]);

  const handleManualScan = async () => {
    if (scanning) return;
    setScanning(true);
    try {
      await fetch('/api/scan', { method: 'POST' });
      refreshRates();
      refreshArb();
    } catch (e) {
      console.error('手動掃描失敗:', e);
    } finally {
      setScanning(false);
    }
  };

  return (
    <div className="min-h-screen pb-10">
      {/* 頂部標題 */}
      <header className="bg-[var(--bg-secondary)] border-b border-[var(--border)] px-4 py-3">
        <div className="max-w-[1800px] mx-auto flex items-center justify-between">
          <div className="flex items-center gap-3">
            <h1 className="text-lg font-bold text-white">Funding Rate Scanner</h1>
            {ratesData?.timestamp && (
              <span className="text-xs text-[var(--text-secondary)]">
                上次掃描: {new Date(ratesData.timestamp).toLocaleTimeString()}
                {ratesData.scan_duration_ms != null && (
                  <> ({(ratesData.scan_duration_ms / 1000).toFixed(1)}s)</>
                )}
              </span>
            )}
          </div>
          <button
            onClick={handleManualScan}
            disabled={scanning}
            className={`px-3 py-1.5 text-white text-xs rounded transition-colors ${
              scanning
                ? 'bg-gray-600 cursor-not-allowed'
                : 'bg-blue-600 hover:bg-blue-700'
            }`}
          >
            {scanning ? '掃描中...' : '手動掃描'}
          </button>
        </div>
      </header>

      {/* Tab 切換 + 搜尋 */}
      <div className="max-w-[1800px] mx-auto px-4 py-3">
        <div className="flex items-center gap-4">
          <div className="flex bg-[var(--bg-card)] rounded-lg p-0.5">
            <button
              onClick={() => setTab('rates')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                tab === 'rates'
                  ? 'bg-blue-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              費率總覽
            </button>
            <button
              onClick={() => setTab('arbitrage')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'arbitrage'
                  ? 'bg-blue-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              套利偵測
            </button>
            <button
              onClick={() => setTab('spot-futures')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'spot-futures'
                  ? 'bg-yellow-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              期現套利
            </button>
            <button
              onClick={() => setTab('rwa-arb')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'rwa-arb'
                  ? 'bg-indigo-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              RWA 期現
            </button>
            <button
              onClick={() => setTab('rwa-spread')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'rwa-spread'
                  ? 'bg-cyan-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              RWA 跨所價差
            </button>
            <button
              onClick={() => setTab('rwa-leveraged')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'rwa-leveraged'
                  ? 'bg-pink-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              槓桿對套利
            </button>
            <button
              onClick={() => setTab('spot-arb')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'spot-arb'
                  ? 'bg-emerald-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              現貨搬磚
            </button>
            <button
              onClick={() => setTab('coinw-meat')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'coinw-meat'
                  ? 'bg-orange-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              碎肉流
            </button>
            <button
              onClick={() => setTab('index-track')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'index-track'
                  ? 'bg-purple-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              指數追蹤
            </button>
            <button
              onClick={() => setTab('premium-index')}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors relative ${
                tab === 'premium-index'
                  ? 'bg-cyan-600 text-white'
                  : 'text-[var(--text-secondary)] hover:text-white'
              }`}
            >
              溢價指數
            </button>
          </div>

          <div className="relative flex-1 max-w-xs">
            <input
              type="text"
              placeholder="搜尋幣種... (BTC, ETH)"
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
              className="w-full px-3 py-1.5 pr-7 bg-[var(--bg-card)] border border-[var(--border)] rounded text-sm text-white placeholder-gray-500 focus:outline-none focus:border-blue-500"
            />
            {searchTerm && (
              <button
                onClick={() => setSearchTerm('')}
                className="absolute right-1.5 top-1/2 -translate-y-1/2 w-5 h-5 flex items-center justify-center text-gray-500 hover:text-white rounded-full hover:bg-gray-700 transition-colors text-xs"
              >
                ✕
              </button>
            )}
          </div>
        </div>
      </div>

      {/* 主內容 */}
      <div className="max-w-[1800px] mx-auto px-4">
        <div className="bg-[var(--bg-card)] rounded-lg border border-[var(--border)] overflow-hidden">
          {tab === 'rates' ? (
            <FundingTable
              records={ratesData?.records}
              searchTerm={searchTerm}
              onRealtimeUpdate={handleRealtimeUpdate}
            />
          ) : tab === 'arbitrage' ? (
            <ArbitragePanel
              arbitrage={arbData?.arbitrage}
              records={ratesData?.records}
              searchTerm={searchTerm}
            />
          ) : tab === 'spot-futures' ? (
            <SpotFuturesArb searchTerm={searchTerm} />
          ) : tab === 'rwa-arb' ? (
            <RwaArb searchTerm={searchTerm} />
          ) : tab === 'rwa-spread' ? (
            <RwaSpread searchTerm={searchTerm} />
          ) : tab === 'rwa-leveraged' ? (
            <RwaLeveraged searchTerm={searchTerm} />
          ) : tab === 'coinw-meat' ? (
            <CoinwMeat searchTerm={searchTerm} />
          ) : tab === 'index-track' ? (
            <IndexTracking searchTerm={searchTerm} />
          ) : tab === 'premium-index' ? (
            <PremiumIndexPanel searchTerm={searchTerm} />
          ) : (
            <SpotArbitrage searchTerm={searchTerm} />
          )}
        </div>
      </div>

      <StatusBar />
    </div>
  );
}

export default App;

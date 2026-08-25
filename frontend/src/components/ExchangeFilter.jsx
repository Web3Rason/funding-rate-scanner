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

export default function ExchangeFilter({ selected, onToggle }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {EXCHANGES.map(ex => {
        const active = selected.has(ex.id);
        return (
          <button
            key={ex.id}
            onClick={() => onToggle(ex.id)}
            className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border ${
              active
                ? 'bg-blue-500/20 text-blue-400 border-blue-500/40'
                : 'bg-gray-800/50 text-gray-500 border-gray-700/50 hover:text-gray-400'
            }`}
          >
            {ex.label}
          </button>
        );
      })}
      {selected.size > 0 && (
        <button
          onClick={() => onToggle('__reset__')}
          className="px-2.5 py-1 rounded text-xs font-medium text-gray-400 hover:text-white border border-gray-700/50"
        >
          清除
        </button>
      )}
    </div>
  );
}

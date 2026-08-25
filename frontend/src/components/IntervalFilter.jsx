const INTERVALS = [
  { value: 1, label: '1h' },
  { value: 2, label: '2h' },
  { value: 4, label: '4h' },
  { value: 8, label: '8h' },
];

export default function IntervalFilter({ selected, onToggle }) {
  return (
    <div className="flex flex-wrap gap-1.5 items-center">
      <span className="text-xs text-gray-500 mr-1">結算</span>
      {INTERVALS.map(iv => {
        const active = selected.has(iv.value);
        return (
          <button
            key={iv.value}
            onClick={() => onToggle(iv.value)}
            className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border ${
              active
                ? 'bg-purple-500/20 text-purple-400 border-purple-500/40'
                : 'bg-gray-800/50 text-gray-500 border-gray-700/50 hover:text-gray-400'
            }`}
          >
            {iv.label}
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

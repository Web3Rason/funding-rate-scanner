import { usePolling } from '../hooks/usePolling';

export default function StatusBar() {
  const { data: status } = usePolling('/api/status', 5000);

  if (!status) return null;

  const errorCount = Object.keys(status.errors || {}).length;
  const exchangeCount = (status.exchanges || []).length;
  const okCount = exchangeCount - errorCount;

  return (
    <div className="fixed bottom-0 left-0 right-0 bg-[var(--bg-secondary)] border-t border-[var(--border)] px-4 py-2 flex items-center justify-between text-xs text-[var(--text-secondary)] z-50">
      <div className="flex items-center gap-4">
        <span className="flex items-center gap-1.5">
          <span className={`w-2 h-2 rounded-full ${status.is_running ? 'bg-green-500' : 'bg-red-500'}`} />
          {status.is_running ? '掃描中' : '已停止'}
        </span>
        <span>
          交易所: <span className="text-green-400">{okCount}</span>
          {errorCount > 0 && <span className="text-red-400"> / {errorCount} 失敗</span>}
        </span>
        <span>幣種: {status.total_records || 0}</span>
        <span>套利: {status.total_arbitrage || 0}</span>
      </div>
      <div className="flex items-center gap-4">
        {status.scan_duration_ms && (
          <span>掃描耗時: {Math.round(status.scan_duration_ms)}ms</span>
        )}
        {status.last_scan && (
          <span>更新: {new Date(status.last_scan).toLocaleTimeString()}</span>
        )}
      </div>
    </div>
  );
}

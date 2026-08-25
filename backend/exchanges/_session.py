"""共用的 aiohttp session 工廠。

掃描器要同時輪詢十幾個交易所 × 上百個幣種，若每個 ClientSession 都用 aiohttp 預設
connector（limit_per_host=0，每主機無上限），併發請求會對單一 host（如
api.hyperliquid.xyz）一口氣開上百條 keep-alive 連線，灌爆路由器連線表 → 全機網路變慢。

統一改用本工廠建立 session，connector 限制每主機並發連線數，從根本擋住連線堆積。

⚠️ connector 為「全程序共用」：limit_per_host 若綁在各自的 connector 上，等於每個 session
各有一份額度（本專案有 19 個常駐 session + 60 處 `async with make_session()`），單一 host
的總連線數就沒有天花板——實測曾對單一 BingX host 累積 133 條。共用同一個 connector 後，
per-host 上限才是真正的全程序上限。
"""

import asyncio

import aiohttp

# 全程序共用的 connector（延遲建立：TCPConnector 會綁定建立當下的 event loop）
_shared_connector: aiohttp.TCPConnector | None = None
_shared_loop = None


def _get_shared_connector() -> aiohttp.TCPConnector:
    """取得全程序共用 connector；已關閉或換了 event loop（測試腳本各自 asyncio.run）時重建。"""
    global _shared_connector, _shared_loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if _shared_connector is None or _shared_connector.closed or loop is not _shared_loop:
        _shared_connector = aiohttp.TCPConnector(
            limit=200,
            limit_per_host=10,
            keepalive_timeout=45,
            ttl_dns_cache=300,
        )
        _shared_loop = loop
    return _shared_connector


async def close_shared_connector() -> None:
    """程式結束時釋放共用 connector（session 用 connector_owner=False 不會關它）。"""
    global _shared_connector, _shared_loop
    if _shared_connector is not None and not _shared_connector.closed:
        try:
            await _shared_connector.close()
        except Exception:
            pass
    _shared_connector = None
    _shared_loop = None


def make_session(timeout_total: float = 30, **kwargs) -> aiohttp.ClientSession:
    """建立帶連線上限的 aiohttp.ClientSession（共用全程序 connector）。

    - limit_per_host=10：**全程序**對每個 host 最多 10 條並發連線，把原本無上限堆積到
      上百條的連線收斂掉。數值對齊歷史回填的 batch_size=10（見
      funding_scanner._fetch_all_histories）；但共用 connector 後這 10 個名額是所有呼叫端
      共享，若回填與其他請求撞上同一 host 仍會排隊（且 ClientTimeout(total=) 含等池時間）。
    - limit=200：全程序總連線上限（涵蓋 18 個交易所 × 各自少數 host 的並發掃描）
    - connector_owner=False：session.close() 不會關掉共用 connector（否則第一個結束的
      `async with make_session()` 就會把全程序的連線池關掉）
    - keepalive_timeout=45：閒置 keep-alive 連線保留 45 秒。掃描迴圈是每 5 分鐘整點對齊
      （funding_scanner._scan_loop），跨掃描的 5 分鐘空檔遠超任何交易所 LB/CDN 的閒置上限
      （CloudFront/Cloudflare/ALB 多在 60~100 秒就關），所以連線「跨掃描復用」本質上做不到，
      每輪都會是新連線——這無法靠 keepalive 解。keepalive 真正的作用是「同一輪掃描內」的
      連續/並發請求（如歷史回填 batch）能復用連線。設 45 秒：足夠涵蓋單輪掃描，又刻意壓在
      常見 LB 60 秒閒置上限「之下」，避免下個請求重用一條 server 已單方面關閉的半死連線
      （aiohttp 重用半死連線會丟 ServerDisconnectedError）。
    - ttl_dns_cache=300：DNS 解析快取 5 分鐘。交易所多走 CloudFront/多 IP，預設每 10 秒
      重解一次，拉長可避免每輪掃描重複 DNS 查詢（這才是降低 REST 端開銷的主要小收益）。
    額外的 kwargs（如 headers）原樣傳給 ClientSession。

    註：原本的 enable_cleanup_closed 在 aiohttp 3.12+/Python 3.13 已是無效參數（內部強制
    關閉並發 DeprecationWarning），故不再傳入。
    """
    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout_total),
        connector=_get_shared_connector(),
        connector_owner=False,
        **kwargs,
    )

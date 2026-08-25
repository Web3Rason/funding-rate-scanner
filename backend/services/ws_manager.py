"""WebSocket 即時推送管理器 — 聚合各交易所 ticker 串流，推送給前端

架構：
- WsManager: 全域管理器（一個 app 一個）
- _SymbolRoom: 每個被觀看的幣種一個，管理交易所連線 + 前端客戶端
- 交易所 handler: 每個交易所一個 async 函數，連接 WS 並回調更新

流程：
1. 前端 SymbolDetail 開啟 → 連 /ws/symbol?symbol=BTC/USDT:USDT
2. WsManager 找/建 _SymbolRoom
3. _SymbolRoom 對各交易所開 WS 連線
4. 交易所推送 ticker → _SymbolRoom 廣播給所有前端客戶端
5. 最後一個客戶端斷線 → _SymbolRoom 關閉所有交易所連線
"""

import asyncio
import gzip
import json
import logging
import time
import uuid
from functools import partial
from pathlib import Path

import ccxt.pro as ccxtpro
from exchanges.ccxt_exchange import scope_bybit_markets
from exchanges._session import make_session
import websockets

logger = logging.getLogger(__name__)


def _load_spot_markets_from_disk(ccxt_id: str) -> list | None:
    """從 scanner 寫的 disk cache 讀現貨 markets（避開 REST load_markets 被封的情境）"""
    try:
        path = Path(__file__).resolve().parent.parent / "data" / "spot_markets" / f"{ccxt_id}.json"
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        markets = data.get("markets") or {}
        return list(markets.values()) if markets else None
    except Exception as e:
        logger.debug(f"[WS] disk markets 載入失敗 {ccxt_id}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# 工具
# ═══════════════════════════════════════════════════════════════

def _float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _to_ws_sym(exchange: str, orig_sym: str) -> str:
    """ccxt symbol → 交易所 WS 格式"""
    base = orig_sym.split("/")[0]
    if exchange == "binance":
        return f"{base.lower()}usdt"
    if exchange == "bybit":
        return f"{base}USDT"
    if exchange == "okx":
        return f"{base}-USDT-SWAP"
    if exchange == "bitget":
        return f"{base}USDT"
    if exchange in ("mexc", "gateio", "ourbit"):
        return f"{base}_USDT"
    if exchange in ("coinw", "hyperliquid"):
        return base
    if exchange == "bingx":
        return f"{base}-USDT"
    if exchange == "kucoinfutures":
        return f"{base}USDTM"
    if exchange == "aster":
        return f"{base.lower()}usdt"
    if exchange == "bitmart":
        return f"{base}USDT"
    if exchange == "deepcoin":
        return f"{base}USDT"
    return base


def _upd(exchange: str, norm_sym: str, orig_sym: str, **kw) -> dict:
    d = {
        "exchange": exchange,
        "symbol": orig_sym,              # 交易所原生 symbol（給 5005 連線用）
        "normalized_symbol": norm_sym,   # 正規化 symbol（給前端分組用）
        "ts": time.time(),
    }
    for k, v in kw.items():
        if v is not None:
            d[k] = v
    return d


# ═══════════════════════════════════════════════════════════════
# 交易所 WS Handler
# 每個 handler 接收 (orig_sym, norm_sym, on_update) 並持續推送
# ═══════════════════════════════════════════════════════════════

async def _ws_binance(orig_sym, norm_sym, on_update):
    """Binance: markPrice@1s + bookTicker

    2026-04-23 起 markPrice 系列 stream 必須走新 endpoint /market/stream，
    bookTicker 仍在舊 /stream，所以開兩條連線各取所需。
    """
    pair = _to_ws_sym("binance", orig_sym)
    mark_url = f"wss://fstream.binance.com/market/stream?streams={pair}@markPrice@1s"
    book_url = f"wss://fstream.binance.com/stream?streams={pair}@bookTicker"
    state = {}

    async def _consume(url, parse_fn):
        async with websockets.connect(url, ping_interval=20) as ws:
            async for raw in ws:
                msg = json.loads(raw)
                parse_fn(msg.get("data", {}))
                if "bid_price" in state and "mark_price" in state:
                    await on_update(_upd("binance", norm_sym, orig_sym, **state))

    def _parse_mark(data):
        state["funding_rate"] = _float(data.get("r"))
        state["mark_price"] = _float(data.get("p"))
        state["index_price"] = _float(data.get("i"))

    def _parse_book(data):
        state["bid_price"] = _float(data.get("b"))
        state["ask_price"] = _float(data.get("a"))

    await asyncio.gather(
        _consume(mark_url, _parse_mark),
        _consume(book_url, _parse_book),
    )


async def _ws_bybit(orig_sym, norm_sym, on_update):
    """Bybit v5: tickers（snapshot + delta 合併）"""
    pair = _to_ws_sym("bybit", orig_sym)
    url = "wss://stream.bybit.com/v5/public/linear"
    state = {}
    async with websockets.connect(url, ping_interval=20) as ws:
        await ws.send(json.dumps({"op": "subscribe", "args": [f"tickers.{pair}"]}))
        async for raw in ws:
            msg = json.loads(raw)
            data = msg.get("data")
            if not data or "topic" not in msg:
                continue
            # snapshot 或 delta，都合併到 state
            for k in ("fundingRate", "markPrice", "indexPrice", "bid1Price", "ask1Price"):
                v = _float(data.get(k))
                if v is not None:
                    state[k] = v
            if state.get("bid1Price") is not None:
                await on_update(_upd("bybit", norm_sym, orig_sym,
                    funding_rate=state.get("fundingRate"),
                    mark_price=state.get("markPrice"),
                    index_price=state.get("indexPrice"),
                    bid_price=state.get("bid1Price"),
                    ask_price=state.get("ask1Price"),
                ))


async def _ws_okx(orig_sym, norm_sym, on_update):
    """OKX: tickers + funding-rate"""
    inst_id = _to_ws_sym("okx", orig_sym)
    url = "wss://ws.okx.com:8443/ws/v5/public"
    state = {}
    async with websockets.connect(url, ping_interval=20) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": [
                {"channel": "tickers", "instId": inst_id},
                {"channel": "funding-rate", "instId": inst_id},
            ]
        }))
        async for raw in ws:
            if raw == "pong":
                continue
            msg = json.loads(raw)
            data_list = msg.get("data")
            if not data_list:
                continue
            d = data_list[0]
            ch = msg.get("arg", {}).get("channel", "")
            if ch == "tickers":
                state["bid_price"] = _float(d.get("bidPx"))
                state["ask_price"] = _float(d.get("askPx"))
                state["mark_price"] = _float(d.get("markPx"))
                state["index_price"] = _float(d.get("idxPx"))
            elif ch == "funding-rate":
                state["funding_rate"] = _float(d.get("fundingRate"))
            if "bid_price" in state:
                await on_update(_upd("okx", norm_sym, orig_sym, **state))


async def _ws_bitget(orig_sym, norm_sym, on_update):
    """Bitget v2: ticker"""
    pair = _to_ws_sym("bitget", orig_sym)
    url = "wss://ws.bitget.com/v2/ws/public"
    async with websockets.connect(url, ping_interval=None) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": [{"instType": "USDT-FUTURES", "channel": "ticker", "instId": pair}]
        }))

        async def _ping():
            while True:
                await asyncio.sleep(25)
                try:
                    await ws.send("ping")
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping())
        try:
            async for raw in ws:
                if raw == "pong":
                    continue
                msg = json.loads(raw)
                data_list = msg.get("data")
                if not data_list:
                    continue
                d = data_list[0]
                await on_update(_upd("bitget", norm_sym, orig_sym,
                    funding_rate=_float(d.get("fundingRate")),
                    mark_price=_float(d.get("markPrice")),
                    index_price=_float(d.get("indexPrice")),
                    bid_price=_float(d.get("bidPr")),
                    ask_price=_float(d.get("askPr")),
                ))
        finally:
            ping_task.cancel()


async def _ws_mexc(orig_sym, norm_sym, on_update):
    """MEXC: sub.ticker"""
    pair = _to_ws_sym("mexc", orig_sym)
    url = "wss://contract.mexc.com/edge"
    async with websockets.connect(url, ping_interval=None) as ws:
        await ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": pair}}))

        async def _ping():
            while True:
                await asyncio.sleep(55)
                try:
                    await ws.send(json.dumps({"method": "ping"}))
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping())
        try:
            async for raw in ws:
                msg = json.loads(raw)
                ch = msg.get("channel", "")
                if ch == "pong":
                    continue
                if ch != "push.ticker":
                    continue
                d = msg.get("data", {})
                await on_update(_upd("mexc", norm_sym, orig_sym,
                    funding_rate=_float(d.get("fundingRate")),
                    mark_price=_float(d.get("fairPrice")),
                    index_price=_float(d.get("indexPrice")),
                    bid_price=_float(d.get("bid1")),
                    ask_price=_float(d.get("ask1")),
                ))
        finally:
            ping_task.cancel()


async def _ws_gateio(orig_sym, norm_sym, on_update):
    """Gate.io: futures.tickers（tickers 不含 bid/ask，用 last 近似）"""
    pair = _to_ws_sym("gateio", orig_sym)
    url = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    async with websockets.connect(url, ping_interval=20) as ws:
        await ws.send(json.dumps({
            "time": int(time.time()),
            "channel": "futures.tickers",
            "event": "subscribe",
            "payload": [pair],
        }))
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("event") != "update":
                continue
            results = msg.get("result", [])
            if not results:
                continue
            d = results[0] if isinstance(results, list) else results
            last = _float(d.get("last"))
            await on_update(_upd("gateio", norm_sym, orig_sym,
                funding_rate=_float(d.get("funding_rate")),
                mark_price=_float(d.get("mark_price")),
                index_price=_float(d.get("index_price")),
                bid_price=last,
                ask_price=last,
            ))


async def _ws_coinw(orig_sym, norm_sym, on_update):
    """CoinW: funding_rate + depth WS（即時費率 + 買一賣一）"""
    base = _to_ws_sym("coinw", orig_sym)
    url = "wss://ws.futurescw.com/perpum"
    state = {}
    async with websockets.connect(url, ping_interval=None, close_timeout=5) as ws:
        # 同時訂閱 funding_rate 和 depth
        for ch_type in ("funding_rate", "depth"):
            await ws.send(json.dumps({
                "event": "sub",
                "params": {"biz": "futures", "type": ch_type, "pairCode": base}
            }))

        async def _ping():
            while True:
                await asyncio.sleep(20)
                try:
                    await ws.send(json.dumps({"event": "ping"}))
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping())
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
                if raw == "pong":
                    continue
                msg = json.loads(raw)

                # 跳過訂閱確認
                if msg.get("channel") == "subscribe":
                    continue

                data = msg.get("data")
                if not data:
                    continue

                msg_type = msg.get("type", "")

                if msg_type == "funding_rate":
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        r = _float(item.get("r"))
                        if r is not None:
                            state["funding_rate"] = r
                else:
                    # depth 推送
                    if isinstance(data, dict):
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])
                        if bids:
                            state["bid_price"] = float(bids[0]["p"])
                        if asks:
                            state["ask_price"] = float(asks[0]["p"])

                if state:
                    await on_update(_upd("coinw", norm_sym, orig_sym, **state))
        except asyncio.TimeoutError:
            pass
        finally:
            ping_task.cancel()


async def _ws_ourbit(orig_sym, norm_sym, on_update):
    """Ourbit: sub.ticker（MEXC 白標，與 MEXC 相同協議）"""
    pair = _to_ws_sym("ourbit", orig_sym)
    url = "wss://futures.ourbit.com/ws"
    async with websockets.connect(url, ping_interval=None) as ws:
        await ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": pair}}))

        async def _ping():
            while True:
                await asyncio.sleep(55)
                try:
                    await ws.send(json.dumps({"method": "ping"}))
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping())
        try:
            async for raw in ws:
                msg = json.loads(raw)
                ch = msg.get("channel", "")
                if ch == "pong":
                    continue
                if ch != "push.ticker":
                    continue
                d = msg.get("data", {})
                await on_update(_upd("ourbit", norm_sym, orig_sym,
                    funding_rate=_float(d.get("fundingRate")),
                    mark_price=_float(d.get("fairPrice")),
                    index_price=_float(d.get("indexPrice")),
                    bid_price=_float(d.get("bid1")),
                    ask_price=_float(d.get("ask1")),
                ))
        finally:
            ping_task.cancel()


class _BingxFundingHub:
    """全域共用的 BingX 資費輪詢（單一 poller，INTERVAL 秒一次 bulk）。

    BingX WS markPrice 不帶 fundingRate，即時資費唯一來源是 REST premiumIndex。
    原實作是「每個開著的詳情頁各建一個 session、帶 symbol 每 2 秒查一次」→ 一個頁面
    每小時 1800 次 REST、且每個 room 一個獨立連線池（limit_per_host 是每 session 各算），
    連線殘骸累積數小時不散，是本機網路變慢的主因（實測單一 BingX host 133 條）。
    改為全程序一支不帶 symbol 的 bulk premiumIndex（一次回全市場 ~714 檔），各房間從快取
    取用：請求量與開幾個頁面無關，連線收斂到 1~2 條。
    """

    URL = "https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex"
    # bulk 是重量級呼叫（一次回全市場 ~714 檔），且 BingX 對頻率敏感（會整支端點回 100410
    # 暫時停用）。5 秒對「資費顯示」的新鮮度已無感差異，但請求量比原本每頁 2 秒低很多。
    INTERVAL = 5

    def __init__(self):
        self._subs: dict[str, set] = {}      # pair -> set(callback)
        self._cache: dict[str, dict] = {}    # pair -> premiumIndex item
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def subscribe(self, pair: str, cb) -> None:
        async with self._lock:
            self._subs.setdefault(pair, set()).add(cb)
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())
        # 不預推快取：快取沒有時效標記，poller 在「所有 BingX 詳情頁都關閉」時會停，
        # 隔數小時再開頁時預推會把陳舊值經 _on_update 標成 live=True + 新 ts 送給前端，
        # 前端無從分辨。等下一輪（INTERVAL 秒內）的真實資料即可。

    async def unsubscribe(self, pair: str, cb) -> None:
        async with self._lock:
            subs = self._subs.get(pair)
            if subs:
                subs.discard(cb)
                if not subs:
                    self._subs.pop(pair, None)
            # 沒人訂閱就停掉輪詢（詳情頁全關 → 不再打 BingX）
            if not self._subs and self._task and not self._task.done():
                self._task.cancel()
                self._task = None

    async def _run(self) -> None:
        async with make_session(10) as sess:
            while True:
                try:
                    async with sess.get(self.URL) as r:
                        d = await r.json()
                    if d.get("code") == 0:
                        items = d.get("data") or []
                        if isinstance(items, dict):
                            items = [items]
                        for it in items:
                            sym = it.get("symbol")
                            if sym:
                                self._cache[sym] = it
                        for pair, cbs in list(self._subs.items()):
                            data = self._cache.get(pair)
                            if not data:
                                continue
                            for cb in list(cbs):
                                try:
                                    await cb(data)
                                except Exception:
                                    pass
                    else:
                        # 不可靜音：BingX 會整支端點回 100410 停用數小時，靜音的話畫面只會是
                        # 「資費不再更新」而 log 一個字都沒有（同 bingx_exchange._fetch_bulk 的結論）
                        logger.warning(f"[WS] bingx bulk funding code={d.get('code')} msg={str(d.get('msg'))[:60]}")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"[WS] bingx bulk funding poll 失敗: {e}")
                await asyncio.sleep(self.INTERVAL)


_bingx_funding_hub = _BingxFundingHub()


async def _ws_bingx(orig_sym, norm_sym, on_update):
    """BingX: markPrice + bookTicker 走 WS（需 gzip），即時資費走共用 bulk poller。
    BingX WS markPrice 不帶 fundingRate（payload 只有 e/E/s/p），即時資費的唯一來源是
    REST premiumIndex 的 lastFundingRate；改由 _BingxFundingHub 全域共用輪詢供應。
    不輪詢的話 LIVE 的資費會凍結在開面板當下的掃描快照值。"""
    pair = _to_ws_sym("bingx", orig_sym)

    async def _on_funding(d):
        rate = _float(d.get("lastFundingRate"))
        if rate is None:
            return
        # 不帶 funding_interval_h：_on_update 一律用掃描快取的值覆蓋（且與此處同源同值）
        upd = {"funding_rate": rate}
        mk = _float(d.get("markPrice"))
        if mk is not None:
            upd["mark_price"] = mk
        ix = _float(d.get("indexPrice"))
        if ix is not None:
            upd["index_price"] = ix
        await on_update(_upd("bingx", norm_sym, orig_sym, **upd))

    async def _ws_loop():
        url = "wss://open-api-swap.bingx.com/swap-market"
        state = {}
        async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
            for stream in (f"{pair}@markPrice", f"{pair}@bookTicker"):
                await ws.send(json.dumps({
                    "id": str(uuid.uuid4()),
                    "reqType": "sub",
                    "dataType": stream,
                }))
            async for msg in ws:
                try:
                    if isinstance(msg, bytes):
                        msg = gzip.decompress(msg).decode()
                    if not msg.strip():
                        continue
                    d = json.loads(msg)
                    if d.get("ping"):
                        await ws.send(json.dumps({"pong": d["ping"]}))
                        continue
                    inner = d.get("data")
                    dtype = d.get("dataType", "")
                    if not inner:
                        continue
                    if "markPrice" in dtype:
                        state["mark_price"] = _float(inner.get("p"))
                    elif "bookTicker" in dtype:
                        state["bid_price"] = _float(inner.get("b"))
                        state["ask_price"] = _float(inner.get("a"))
                    if "bid_price" in state:
                        await on_update(_upd("bingx", norm_sym, orig_sym, **state))
                except Exception:
                    pass

    # 資費走全域共用 bulk poller（不再每個房間各建 session 輪詢）
    await _bingx_funding_hub.subscribe(pair, _on_funding)
    try:
        await _ws_loop()
    finally:
        await _bingx_funding_hub.unsubscribe(pair, _on_funding)


async def _ws_kucoin(orig_sym, norm_sym, on_update):
    """KuCoin: tickerV2 (bid/ask) + instrument (markPrice/indexPrice/fundingRate)
    需先 POST bullet-public 取得 token"""
    pair = _to_ws_sym("kucoinfutures", orig_sym)
    # 取得 WS token
    async with make_session() as sess:
        async with sess.post(
            "https://api-futures.kucoin.com/api/v1/bullet-public"
        ) as resp:
            body = await resp.json()
            token = body["data"]["token"]
            ep = body["data"]["instanceServers"][0]["endpoint"]

    url = f"{ep}?token={token}&connectId={uuid.uuid4()}"
    state = {}
    async with websockets.connect(url, ping_interval=15) as ws:
        # 讀取 welcome
        await asyncio.wait_for(ws.recv(), timeout=5)
        # 訂閱 tickerV2 + instrument
        for topic in (
            f"/contractMarket/tickerV2:{pair}",
            f"/contract/instrument:{pair}",
        ):
            await ws.send(json.dumps({
                "id": str(uuid.uuid4()),
                "type": "subscribe",
                "topic": topic,
                "privateChannel": False,
                "response": True,
            }))

        async def _ping():
            while True:
                await asyncio.sleep(15)
                try:
                    await ws.send(json.dumps({
                        "id": str(uuid.uuid4()),
                        "type": "ping",
                    }))
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping())
        try:
            async for raw in ws:
                msg = json.loads(raw)
                mtype = msg.get("type", "")
                if mtype in ("pong", "ack", "welcome"):
                    continue
                subj = msg.get("subject", "")
                data = msg.get("data")
                if not data:
                    continue
                if subj == "tickerV2":
                    state["bid_price"] = _float(data.get("bestBidPrice"))
                    state["ask_price"] = _float(data.get("bestAskPrice"))
                elif subj == "mark.index.price":
                    state["mark_price"] = _float(data.get("markPrice"))
                    state["index_price"] = _float(data.get("indexPrice"))
                elif subj == "funding.rate":
                    state["funding_rate"] = _float(data.get("fundingRate"))
                if "bid_price" in state:
                    await on_update(_upd("kucoinfutures", norm_sym, orig_sym, **state))
        finally:
            ping_task.cancel()


async def _ws_aster(orig_sym, norm_sym, on_update):
    """Aster: Binance 相容格式（markPrice + bookTicker）"""
    pair = _to_ws_sym("aster", orig_sym)
    url = f"wss://fstream.asterdex.com/ws"
    state = {}
    async with websockets.connect(url, ping_interval=20) as ws:
        await ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [f"{pair}@markPrice@1s", f"{pair}@bookTicker"],
            "id": 1,
        }))
        async for raw in ws:
            msg = json.loads(raw)
            if "result" in msg:
                continue
            e = msg.get("e", "")
            if e == "markPriceUpdate":
                state["funding_rate"] = _float(msg.get("r"))
                state["mark_price"] = _float(msg.get("p"))
                state["index_price"] = _float(msg.get("i"))
            elif e == "bookTicker":
                state["bid_price"] = _float(msg.get("b"))
                state["ask_price"] = _float(msg.get("a"))
            if "bid_price" in state:
                await on_update(_upd("aster", norm_sym, orig_sym, **state))


class _HyperliquidWsHub:
    """全域共用的 Hyperliquid WS（單一連線多路訂閱 l2Book）。

    Hyperliquid 官方支援一條連線訂閱多個 coin，但原實作是「每個幣種各開一條 WS」到同一個
    host，實測累積 68 條（Trade.xyz 是同 host 白標，一起共用）。改為單一連線多路訂閱，
    連線數與開幾個幣種無關（1 條），並在斷線時自動重連並重新訂閱全部 coin。
    """

    URL = "wss://api.hyperliquid.xyz/ws"

    def __init__(self):
        self._subs: dict[str, set] = {}      # coin -> set(callback)
        self._task: asyncio.Task | None = None
        self._ws = None
        self._lock = asyncio.Lock()

    async def subscribe(self, coin: str, cb) -> None:
        async with self._lock:
            is_new_coin = coin not in self._subs
            self._subs.setdefault(coin, set()).add(cb)
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())
            elif is_new_coin and self._ws is not None:
                try:
                    await self._send(self._ws, "subscribe", coin)
                except Exception:
                    pass  # 送不出去代表連線正在重建，重連時會補訂閱

    async def unsubscribe(self, coin: str, cb) -> None:
        async with self._lock:
            subs = self._subs.get(coin)
            if subs:
                subs.discard(cb)
                if not subs:
                    self._subs.pop(coin, None)
                    if self._ws is not None:
                        try:
                            await self._send(self._ws, "unsubscribe", coin)
                        except Exception:
                            pass
            if not self._subs and self._task and not self._task.done():
                self._task.cancel()
                self._task = None

    @staticmethod
    async def _send(ws, method: str, coin: str) -> None:
        # 加逾時：subscribe/unsubscribe 是在持有全域 _lock 時呼叫，半死連線（寫緩衝滿）
        # 會讓 send 一直掛著並卡住所有幣種詳情頁的開啟/關閉。
        await asyncio.wait_for(ws.send(json.dumps({
            "method": method,
            "subscription": {"type": "l2Book", "coin": coin},
        })), timeout=2)

    async def _run(self) -> None:
        backoff = 3
        while True:
            ws_local = None
            try:
                async with websockets.connect(self.URL, ping_interval=20) as ws:
                    ws_local = ws
                    self._ws = ws
                    for coin in list(self._subs):
                        await self._send(ws, "subscribe", coin)
                    backoff = 3
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if msg.get("channel") != "l2Book":
                            continue
                        data = msg.get("data") or {}
                        coin = data.get("coin")
                        cbs = self._subs.get(coin)
                        if not cbs:
                            continue
                        for cb in list(cbs):
                            try:
                                await cb(data)
                            except Exception:
                                pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # 這兩所的 handler 卡在 Event().wait() 永不 raise，_run_handler 的斷線
                # 告警對它們不會觸發 → 斷線訊號只剩這裡，必須是 warning 才看得見。
                logger.warning(f"[WS] hyperliquid hub 斷線: {e}")
            finally:
                # 只有「self._ws 還是自己這條」才清空：舊 task 收尾（關閉握手可能耗到
                # close_timeout）期間，新 task 可能已連上並設好 self._ws，無條件清空會把
                # 活連線洗成 None → 之後新訂閱的 coin 永遠送不出 subscribe、永久收不到報價。
                if self._ws is ws_local:
                    self._ws = None
            if not self._subs:
                return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


_hyperliquid_hub = _HyperliquidWsHub()


def _hl_make_callback(exchange_id, norm_sym, orig_sym, on_update):
    """把 l2Book data 轉成 bid/ask 更新（hyperliquid 與 tradexyz 共用同一份解析）"""
    state = {}

    async def _cb(data):
        levels = data.get("levels", [[], []])
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []
        if bids:
            state["bid_price"] = _float(bids[0].get("px"))
        if asks:
            state["ask_price"] = _float(asks[0].get("px"))
        if state:
            await on_update(_upd(exchange_id, norm_sym, orig_sym, **state))

    return _cb


async def _ws_hyperliquid(orig_sym, norm_sym, on_update):
    """Hyperliquid: l2Book（bid/ask 即時更新），走全域共用連線多路訂閱"""
    coin = _to_ws_sym("hyperliquid", orig_sym)
    cb = _hl_make_callback("hyperliquid", norm_sym, orig_sym, on_update)
    await _hyperliquid_hub.subscribe(coin, cb)
    try:
        await asyncio.Event().wait()   # 由 _run_handler 取消時結束
    finally:
        await _hyperliquid_hub.unsubscribe(coin, cb)


async def _ws_bitmart(orig_sym, norm_sym, on_update):
    """BitMart V2: futures/ticker + futures/fundingRate"""
    pair = _to_ws_sym("bitmart", orig_sym)
    url = "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1"
    state = {}
    async with websockets.connect(url, ping_interval=None) as ws:
        await ws.send(json.dumps({
            "action": "subscribe",
            "args": [f"futures/ticker:{pair}", f"futures/fundingRate:{pair}"],
        }))

        async def _ping():
            while True:
                await asyncio.sleep(15)
                try:
                    await ws.send(json.dumps({"action": "ping"}))
                except Exception:
                    break

        ping_task = asyncio.create_task(_ping())
        try:
            async for raw in ws:
                if raw == "pong":
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                group = msg.get("group", "")
                data = msg.get("data")
                if not data:
                    continue
                if group.startswith("futures/ticker"):
                    state["bid_price"] = _float(data.get("bid_price"))
                    state["ask_price"] = _float(data.get("ask_price"))
                    state["mark_price"] = _float(data.get("mark_price"))
                    state["index_price"] = _float(data.get("index_price"))
                elif group.startswith("futures/fundingRate"):
                    # BitMart: nextFundingRate = 即將結算那一期（App 顯示），fundingRate = 已結算歷史
                    rate = _float(data.get("nextFundingRate"))
                    if rate is None:
                        rate = _float(data.get("fundingRate"))
                    state["funding_rate"] = rate
                if "bid_price" in state:
                    await on_update(_upd("bitmart", norm_sym, orig_sym, **state))
        finally:
            ping_task.cancel()


async def _ws_deepcoin(orig_sym, norm_sym, on_update):
    """DeepCoin Public WS v2: market topic（每秒推 funding/mark/index/bid/ask/last）

    訊息欄位：I=instrument, U=ts_ms, E=fundingRate, M=markPrice, D=indexPrice,
              N=last, BP1/AP1=bid/ask, PF=nextSettleTime(秒)
    """
    pair = _to_ws_sym("deepcoin", orig_sym)
    url = "wss://stream.deepcoin.com/streamlet/trade/public/swap?platform=api&version=v2"
    state = {}
    async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
        await ws.send(json.dumps({
            "Action": "1", "Symbol": pair, "Topic": "market",
            "LocalNo": 1, "ResumeNo": -1,
        }))
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("a") != "PO":
                continue  # 訂閱確認 / 其他訊息略過
            for d in (msg.get("d") or []):
                fr = _float(d.get("E"))
                if fr is not None:
                    state["funding_rate"] = fr
                mk = _float(d.get("M"))
                if mk is not None:
                    state["mark_price"] = mk
                idx = _float(d.get("D"))
                if idx is not None:
                    state["index_price"] = idx
                bp = _float(d.get("BP1"))
                if bp is not None:
                    state["bid_price"] = bp
                ap = _float(d.get("AP1"))
                if ap is not None:
                    state["ask_price"] = ap
                if "bid_price" in state:
                    await on_update(_upd("deepcoin", norm_sym, orig_sym, **state))


async def _ws_lbank(orig_sym, norm_sym, on_update):
    """LBank: 合約 WS（真實端點 wss://uuws.lbank.com/ws/v3）未公開文檔，
    各種訂閱格式皆被拒(回 {w,x,z:11} 後關閉連線, 實測 2026-06-12)，故改 REST 每 3 秒輪詢。
    這是「WS 優先」規則的刻意例外；若日後取得官方 WS 訂閱格式應改回。
    費率/標記/指數從批次 marketData 取；bid/ask 從單合約訂單簿 marketOrder 取（無批次盤口）。
    """
    base = orig_sym.split("/")[0]
    raw = f"{base}USDT"
    md_url = "https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketData"
    ob_url = "https://lbkperp.lbank.com/cfd/openApi/v1/pub/marketOrder"
    async with make_session(10) as session:
        while True:
            try:
                async def _md():
                    async with session.get(md_url, params={"productGroup": "SwapU"}) as r:
                        return await r.json()

                async def _ob():
                    async with session.get(ob_url, params={"symbol": raw, "depth": "5"}) as r:
                        return await r.json()

                md_d, ob_d = await asyncio.gather(_md(), _ob(), return_exceptions=True)

                bid = ask = None
                if not isinstance(ob_d, Exception):
                    data = (ob_d or {}).get("data") or {}
                    asks = data.get("asks") or []
                    bids = data.get("bids") or []
                    ask = _float(asks[0]["price"]) if asks else None
                    bid = _float(bids[0]["price"]) if bids else None

                if not isinstance(md_d, Exception):
                    for m in (md_d or {}).get("data") or []:
                        if m.get("symbol") == raw:
                            last = _float(m.get("lastPrice"))
                            await on_update(_upd("lbank", norm_sym, orig_sym,
                                funding_rate=_float(m.get("fundingRate")),
                                mark_price=_float(m.get("markedPrice")),
                                index_price=_float(m.get("underlyingPrice")),
                                bid_price=bid if bid is not None else last,
                                ask_price=ask if ask is not None else last,
                            ))
                            break
            except Exception as e:
                logger.debug(f"[WS] lbank poll {orig_sym} 失敗: {e}")
            await asyncio.sleep(3)


async def _ws_tradexyz(orig_sym, norm_sym, on_update):
    """Trade.xyz: Hyperliquid 白標（coin 名稱帶 xyz: 前綴），與 hyperliquid 共用同一條 WS"""
    coin = f"xyz:{_to_ws_sym('hyperliquid', orig_sym)}"
    cb = _hl_make_callback("tradexyz", norm_sym, orig_sym, on_update)
    await _hyperliquid_hub.subscribe(coin, cb)
    try:
        await asyncio.Event().wait()   # 由 _run_handler 取消時結束
    finally:
        await _hyperliquid_hub.unsubscribe(coin, cb)


# ═══════════════════════════════════════════════════════════════
# Kraken / Deribit —— 單一連線多路訂閱 hub
#
# 兩家官方 WS 都支援「一條連線訂閱多個商品」，所以照 Hyperliquid 的做法共用連線，
# 連線數與開幾個幣種詳情無關（各 1 條）。專案鐵則：WS 走 websockets、不套 make_session。
# ═══════════════════════════════════════════════════════════════

class _MuxWsHub:
    """多路訂閱 WS hub 的共用骨架（Kraken / Deribit 各繼承一份）。

    子類只要提供 URL、訂閱/退訂訊息、以及「把收到的訊息路由到哪個 key」。
    重連時會自動補訂閱全部 key。
    """

    URL = ""
    NAME = ""

    def __init__(self):
        self._subs: dict[str, set] = {}      # key(交易所商品代號) -> set(callback)
        self._task: asyncio.Task | None = None
        self._ws = None
        self._lock = asyncio.Lock()

    # ── 子類實作 ──────────────────────────────────────────
    # _sub_msg/_unsub_msg 可回單一字串（一則訊息帶多個 key，如 Kraken/Deribit），
    # 也可回字串 list（協定一則只能帶一個 key，如 Lighter 的 market_stats/{id}）。
    def _sub_msg(self, keys: list[str]) -> str | list[str]:
        raise NotImplementedError

    def _unsub_msg(self, keys: list[str]) -> str | list[str]:
        raise NotImplementedError

    def _route(self, msg: dict):
        """回傳 (key, data)；不是行情訊息就回 (None, None)"""
        raise NotImplementedError

    # ── 共用邏輯 ──────────────────────────────────────────
    async def subscribe(self, key: str, cb) -> None:
        async with self._lock:
            is_new = key not in self._subs
            self._subs.setdefault(key, set()).add(cb)
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())
            elif is_new and self._ws is not None:
                try:
                    await self._send(self._ws, self._sub_msg([key]))
                except Exception:
                    pass          # 送不出去代表連線正在重建，重連時會補訂閱

    async def unsubscribe(self, key: str, cb) -> None:
        async with self._lock:
            subs = self._subs.get(key)
            if subs:
                subs.discard(cb)
                if not subs:
                    self._subs.pop(key, None)
                    if self._ws is not None:
                        try:
                            await self._send(self._ws, self._unsub_msg([key]))
                        except Exception:
                            pass
            if not self._subs and self._task and not self._task.done():
                self._task.cancel()
                self._task = None

    @staticmethod
    async def _send(ws, payload: str | list[str]) -> None:
        # 加逾時：subscribe/unsubscribe 是在持有 _lock 時呼叫，半死連線（寫緩衝滿）
        # 會讓 send 一直掛著並卡住所有幣種詳情頁的開啟/關閉。
        for one in ([payload] if isinstance(payload, str) else payload):
            await asyncio.wait_for(ws.send(one), timeout=2)

    async def _run(self) -> None:
        backoff = 3
        while True:
            ws_local = None
            try:
                async with websockets.connect(self.URL, ping_interval=20) as ws:
                    ws_local = ws
                    self._ws = ws
                    if self._subs:
                        await self._send(ws, self._sub_msg(list(self._subs)))
                    backoff = 3
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        key, data = self._route(msg)
                        cbs = self._subs.get(key) if key else None
                        if not cbs:
                            continue
                        for cb in list(cbs):
                            try:
                                await cb(data)
                            except Exception:
                                pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # handler 端是 Event().wait() 永不 raise，_run_handler 的斷線告警對它們
                # 不會觸發 → 斷線訊號只剩這裡，必須是 warning 才看得見。
                logger.warning(f"[WS] {self.NAME} hub 斷線: {e}")
            finally:
                # 只有「self._ws 還是自己這條」才清空：舊 task 收尾期間新 task 可能已連上
                # 並設好 self._ws，無條件清空會把活連線洗成 None → 新訂閱永遠送不出去。
                if self._ws is ws_local:
                    self._ws = None
            if not self._subs:
                return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


class _KrakenWsHub(_MuxWsHub):
    """Kraken Futures ticker feed（官方文檔：wss://futures.kraken.com/ws/v1，feed=ticker）"""

    URL = "wss://futures.kraken.com/ws/v1"
    NAME = "kraken"

    def _sub_msg(self, keys):
        return json.dumps({"event": "subscribe", "feed": "ticker", "product_ids": keys})

    def _unsub_msg(self, keys):
        return json.dumps({"event": "unsubscribe", "feed": "ticker", "product_ids": keys})

    def _route(self, msg):
        # 行情訊息帶 feed=ticker 與 product_id；event 類（info/subscribed）沒有 product_id
        if msg.get("feed") == "ticker" and msg.get("product_id"):
            return msg["product_id"], msg
        return None, None


class _DeribitWsHub(_MuxWsHub):
    """Deribit ticker channel（官方 JSON-RPC：wss://www.deribit.com/ws/api/v2）

    interval 用 agg2（聚合）而非 100ms：100ms 是每秒 10 筆的消防栓，對只要顯示報價的
    用途純屬浪費；raw 需要登入（未授權會回 13778 raw_subscriptions_not_available）。
    """

    URL = "wss://www.deribit.com/ws/api/v2"
    NAME = "deribit"
    INTERVAL = "agg2"

    def __init__(self):
        super().__init__()
        self._msg_id = 0

    def _chan(self, key: str) -> str:
        return f"ticker.{key}.{self.INTERVAL}"

    def _rpc(self, method: str, keys) -> str:
        self._msg_id += 1
        return json.dumps({"jsonrpc": "2.0", "id": self._msg_id, "method": method,
                           "params": {"channels": [self._chan(k) for k in keys]}})

    def _sub_msg(self, keys):
        return self._rpc("public/subscribe", keys)

    def _unsub_msg(self, keys):
        return self._rpc("public/unsubscribe", keys)

    def _route(self, msg):
        if msg.get("method") != "subscription":
            return None, None                       # 訂閱回執/心跳
        data = (msg.get("params") or {}).get("data") or {}
        inst = data.get("instrument_name")
        return (inst, data) if inst else (None, None)


_kraken_hub = _KrakenWsHub()
_deribit_hub = _DeribitWsHub()


def _kraken_product(orig_sym: str) -> str:
    """掃描器 symbol → Kraken 商品代號：BTC/USD:USD → PF_XBTUSD（Kraken 用 XBT 表示 BTC）"""
    base = orig_sym.split("/")[0].upper()
    return f"PF_{'XBT' if base == 'BTC' else base}USD"


def _deribit_instrument(orig_sym: str) -> str:
    """掃描器 symbol → Deribit 商品名：
    BTC/USD:USD → BTC-PERPETUAL（inverse）；SOL/USDC:USDC → SOL_USDC-PERPETUAL（線性）"""
    base, _, rest = orig_sym.partition("/")
    quote = rest.split(":")[0].upper() if rest else "USD"
    base = base.upper()
    return f"{base}-PERPETUAL" if quote == "USD" else f"{base}_{quote}-PERPETUAL"


async def _ws_kraken(orig_sym, norm_sym, on_update):
    """Kraken Futures: ticker feed（共用連線多路訂閱）

    費率直接用官方的 relative_funding_rate（每小時相對費率）。
    REST 那條路徑沒有這個欄位，只能自己算 fundingRate / indexPrice（絕對值換相對值），
    兩者差異僅來自 index 取樣時點，用官方欄位更準。取不到才退回自己算。
    """
    product = _kraken_product(orig_sym)
    state = {}

    async def _cb(d):
        rate = _float(d.get("relative_funding_rate"))
        if rate is None:
            abs_fr, idx = _float(d.get("funding_rate")), _float(d.get("index"))
            rate = (abs_fr / idx) if (abs_fr is not None and idx) else None
        for k, v in (("funding_rate", rate),
                     ("mark_price", _float(d.get("markPrice"))),
                     ("index_price", _float(d.get("index"))),
                     ("bid_price", _float(d.get("bid"))),
                     ("ask_price", _float(d.get("ask")))):
            if v is not None:
                state[k] = v
        if state.get("bid_price") is not None:
            await on_update(_upd("kraken", norm_sym, orig_sym, **state))

    await _kraken_hub.subscribe(product, _cb)
    try:
        await asyncio.Event().wait()   # 由 _run_handler 取消時結束
    finally:
        await _kraken_hub.unsubscribe(product, _cb)


async def _ws_deribit(orig_sym, norm_sym, on_update):
    """Deribit: ticker channel（共用連線多路訂閱）

    費率用 funding_8h（Deribit 連續結算、官方以 8h 口徑報價），與 REST 那條路徑同一個
    欄位、同一個 interval=8h，WS/REST 切換時數值不會跳。
    index 用 index_price（REST 那邊取 estimated_delivery_price，永續兩者同值）。
    """
    inst = _deribit_instrument(orig_sym)
    state = {}

    async def _cb(d):
        for k, v in (("funding_rate", _float(d.get("funding_8h"))),
                     ("mark_price", _float(d.get("mark_price"))),
                     ("index_price", _float(d.get("index_price"))),
                     ("bid_price", _float(d.get("best_bid_price"))),
                     ("ask_price", _float(d.get("best_ask_price")))):
            if v is not None:
                state[k] = v
        if state.get("bid_price") is not None:
            await on_update(_upd("deribit", norm_sym, orig_sym, **state))

    await _deribit_hub.subscribe(inst, _cb)
    try:
        await asyncio.Event().wait()   # 由 _run_handler 取消時結束
    finally:
        await _deribit_hub.unsubscribe(inst, _cb)


class _LighterWsHub(_MuxWsHub):
    """Lighter market_stats feed（主站與 RH Chain 共用實作，只差網域）

    頻道是 `market_stats/{market_id}`，所以 key 用 market_id 字串（不是 symbol）。
    回推的訊息 channel 長 `market_stats:1`，type 是 subscribed/market_stats 或 update/market_stats。
    """

    URL = "wss://mainnet.zklighter.elliot.ai/stream"
    NAME = "lighter"

    def _sub_msg(self, keys):
        # 官方協定一則訊息只能帶一個 channel → 回 list，_send 會逐一送出
        return [json.dumps({"type": "subscribe", "channel": f"market_stats/{k}"}) for k in keys]

    def _unsub_msg(self, keys):
        return [json.dumps({"type": "unsubscribe", "channel": f"market_stats/{k}"}) for k in keys]

    def _route(self, msg):
        if not str(msg.get("type", "")).endswith("market_stats"):
            return (None, None)
        stats = msg.get("market_stats")
        if not isinstance(stats, dict):
            return (None, None)
        # 單一市場訂閱回的是攤平的 dict，直接讀 market_id
        mid = stats.get("market_id")
        if mid is None:
            chan = str(msg.get("channel", ""))          # "market_stats:1"
            mid = chan.split(":")[-1] if ":" in chan else None
        return (str(mid), stats) if mid is not None else (None, None)


class _LighterRhWsHub(_LighterWsHub):
    """Lighter on Robinhood Chain：獨立部署，另一條 WS 連線、market_id 也是獨立編號"""

    URL = "wss://api.rh.lighter.xyz/stream"
    NAME = "lighter_rh"


_lighter_hub = _LighterWsHub()
_lighter_rh_hub = _LighterRhWsHub()

# 兩個部署的 market_id 各自編號、互不相通，所以對照表要按 rest_base 分開存
_lighter_market_ids: dict[str, dict[str, int]] = {}
_lighter_ids_lock = asyncio.Lock()

# exchange id → (REST base, WS hub)
_LIGHTER_VENUES = {
    "lighter": ("https://mainnet.zklighter.elliot.ai/api/v1", _lighter_hub),
    "lighter_rh": ("https://api.rh.lighter.xyz/api/v1", _lighter_rh_hub),
}


async def _lighter_market_id(rest_base: str, orig_sym: str) -> str | None:
    """掃描器 symbol → Lighter market_id（Lighter 的 WS 頻道用整數 id 定址，不是 symbol）"""
    base = orig_sym.split("/")[0].upper()
    async with _lighter_ids_lock:
        if rest_base not in _lighter_market_ids:
            table: dict[str, int] = {}
            try:
                async with make_session(20) as session:   # REST 一律走 make_session
                    async with session.get(f"{rest_base}/orderBookDetails") as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json()
                for d in (data.get("order_book_details") or []):
                    if d.get("symbol") and d.get("market_id") is not None:
                        table[str(d["symbol"]).upper()] = d["market_id"]
            except Exception as e:
                logger.debug(f"[WS] lighter market_id 對照表取得失敗 {rest_base}: {e}")
                return None
            _lighter_market_ids[rest_base] = table
    mid = _lighter_market_ids[rest_base].get(base)
    return str(mid) if mid is not None else None


async def _ws_lighter_generic(exchange_id, orig_sym, norm_sym, on_update):
    """Lighter market_stats channel（主站與 RH Chain 共用；共用連線多路訂閱）

    ⚠ 費率口徑必須跟 exchanges/lighter_exchange.py 的掃描路徑一致，否則 WS/REST 切換數值會跳：
      current_funding_rate 是【每小時百分比】（BTC "0.0010" = 0.001%/h）→ 除以 100 換成小數。
      同 dict 裡那個叫 funding_rate 的欄位是【上一小時已結算】的值，不是當前費率，不能用。
    """
    rest_base, hub = _LIGHTER_VENUES[exchange_id]
    market_id = await _lighter_market_id(rest_base, orig_sym)
    if market_id is None:
        logger.debug(f"[WS] {exchange_id} 找不到 {orig_sym} 的 market_id，跳過")
        return
    state = {}

    async def _cb(d):
        cur_pct = _float(d.get("current_funding_rate"))
        for k, v in (("funding_rate", (cur_pct / 100.0) if cur_pct is not None else None),
                     ("mark_price", _float(d.get("mark_price"))),
                     ("index_price", _float(d.get("index_price"))),
                     ("bid_price", _float(d.get("best_bid_price"))),
                     ("ask_price", _float(d.get("best_ask_price")))):
            if v is not None:
                state[k] = v
        if state.get("bid_price") is not None:
            await on_update(_upd(exchange_id, norm_sym, orig_sym, **state))

    await hub.subscribe(market_id, _cb)
    try:
        await asyncio.Event().wait()   # 由 _run_handler 取消時結束
    finally:
        await hub.unsubscribe(market_id, _cb)


async def _ws_lighter(orig_sym, norm_sym, on_update):
    await _ws_lighter_generic("lighter", orig_sym, norm_sym, on_update)


async def _ws_lighter_rh(orig_sym, norm_sym, on_update):
    await _ws_lighter_generic("lighter_rh", orig_sym, norm_sym, on_update)


# ═══════════════════════════════════════════════════════════════
# 現貨 WS Handler（ccxt.pro watch_order_book 通用實作）
# 用於幣種詳情的「納入現貨」套利建議，現貨只能當做多方
# exchange id 統一用 "{base}_spot" 格式
# ═══════════════════════════════════════════════════════════════

# 現貨 exchange id → 合約 exchange id（用於查 markets、aliases）
_SPOT_TO_FUTURES = {
    "binance_spot": "binance",
    "bybit_spot": "bybit",
    "okx_spot": "okx",
    "bitget_spot": "bitget",
    "mexc_spot": "mexc",
    "gateio_spot": "gateio",
    "bingx_spot": "bingx",
}

# ccxt.pro 現貨實例 singleton（每個交易所共用一個，不同幣種訂閱會疊加）
_SPOT_CCXT_EXCHANGES: dict[str, object] = {}


def _rwa_spot_map(base: str) -> dict[str, list[str]]:
    """RWA 標的在各所的現貨代號：{合約所 id: [該所現貨 base, ...]}，非 RWA 回空。

    代幣化股票／商品的現貨代號各所不同（黃金 GLD 在 Gate 叫 GLDX、AMZN 在 Binance
    叫 AMZNB、在 OKX 叫 XAMZN），只用 f"{base}/USDT" 找一定找不到。
    rwa_universe.json 已存有各所現貨代號（掃描器定期重建），直接沿用同一份對照。
    """
    try:
        from services import rwa_arb
        meta = ((rwa_arb.load_universe() or {}).get("universe") or {}).get(base.upper()) or {}
    except Exception as e:
        logger.debug(f"[WS] RWA universe 載入失敗: {e}")
        return {}
    out = {}
    for ex_id, raws in (meta.get("spot") or {}).items():
        out[ex_id] = [r.upper() for r in (raws if isinstance(raws, list) else [raws])]
    return out


def _resolve_spot_symbol(markets: dict, base: str, rwa_bases: list[str]) -> str | None:
    """在該所 markets 找出此標的的現貨 ccxt symbol；找不到回 None。

    候選順序：同名 base（原行為）→ RWA universe 記錄的該所現貨代號。
    嚴格過濾 market["spot"]：排除合約 symbol 污染（gateio/bingx 會把 :USDT 合約也載入）。
    """
    for b in [base.upper(), *rwa_bases]:
        m = markets.get(f"{b}/USDT")
        if m and m.get("spot"):
            return f"{b}/USDT"
    return None


def _get_spot_ccxt(ccxt_id: str):
    """取得/建立 ccxt.pro 現貨實例（defaultType=spot）。"""
    ex = _SPOT_CCXT_EXCHANGES.get(ccxt_id)
    if ex is not None:
        return ex
    cls = getattr(ccxtpro, ccxt_id, None)
    if cls is None:
        logger.warning(f"[WS] ccxt.pro 無 {ccxt_id}")
        return None
    ex = cls({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    })
    scope_bybit_markets(ex, ccxt_id)
    _SPOT_CCXT_EXCHANGES[ccxt_id] = ex
    return ex


async def _reset_spot_ccxt(ccxt_id: str):
    """強拆 ccxt.pro spot exchange 並從 cache 移除（仿 5005 _close_exchange_clean）。

    必要性：bingx_spot 等所撞到 'no close frame received or sent' 後，
    底層 aiohttp WS 連線半死；單純 raise 讓 handler 重連無用（共用 ccxt 實例的
    session/connector 還在），必須強拆 session+connector 才能讓所有卡在
    watch_order_book 的 task 拋例外，下次 _get_spot_ccxt 會建全新實例。
    """
    ex = _SPOT_CCXT_EXCHANGES.pop(ccxt_id, None)
    if not ex:
        return
    try:
        if hasattr(ex, "orderbooks"): ex.orderbooks.clear()
        if hasattr(ex, "tickers"): ex.tickers.clear()
        if hasattr(ex, "trades"): ex.trades.clear()
        try:
            await asyncio.wait_for(ex.close(), timeout=5.0)
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"[WS] {ccxt_id} ex.close() 逾時/失敗（{type(e).__name__}），強拆 session")
        if hasattr(ex, "session") and ex.session and not ex.session.closed:
            try:
                await asyncio.wait_for(ex.session.close(), timeout=2.0)
            except Exception:
                pass
        if hasattr(ex, "connector") and ex.connector and not ex.connector.closed:
            try:
                await asyncio.wait_for(ex.connector.close(), timeout=2.0)
            except Exception:
                pass
        await asyncio.sleep(0.25)
    except Exception:
        pass


async def _ws_spot_generic(ccxt_id, spot_id, spot_sym, norm_sym, on_update):
    """ccxt.pro watch_order_book 通用現貨 handler。

    spot_sym: 該所現貨 ccxt symbol（如 'BTC/USDT'，Gate 的黃金是 'GLDX/USDT'）。
    由 _SymbolRoom.enable_spot 對照該所 markets 解析後傳入——各所代號不同，
    這裡不可再自行用 base 拼，否則 RWA 標的一定訂不到。
    """
    ex = _get_spot_ccxt(ccxt_id)
    if ex is None:
        return
    if not getattr(ex, "markets", None):
        try:
            await ex.load_markets()
        except Exception as e:
            # REST 被封（例：MEXC Akamai 403）→ 改讀 scanner 寫的 disk cache
            cached = _load_spot_markets_from_disk(ccxt_id)
            if cached:
                ex.set_markets(cached)
                logger.info(f"[WS] {ccxt_id} load_markets 失敗，已從 disk cache 注入 {len(cached)} 個 markets")
            else:
                logger.warning(f"[WS] {ccxt_id} load_markets 失敗且無 disk cache：{e}")
                return
    # 參考 5005：bybit=50, kucoin=20, 其他=5
    limit = 50 if ccxt_id == "bybit" else (20 if ccxt_id == "kucoin" else 5)
    crossed_count = 0
    while True:
        # 包 timeout：bingx 等所偶發訂閱後不推訊息但 watch 不 raise，靠 timeout 觸發重連
        try:
            ob = await asyncio.wait_for(
                ex.watch_order_book(spot_sym, limit=limit), timeout=60
            )
        except asyncio.TimeoutError:
            # 強拆底層 aiohttp session，讓所有卡在 watch 的 task 都 raise，下次重建全新實例
            await _reset_spot_ccxt(ccxt_id)
            raise RuntimeError(f"{spot_id} {spot_sym} watch_order_book 60s 無更新，已強拆")
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if not bids or not asks:
            continue
        best_bid = _float(bids[0][0])
        best_ask = _float(asks[0][0])
        if best_bid is None or best_ask is None:
            continue
        # Crossed orderbook（ask <= bid）= ccxt 內部 incremental merge 壞掉
        # 跳過壞值不推給前端，並嘗試清 cache 讓 ccxt 重建
        if best_ask <= best_bid:
            crossed_count += 1
            if crossed_count == 1:
                logger.warning(
                    f"[WS] {spot_id} {spot_sym} crossed orderbook "
                    f"(bid={best_bid} ask={best_ask})，清 ccxt cache 重建"
                )
            try:
                if hasattr(ex, "orderbooks") and spot_sym in ex.orderbooks:
                    del ex.orderbooks[spot_sym]
            except Exception:
                pass
            if crossed_count >= 10:
                # 連續 10 次仍 crossed → 強拆 + raise 讓全新實例重訂閱
                await _reset_spot_ccxt(ccxt_id)
                raise RuntimeError(
                    f"{spot_id} {spot_sym} persistent crossed orderbook，已強拆"
                )
            continue
        crossed_count = 0
        await on_update(_upd(spot_id, norm_sym, spot_sym,
            bid_price=best_bid,
            ask_price=best_ask,
        ))


# ccxt.pro 的 exchange id 對應（與合約 id 相同）
_SPOT_CCXT_IDS = {
    "binance_spot": "binance",
    "bybit_spot": "bybit",
    "okx_spot": "okx",
    "bitget_spot": "bitget",
    "mexc_spot": "mexc",
    "gateio_spot": "gateio",
    "bingx_spot": "bingx",
}

# 現貨 exchange id → handler（partial 預綁 ccxt_id / spot_id）
_SPOT_WS_HANDLERS = {
    sid: partial(_ws_spot_generic, cid, sid)
    for sid, cid in _SPOT_CCXT_IDS.items()
}


# 交易所 → WS handler 對照表
_WS_HANDLERS = {
    "binance": _ws_binance,
    "bybit": _ws_bybit,
    "okx": _ws_okx,
    "bitget": _ws_bitget,
    "mexc": _ws_mexc,
    "gateio": _ws_gateio,
    "coinw": _ws_coinw,
    "ourbit": _ws_ourbit,
    "bingx": _ws_bingx,
    "kucoinfutures": _ws_kucoin,
    "aster": _ws_aster,
    "hyperliquid": _ws_hyperliquid,
    "tradexyz": _ws_tradexyz,
    # "bitmart": _ws_bitmart,   # BitMart 已停止營運，不再連線
    "deepcoin": _ws_deepcoin,
    "lbank": _ws_lbank,
    "kraken": _ws_kraken,
    "deribit": _ws_deribit,
    "lighter": _ws_lighter,
    "lighter_rh": _ws_lighter_rh,
}


# ═══════════════════════════════════════════════════════════════
# _SymbolRoom — 管理單一幣種的所有連線
# ═══════════════════════════════════════════════════════════════

class _SymbolRoom:

    def __init__(self, symbol: str, exchange_symbols: dict, scanner):
        self.symbol = symbol
        self.exchange_symbols = exchange_symbols  # {exchange: orig_sym}
        self.scanner = scanner
        self.clients: set = set()
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._latest: dict[str, dict] = {}
        self._last_send: dict[str, float] = {}
        # 從快取讀取 funding_interval_h；詳情頁開著時會隨掃描快取刷新（見 _refresh_intervals_if_stale），
        # 讓滿資費縮週期切換（如 8h→1h）在頁面開著時也能更新、不必重開。
        self._intervals: dict[str, float] = {}
        self._intervals_src = None
        self._refresh_intervals_if_stale()
        # 現貨動態啟用
        self._spot_requestors: set = set()
        self._spot_tasks: list[asyncio.Task] = []
        self._spot_started: bool = False
        self._spot_exchanges_used: list[str] = []
        self._spot_symbols: dict[str, str] = {}   # {現貨所 id: 該所現貨 ccxt symbol}

    async def join(self, ws):
        self.clients.add(ws)
        if not self._running:
            self._start()
        # 傳送初始快照（來自掃描快取 + 已收到的 WS 資料）
        snapshot = self._build_snapshot()
        if snapshot:
            try:
                await ws.send_text(json.dumps({"type": "snapshot", "data": snapshot}))
            except Exception:
                self.clients.discard(ws)

    def _refresh_intervals_if_stale(self):
        """掃描快取更新時（scanner 每輪替換 last_result），重載本幣種各所的 funding_interval_h。
        用物件識別判斷是否換新（O(1)），換了才掃 records，成本攤在每輪掃描一次。"""
        lr = self.scanner.last_result
        if lr is None or lr is self._intervals_src:
            return
        self._intervals_src = lr
        for r in lr.records:
            norm = r.normalized_symbol or r.symbol
            if norm == self.symbol and r.funding_interval_h:
                self._intervals[r.exchange] = r.funding_interval_h

    def leave(self, ws):
        self.clients.discard(ws)
        self._spot_requestors.discard(ws)
        if self._spot_started and not self._spot_requestors:
            self._stop_spot()
        if not self.clients:
            self._stop()

    @property
    def empty(self):
        return not self.clients

    def _build_snapshot(self) -> list[dict]:
        """從掃描快取 + WS 最新資料組合快照"""
        snapshot = {}
        # 先用掃描快取打底
        if self.scanner.last_result:
            for r in self.scanner.last_result.records:
                norm = r.normalized_symbol or r.symbol
                if norm == self.symbol:
                    snapshot[r.exchange] = {
                        "exchange": r.exchange,
                        "symbol": r.symbol,
                        "normalized_symbol": norm,
                        "funding_rate": r.funding_rate,
                        "bid_price": r.bid_price,
                        "ask_price": r.ask_price,
                        "mark_price": r.mark_price,
                        "index_price": r.index_price,
                        "funding_interval_h": r.funding_interval_h,
                        "ts": time.time(),
                        "live": r.exchange in _WS_HANDLERS,
                    }
        # 再用 WS 即時資料覆蓋
        for ex, update in self._latest.items():
            if ex in snapshot:
                for k, v in update.items():
                    if v is not None:
                        snapshot[ex][k] = v
                snapshot[ex]["live"] = True
            else:
                snapshot[ex] = update
        return list(snapshot.values())

    def _start(self):
        self._running = True
        for ex, orig_sym in self.exchange_symbols.items():
            handler = _WS_HANDLERS.get(ex)
            if handler:
                task = asyncio.create_task(
                    self._run_handler(ex, orig_sym, handler),
                    name=f"ws-{ex}-{self.symbol}"
                )
                self._tasks.append(task)
            # 沒有 WS handler 的交易所用掃描快取（已在 snapshot 中）
        ws_count = sum(1 for ex in self.exchange_symbols if ex in _WS_HANDLERS)
        rest_count = len(self.exchange_symbols) - ws_count
        logger.info(
            f"[WS] 開始串流 {self.symbol}"
            f"（WS:{ws_count} 快取:{rest_count}）"
        )

    def _stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
        self._stop_spot()
        self._latest.clear()
        self._last_send.clear()
        logger.info(f"[WS] 停止串流 {self.symbol}")

    def _stop_spot(self):
        for t in self._spot_tasks:
            t.cancel()
        self._spot_tasks.clear()
        for sid in self._spot_exchanges_used:
            self._latest.pop(sid, None)
            self._last_send.pop(sid, None)
        self._spot_exchanges_used = []
        self._spot_symbols = {}
        self._spot_started = False
        self._spot_requestors.clear()

    async def enable_spot(self, ws):
        """client 要求啟用現貨。首次呼叫檢查 markets → 啟動各現貨所 WS。"""
        self._spot_requestors.add(ws)
        if self._spot_started or not self._running:
            if self._spot_started:
                await self._send_spot_snapshot(ws)
            return
        self._spot_started = True

        base = self.symbol.split("/")[0]
        rwa_map = _rwa_spot_map(base)
        active = []
        for spot_id, fut_id in _SPOT_TO_FUTURES.items():
            ex = self.scanner._spot_exchanges.get(fut_id)
            if not ex:
                continue
            # markets 未載入則補載（初始化失敗或尚未完成時）
            if not getattr(ex, "markets", None):
                try:
                    await ex.load_markets()
                except Exception as e:
                    logger.warning(f"[WS] {fut_id} spot load_markets 失敗: {e}")
                    continue
            spot_sym = _resolve_spot_symbol(ex.markets, base, rwa_map.get(fut_id) or [])
            if spot_sym:
                active.append(spot_id)
                self._spot_symbols[spot_id] = spot_sym

        self._spot_exchanges_used = active
        for spot_id in active:
            handler = _SPOT_WS_HANDLERS.get(spot_id)
            if not handler:
                continue
            task = asyncio.create_task(
                self._run_spot_handler(spot_id, handler),
                name=f"ws-{spot_id}-{self.symbol}"
            )
            self._spot_tasks.append(task)
        used = ", ".join(f"{sid}={self._spot_symbols[sid]}" for sid in active)
        logger.info(f"[WS] {self.symbol} 現貨啟用 {len(active)} 所：{used or '（無）'}")
        # 告知 client 哪些現貨被啟用
        await self._send_spot_snapshot(ws)

    async def _send_spot_snapshot(self, ws):
        try:
            await ws.send_text(json.dumps({
                "type": "spot_enabled",
                "exchanges": list(self._spot_exchanges_used),
            }))
        except Exception:
            pass

    def disable_spot(self, ws):
        self._spot_requestors.discard(ws)
        if self._spot_requestors or not self._spot_started:
            return
        self._stop_spot()
        logger.info(f"[WS] {self.symbol} 現貨關閉")

    async def _run_spot_handler(self, spot_id, handler):
        """現貨 WS handler 執行（自動重連，指數退避，理由同 _run_handler）"""
        spot_sym = self._spot_symbols.get(spot_id)
        if not spot_sym:
            return
        backoff = 3
        while self._running and self._spot_started:
            try:
                await handler(spot_sym, self.symbol, self._on_update)
                backoff = 3
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[WS] {spot_id} {self.symbol} 斷線: {e}")
            if self._running and self._spot_started:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _on_update(self, update: dict):
        ex = update["exchange"]
        now = time.time()

        # 節流：每交易所最多每 500ms 推送一次
        last = self._last_send.get(ex, 0)
        if now - last < 0.5:
            self._latest[ex] = update
            return
        self._last_send[ex] = now

        # 附加 funding_interval_h（隨掃描快取刷新，滿資費縮週期時開著的頁面也會更新）
        self._refresh_intervals_if_stale()
        if ex in self._intervals:
            update["funding_interval_h"] = self._intervals[ex]
        update["live"] = True

        # 合併：保留舊欄位，用新欄位覆蓋
        if ex in self._latest:
            merged = {**self._latest[ex], **{k: v for k, v in update.items() if v is not None}}
            self._latest[ex] = merged
        else:
            self._latest[ex] = update

        await self._broadcast({"type": "update", "data": self._latest[ex]})

    async def _broadcast(self, msg):
        if not self.clients:
            return
        data = json.dumps(msg)
        dead = set()
        for ws in self.clients:
            try:
                await ws.send_text(data)
            except Exception:
                dead.add(ws)
        self.clients -= dead

    async def _run_handler(self, exchange, orig_sym, handler):
        """執行 WS handler（自動重連，指數退避）。
        退避是關鍵：多所同時失敗時（網路抖動/冷門幣），固定 3s 硬重連會 23 條一起狂打
        → 打爆本機 DNS/連線表 → 更多失敗 → 死亡螺旋。退避到 60s 上限把重連率壓死。"""
        backoff = 3
        while self._running:
            try:
                logger.debug(f"[WS] {exchange} {self.symbol} 連線中 (orig={orig_sym})")
                await handler(orig_sym, self.symbol, self._on_update)
                logger.debug(f"[WS] {exchange} {self.symbol} handler 正常結束")
                backoff = 3  # 有連上（正常結束）→ 重置退避
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[WS] {exchange} {self.symbol} 斷線: {e}")
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)


# ═══════════════════════════════════════════════════════════════
# WsManager — 全域管理器
# ═══════════════════════════════════════════════════════════════

class WsManager:

    def __init__(self, scanner):
        self.scanner = scanner
        self._rooms: dict[str, _SymbolRoom] = {}

    def _get_exchange_symbols(self, symbol: str) -> dict[str, str]:
        """從最近掃描結果找出哪些交易所有此幣種"""
        result = {}
        if not self.scanner.last_result:
            return result
        for r in self.scanner.last_result.records:
            norm = r.normalized_symbol or r.symbol
            if norm == symbol:
                result[r.exchange] = r.symbol
        if result:
            return result
        # 退路：呼叫端送的是「交易所原始代號」而非正規化代號時（例：RWA 各分頁的快取
        # 每小時才重建，別名更新後那段時間按鈕仍送舊代號），用 raw base 再比一次，
        # 免得整個房間開不起來、前端只能顯示 Symbol not found。
        # 只在正規化完全比不中時才走（正常情況永不觸發），故不會誤併已成組的幣種。
        base = symbol.split("/")[0].upper()
        for r in self.scanner.last_result.records:
            if r.symbol.split("/")[0].upper() == base:
                result[r.exchange] = r.symbol
        if result:
            logger.info(f"[WS] {symbol} 非正規化代號，改用 raw base 比對到 {len(result)} 所")
        return result

    async def handle_client(self, ws, symbol: str):
        """處理前端 WebSocket 連線（由 FastAPI endpoint 呼叫）"""
        if symbol not in self._rooms:
            ex_syms = self._get_exchange_symbols(symbol)
            if not ex_syms:
                await ws.send_text(json.dumps({
                    "type": "error", "message": "Symbol not found"
                }))
                return
            self._rooms[symbol] = _SymbolRoom(symbol, ex_syms, self.scanner)

        room = self._rooms[symbol]
        await room.join(ws)

        try:
            while True:
                try:
                    data = await ws.receive_text()
                    if data == "ping":
                        await ws.send_text("pong")
                        continue
                    try:
                        msg = json.loads(data)
                    except Exception:
                        continue
                    action = msg.get("action") if isinstance(msg, dict) else None
                    if action == "spot_on":
                        await room.enable_spot(ws)
                    elif action == "spot_off":
                        room.disable_spot(ws)
                except Exception:
                    break
        finally:
            room.leave(ws)
            if room.empty and symbol in self._rooms:
                del self._rooms[symbol]

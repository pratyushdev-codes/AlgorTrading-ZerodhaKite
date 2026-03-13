import json
import os
import struct
from io import StringIO

import requests
import websocket

# Load from environment, or use defaults (from Kite login response). Access token expires at EOD.
API_KEY = os.environ.get("KITE_API_KEY", "rd43h89wjrd9a6ly")
ACCESS_TOKEN = os.environ.get("KITE_ACCESS_TOKEN", "M4LJ4E4no5jdz0jpr7PSmAlV1erp4PPp")

WS_URL = "wss://ws.kite.trade"
if API_KEY and ACCESS_TOKEN:
    WS_URL = f"{WS_URL}?api_key={API_KEY}&access_token={ACCESS_TOKEN}"


# Underlying indices (spot) – NIFTY 50 and NIFTY BANK only
INDEX_TOKENS = [
    256265,  # NIFTY 50
    260105,  # NIFTY BANK
]
TOKENS = INDEX_TOKENS

# Option instruments metadata (filled from instruments API)
INSTRUMENT_META: dict[int, dict] = {}


def fetch_option_instruments(expiry_filter: str | None = None) -> None:
    """
    Fetch all NIFTY / BANKNIFTY NFO-OPT instruments from Kite instruments API
    and extend TOKENS with their instrument_tokens.

    expiry_filter: optional exact expiry date string as in instruments dump,
                   e.g. '2026-03-26'. If None, uses all expiries.
    """
    global TOKENS, INSTRUMENT_META

    url = "https://api.kite.trade/instruments"
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {API_KEY}:{ACCESS_TOKEN}",
    }
    try:
        print("Fetching instruments from Kite API for option chain ...")
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"⚠️  Failed to fetch instruments: {e}")
        print("    Proceeding with only index tokens.\n")
        return

    text = resp.text
    reader = None
    try:
        reader = csv.DictReader(StringIO(text))
    except NameError:
        # csv module is part of stdlib; but if missing import, just return.
        import csv as _csv  # type: ignore[no-redef]
        globals()["csv"] = _csv
        reader = _csv.DictReader(StringIO(text))

    option_tokens: list[int] = []
    # Allow overriding expiry via env
    if expiry_filter is None:
        expiry_filter = os.environ.get("EXPIRY_FILTER", "").strip()

    for row in reader:
        try:
            if row.get("segment") != "NFO-OPT":
                continue
            name = (row.get("name") or "").upper()
            if name not in ("NIFTY", "BANKNIFTY"):
                continue
            if expiry_filter:
                if (row.get("expiry") or "").strip() != expiry_filter:
                    continue
            token = int(row["instrument_token"])
            strike = float(row.get("strike") or 0)
            opt_type = (row.get("instrument_type") or "").upper()  # CE / PE
            trading_symbol = row.get("tradingsymbol") or ""
            expiry = row.get("expiry") or ""

            option_tokens.append(token)
            INSTRUMENT_META[token] = {
                "underlying": name,
                "strike": strike,
                "option_type": opt_type,
                "tradingsymbol": trading_symbol,
                "expiry": expiry,
            }
        except (KeyError, ValueError):
            continue

    TOKENS = INDEX_TOKENS + option_tokens
    print(f"✅ Total instruments subscribed: {len(TOKENS)}")
    print(f"   Indices: {INDEX_TOKENS}, Options: {len(option_tokens)}")


def parse_binary(data):
    """
    Parse Kite binary packets for:
    - Indices (NIFTY 50, NIFTY BANK)
    - NFO-OPT options (full/quote/ltp)
    """
    packets = []

    number_of_packets = struct.unpack(">H", data[0:2])[0]
    offset = 2

    for i in range(number_of_packets):

        packet_len = struct.unpack(">H", data[offset:offset+2])[0]
        offset += 2

        packet = data[offset:offset+packet_len]
        offset += packet_len

        token = struct.unpack(">I", packet[0:4])[0]

        # LTP-only packet (indices or instruments)
        if packet_len == 8:
            ltp = struct.unpack(">I", packet[4:8])[0] / 100
            packets.append({
                "instrument_token": token,
                "mode": "ltp",
                "is_index": token in INDEX_TOKENS,
                "ltp": ltp,
            })
            continue

        # Index packets (NIFTY 50 / NIFTY BANK)
        if packet_len in (28, 32):
            ltp = struct.unpack(">I", packet[4:8])[0] / 100
            high = struct.unpack(">I", packet[8:12])[0] / 100
            low = struct.unpack(">I", packet[12:16])[0] / 100
            opn = struct.unpack(">I", packet[16:20])[0] / 100
            close = struct.unpack(">I", packet[20:24])[0] / 100
            change = struct.unpack(">I", packet[24:28])[0] / 100
            ts = struct.unpack(">I", packet[28:32])[0] if packet_len == 32 else None
            packets.append({
                "instrument_token": token,
                "mode": "index",
                "is_index": True,
                "ltp": ltp,
                "open": opn,
                "high": high,
                "low": low,
                "close": close,
                "change": change,
                "timestamp": ts,
            })
            continue

        # Regular instruments (options etc.)
        # Quote mode: 44 bytes, Full mode: 184 bytes (with depth)
        is_full = packet_len == 184

        ltp = struct.unpack(">I", packet[4:8])[0] / 100
        last_qty = struct.unpack(">I", packet[8:12])[0]
        avg_price = struct.unpack(">I", packet[12:16])[0] / 100
        volume = struct.unpack(">I", packet[16:20])[0]
        buy_qty = struct.unpack(">I", packet[20:24])[0]
        sell_qty = struct.unpack(">I", packet[24:28])[0]
        opn = struct.unpack(">I", packet[28:32])[0] / 100
        high = struct.unpack(">I", packet[32:36])[0] / 100
        low = struct.unpack(">I", packet[36:40])[0] / 100
        close = struct.unpack(">I", packet[40:44])[0] / 100

        quote = {
            "instrument_token": token,
            "mode": "full" if is_full else "quote",
            "is_index": False,
            "ltp": ltp,
            "last_quantity": last_qty,
            "average_price": avg_price,
            "volume": volume,
            "total_buy_quantity": buy_qty,
            "total_sell_quantity": sell_qty,
            "open": opn,
            "high": high,
            "low": low,
            "close": close,
        }

        if is_full and packet_len >= 184:
            last_trade_ts = struct.unpack(">I", packet[44:48])[0]
            oi = struct.unpack(">I", packet[48:52])[0]
            oi_high = struct.unpack(">I", packet[52:56])[0]
            oi_low = struct.unpack(">I", packet[56:60])[0]
            exch_ts = struct.unpack(">I", packet[60:64])[0]

            # Market depth: 5 bid + 5 ask, each 12 bytes
            depth = {"bids": [], "asks": []}
            depth_offset = 64
            for i in range(5):  # bids
                qty = struct.unpack(">I", packet[depth_offset:depth_offset+4])[0]
                price = struct.unpack(">I", packet[depth_offset+4:depth_offset+8])[0] / 100
                orders = struct.unpack(">H", packet[depth_offset+8:depth_offset+10])[0]
                depth["bids"].append(
                    {"quantity": qty, "price": price, "orders": orders}
                )
                depth_offset += 12
            for i in range(5):  # asks
                qty = struct.unpack(">I", packet[depth_offset:depth_offset+4])[0]
                price = struct.unpack(">I", packet[depth_offset+4:depth_offset+8])[0] / 100
                orders = struct.unpack(">H", packet[depth_offset+8:depth_offset+10])[0]
                depth["asks"].append(
                    {"quantity": qty, "price": price, "orders": orders}
                )
                depth_offset += 12

            quote.update(
                {
                    "last_trade_time": last_trade_ts,
                    "oi": oi,
                    "oi_day_high": oi_high,
                    "oi_day_low": oi_low,
                    "exchange_timestamp": exch_ts,
                    "depth": depth,
                }
            )

        packets.append(quote)

    return packets


def on_open(ws):
    print("Connected to Kite WebSocket")

    # Subscribe to all selected instruments
    subscribe_message = {
        "a": "subscribe",
        "v": TOKENS
    }
    ws.send(json.dumps(subscribe_message))

    # Set indices + options to FULL mode (includes LTP, quote fields, and depth)
    if TOKENS:
        mode_message = {
            "a": "mode",
            "v": ["full", TOKENS],
        }
        ws.send(json.dumps(mode_message))


def on_message(ws, message):

    # Binary message (market data)
    if isinstance(message, bytes):

        if len(message) <= 2:
            return  # heartbeat

        packets = parse_binary(message)

        for pkt in packets:
            token = pkt["instrument_token"]
            if pkt.get("is_index"):
                print(
                    f"INDEX {token} | LTP: {pkt['ltp']} | O:{pkt.get('open')} "
                    f"H:{pkt.get('high')} L:{pkt.get('low')} C:{pkt.get('close')}"
                )
                continue

            meta = INSTRUMENT_META.get(token)
            if not meta:
                # Non-index instrument without metadata (shouldn't happen for our chain)
                print(f"Token: {token} | LTP: {pkt['ltp']}")
                continue

            side = meta["option_type"]
            underlying = meta["underlying"]
            strike = meta["strike"]
            oi = pkt.get("oi")

            top_bid = top_ask = None
            depth = pkt.get("depth") or {}
            bids = depth.get("bids") or []
            asks = depth.get("asks") or []
            if bids:
                top_bid = bids[0]
            if asks:
                top_ask = asks[0]

            print(
                f"{underlying} {strike:.0f} {side} | LTP:{pkt['ltp']} OI:{oi} "
                f"Bid:{top_bid} Ask:{top_ask}"
            )

    else:
        # Text message (order updates, errors, etc.)
        try:
            data = json.loads(message)
            print("Text message:", data)
        except json.JSONDecodeError:
            print("Text (raw):", message)


def on_error(ws, error):
    err_str = str(error)
    print("Error:", err_str)
    if "403" in err_str or "Authentication failed" in err_str:
        print("\n  → Kite access_token expires at end of day. Get a new token:")
        print("    1. Open https://kite.zerodha.com and log in")
        print("    2. After login, copy the 'request_token' from the redirect URL")
        print("    3. Exchange it for access_token via Kite API (session endpoint)")
        print("    4. Run: KITE_API_KEY=your_key KITE_ACCESS_TOKEN=your_token python main.py")
        print("  Or set env vars: KITE_API_KEY and KITE_ACCESS_TOKEN\n")


def on_close(ws, close_status_code, close_msg):
    print("Connection closed")


def run():
    global TOKENS
    if not API_KEY or not ACCESS_TOKEN:
        print("Set KITE_API_KEY and KITE_ACCESS_TOKEN (access_token expires daily).")
        print("Example: KITE_API_KEY=xxx KITE_ACCESS_TOKEN=yyy python main.py")
        return

    # Build option-chain instruments (NIFTY / BANKNIFTY options) + indices
    fetch_option_instruments()

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()


if __name__ == "__main__":
    run()
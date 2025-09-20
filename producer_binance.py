import asyncio
import json
import ssl
import websockets
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:29092"})

async def stream_binance(symbol="btcusdt"):
    uri = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with websockets.connect(uri, ssl=ssl_context) as ws:
        print(f"OK Connected to Binance for {symbol}")
        async for msg in ws:
            data = json.loads(msg)
            tick = {
                "symbol": data["s"],
                "price": float(data["p"]),
                "qty": float(data["q"]),
                "ts": data["T"]
            }
            producer.produce("crypto_ticks", json.dumps(tick).encode("utf-8"))
            producer.poll(0)  # vide le buffer

if __name__ == "__main__":
    asyncio.run(stream_binance())
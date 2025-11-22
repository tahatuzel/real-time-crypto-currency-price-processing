import time
from confluent_kafka import Producer
import websocket
import json

def main():
    producer=Producer({
        "bootstrap.servers":"kafka:9092"
    })

    symbols = ["btcusdt", "ethusdt","bnbusdt"]
    streams = "/".join([f"{sym}@miniTicker" for sym in symbols])
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    topic="raw_prices"

    def  on_message(ws,message):
        msg_json=json.loads(message)
        data=msg_json["data"]

        symbol=data["s"]
        price=data["c"]
        timestamp=data["E"]

        data={"timestamp":timestamp,"symbol":symbol,"price":price}

        print(data)

        payload=json.dumps(data)

        producer.produce(
            topic=topic,
            value=payload.encode('utf-8')
        )

    def on_error(ws, error):
        print(f"Error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("--- Connection Closed ---")

    def on_open(ws):
        print("--- Connection Opened ---")
        print("Waiting for price data...")


    ws = websocket.WebSocketApp(url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)


    try:
        ws.run_forever()
    except Exception as e:
        print(e)
    finally:
        producer.flush()

if __name__ == '__main__':
    time.sleep(10)
    main()
import json
import os
from datetime import datetime

from confluent_kafka import Producer

from schwab_client import SchwabClient

# 创建生产者配置
conf = {
    'bootstrap.servers': 'www.aixohub.com:9092'  # Kafka 服务器地址

}


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}")


def sendKafka(data, *args, **kwargs):
    producer = Producer(conf)
    topic = 'stock-nvda'
    try:
        data_json = json.loads(data)
        response_value = data_json.get("response", None)
        notify_values = data_json.get("notify", None)
        data_values = data_json.get("data", None)
        if response_value is not None:
            print(f"response 字段的值为: {response_value}")
        elif notify_values is not None:
            print(f"notify 字段的值为: {notify_values}")
            pass
        elif data_values is not None:
            for v1 in data_values:
                service = v1.get("service", None)
                timestamp = v1.get("timestamp", None)
                timestamp_s = timestamp / 1000
                # 转换为可读的日期时间格式
                readable_time = datetime.utcfromtimestamp(timestamp_s)
                utc_now = datetime.utcnow()
                server_time = datetime.utcfromtimestamp(utc_now)
                content_datas = v1.get("content", None)
                for c_data in content_datas:
                    stock_code = c_data.get("key", None)
                    open_price = c_data.get("17", None)
                    high = c_data.get("10", None)
                    low = c_data.get("11", None)
                    close = c_data.get("12", None)
                    volume = c_data.get("8", None)
                    bid_price = c_data.get("1", None)
                    ask_price = c_data.get("2", None)
                    last_price = c_data.get("3", None)
                    bid_size = c_data.get("4", None)
                    ask_size = c_data.get("5", None)
                    data_stock = {
                        "symbol": stock_code,
                        "date": readable_time,
                        "serverTime": server_time,
                        "open": open_price,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": volume,
                        "lastPrice": last_price,
                        "bidPrice": bid_price,
                        "bidSize": bid_size,
                        "askPrice": ask_price,
                        "askSize": ask_size
                    }
                    # 将字典转换为 JSON 字符串
                    json_str = json.dumps(data_stock)
                    print(json_str)
                    json_bytes = json_str.encode('utf-8')
                    # 发送消息到 Kafka
                    producer.produce(topic, key='key344', value=json_bytes, callback=delivery_report)
                    producer.flush()

    except:
        pass


def main():
    # place your app key and app secret in the .env file

    client = SchwabClient(os.getenv('callback_url'))

    # define a variable for the steamer:
    streamer = client.stream

    """
    # example of using your own response handler, prints to main terminal.
    # the first parameter is used by the stream, additional parameters are passed to the handler
    def my_handler(message):
        print("test_handler:" + message)
    streamer.start(my_handler)
    """

    # start steamer with default response handler (print):
    streamer.start(receiver=sendKafka)

    """
    You can stream up to 500 keys.
    By default all shortcut requests (below) will be "ADD" commands meaning the list of symbols will be added/appended 
    to current subscriptions for a particular service, however if you want to overwrite subscription (in a particular 
    service) you can use the "SUBS" command. Unsubscribing uses the "UNSUBS" command. To change the list of fields use
    the "VIEW" command.
    """

    # these three do the same thing
    # streamer.send(streamer.basic_request("LEVELONE_EQUITIES", "ADD", parameters={"keys": "AMD,INTC", "fields": "0,1,2,3,4,5,6,7,8"}))
    # streamer.send(streamer.level_one_equities("AMD,INTC", "0,1,2,3,4,5,6,7,8", command="ADD"))
    streamer.send(
        streamer.level_one_equities("NVDA,SMCI", "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"))

    # streamer.send(streamer.level_one_options("GOOGL 240712C00200000", "0,1,2,3,4,5,6,7,8")) # key must be from option chains api call.

    # streamer.send(streamer.level_one_futures("/ES", "0,1,2,3,4,5,6"))

    # streamer.send(streamer.level_one_futures_options("./OZCZ23C565", "0,1,2,3,4,5"))

    # streamer.send(streamer.level_one_forex("EUR/USD", "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.nyse_book(["F", "NIO"], "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.nasdaq_book("AMD", "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.options_book("GOOGL 240712C00200000", "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.chart_equity("AMD", "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.chart_futures("/ES", "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.screener_equity("NASDAQ_VOLUME_30", "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.screener_options("OPTION_CALL_TRADES_30", "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.account_activity("Account Activity", "0,1,2,3"))

    # stop the stream after 60 seconds (since this is a demo)
    import time
    try:
        time.sleep(4 * 60 * 60)
    finally:
        streamer.stop()
    # if you don't want to clear the subscriptions, set clear_subscriptions=False
    # streamer.stop(clear_subscriptions=False)


if __name__ == '__main__':
    print("Welcome to the unofficial Schwab interface!")
    main()  # call the user code above

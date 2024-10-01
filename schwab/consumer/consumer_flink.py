import json
import sys

from pyflink.common import Types, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.state import ValueStateDescriptor

from common.utils import load_environment_variables

# append the path of the parent directory
sys.path.append("/app")

import os

from InfluxDBWriter import InfluxDBWriter

KAFKA_TOPIC = "stock-nvda"
KAFKA_BOOTSTRAP_SERVERS = "www.aixohub.com:9092"


def process_message(message, influxdb_writer):
    # 解析 Kafka 消息（假设消息为 JSON 格式）
    try:
        data = json.loads(message)
        tags = {"stock": data["code"],
                "date": data['date']
                }
        fields = {
            "open": data['open'],
            "high": data['high'],
            "low": data['low'],
            "close": data['close'],
            "volume": data['volume']
        }
        influxdb_writer.process(tags, fields)
    except Exception as e:
        print(f"Failed to process message: {e}")


class flink_task:
    def __init__(self):
        self.process_message = None
        load_environment_variables()
        self.streamEnv = StreamExecutionEnvironment.get_execution_environment()
        current_working_directory = os.getcwd()
        self.streamEnv.add_jars(
            f"file://{current_working_directory}/jar/flink-connector-kafka-3.2.0-1.19.jar",
            f"file://{current_working_directory}/jar/kafka-clients-3.7.1.jar")

        # 配置 Kafka 消费者
        self.kafka_source = FlinkKafkaConsumer(
            topics=KAFKA_TOPIC,
            properties={
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': 'flink-group'
            },
            deserialization_schema=SimpleStringSchema(),  # 将数据反序列化为字符串
        )
        # 从 Kafka 中接收数据流
        self.data_stream = self.streamEnv.add_source(self.kafka_source)
        self.influxdb_writer = InfluxDBWriter(os.environ.get("INFLUXDB_BUCKET"), os.environ.get("INFLUXDB_MEASUREMENT"))

    def start_job(self):
        # 对每条消息进行处理
        self.data_stream.print(self.process_message)

        # 启动流处理作业
        self.streamEnv.execute("Kafka to InfluxDB")

    # 处理 Kafka 接收的数据并写入 InfluxDB


class SMAProcessFunction(KeyedProcessFunction):
    def __init__(self, window_size):
        self.window_size = window_size
        self.price_state = ValueStateDescriptor("price_state", Types.LIST(Types.FLOAT()))

    def process_element(self, value, ctx, out):
        # 解析 JSON 消息
        data = json.loads(value)
        stock_symbol = data["symbol"]
        new_price = data["price"]

        # 获取当前价格列表
        price_list = ctx.get_keyed_state(self.price_state, [])
        price_list.append(new_price)

        # 更新状态
        ctx.update_keyed_state(self.price_state, price_list)

        # 如果价格数量达到窗口大小，计算 SMA
        if len(price_list) >= self.window_size:
            sma = sum(price_list[-self.window_size:]) / self.window_size
            out.collect(f"Stock: {stock_symbol}, New Price: {new_price}, SMA: {sma:.2f}")


if __name__ == "__main__":
    task = flink_task()
    task.start_job()

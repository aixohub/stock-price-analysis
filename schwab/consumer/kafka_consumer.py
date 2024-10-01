import sys

from common.logger import setup_logger
from common.utils import load_environment_variables

# append the path of the parent directory
sys.path.append("/app")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

from InfluxDBWriter import InfluxDBWriter
import findspark

findspark.init()

KAFKA_TOPIC_NAME = "stock-nvda"
KAFKA_BOOTSTRAP_SERVERS = "www.aixohub.com:9092"

scala_version = '2.12'
spark_version = '3.5.3'

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-streaming-kafka-0-10_{scala_version}:{spark_version},org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version} pyspark-shell'


class spark_task:
    def __init__(self):
        load_environment_variables()
        self.spark = (
            SparkSession.builder.appName("KafkaInfluxDBStreaming")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("ERROR")

        self.stockDataframe = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC_NAME) \
            .load()
        self.influxdb_writer = InfluxDBWriter(os.environ.get("INFLUXDB_BUCKET"), os.environ.get("INFLUXDB_MEASUREMENT"))

    def spark_start(self):
        stockDataframe = self.stockDataframe.select(col("value").cast("string").alias("data"))

        inputStream = stockDataframe.selectExpr("CAST(data as STRING)")

        stock_price_schema = StructType([
            StructField("code", StringType(), True),
            StructField("date", TimestampType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True)
        ])

        stock_price_schema2 = StructType([
            StructField("data", StructType([
                StructField("service", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("command", StringType(), True),
                StructField("content", StructType([
                    StructField("key", StringType(), True),
                    StructField("1", DoubleType(), True),  # 对应 120.76
                    StructField("2", DoubleType(), True),  # 对应 120.77
                    StructField("3", DoubleType(), True),  # 对应 120.77
                    StructField("4", IntegerType(), True),  # 对应 10
                    StructField("5", IntegerType(), True),  # 对应 3
                    StructField("7", StringType(), True),  # 对应 'P'
                    StructField("8", LongType(), True)  # 对应 114189260
                ]), True)
            ]), True)
        ])

        # Parse JSON data and select columns
        stockDataframe = inputStream.select(from_json(col("data"), stock_price_schema).alias("stock_price"))
        expandedDf = stockDataframe.select("stock_price.*")
        print(expandedDf)

        query = stockDataframe \
            .writeStream \
            .foreachBatch(self.process_batch) \
            .outputMode("append") \
            .start()

        query.awaitTermination()

    def process_batch(self, batch_df, batch_id):
        logger = setup_logger(__name__, 'consumer.log')
        # influxdb_writer = InfluxDBWriter('stock-prices-bucket', 'stock-price-v1')

        logger.info(f"Processing batch {batch_id}")
        realtimeStockPrices = batch_df.select("stock_price.*")
        for realtimeStockPrice in realtimeStockPrices.collect():
            timestamp = realtimeStockPrice["date"]
            tags = {"stock": realtimeStockPrice["code"],
                    "date": realtimeStockPrice['date']
                    }
            fields = {
                "open": realtimeStockPrice['open'],
                "high": realtimeStockPrice['high'],
                "low": realtimeStockPrice['low'],
                "close": realtimeStockPrice['close'],
                "volume": realtimeStockPrice['volume']
            }
            self.influxdb_writer.process(timestamp, tags, fields)


if __name__ == "__main__":
    task = spark_task()
    task.spark_start()

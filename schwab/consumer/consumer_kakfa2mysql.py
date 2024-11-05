import os

import mysql.connector
from confluent_kafka import Consumer
from dotenv import load_dotenv

c = Consumer({
    'bootstrap.servers': 'www.aixohub.com:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

if __name__ == '__main__':
    load_dotenv()
    mysql_host = os.environ.get("mysql_host")
    mysql_user = os.environ.get("mysql_user")
    mysql_passwd = os.environ.get("mysql_passwd")

    cnx = mysql.connector.connect(
        host=mysql_host,
        port=3306,
        user=mysql_user,
        password=mysql_passwd, database='stock_us')
    c.subscribe(['stock-nvda'])

    cursor = cnx.cursor()
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        sql = msg.value().decode('utf-8')
        try:
            cursor.execute(sql)
            cnx.commit()
        except:
            pass
        print('Received message: {}'.format(sql))
    cursor.close()
    cnx.close()
    c.close()

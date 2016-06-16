import logging
import pickle
import pandas as pd
import numpy as np

from cassandra.cluster import Cluster
from pykafka import KafkaClient

from settings import KAFKA_ZOOKEEPER_HOST, CASSANDRA_HOST

cluster = Cluster([CASSANDRA_HOST])

logger = logging.getLogger(__name__)

quiet_loggers = ['pykafka', 'kazoo', 'requests', 'cassandra']
for qlog in quiet_loggers:
    logging.getLogger(qlog).propagate = False


# TODO: multi-process
def get_messages():
    client = KafkaClient(zookeeper_hosts=KAFKA_ZOOKEEPER_HOST)
    topic = client.topics[b'eod.play']
    balanced_consumer = topic.get_balanced_consumer(
        consumer_group=b'testgroup',
        auto_commit_enable=True,
    )
    logger.info('Start listening')
    for msg in balanced_consumer:
        payload = pickle.loads(msg.value)
        save_eod_data_to_db(payload)


def save_eod_data_to_db(data):
    logger.info('Received Data')
    session = cluster.connect('test')
    df = data['df']
    df = df.dropna()
    for eoddate, row in df.iterrows():
        exchange = data['exchange']
        ticker = data['ticker']
        open = row['Open']
        close = row['Close']
        high = row['High']
        low = row['Low']
        volume = row['Volume']
        q = """
          INSERT INTO historical_data (exchange, ticker, eoddate, open, close, high, low, volume)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        # TODO: batch inserts
        session.execute(q, [exchange, ticker, str(eoddate), open, close, high, low, int(volume)])

    logger.info('Done')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    get_messages()

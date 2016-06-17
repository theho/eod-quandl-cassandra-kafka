import logging
import pickle
import pandas as pd
import numpy as np

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pykafka import KafkaClient

from settings import KAFKA_ZOOKEEPER_HOST, CASSANDRA_HOST, TOPIC

cluster = Cluster([CASSANDRA_HOST])

logger = logging.getLogger(__name__)

quiet_loggers = ['pykafka', 'kazoo', 'requests', 'cassandra']
for qlog in quiet_loggers:
    logging.getLogger(qlog).propagate = False


# TODO: multi-process
def get_messages():
    client = KafkaClient(zookeeper_hosts=KAFKA_ZOOKEEPER_HOST)
    topic = client.topics[TOPIC]
    consumer = topic.get_balanced_consumer(
        consumer_group=b'testgroup',
        # auto_commit_enable=True,
        zookeeper_connect=KAFKA_ZOOKEEPER_HOST
    )
    logger.info('Start listening')
    while True:
        msg = consumer.consume()
        payload = pickle.loads(msg.value)
        save_eod_data_to_db(payload)
        consumer.commit_offsets()


def save_eod_data_to_db(data):
    logger.info('Received Data: {}'.format(data['ticker']))

    df = data['df'].dropna()

    q = """
        INSERT INTO historical_data (exchange, ticker, eoddate, open, close, high, low, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
      """

    session = cluster.connect('test')

    for df in np.array_split(df, len(df) / 200):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        for eoddate, row in df.iterrows():
            batch.add(q, [data['exchange'], data['ticker'], str(eoddate),
                          row['Open'], row['Close'], row['High'], row['Low'],
                          int(row['Volume'])])

        session.execute(batch)

    logger.info('Done')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    get_messages()

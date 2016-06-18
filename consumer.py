"""
Read off Kafka and write the data into Cassandra
"""
import logging
import pickle
import numpy as np

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pykafka import KafkaClient

from settings import KAFKA_ZOOKEEPER_HOST, CASSANDRA_HOST, TOPIC

logger = logging.getLogger(__name__)

cluster = Cluster([CASSANDRA_HOST])

quiet_loggers = ['pykafka', 'kazoo', 'requests', 'cassandra']
for qlog in quiet_loggers:
    logging.getLogger(qlog).propagate = False


# TODO: multi-process
def get_messages():
    """
    Fetch message off the queue and writes to DB
    """
    client = KafkaClient(zookeeper_hosts=KAFKA_ZOOKEEPER_HOST)
    topic = client.topics[TOPIC]
    consumer = topic.get_balanced_consumer(
        consumer_group=b'testgroup',
        auto_commit_enable=True,
        zookeeper_connect=KAFKA_ZOOKEEPER_HOST
    )
    logger.info('Start listening')
    while True:
        msg = consumer.consume()
        payload = pickle.loads(msg.value)
        save_eod_data_to_db(payload)
        consumer.commit_offsets()


def save_eod_data_to_db(data):
    """
    Saves Data to Cassandra in row batches
    """
    logger.info('Received Data: {}'.format(data['ticker']))

    q = """
        INSERT INTO historical_data (exchange, ticker, eoddate, open, close, high, low, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
      """

    session = cluster.connect('test')

    df = data['df'].dropna()
    for df in np.array_split(df, len(df) / 200):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        for eoddate, row in df.iterrows():
            batch.add(q, [data['exchange'],
                          data['ticker'],
                          str(eoddate),
                          row['Open'],
                          row['Close'],
                          row['High'],
                          row['Low'],
                          int(row['Volume'])]
                      )

        session.execute(batch)

    logger.info('Done')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    get_messages()

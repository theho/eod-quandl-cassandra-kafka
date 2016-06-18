"""
Snaps EOD data from Quandl and publish to Kafka Q
"""

import io
import logging
import pickle
import hashlib
import requests
from concurrent import futures

import pandas as pd
import quandl
from pykafka import KafkaClient

from settings import KAFKA_ZOOKEEPER_HOST, TOPIC, QUANDL_API_KEY

EXCHANGES = [
    ('FTSE', 'https://s3.amazonaws.com/static.quandl.com/tickers/FTSE100.csv',),
    # ('????', 'https:/ /s3.amazonaws.com/static.quandl.com/tickers/SP500.csv',),
    # ('NASDAQ', 'https://s3.amazonaws.com/static.quandl.com/tickers/NASDAQComposite.csv',),
]

# quieten 3rd party loggers
for qlog in ('pykafka', 'kazoo', 'requests',):
    logging.getLogger(qlog).propagate = False

logger = logging.getLogger('eod.producer')


def generate_tickers():
    """
    Generate tickers by reading from a list of tickers from the web.
    """
    logger.debug('generate_tickers()')

    for exch, url in EXCHANGES:
        logger.info('Get tickers for {} from {}'.format(exch, url))
        try:
            resp = requests.get(url)
            df = pd.DataFrame.from_csv(io.BytesIO(resp.content), index_col=['ticker'])
            for ticker, row in df[pd.notnull(df['free_code'])].iterrows():
                yield exch, ticker, row['free_code']

        except Exception as e:
            logger.exception(e)
            continue


def my_partitioner(partitions, key):
    """
    Partitioner function to hash keys into n partitions
    """
    return partitions[int(hashlib.md5(key).hexdigest(), 16) % len(partitions)]


def get_historical_data(exch, ticker, free_code):
    """
    Request historical data from Quandl and publish to Kafka.
    """
    logger.debug('Req: {}'.format(free_code))

    df = quandl.get(free_code, authtoken=QUANDL_API_KEY)
    logger.debug('Recv: {} ({})'.format(free_code, len(df)))

    client = KafkaClient(zookeeper_hosts=KAFKA_ZOOKEEPER_HOST)
    topic = client.topics[TOPIC]

    with topic.get_sync_producer(partitioner=my_partitioner) as producer:
        buf = io.BytesIO()
        pickle.dump(dict(exchange=exch, ticker=ticker, quandl_code=free_code, df=df), buf)
        producer.produce(buf.getvalue(), partition_key=bytes(ticker, 'utf-8'))
        logger.info('Published: {}'.format(free_code))
    return exch, ticker, free_code, df


def fetch_data():
    """
    Main method
    """
    quandl_futures = []
    with futures.ThreadPoolExecutor(max_workers=20) as e:
        for exch, ticker, free_code in generate_tickers():
            quandl_futures.append(e.submit(get_historical_data, exch, ticker, free_code))

        futures.wait(quandl_futures)
        logger.info('finished')

    return quandl_futures


if __name__ == '__main__':
    FORMAT = '%(asctime)-15s - %(message)s'
    logging.basicConfig(level=logging.DEBUG)

    qfutures = fetch_data()

    # Print errors if any
    for f in qfutures:
        if f.exception():
            logger.error(str(f.exception()))

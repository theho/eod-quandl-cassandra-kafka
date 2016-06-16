import io
import os
import requests
import logging
from concurrent import futures
from queue import Queue

import pandas as pd
import quandl

EXCHANGES = [
    ('FTSE', 'https://s3.amazonaws.com/static.quandl.com/tickers/FTSE100.csv',),
    # ('SP500', 'https://s3.amazonaws.com/static.quandl.com/tickers/SP500.csv',),
    # ('NASDAQ', 'https://s3.amazonaws.com/static.quandl.com/tickers/NASDAQComposite.csv',),
]

Q = Queue()

logger = logging.getLogger(__name__)


def generate_tickers():
    logger.debug('generate_tickers()')

    for exch, url in EXCHANGES:
        logger.info('Get tickers for {} from {}'.format(exch, url))
        # resp = requests.get(url)
        try:
            resp = requests.get(url)
            df = pd.DataFrame.from_csv(io.BytesIO(resp.content), index_col=['ticker'])
            for ticker, row in df[pd.notnull(df['free_code'])].iterrows():
                yield exch, ticker, row['free_code']
                return
        except Exception as e:
            logger.exception(e)
            continue


def get_historical_data(exch, ticker, free_code):
    logger.debug('Req: {}'.format(free_code))

    df = quandl.get(free_code, authtoken=os.environ['QUANDL_API_KEY'])
    logger.debug('Recv: {}'.format(free_code))

    # TODO: save to remote queue
    Q.put(dict(exchange=exch, ticker=ticker, quandl_code=free_code, df=df))

    return exch, ticker, free_code, df


def fetch_data():
    quandl_futures = []
    with futures.ThreadPoolExecutor(max_workers=20) as e:
        for exch, ticker, free_code in generate_tickers():
            quandl_futures.append(e.submit(get_historical_data, exch, ticker, free_code))

        futures.wait(quandl_futures)
        logger.info('finished')

    return quandl_futures


if __name__ == '__main__':
    FORMAT = '%(asctime)-15s - %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    fetch_data()

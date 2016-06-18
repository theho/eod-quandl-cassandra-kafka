"""
Setup an initial KeySpace and Table in Cassandra
"""

from settings import CASSANDRA_HOST
from cassandra.cluster import Cluster

cluster = Cluster([CASSANDRA_HOST])


def create_key_space(key_space):
    q = "CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};" \
        % key_space

    cluster.connect().execute(q)


def create_schema(key_space):
    q = """
        CREATE TABLE historical_data (
          exchange text,
          ticker text,
          eoddate timestamp,
          open double,
          close double,
          high double,
          low double,
          volume int,
          PRIMARY KEY((exchange, ticker), eoddate)
        ) WITH CLUSTERING ORDER BY (eoddate DESC);"""

    sess = cluster.connect()
    sess.set_keyspace(key_space)
    sess.execute(q)


if __name__ == '__main__':
    create_key_space('test')
    create_schema('test')

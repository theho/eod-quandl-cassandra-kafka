# eod-data-cassandra-kafka

A POC to demonstrate the use of Cassandra/Kafka/Python for EOD (overly-simple) data pipeline.

This was developed in python 3.5

## Setup

#### 1. Codebase & Python

Check out this git repo:
```
git clone git@github.com:jimmyho/eod-data-cassandra-kafka.git
pip install -r requirements.txt
```

Open settings.py, change host names to match docker host
```
KAFKA_ZOOKEEPER_HOST = '192.168.0.4'
CASSANDRA_HOST = '192.168.0.4'
TOPIC = b'eod'
QUANDL_API_KEY = 'XXXXXX'
```

#### 2. Docker Containers
##### Cassandra 
[https://hub.docker.com/_/cassandra/](https://hub.docker.com/_/cassandra/)

```
# get the latest cassandra's tick-tock release
docker run --name some-cassandra -d cassandra

```

##### Kafka/Zookeeper
[https://github.com/confluentinc/docker-images](https://github.com/confluentinc/docker-images)

Follow the quick start instructions at the link above



#### 3. Setup Tables and Topic

Assuming your host/VM has address 1.2.3.4

##### Topic
Docs: http://kafka.apache.org/documentation.html#quickstart_createtopic

```
DOCKER_IP=1.2.3.4
KAFKA_CONTAINER=abcdefg12345
TOPIC=eod

docker exec -it $KAFKA_CONTAINER /usr/bin/kafka-topics --zookeeper $DOCKER_IP:2181 --create --topic $TOPIC --partitions 5 --replication-factor 1

```

##### Table Schema
Setup keyspace *test*, and a table *historical_data*
```
python cassandra_setup.py
```

## Start the Jobs !

```
# Starts a terminal and run a consumer
python consumer.py

# More cosumers!  Max # consumers=topic partition size
python consumer.py
python consumer.py
python consumer.py
python consumer.py

# Start a single producer
python producer.py
```

## TODOS
When/If I get some time (am juggling between a few other pet-projects), a few TODOS:

1. Temporal data model in Cassandra:
	* Tracking historical changes by data provider
	* Allowing manual retro-changes by user
	* Allow multiple data source
2. Multiple data-sources
3. Materialized Views
4. Scaling data producer to collect data in a distributed manner
5. Test for performance with different multi-node cluster setup
6. Dockerized the consumer/producer for easy deployment onto EC2/Kubernetes.
7. Simple visualisation, maybe angular2

# eod-data-cassandra-kafka
Load EOD Stocks Data into DB

## Setup Kafka topic

http://kafka.apache.org/documentation.html#quickstart_createtopic

```
$ docker exec -it f820efe57ca7 /usr/bin/kafka-topics --zookeeper <Docker/VM IP>:2181 --create --topic eod --partitions 1 --replication-factor 1

```
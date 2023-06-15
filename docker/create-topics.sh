#!/bin/sh

docker exec docker_kafka_1 \
/opt/landoop/kafka/bin/kafka-topics \
--create \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--topic streams-plaintext-input


docker exec docker_kafka_1 \
/opt/landoop/kafka/bin/kafka-topics \
--create \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--topic streams-wordcount-output \
--config cleanup.policy=compact

docker exec docker_kafka_1 \
/opt/landoop/kafka/bin/kafka-topics \
--bootstrap-server localhost:9092 --describe


docker exec docker-kafka-1 \
/opt/landoop/kafka/bin/kafka-topics \
--create \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--topic straight-through-stock-price
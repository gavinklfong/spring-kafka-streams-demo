#!/bin/sh

docker exec -it docker_kafka_1 \
/opt/landoop/kafka/bin/kafka-console-producer \
--bootstrap-server localhost:9092 \
--topic streams-plaintext-input


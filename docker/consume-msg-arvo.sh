#!/bin/sh

docker exec -it docker_kafka_1 \
/opt/landoop/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 \
--topic stock-price \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=io.confluent.kafka.serializers.KafkaAvroSerializer \
--property value.deserializer=io.confluent.kafka.serializers.KafkaAvroSerializer

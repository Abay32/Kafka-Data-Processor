
# Start kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties

#start zookeeper
 .\bin\windows\zookeeper-server-start .\config\zookeeper.properties

# Create a topic with cleanup
.\bin\windows\kafka-topics.bat \
    --create \
    --bootstrap-server localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic user-keys-and-colours \
    --config cleanup.policy=compact

# topic description
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic fav-colour-output

# Kafka consumer with config
.\bin\windows\kafka-console-consumer.bat \
    --bootstrap-server localhost:9092 \
    --topic fav-colour-output --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter
    --property print.key=true
    --property print.value=true
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# Kafka producer
.\bin\windows\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic word-count-input




# Kafka-Training
- Cognizant Kafka Training with Manish

## Kafka Commands
- Start Zookeeper
```sh
zookeeper-server-start.bat config/zookeeper.properties
```
- Start Broker
```sh
kafka-server-start.bat config/server.properties
```
- Read Details Regarding <TOPIC_NAME>
```sh
kafka-topics.bat --describe --bootstrap-server localhost:9092 --topic <TOPIC_NAME>
```

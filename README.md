# Kafka-Training
- Kafka Training with Manish

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

## Code Functionality
- The Program Generates Random Temperatures between 0 and 100, and sends their data to a specific partition with the value being the temperature and the time it was recorded
### - Read Data to Specific Partition
- Reads Custom Header Values

### - Change number of partitions for a specific topic

### - Send Producer Data to Consumers of different Groups


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

## Code Functionality
- The Program Generates Random Temperatures between 0 and 100, and sends their data to a specific Topic along with the temperature and the time it was recorded
### - Change number of partitions for a specific topic
### - Send Producer Data to Consumers of different Groups

## Running Avro Topics Consumer/Producer With Docker
### Execution Process
#### In IDE
1. Set Run Configuration to *Docker*
   * Generate Docker Container
2. Run Consumer
   * Use Normal Run Configuration
3. Run Producer
   * Use Normal Run Configuration

### To View Kafka Data
```sh
localhost:3030
```

### Avro Schema Configuration
~~~~
"fields": [
    {"name": "TemperatureRecorded", "type": "string"},
    {"name": "TimeRecorded", "type": "string"},
    {"name": "Temperature", "type": "int"}
]
~~~~

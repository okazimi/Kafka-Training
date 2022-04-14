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
- V1
  - Basic Kafka Producer and Consumer
  - Multihreaded Producer and Consumer that creates two consumer groups evenly dividing the consumers between groups
  - Kafka Producer and Consumer that produce and consume Avro records using V1 schema
- V2
  - Kafka Producer and Consumer that produce and consume Avro records using V2 schema

## Running Avro Topics Consumer/Producer With Docker
### Execution Process
#### In IDE
1. Set Run Configuration to *Docker*
   * Generate Docker Container using docker-compose file
2. Run Consumer
   * Use Normal Run Configuration
3. Run Producer
   * Use Normal Run Configuration

### To View Landoop UI with Schema Registry and Topics
```sh
localhost:3030
```

### Avro Schema V1 Configuration
~~~~
"fields": [
    {"name": "firstField", "type": "string", "default": "default"},
    {"name": "secondField", "type": "int", "default": 1}
  ]
~~~~
### Avro Schema V2 Configuration
~~~~
"fields": [
    {"name": "firstFieldV2", "type": "string", "default": "default"},
    {"name": "secondField", "type": "int", "default": 1},
    {"name": "thirdField", "type": "string", "default": "wawaweewa"}
  ]
~~~~

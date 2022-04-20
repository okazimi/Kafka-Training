# Kafka-Training
- <UNNAMED COMPANY> Kafka Training with Manish

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
  
## CQRS Postman Commands
### EmployeeProducer Commands (Port 8088)
#### Add Employee (Post)
  ```sh
  localhost:8088/employee/add
  ```
#### Update Employee - Found In DB (Put)
  ```sh
  localhost:8088/employee/update?empid=18&name=Paul Smith
  ```
#### Update Employee - Not Found In DB (Put)
  ```sh
  localhost:8088/employee/update?empid=30&name=Paul Smith
  ```
#### SyncEmployees (Get)
  ```sh
  localhost:8088/employee/sync
  ```
### EmployeeConsumer Commands (Port 8071)
#### Find All (Get)
  ```sh
  localhost:8071/employee/
  ```
#### Find Employee - Found In DB (Get)
  ```sh
  localhost:8071/employee/findByID/?empid=1
  ```
#### Find Employee - Not Found In DB (Get)
  ```sh
  localhost:8071/employee/findByID/?empid=-1
  ```

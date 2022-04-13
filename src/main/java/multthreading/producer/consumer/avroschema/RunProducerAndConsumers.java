package multthreading.producer.consumer.avroschema;

import java.time.Duration;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class RunProducerAndConsumers {

  // INITIALIZE VARIABLES
  private final static int PARTITION_COUNT = 3;
  private final static String TOPIC_NAME1 = "multithreading-regex";
  private final static String TOPIC_NAME2 = "another-multithreading-regex";
  private final static int MSG_COUNT = 4;
  private static int totalMsgToSend;
  private static AtomicInteger msg_received_counter = new AtomicInteger(0);

  // MULTI-THREADING CONSUMERS FUNCTION
  public static void run(int consumerCount, String[] consumerGroups) throws InterruptedException, ExecutionException {
    // CHECK TOPIC CREATION
    TopicCreator.createTopic(TOPIC_NAME1, PARTITION_COUNT);
    // CHECK TOPIC CREATION
    TopicCreator.createTopic(TOPIC_NAME2, PARTITION_COUNT);
    // USE TREE SET THAT EXTENDS SET INTERFACE TO COUNT THE NUMBER OF DISTINCT GROUPS A.K.A DOES NOT COUNT DUPLICATES
    int distinctGroups = new TreeSet<>(Arrays.asList(consumerGroups)).size();
    // CALCULATE TOTAL MESSAGES TO SEND BASED ON MSGCOUNT, PARTITION COUNT, GROUPS
    totalMsgToSend = MSG_COUNT * PARTITION_COUNT * distinctGroups;
    // INITIALIZE EXECUTOR SERVICE
    ExecutorService executorService = Executors.newFixedThreadPool(distinctGroups*consumerCount + 1);
    // LOOP THROUGH EACH GROUP
    for (int j = 0; j < distinctGroups; j++) {
      // START 3 CONSUMERS FOR GROUP
      for (int i = 0; i < consumerCount; i++) {
        // SET A CONSUMER ID FOR EACH CONSUMER
        String consumerId = Integer.toString(i+1);
        // CREATE EFFECTIVELY FINAL i VARIABLE FOR BELOW LAMBDA EXPRESSION
        int finalJ = j;
        // START CONSUMER
        executorService.execute(() -> startConsumer(consumerId, consumerGroups[finalJ]));
      }
    }
    // EXECUTE PRODUCER THREAD TO SEND MESSAGES
    executorService.execute(RunProducerAndConsumers::sendMessages);
    // SHUTDOWN EXECUTOR SERVICE
    executorService.shutdown();
    // WAIT 10 MIN TO SHUTDOWN
    executorService.awaitTermination(10, TimeUnit.MINUTES);
  }

  // START CONSUMER
  private static void startConsumer(String consumerId, String consumerGroup) {
    // PRINT OUT THE CURRENT CONSUMER THAT IS STARTING
    System.out.printf("Starting consumer: %s, Group: %s%n", consumerId, consumerGroup);
    // CREATE KAFKA CONSUMER AND SET PROPERTIES FOR CONSUMER
    KafkaConsumer<String,GenericRecord> consumer = new KafkaConsumer<>(ProducerAndConsumerProperties.getConsumerProperties(consumerGroup));
    // SUBSCRIBE TO TOPICS THAT COMPLY WITH PATTERN
    consumer.subscribe(Pattern.compile("[A-Za-z1-9].+"));
    // WHILE LOOP TO POLL AND OBTAIN RECORDS
    while (true) {
      // POLL FOR 2 SECONDS AND OBTAIN RECORDS
      ConsumerRecords<String,GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
      // LOOP THROUGH EACH RECORD IN RECORDS
      for (ConsumerRecord<String,GenericRecord> record : records) {
        // INCREMENT MESSAGE RECEIVED COUNTER
        msg_received_counter.incrementAndGet();
        // PRINT OUT CONSUMER INFO
        System.out.printf("%nConsumer Info %nConsumer Group: %s%n Consumer ID: %s%n Topic: %s%n Header: Key = %s, Value = %s%n Partition ID = %s%n Key = %s%n Value = %s%n"
                + " Offset = %s%n",
            consumerGroup, consumerId, record.topic(), record.headers().iterator().next().key(), new String(record.headers().iterator().next().value()), record.partition(), record.key(), record.value(), record.offset());
      }
      // SYNCHRONOUS OFFSET COMMIT
      consumer.commitSync();
      // IF MESSAGES RECEIVED = TOTAL MESSAGES TO SEND BREAK
      if (msg_received_counter.get() == totalMsgToSend) {
        break;
      }
    }
  }

  // SEND MESSAGE METHODS
  public static void sendMessages() {
    // INITIALIZE KAFKA PRODUCER AND SET PRODUCER PROPERTIES
    KafkaProducer producer = new KafkaProducer<>(ProducerAndConsumerProperties.getProducerProperties());
    // INITIALIZE SCHEMA AND OBTAIN INSERTABLE GENERIC RECORD
    GenericRecord avroRecord = AvroSchemaRegistry.createSchema();
    // INITIALIZE KEY VARIABLE
    int key = 0;
    // LOOP THROUGH EACH MESSAGE
    for (int i = 0; i < MSG_COUNT; i++) {
      // SEND THE MESSAGE TO EACH PARTITION
      for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
        // INCREMENTAL MESSAGE TO BE SENT
        avroRecord.put("f1", "message-" + i);
        // INCREMENTAL KEY VALUE
        key++;
        // PRINT OUT INFORMATION TO BE SENT
        System.out.printf("%nSending Message%nTopic: %s%n Key: %s%n Value: %s%n Partition ID: %s%n", TOPIC_NAME1, key, avroRecord, partitionId);
        System.out.printf("%nSending Message%nTopic: %s%n Key: %s%n Value: %s%n Partition ID: %s%n", TOPIC_NAME2, key, avroRecord, partitionId);

        try{
          // INITIALIZE PRODUCER RECORDS
          ProducerRecord<String,GenericRecord> topic1Record = new ProducerRecord<>(TOPIC_NAME1, partitionId, Integer.toString(key), avroRecord);
          ProducerRecord<String,GenericRecord> topic2Record = new ProducerRecord<>(TOPIC_NAME2, partitionId, Integer.toString(key), avroRecord);
          // ADD CUSTOM HEADERS TO RECORDS
          topic1Record.headers().add("City", ("Atlanta").getBytes());
          topic2Record.headers().add("City", ("Dallas").getBytes());
          // SEND RECORD
          producer.send(topic1Record);
          // SEND RECORD
          producer.send(topic2Record);
        } catch (SerializationException e) {
          System.out.println("Error in sending message");
          e.printStackTrace();
        }
      }
    }
    // CLOSE AND FLUSH PRODUCER
    producer.close();
  }
}

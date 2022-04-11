package simple.producer.consumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

  // INITIALIZE LOGGER
  private static Logger LOG = LoggerFactory.getLogger(Producer.class);

  // CONFIGURE PRODUCER
  public static Properties producerProperties() {
    // INITIALIZE PROPERTIES VARIABLE
    Properties props = new Properties();
    // CONFIGURE KAFKA PRODUCER FACTORY PROPERTIES
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    // RETURN PRODUCER PROPERTIES
    return props;
  }

  // SEND MESSAGE
  public static void sendMessages() throws InterruptedException {

    // CREATE PRODUCER
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties());

    // CREATE NEW TOPICS
    NewTopic topic = new NewTopic("AtlantaGaTopic", 3, (short) 1);
    NewTopic topic1 = new NewTopic("DallasTxTopic", 3, (short) 1);

    // ADMIN API CONFIGURATIONS
    Admin admin = Admin.create(Producer.producerProperties());

    try {
      // CHECK IF ATLANTAGATOPIC EXISTS A.K.A ADMIN DESCRIBE TOPICS
      admin.describeTopics(Collections.singleton(topic.name())).topicNameValues().get(topic.name()).get();
      // CHECK IF DALLASTXTOPIC EXISTS A.K.A ADMIN DESCRIBE TOPICS
      admin.describeTopics(Collections.singleton(topic1.name())).topicNameValues().get(topic1.name()).get();
//      // CREATE HASHMAP TO INSERT NEW PARTITIONS (TOPIC, PARTITION COUNT)
//      Map<String, NewPartitions> numOfPartitions = new HashMap<>();
//      // INSERT THE DESIRED TOPIC AND PARTITION INCREASE
//      numOfPartitions.putIfAbsent("AtlantaTopic", NewPartitions.increaseTo(3));
//      // CREATE NEW PARTITION COUNT
//      admin.createPartitions(numOfPartitions);
    } catch (ExecutionException e) {
      // IF TOPIC DOESNT EXIST CREATE IT
      admin.createTopics(Collections.singleton(topic));
      admin.createTopics(Collections.singleton(topic1));
    } catch (InterruptedException e) {
      // OTHER EXCEPTION, THEN PRINT EXCEPTION AND EXIT
      e.printStackTrace();
      System.exit(0);
    }
    // AFTER TOPIC EXISTS OR IS CREATED, SEND 10 MESSAGES
    finally {
      for (int i = 0; i <= 10; i++) {
        // CREATE PRODUCER RECORDS
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic.name(),"Bitcoin " + i);
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(topic1.name(),"Ethereum " + i);
        // CREATE CUSTOM HEADER
        record.headers().add("Price", ("$1,000,000").getBytes());
        record1.headers().add("Price", ("15,000").getBytes());
        // SEND RECORD
        producer.send(record, new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            // IF NO EXCEPTION EXISTS
            if (exception == null) {
              LOG.info("Received new metadata: \n"
                  + "Topic: " + metadata.topic() + "\n" +
                  "Partition: " + metadata.partition() + "\n" +
                  "Offset: " + metadata.offset() + "\n" +
                  "Timestamp: " + metadata.timestamp() + "\n" +
                  "Value: " + record.value() + "\n" +
                  "Header: " + new String(record.headers().iterator().next().value()));
            }
          }
        });
        // THREAD SLEEP TO ASSURE MESSAGE TO DIFFERENT PARITIONS
        Thread.sleep(1000);
        producer.send(record1, new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            // IF NO EXCEPTION EXISTS
            if (exception == null) {
              LOG.info("Received new metadata: \n"
                  + "Topic: " + metadata.topic() + "\n" +
                  "Partition: " + metadata.partition() + "\n" +
                  "Offset: " + metadata.offset() + "\n" +
                  "Timestamp: " + metadata.timestamp() + "\n" +
                  "Value: " + record1.value() + "\n" +
                  "Header: " + new String(record1.headers().iterator().next().value()));
            }
          }
        });
      }
      // FLUSH AND CLOSE PRODUCER
      producer.close();
    }
  }


  public static void main(String[] args) throws InterruptedException {
    // SEND MESSAGES
    sendMessages();
  }
}

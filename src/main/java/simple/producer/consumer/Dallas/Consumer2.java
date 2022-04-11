package simple.producer.consumer.Dallas;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer2 {

  // CREATE LOGGER
  private static final Logger logger = LoggerFactory.getLogger(Consumer2.class);

  // CREATE ATLANTA CONSUMER PROPERTIES, CREATE CONSUMER AND RETURN CONSUMER
  public static KafkaConsumer<String,String> createAtlantaConsumer() {
    // INITIALIZE PROPERTIES
    Properties properties = new Properties();
    // SET PROPERTIES
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "DallasGroup");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // CREATE CONSUMER FROM CURRENT PROPERTIES
    KafkaConsumer<String, String> consumer2 = new KafkaConsumer<String, String>(properties);
    // RETURN CONSUMER
    return consumer2;
  }

  public static void runConsumer() {
    // CREATE CONSUMER
    KafkaConsumer<String, String> consumer2 = createAtlantaConsumer();

    // DESCRIBE DESIRED TOPIC AND PARTITIONS
    TopicPartition atlantaGaTopic2 = new TopicPartition("AtlantaGaTopic", 2);
    TopicPartition dallasTxTopic2 = new TopicPartition("DallasTxTopic", 2);

    // SUBSCRIBE CONSUMER
    consumer2.assign(Arrays.asList(atlantaGaTopic2,dallasTxTopic2));

    // CONTINUOUS WHILE LOOP
    while(true) {

      // POLLS KAFKA AND CHECK FOR ANY RECORDS, IF NO RECORDS BY 1000 MS THEN MOVE TO NEXT LINE OF CODE
      ConsumerRecords<String,String> records = consumer2.poll(Duration.ofMillis(1000));

      // LOOP THROUGH EACH RECORD IN THE RECORDSS AND PRINT LOG INFO (KEY,VALUE,PARTITION,OFFSET)
      for(ConsumerRecord<String,String> record : records) {
        logger.info("Key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
      }
    }
  }

  public static void main (String[]args){
    // RUN ATLANTA GROUP OF CONSUMER
    Consumer2.runConsumer();
  }
}

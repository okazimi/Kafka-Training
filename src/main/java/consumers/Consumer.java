<<<<<<< HEAD
package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());
    //public static final String GROUP_ID = "group_id_0";
    public static final String bootstrapServer = "127.0.0.1:9092";


    public static void main(String args[]) throws IOException {
        final String TOPIC0 = args[1];
        final String TOPIC1 = args[2];
        FileReader reader = new FileReader("src/main/resources/application.properties");
        Properties properties = new Properties();
        properties.load(reader);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, args[0]);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition zerothTopic = new TopicPartition(TOPIC0, Integer.parseInt(args[3]));
        TopicPartition firstTopics = new TopicPartition(TOPIC1, Integer.parseInt(args[3]));
        //consumer.assign(Collections.singleton(new TopicPartition(TOPIC, Integer.parseInt(args[2]))));
        consumer.assign(Arrays.asList(zerothTopic,firstTopics));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
                log.info("Topic: " + record.topic());
                log.info("Key: " + record.key());
                log.info("Value: " + record.value());
                log.info("Partition: " + record.partition());
                log.info("Offset: " + record.offset());
            }
        }
    }

}
=======
package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());
    //public static final String GROUP_ID = "group_id_0";
    public static final String bootstrapServer = "127.0.0.1:9092";


    public static void main(String args[]) throws IOException {
        final String TOPIC0 = args[1];
        final String TOPIC1 = args[2];
        FileReader reader = new FileReader("src/main/resources/application.properties");
        Properties properties = new Properties();
        properties.load(reader);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, args[0]);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition zerothTopic = new TopicPartition(TOPIC0, Integer.parseInt(args[3]));
        TopicPartition firstTopics = new TopicPartition(TOPIC1, Integer.parseInt(args[3]));
        //consumer.assign(Collections.singleton(new TopicPartition(TOPIC, Integer.parseInt(args[2]))));
        consumer.assign(Arrays.asList(zerothTopic,firstTopics));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
                log.info("Topic: " + record.topic());
                log.info("Key: " + record.key());
                log.info("Value: " + record.value());
                log.info("Partition: " + record.partition());
                log.info("Offset: " + record.offset());
            }
        }
    }

}
>>>>>>> aae3fa247f4f79641c9d990116c362483ac9eed6

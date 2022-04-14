package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import properties.HelperClass;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String args[]) throws IOException {
        // Get topics from console arguments
        final String TOPIC0 = args[1];
        final String TOPIC1 = args[2];

        Properties properties = HelperClass.getConsumerProperties();

        // Set group ID from console argument
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, args[0]);

        // Instantiate consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Assign specific partitions from topics to this consumer from the console arguments
        TopicPartition zerothTopic = new TopicPartition(TOPIC0, Integer.parseInt(args[3]));
        TopicPartition firstTopics = new TopicPartition(TOPIC1, Integer.parseInt(args[3]));

        // Assign partitions to read from to consumer
        consumer.assign(Arrays.asList(zerothTopic,firstTopics));

        while(true) {
            // Poll for new records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // Log records
            for(ConsumerRecord<String, String> record : records) {
                Header customHeader = record.headers().iterator().next();
                log.info("Topic: " + record.topic());
                log.info("Key: " + record.key());
                log.info("Value: " + record.value());
                log.info("Partition: " + record.partition());
                log.info("Offset: " + record.offset());
                log.info("Custom key: " +  new String(customHeader.key()) + " Custom value: " + new String(customHeader.value()));
            }
        }
    }

}

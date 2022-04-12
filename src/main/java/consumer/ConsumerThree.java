package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;


public class ConsumerThree
{

    private static final Logger log = LoggerFactory.getLogger(ConsumerThree.class.getSimpleName());

    public static void main(String[] args)
    {

        Logger log = LoggerFactory.getLogger(ConsumerThree.class.getName());

        log.info("I will consume the producer");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "new_temperature_5";

        //created consumer configs here
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribing
        //consumer.subscribe(Arrays.asList(topic));

        //cross threading
        TopicPartition topic1 = new TopicPartition("new_temperature_5", 2);
        TopicPartition topic2 = new TopicPartition("mondayFunDay", 2);

        //assigning a partitoin to a consumer
        //consumer.assign(Collections.singleton(new TopicPartition("new_temperature_5", 2)));

        consumer.assign(Arrays.asList(topic1, topic2));

        //poll the data
        while(true)
        {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record :consumerRecords)
            {
                log.info("Topic name: " + record.topic());
                log.info("key: " + record.key() + "\n" +
                         "Value: " + record.value());
                log.info("Partition: " + record.partition() + "\n" +
                         "Offset: " + record.offset());
            }
        }
    }
}

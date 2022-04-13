package consumers;

import configs.ConfigGenerator;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerGroup {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String args[]) throws InterruptedException {
        run(6);
    }

    public static void run(int consumerCount) throws InterruptedException {
        // First half of consumers are in group "consumerGroup0"
        int currentGroup = 0;

        // Create threads for each consumer
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount+1);

        for (int i = 0; i < consumerCount; i++) {
            // Second half of consumers are in group "consumerGroup1"
            if(i >= consumerCount/2)
                currentGroup = 1;
            int finalCurrentGroup = currentGroup;

            // Pass startConsumer method as argument of executorService.execute()
            executorService.execute(() -> {
                try {
                    startConsumer("consumerGroup" + finalCurrentGroup); // Pass consumer group id to startConsumer()
                } catch (IOException | ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }
    private static void startConsumer(String consumerGroup) throws IOException, ExecutionException, InterruptedException {
        // Set consumer properties
        Properties properties = ConfigGenerator.getConsumerProps(consumerGroup);
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        // Set options for topics
        String topic1 = "NewSimpleTopic0";
        String topic2 = "NewSimpleTopic1";

        // Create topics if they don't exist
        ConfigGenerator.createTopic(topic1,3);
        ConfigGenerator.createTopic(topic2,3);

        // Initialize consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to topics
        consumer.subscribe(Arrays.asList(topic1,topic2));

        while(true) {
            // Poll for new records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // Log records
            for(ConsumerRecord<String, String> record : records) {
                Header customHeader = record.headers().iterator().next();
                log.info(Thread.currentThread().getName() + "\n" +
                        "Topic: " + record.topic() + "\n" +
                        "Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        "Custom key: " +  new String(customHeader.key()) + " Custom value: " + new String(customHeader.value()));
            }
        }
    }

}
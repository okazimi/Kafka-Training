package consumers;

import com.example.TestRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import properties.HelperClass;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class AvroConsumer {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    // Set options for topics
    private static final NewTopic topic0 = new NewTopic("topicAvroRegex",3, (short) 1);
    private static final NewTopic topic1 = new NewTopic("newTopicAvroRegex",3, (short) 1);

    public static void main(String args[]) throws InterruptedException {
        run(1);
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
        Properties properties = HelperClass.getAvroConsumerProperties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup); // Set consumer ID
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5000); // Set interval to check for new topics

        // Create topics if they don't exist
        HelperClass.createTopic(topic0);
        HelperClass.createTopic(topic1);

        // Initialize consumer
        KafkaConsumer<String, TestRecord> consumer = new KafkaConsumer<>(properties);

        //consumer.subscribe(Arrays.asList(topic0.name(),topic1.name()));
        consumer.subscribe(Pattern.compile("(\\w*AvroRegex)")); // Subscribe to any topic ending in AvroRegex

        while(true) {
            // Poll for new records
            ConsumerRecords<String, TestRecord> records = consumer.poll(Duration.ofMillis(100));

            // Log records
            for(ConsumerRecord<String, TestRecord> record : records) {
                // If the record is a generic record, it will have a custom header
                Header customHeader;
                String customHeaderText = "CustomHeader: ";
                if(record.headers().iterator().hasNext()) {
                    customHeader = record.headers().iterator().next();
                    customHeaderText += ("Key: " + customHeader.key() + "Value: " + new String(customHeader.value()));
                }else {
                    customHeaderText += "None";
                }
                log.info(Thread.currentThread().getName() + "\n" +
                        "Topic: " + record.topic() + "\n" +
                        "Key: " + record.key() + "\n" +
                        "Value: " + record.value().toString() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n" +
                        customHeaderText + "\n");
            }
        }
    }


}

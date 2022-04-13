package consumers;

import configs.ConfigGenerator;
import generatedRecords.AvroRecord;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

public class AvroConsumer {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String args[]) throws Exception {
        run(6);
    }
    public static void run(int consumerCount) throws Exception{

        int currentGroup = 0;

        // Create Threads For Each Consumer (1 thread for each consumer, 1 for main exec)
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount+1);

        for(int i = 0; i < consumerCount; i++){
            if(i >= consumerCount/2){
                currentGroup = 1;
            }
            int finalGroup = currentGroup;
            executorService.execute(() ->{
                try {
                    startConsumer("group-" + finalGroup);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }
    private static void startConsumer(String consumerGroup) throws Exception{
        // Set Consumer Properties
        Properties properties = ConfigGenerator.getAvroConsumerProps();

        // Set Group
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        // Check for new topics every 5 seconds
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5000);

        // Create Topics
        ConfigGenerator.createTopic("AvroTopic-1", 3);
        ConfigGenerator.createTopic("AvroTopic-2",3);
        ConfigGenerator.createTopic("AvroTopic-3",3);

        // Instantiate Consumer
        KafkaConsumer<String, AvroRecord> consumer = new KafkaConsumer<>(properties);

        // Subscribe to any topic beginning with "AvroTopic-<DIGIT>"
        consumer.subscribe(Pattern.compile("(AvroTopic-\\d)"));

        while(true) {
            // Poll for new records
            ConsumerRecords<String, AvroRecord> records = consumer.poll(Duration.ofMillis(100));

            // Log records
            for(ConsumerRecord<String, AvroRecord> record : records) {
                //Header customHeader = record.headers().iterator().next();
                log.info(Thread.currentThread().getName() + "\n" +
                        "Topic: " + record.topic() + "\n" +
                        "Key: " + record.key() + "\n" +
                        "Value: " + record.value().toString() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n");// +
                //"Custom key: " +  new String(customHeader.key()) + " Custom value: " + new String(customHeader.value()));
            }
        }
    }
}

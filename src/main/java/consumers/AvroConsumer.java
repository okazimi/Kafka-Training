package consumers;

import com.example.TestRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import properties.HelperClass;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class AvroConsumer {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String args[]) throws InterruptedException {
        run(4);
    }

    public static void run(int consumerCount) throws InterruptedException {
        int currentGroup = 0;
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount+1);
        for (int i = 0; i < consumerCount; i++) {
            if(i >= consumerCount/2)
                currentGroup = 1;
            int finalCurrentGroup = currentGroup;
            executorService.execute(() -> {
                try {
                    startConsumer("consumerGroup" + finalCurrentGroup);
                } catch (IOException | ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }
    private static void startConsumer(String consumerGroup) throws IOException, ExecutionException, InterruptedException {

        Properties properties = HelperClass.getAvroConsumerProperties();

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        //properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5000);

        NewTopic topic0 = new NewTopic("AvroTopic0",3, (short) 1);
        NewTopic topic1 = new NewTopic("AvroTopic1",3, (short) 1);

        HelperClass.createTopic(topic0);
        HelperClass.createTopic(topic1);

        KafkaConsumer<String, TestRecord> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic0.name(),topic1.name()));
        //consumer.subscribe(Pattern.compile("(\\w*regex)"));

        while(true) {
            ConsumerRecords<String, TestRecord> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, TestRecord> record : records) {
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

package consumers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class MultithreadedConsumer {
    public static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String args[]) throws InterruptedException {
        run(6);
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

        //final String TOPIC0 = "NewSimpleTopic0";
        //final String TOPIC1 = "NewSimpleTopic1";

        FileReader reader = new FileReader("src/main/resources/application.properties");
        Properties properties = new Properties();
        properties.load(reader);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        NewTopic topic0 = new NewTopic("NewSimpleTopic0",3, (short) 2);
        NewTopic topic1 = new NewTopic("NewSimpleTopic1",3, (short) 2);

        createTopic(topic0,properties);
        createTopic(topic1,properties);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //consumer.assign(Collections.singleton(new TopicPartition(TOPIC, Integer.parseInt(args[2]))));
        //consumer.assign(Arrays.asList(zerothTopic,firstTopics));
        consumer.subscribe(Arrays.asList(topic0.name(),topic1.name()));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
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

    private static void createTopic(NewTopic topic, Properties properties) throws ExecutionException, InterruptedException {
        AdminClient admin = AdminClient.create(properties);
        if(!admin.listTopics().names().get().stream().anyMatch(expectedTopic->expectedTopic.equals(topic.name()))){
            admin.createTopics(Collections.singleton(topic));
        }
    }
}

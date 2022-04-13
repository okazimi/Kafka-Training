package com.logicbig.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerGroupExample{
    private final static int PARTITION_COUNT = 3;
    private final static String TOPIC_NAME = "example-topic";
    private final static int MSG_COUNT = 4;
    private static int totalMsgToSend;
    private static AtomicInteger msg_received_counter = new AtomicInteger(0);

    public static void run(int consumerCount, String[] consumerGroups) throws Exception {
        int distinctGroups = new TreeSet<>(Arrays.asList(consumerGroups)).size();
        totalMsgToSend = MSG_COUNT * PARTITION_COUNT * distinctGroups;
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);
        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            int finalI = i;
            executorService.execute(() -> startConsumer(consumerId, consumerGroups[finalI]));
        }
        executorService.execute(ConsumerGroupExample::sendMessages);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    private static void startConsumer(String consumerId, String consumerGroup) {
        System.out.printf("starting consumer: %s, group: %s%n", consumerId, consumerGroup);
        Properties consumerProps = ExampleConfig.getConsumerProps(consumerGroup);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                msg_received_counter.incrementAndGet();
                System.out.printf("consumer id:%s, partition id= %s, key = %s, value = %s"
                                + ", offset = %s%n",
                        consumerId, record.partition(),record.key(), record.value(), record.offset());
            }

            consumer.commitSync();
            if(msg_received_counter.get()== totalMsgToSend){
                break;
            }
        }
    }

    private static void sendMessages() {
        Properties producerProps = ExampleConfig.getProducerProps();
        KafkaProducer producer = new KafkaProducer<>(producerProps);
        int key = 0;
        for (int i = 0; i < MSG_COUNT; i++) {
            for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                String value = "message-" + i;
                key++;
                System.out.printf("Sending message topic: %s, key: %s, value: %s, partition id: %s%n",
                        TOPIC_NAME, key, value, partitionId);
                producer.send(new ProducerRecord<>(TOPIC_NAME, partitionId,
                        Integer.toString(key), value));
            }
        }
    }
}
package com.example.KafkaDemo.consumer;

import com.example.KafkaDemo.configs.ConfigGenerator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerGroup {
    private final static int PARTITION_COUNT = 3;
    private final static ArrayList<String> TOPIC_LIST = new ArrayList<>();
    private final static int MSG_COUNT = 4;
    private static int totalMsgSent;
    private static AtomicInteger msg_received_counter = new AtomicInteger(0);

    public static void run(int consumerCount, ArrayList<String> consumerGroups, ArrayList<String> topics) throws Exception {
        TOPIC_LIST.equals(topics);
        int numGroups = consumerGroups.size();
        totalMsgSent = MSG_COUNT * PARTITION_COUNT * numGroups;

        // Establish Thread Pool for consumers
        ExecutorService executorService = Executors.newFixedThreadPool(consumerCount + 1);

        for (int i = 0; i < consumerCount; i++) {
            String consumerId = Integer.toString(i + 1);
            int id = i;
            System.out.println("Starting Thread For: " + consumerGroups.get(id));
            executorService.execute(() -> startConsumer(consumerId, consumerGroups.get(id), topics));
        }
        executorService.execute(ConsumerGroup::sendMessages);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    private static void startConsumer(String consumerId, String consumerGroup, ArrayList<String> topics){
        System.out.printf("starting consumer: %s, group: %s%n", consumerId, consumerGroup);
        Properties consumerProps = ConfigGenerator.getConsumerProps(consumerGroup);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
//        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                msg_received_counter.incrementAndGet();
                System.out.printf("consumer id:%s, partition id= %s, key = %s, value = %s"
                                + ", offset = %s%n",
                        consumerId, record.partition(),record.key(), record.value(), record.offset());
            }

            consumer.commitSync();
            if(msg_received_counter.get()== totalMsgSent){
                break;
            }
        }

    }


    private static void sendMessages() {
        Properties producerProps = ConfigGenerator.getProducerProps();
        KafkaProducer producer = new KafkaProducer<>(producerProps);
        int key = 0;
        for (int i = 0; i < MSG_COUNT; i++) {
            for(String topic : TOPIC_LIST){
                System.out.println("You Are On " + topic + "\n");
                for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                    String value = "message-" + i;
                    key++;
                    System.out.printf("Sending message topic: %s, key: %s, value: %s, partition id: %s%n",
                            topic, key, value, partitionId);
                    producer.send(new ProducerRecord<>(topic, partitionId,
                            Integer.toString(key), value));
                }
            }
        }
    }
}

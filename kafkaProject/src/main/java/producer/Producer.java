package producer;

import consumer.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.util.concurrent.ExecutionException;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        // CD into Kafka Folder
        // Start Zookeeper
        //        zookeeper-server-start.bat config/zookeeper.properties
        // Start Kafka Server
        //        kafka-server-start.bat config/server.properties
        // Start Broker
        //        kafka-topics.bat --describe --bootstrap-server localhost:9092 --topic new_temperature

        //create topic
        String topicName1 = "new_temperature";
        String topicName2 = "new_temperature_1";
        int partitions = 3;
        short replicationFactor = 1;
        NewTopic firstTopic = new NewTopic(topicName1, partitions, replicationFactor);
        NewTopic secondTopic = new NewTopic(topicName2, partitions, replicationFactor);

        log.info("The temperature producer");

        //create random
        Random rand = new Random();

        //create an upper bound
        int temp = 100;

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create an admin to increase partitions
        Admin admin = Admin.create(properties);

        //create the topic
        try
        {
            //check to see if topics exist
            admin.describeTopics(Collections.singleton(firstTopic.name())).topicNameValues().get(firstTopic.name()).get();
            admin.describeTopics(Collections.singleton(secondTopic.name())).topicNameValues().get(secondTopic.name()).get();

//            // CREATE HASHMAP TO INSERT NEW PARTITIONS (TOPIC, PARTITION COUNT)
//            Map<String, NewPartitions> numOfPartitions = new HashMap<>();
//            // INSERT THE DESIRED TOPIC AND PARTITION INCREASE
//            numOfPartitions.putIfAbsent(firstTopic.name(), NewPartitions.increaseTo(5));
//            // CREATE NEW PARTITION COUNT
//            admin.createPartitions(numOfPartitions);
        }
        catch (ExecutionException e)
        {
            //if topic doesn't exist, create a brilliant topic- with custom # of partitions
            admin.createTopics(Collections.singleton(firstTopic));
            admin.createTopics(Collections.singleton(secondTopic));
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            e.printStackTrace();
        }

        //i = 10 messages
        for(int i = 0; i < 10; i++){

            //generate numbers from 0 to 100
            int randTemp = rand.nextInt(temp);
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();
            String tempTime = dtf.format(now);

            //creating custom headers
            String headerKey = "tempRange";
            String headerValue;
            int partition;
            if(randTemp <= 25){
                headerValue = "low";
                partition = 0;
            } else if (randTemp > 25 && randTemp <= 75){
                headerValue = "medium";
                partition = 1;
            } else{
                headerValue = "high";
                partition = 2;
            }
            ProducerRecord<String, String> firstProducerRecord =
                    new ProducerRecord<>(firstTopic.name(),partition, null, "temperature is " + randTemp + " degrees at " + tempTime);
            firstProducerRecord.headers()
                    .add(headerKey, headerValue.getBytes());

            producer.send(firstProducerRecord, new Callback()
            {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e)
                {
                    if(e == null){
                        log.info("Reciceved new temperature data \n" +
                                 "topic: " + metadata.topic() + "\n" +
                                 "value: " + firstProducerRecord.value() + "\n" +
                                 "Partition: " + metadata.partition() + "\n" +
                                 "Offset: " + metadata.timestamp() + "\n" +
                                 "Headers: " + new String(firstProducerRecord.headers().iterator().next().value()));
                    }
                }
            });

            try{
                Thread.sleep(1000);
            }catch(InterruptedException e){
                e.printStackTrace();
            } finally {
                ProducerRecord<String, String> secondProducerRecord =
                        new ProducerRecord<>(secondTopic.name(),partition, null, "temperature is " + randTemp + " degrees at " + tempTime);
                secondProducerRecord.headers()
                        .add(headerKey, headerValue.getBytes());
                producer.send(secondProducerRecord, new Callback()
                {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e)
                    {
                        if(e == null){
                            log.info("Reciceved new temperature data \n" +
                                    "topic: " + metadata.topic() + "\n" +
                                    "value: " + secondProducerRecord.value() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.timestamp() + "\n" +
                                    "Headers: " + new String(secondProducerRecord.headers().iterator().next().value()));
                        }
                    }
                });
            }
        }

        //send data - asychronous

        //flush data - synchronoius
        producer.flush();

        //flush and close
        producer.close();
    }


}

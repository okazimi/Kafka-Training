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
import java.util.*;

import java.util.concurrent.ExecutionException;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {

        //create topic, create partitions, then create replication factor, can also send to a specific partition by using
        // name,partition,key,replication factor
        NewTopic newTopic = new NewTopic("new_temperature_5", 3, (short) 1);
        NewTopic newTopic2 = new NewTopic("mondayFunDay", 3, (short) 1);


        log.debug("The temperature producer");

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
            //check to see if topic exists
            admin.describeTopics(Collections.singleton(newTopic.name())).topicNameValues().get(newTopic.name()).get();
            admin.describeTopics(Collections.singleton(newTopic2.name())).topicNameValues().get(newTopic2.name()).get();
        }
        catch (ExecutionException e)
        {
            //if topic doesn't exist, create a brilliant topic- with custom # of partitions
            admin.createTopics(Collections.singleton(newTopic));
            admin.createTopics(Collections.singleton(newTopic2));
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            e.printStackTrace();
        }

        //10 messages
        for(int i = 0; i < 30; i++){

            //generate numbers from 0 to 100
            int randTemp = rand.nextInt(temp);

            //create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(newTopic.name(),"temperature is " + randTemp + " degrees");

            //create second producer record
            ProducerRecord<String, String> producerRecord1 =
                    new ProducerRecord<>(newTopic2.name(), "testing the new topic: " + i);

            //creating custom headers
            producerRecord.headers()
                    .add("one", "temperature".getBytes());

            producer.send(producerRecord, new Callback()
            {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e)
                {
                    if(e == null)
                    {
                        log.debug("Reciceved new temperature data \n" +
                                 "topic: " + metadata.topic() + "\n" +
                                 "value: " + producerRecord.value() + "\n" +
                                 "Partition: " + metadata.partition() + "\n" +
                                 "Offset: " + metadata.timestamp() + "\n" +
                                 "Headers: " + new String(producerRecord.headers().iterator().next().value()));
                    }
                }
            });

            producer.send(producerRecord1, new Callback()
            {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e)
                {
                    if(e == null)
                    {
                        log.debug("Reciceved a day data \n" +
                                "topic: " + metadata.topic() + "\n" +
                                "value: " + producerRecord1.value() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.timestamp() + "\n");
                                //"Headers: " + new String(producerRecord1.headers().iterator().next().value()));
                    }
                }
            });

            try
            {
                Thread.sleep(500);
            }
            catch(InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        //flush data - synchronous
        producer.flush();

        //flush and close
        producer.close();
    }


}

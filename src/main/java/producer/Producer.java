package producer;

import configuation.allConfigs;
import consumer.Consumer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import java.util.concurrent.ExecutionException;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //create topic, create partitions, then create replication factor, can also send to a specific partition by using
        // name,partition,key,replication factor
        NewTopic newTopic = new NewTopic("new_temperature_8", 3, (short) 2);
        NewTopic newTopic2 = new NewTopic("wednesdayFunDay", 3, (short) 2);


        log.debug("The temperature producer");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        allConfigs.createTopic(newTopic);
        allConfigs.createTopic(newTopic2);


        //create random
        Random rand = new Random();

        //create an upper bound
        int temp = 100;

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
                        log.info("Reciceved new temperature data \n" +
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
                                "Offset: " + metadata.timestamp());
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

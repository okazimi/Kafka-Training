package producer;

import avero.AvroConsumer;
import consumer.Consumer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import configuation.allConfigs;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class avroProducer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        NewTopic newTopic = new NewTopic("topicAvero_5", 3, (short) 1);
        NewTopic newTopic2 = new NewTopic("topicAvero_6", 3, (short) 1);

        Properties props = allConfigs.getAvroProducerProperties();

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

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

            GenericRecord avroRecord = AvroConsumer.avroRecord();
            avroRecord.put("f1", " : " +randTemp);

            GenericRecord averRecord_2 = AvroConsumer.avroRecord();
            averRecord_2.put("f1"," : " + i );

            //create a producer record
            ProducerRecord<String, GenericRecord> producerRecord =
                    new ProducerRecord<>(newTopic.name(),avroRecord);

            //create second producer record
            ProducerRecord<String, GenericRecord> producerRecord1 =
                    new ProducerRecord<>(newTopic2.name(), averRecord_2);

            //creating custom headers
            producerRecord.headers().add("one", "temperature".getBytes());

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

package avero;

import configuation.allConfigs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AvroConsumer {
    public static final Logger log = LoggerFactory.getLogger(AvroConsumer.class.getSimpleName());

    public static void main(String[] args) {
        try
        {
            //give the number of consumers and the number of groups
            String[] consumerGroups = new String[3];
            for (int i = 0; i < consumerGroups.length; i++) {
                consumerGroups[i] ="test-consumer-group-"+i;
            }
            run(3, consumerGroups);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    //run
    public static void run(int numOfConsumers, String[] consumerGroups) throws InterruptedException, Exception{
        //create consumer groups
        int distinctGroups = new TreeSet<>(Arrays.asList(consumerGroups)).size();
        ExecutorService executorService = Executors.newFixedThreadPool(numOfConsumers + 1);

        //i will loop through the groups
        for(int i = 0; i < distinctGroups; i++)
        {
            //j will loop though the consumers
            for(int j = 0; j < numOfConsumers; j++)
            {
                String consumerID = Integer.toString( j + 1);
                int finalID = i;
                executorService.execute(() -> {
                    try
                    {
                        startConsumer(consumerID, consumerGroups[finalID]);
                    }
                    catch (ExecutionException e)
                    {
                        e.printStackTrace();
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                });
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    public static GenericRecord avroRecord(){

        // CREATE SCHEMA
        String schema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\",\"default\":\"Default value for field1 field\"}]}";
        // INITIALIZE SCHEMA PARSER
        Schema.Parser parser = new Schema.Parser();
        // PASS SCHEMA TO SCHEMA PARSER AND OBTAIN PARSED SCHEMA
        Schema parsedSchema = parser.parse(schema);
        // INITIALIZE GENERIC RECORD
        GenericRecord avroRecord = new GenericData.Record(parsedSchema);
        // RETURN GENERIC RECORD
        return avroRecord;

    }


    public static void startConsumer(String consumerID, String consumerGroup) throws ExecutionException, InterruptedException {
        //------------------------add stuff------------------------------
        //create topics
        NewTopic averoTopic_1 = new NewTopic("topicAvero_5", 3, (short) 1);
        NewTopic averoTopic_2 = new NewTopic("topicAvero_6", 3, (short) 1);

        //create topics if not create yet
        //allConfigs.createTopic(averoTopic_1);
        //allConfigs.createTopic(averoTopic_2);

        //start consumer
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(allConfigs.getAvroConsumerProperties(consumerGroup));

        consumer.subscribe(Arrays.asList(averoTopic_1.name(), averoTopic_2.name()));
        //when a new topic is created and matches pattern then have new consumer subscribe to topic
        //consumer.subscribe(Pattern.compile("(\\w*regex)"));

        while(true)
        {
            ConsumerRecords<String, GenericRecord> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, GenericRecord> record :consumerRecords)
            {
                log.info(Thread.currentThread().getName() + "\n"
                        + "Topic: " + record.topic() + "\n" +
                        "Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n");
            }
        }

    }

}





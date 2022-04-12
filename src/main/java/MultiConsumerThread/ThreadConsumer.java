package MultiConsumerThread;
import configuation.allConfigs;

import consumer.Consumer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ThreadConsumer {
    //logger
    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        try
        {
            //give the number of consumers and the number of groups
            String[] consumerGroups = new String[2];
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
                    try {
                        startConsumer(consumerID, consumerGroups[finalID]);
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }



    //get configs
    //start consumer
    public static void startConsumer(String consumerID, String consumerGroup) throws ExecutionException, InterruptedException {
        //------------------------add stuff------------------------------
        NewTopic newTopic = new NewTopic("new_temperature_8", 3, (short) 2);
        NewTopic newTopic2 = new NewTopic("wednesdayFunDay", 3, (short) 2);

        allConfigs.createTopic(newTopic);
        allConfigs.createTopic(newTopic2);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(allConfigs.getConsumerProps(consumerGroup));

        consumer.subscribe(Arrays.asList(newTopic.name(), newTopic2.name()));

            while(true)
            {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record :consumerRecords)
                {
                    //Header customHeader = record.headers().iterator().next();
                    log.info(Thread.currentThread().getName() + "\n"
                          + "Topic: " + record.topic() + "\n" +
                            "Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset());
                            //"Custom header: " + new String(customHeader.key()) + " Custom value: " + new String(customHeader.value()));
                }
            }

        }



}

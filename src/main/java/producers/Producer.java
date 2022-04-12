package producers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import properties.HelperClass;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private static final NewTopic topic0 = new NewTopic("NewSimpleTopic0",3, (short) 2);
    private static final NewTopic topic1 = new NewTopic("NewSimpleTopic1",3, (short) 2);

    public static void main(String args[]) throws IOException, ExecutionException, InterruptedException {
        Properties properties = HelperClass.getProducerProperties();

        HelperClass.createTopic(topic0);
        HelperClass.createTopic(topic1);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String customKey = "Batch";
        String customValue0 = "firstBatch";
        String customValue1 = "secondBatch";

        for(int i = 0; i < 10; i++){
            ProducerRecord<String, String> record0 = new ProducerRecord<String, String>(topic0.name(), "first " + i);
            ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(topic1.name(), "second " + i);


            if(i <= 5){
                record0.headers().add(customKey,customValue0.getBytes());
                record1.headers().add(customKey,customValue0.getBytes());
            }else {
                record0.headers().add(customKey,customValue1.getBytes());
                record1.headers().add(customKey,customValue1.getBytes());
            }

            Thread.sleep(1000);

            producer.send(record0);
            producer.send(record1);

        }

        producer.close();
    }

}


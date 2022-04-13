package producers;


import configs.ConfigGenerator;
import org.apache.kafka.clients.producer.*;
import java.util.*;


public class Producer {

    public static void main(String[] args) throws Exception {

        Properties properties = ConfigGenerator.getProducerProps();
        String topic1 = "NewSimpleTopic0";
        String topic2 = "NewSimpleTopic1";

        // Create topics if they don't exist
        ConfigGenerator.createTopic(topic1, 3);
        ConfigGenerator.createTopic(topic2, 3);

        // Instantiate Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Header Key
        String customKey = "Batch";

        // Header Values
        String customValue0 = "batch-1";
        String customValue1 = "batch-2";

        for(int i = 0; i < 10; i++){

            // Create producer records
            ProducerRecord<String, String> record0 = new ProducerRecord<>(topic1, "first " + i);
            ProducerRecord<String, String> record1 = new ProducerRecord<>(topic2, "second " + i);

            if(i <= 5){
                // Set first half of records' "Batch" value to "batch-1"
                record0.headers().add(customKey,customValue0.getBytes());
                record1.headers().add(customKey,customValue0.getBytes());
            }else {
                // Set second half of records' "Batch" value to "batch-2"
                record0.headers().add(customKey,customValue1.getBytes());
                record1.headers().add(customKey,customValue1.getBytes());
            }

            // Prevents batching so that records are sent to different partitions in a round-robin fashion
            Thread.sleep(1000);

            // Send records to kafka
            producer.send(record0);
            producer.send(record1);

        }

        producer.close();
    }


}

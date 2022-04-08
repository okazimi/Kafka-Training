package producers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {


    private static final NewTopic topic0 = new NewTopic("NewSimpleTopic0",3, (short) 2);
    private static final NewTopic topic1 = new NewTopic("NewSimpleTopic1",3, (short) 2);

    public static void main(String args[]) throws IOException, ExecutionException, InterruptedException {
        FileReader reader = new FileReader("src/main/resources/application.properties");
        Properties properties = new Properties();
        properties.load(reader);

        createTopic(topic0,properties);
        createTopic(topic1,properties);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i < 10; i++){
            ProducerRecord<String, String> record0 = new ProducerRecord<String, String>(topic0.name(), "first " + i);
            ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(topic1.name(), "second " + i);

            Thread.sleep(1000);

            producer.send(record0);
            producer.send(record1);

        }

        producer.close();
    }

    private static void createTopic(NewTopic topic, Properties properties) throws ExecutionException, InterruptedException {
        AdminClient admin = AdminClient.create(properties);
        if(!admin.listTopics().names().get().stream().anyMatch(expectedTopic->expectedTopic.equals(topic.name()))){
            admin.createTopics(Collections.singleton(topic));
        }
    }

}

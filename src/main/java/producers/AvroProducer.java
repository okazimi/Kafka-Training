package producers;

import com.example.TestRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import properties.HelperClass;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvroProducer {

    // Set options for topics
    private static final NewTopic topic0 = new NewTopic("topicAvroRegex",3, (short) 1);
    private static final NewTopic topic1 = new NewTopic("newTopicAvroRegex",3, (short) 1);

    public static void main(String args[]) throws IOException, ExecutionException, InterruptedException {
        Properties properties = HelperClass.getAvroProducerProperties();

        // Create topics if they don't exist
        HelperClass.createTopic(topic0);
        HelperClass.createTopic(topic1);

        // Initialize producer
        KafkaProducer<String, TestRecord> producer = new KafkaProducer<>(properties);

        // Create Avro records using TestRecord
        // TestRecord is generated using the test.avsc schema file
        TestRecord testRecord0 = TestRecord.newBuilder()
                .setFirstField("record12")
                .setSecondField(12).build();
        TestRecord testRecord1 = TestRecord.newBuilder()
                .setFirstField("record13")
                .setSecondField(23).build();

        // Create producer records using Avro records
        ProducerRecord<String, TestRecord> record0 = new ProducerRecord<String,TestRecord>(topic0.name(), testRecord0);
        ProducerRecord<String, TestRecord> record1 = new ProducerRecord<String,TestRecord>(topic1.name(), testRecord1);

        //record0.headers().add("customKey0","customValue0".getBytes());
        //record1.headers().add("customKey1","customValue1".getBytes());

        // Send records to Kafka
        producer.send(record0);
        producer.send(record1);

        producer.flush();
        producer.close();
    }


}


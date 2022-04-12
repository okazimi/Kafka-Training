package producers;

import com.example.TestRecord;
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

public class AvroProducer {

    private static final NewTopic topic0 = new NewTopic("AvroTopic0",3, (short) 1);
    private static final NewTopic topic1 = new NewTopic("AvroTopic1",3, (short) 1);

    public static void main(String args[]) throws IOException, ExecutionException, InterruptedException {
        Properties properties = HelperClass.getAvroProducerProperties();

        HelperClass.createTopic(topic0);
        HelperClass.createTopic(topic1);

        KafkaProducer<String, TestRecord> producer = new KafkaProducer<>(properties);

        TestRecord testRecord0 = TestRecord.newBuilder()
                .setFirstField("record12")
                .setSecondField(12).build();
        TestRecord testRecord1 = TestRecord.newBuilder()
                .setFirstField("record13")
                .setSecondField(23).build();

        ProducerRecord<String, TestRecord> record0 = new ProducerRecord<String,TestRecord>(topic0.name(), testRecord0);
        ProducerRecord<String, TestRecord> record1 = new ProducerRecord<String,TestRecord>(topic1.name(), testRecord1);

        //record0.headers().add("customKey0","customValue0".getBytes());
        //record1.headers().add("customKey1","customValue1".getBytes());

        producer.send(record0);
        producer.send(record1);

        producer.flush();
        producer.close();
    }


}


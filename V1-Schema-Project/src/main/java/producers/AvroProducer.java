package producers;

import com.example.TestRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import properties.HelperClass;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvroProducer {

    // Set options for topics
    private static final NewTopic topic0 = new NewTopic("topicAvroRegexV3",3, (short) 1);
    private static final NewTopic topic1 = new NewTopic("newTopicAvroRegexV3",3, (short) 1);

    public static void main(String args[]) throws IOException, ExecutionException, InterruptedException {
        Properties properties = HelperClass.getAvroProducerProperties();

        // Create topics if they don't exist
        HelperClass.createTopic(topic0);
        HelperClass.createTopic(topic1);

        // Initialize producers
        KafkaProducer<String, TestRecord> producer = new KafkaProducer<>(properties);
        KafkaProducer<String, GenericRecord> genericProducer = new KafkaProducer<>(properties);

        // Create Avro records using TestRecord
        // TestRecord is generated using the test.avsc schema file
        TestRecord testRecord0 = TestRecord.newBuilder()
                .setFirstField("v1")
                .setSecondField(1).build();
        TestRecord testRecord1 = TestRecord.newBuilder()
                .setFirstField("v1")
                .setSecondField(2).build();

        // Create genericRecord to test
        GenericRecord genericRecord = new GenericRecordBuilder(new Schema.Parser().parse(new File("src/main/resources/avro/test.avsc")))
                .set("firstField", "oogaBooga")
                .set("secondField", 3).build();

        // Create producer records using Avro records
        ProducerRecord<String, TestRecord> record0 = new ProducerRecord<String,TestRecord>(topic0.name(), testRecord0);
        ProducerRecord<String, TestRecord> record1 = new ProducerRecord<String,TestRecord>(topic1.name(), testRecord1);
        ProducerRecord<String, GenericRecord> record2 = new ProducerRecord<String,GenericRecord>(topic1.name(), genericRecord);

        //record0.headers().add("customKey0","customValue0".getBytes());
        //record1.headers().add("customKey1","customValue1".getBytes());
        record2.headers().add("customKey2","customValue2".getBytes()); // Only generic records can have custom headers

        // Send records to Kafka
        producer.send(record0);
        producer.send(record1);
        genericProducer.send(record2);

        producer.flush();
        producer.close();
        genericProducer.flush();
        genericProducer.close();
    }


}


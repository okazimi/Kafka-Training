package configuation;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class allConfigs {
    public static final String BROKERS = "localhost:9092";
    private static String schemaRegistryUrl = "http://127.0.0.1:8081";
    private static String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static String SPECIFIC_AVRO_READER = "specific.avro.reader";

    public static Properties getProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static Properties getConsumerProps(String consumerGroup) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BROKERS);
        props.setProperty("group.id", consumerGroup);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //look for a new topic every 5 second
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5000);
        return props;
    }

    public static Properties getAvroConsumerProperties(String consumerGroup){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.setProperty("bootstrap.servers", BROKERS);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        //props.setProperty(SPECIFIC_AVRO_READER, "true");
        props.setProperty(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5000);
        return props;
    }

    public static Properties getAvroProducerProperties(){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",BROKERS );
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.setProperty(SCHEMA_REGISTRY_URL,schemaRegistryUrl);
        return props;
    }



    public static void createTopic(NewTopic newTopic) throws ExecutionException, InterruptedException
    {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        AdminClient admin = AdminClient.create(properties);
        if(!admin.listTopics().names().get().stream().anyMatch(expectedTopic->expectedTopic.equals(newTopic.name()))){
            admin.createTopics(Collections.singleton(newTopic));
        }
    }

}

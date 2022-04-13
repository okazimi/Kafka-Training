package configs;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConfigGenerator {
    private static String bootstrapServer = "127.0.0.1:9092";
    private static String schemaRegistryUrl = "http://127.0.0.1:8081";
    private static String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static String SPECIFIC_AVRO_READER = "specific.avro.reader";

    public static Properties getProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public static Properties getAvroProducerProps(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(SCHEMA_REGISTRY_URL,schemaRegistryUrl);
        return properties;
    }

    public static Properties getConsumerProps(String consumerGroup) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServer);
        props.setProperty("group.id", consumerGroup);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public static Properties getAvroConsumerProps(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(SPECIFIC_AVRO_READER, "true");
        properties.setProperty(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    // Default topic generator (Replication Factor of 1)
    public static void createTopic(String topicName, int numPartitions) throws ExecutionException, InterruptedException
    {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigGenerator.bootstrapServer);
        AdminClient admin = AdminClient.create(config);

        //checking if topic already exists
        boolean alreadyExists = admin.listTopics().names().get().stream()
                .anyMatch(existingTopicName -> existingTopicName.equals(topicName));
        if (alreadyExists) {
            System.out.printf("topic already exits: %s%n", topicName);
        } else {
            //creating new topic
            System.out.printf("creating topic: %s%n", topicName);
            try{
                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
                admin.createTopics(Collections.singleton(newTopic)).all().get();
            } catch(TopicExistsException | ExecutionException ex){
                ex.printStackTrace();
            }
        }
    }

    // Topic Generator that allows for replication factor to be modified
    public static void createTopic(String topicName, int numPartitions, short replicationFactor) throws ExecutionException, InterruptedException
    {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigGenerator.bootstrapServer);
        AdminClient admin = AdminClient.create(config);

        //checking if topic already exists
        boolean alreadyExists = admin.listTopics().names().get().stream()
                .anyMatch(existingTopicName -> existingTopicName.equals(topicName));
        if (alreadyExists) {
            System.out.printf("topic already exits: %s%n", topicName);
        } else {
            //creating new topic
            System.out.printf("creating topic: %s%n", topicName);
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }
    }
}

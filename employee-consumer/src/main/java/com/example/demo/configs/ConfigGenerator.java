package com.example.demo.configs;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConfigGenerator {

    private static String bootstrapServer = "127.0.0.1:9092";
    private static String schemaRegistryUrl = "http://127.0.0.1:8081";
    private static String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static String SPECIFIC_AVRO_READER = "specific.avro.reader";

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
        properties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        properties.setProperty(SPECIFIC_AVRO_READER, "true");
        properties.setProperty(SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}

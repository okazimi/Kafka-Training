package multthreading.producer.consumer.avroschema;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerAndConsumerProperties {

  // PRODUCER
  public static Properties getProducerProperties() {
    // INITIALIZE PROPERTIES VARIABLE
    Properties properties = new Properties();
    // CONFIGURE PRODUCER PROPERTIES
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    // RETURN PROPERTIES
    return properties;
  }

  // CONSUMER
  public static Properties getConsumerProperties(String consumerGroup) {
    // INITIALIZE PROPERTIES VARIABLE
    Properties properties = new Properties();
    // CONFIGURE CONSUMER PROPERTIES
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    // RETURN PROPERTIES
    return properties;
  }

}

package multthreading.producer.consumer.avroschema;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

public class TopicCreator {

  // CREATE TOPIC FUNCTION
  public static void createTopic(String topicName, int numOfPartitions) throws ExecutionException, InterruptedException {
    // BROKER CONFIGURATION
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // INITIALIZE ADMIN CLIENT
    AdminClient admin = AdminClient.create(properties);


    // CHECK IF TOPIC ALREADY EXISTS
    boolean topicAlreadyExists = admin.listTopics().names().get().stream().anyMatch(existingTopicName -> existingTopicName.equals(topicName));

    // TOPIC ALREADY EXISTS
    if (topicAlreadyExists) {
      System.out.printf("Topic: %s already exists", topicName);
    }
    // TOPIC DOES NOT EXISTS
    else {
      // CREATE TOPIC
      System.out.printf("Creating topic: %s", topicName);
      NewTopic newTopic = new NewTopic(topicName, numOfPartitions, (short) 1);
      admin.createTopics(Collections.singleton(newTopic)).all().get();
    }

    // DESCRIBING TOPIC
    System.out.printf("%n-- Describing Topic --");
    // OBTAIN ALL TOPICS WITH DESCRIPTION
    admin.describeTopics(Collections.singleton(topicName)).all().get()
        // FOR EACH TOPIC,DESCRIPTION
        .forEach((topic,desc) -> {
          // PRINT OUT TOPIC NAME
          System.out.printf("%nTopic: " + topic);
           // PRINT OUT NUMBER OF PARTITIONS AND EACH PARTITION ID
          System.out.printf("%nPartitions: %s, Partition ID's: %s%n%n", desc.partitions().size(),
              desc.partitions()
              .stream()
              .map(p -> Integer.toString(p.partition()))
              .collect(Collectors.joining(",")));
        });

    // CLOSE ADMIN
    admin.close();
  }
}

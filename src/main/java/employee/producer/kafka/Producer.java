package employee.producer.kafka;

import employee.producer.model.Employee;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.avro.EmployeeAvroRecord;

public class Producer {

  // CONFIGURE PRODUCE PROPERTIES
  private static Properties getProducerProperties(){

    // INITIALIZE PROPERTIES VARIABLE
    Properties properties = new Properties();

    // CONFIGURE PRODUCER PROPERTIES
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

    // RETURN PROPERTIES
    return properties;
  }

  // SEND MESSAGE
  public static void sendMessage(String topicName, List<Employee> employeeList, String operation)
      throws InterruptedException {

    // INITIALIZE LOGGER
    Logger logger = LoggerFactory.getLogger(Producer.class.getSimpleName());

    // INITIALIZE TOPIC
    NewTopic topic = new NewTopic(topicName, 3, (short) 1);

    // INITIALIZE KAFKA PRODUCER
    KafkaProducer<String, EmployeeAvroRecord> producer = new KafkaProducer<>(getProducerProperties());

    // INITIALIZE KAFKA ADMIN
    Admin admin = Admin.create(Producer.getProducerProperties());

    // CHECK TOPIC CREATION
    try {

      // CHECK IF TOPIC EXISTS A.K.A ADMIN DESCRIBE TOPICS
      admin.describeTopics(Collections.singleton(topic.name())).values().get(topic.name()).get();

//      // CREATE HASHMAP TO INSERT NEW PARTITIONS (TOPIC, PARTITION COUNT)
//      Map<String, NewPartitions> numOfPartitions = new HashMap<>();
//
//      // INSERT THE DESIRED TOPIC AND PARTITION INCREASE
//      numOfPartitions.putIfAbsent("userSignedUp", NewPartitions.increaseTo(5));
//
//      // CREATE NEW PARTITION COUNT
//      admin.createPartitions(numOfPartitions);

    } catch (ExecutionException e) {

        System.out.println("Execution Exception due to the topic not previously existing...");
        System.out.println("Topic is currently being created...");

        // IF TOPIC DOES NOT EXIST CREATE IT
        admin.createTopics(Collections.singleton(topic));

    } catch (InterruptedException e) {

        // UNEXPECTED EXCEPTION
        System.out.println("UNEXPECTED EXCEPTION");
        e.printStackTrace();

    } finally {

          // LOOP THROUGH EACH EMPLOYEE IN EMPLOYEE LIST
          for(Employee employee : employeeList) {
            // CREATE EMPLOYEE AVRO RECORD FOR EACH EMPLOYEE
            EmployeeAvroRecord employeeAvroRecord = EmployeeAvroRecord.newBuilder()
                .setEmpId(employee.getEmpId())
                .setEmpName(employee.getEmpName())
                .build();

            // CREATE PRODUCER RECORD FOR EACH EMPLOYEE
            ProducerRecord<String,EmployeeAvroRecord> record = new ProducerRecord<>(topic.name(), employeeAvroRecord);

            record.headers().add("Operation", (operation).getBytes());

            // TRIGGER PRODUCER TO SEND RECORD AND UTILIZE CALLBACK TO LOG PRODUCER RECORD INFO
            producer.send(record, new Callback() {
              @Override
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // IF "E" IS NULL OR NO EXCEPTION EXISTS
                if (e == null) {
                  // RECORD WAS SUCCESSFULLY SENT, LOG INFO
                  logger.info("\n\nSending Message: \n"
                      + "Topic: " + recordMetadata.topic() + "\n" +
                      "Value: " +record.value()+ "\n" +
                      "Key: " + record.key() + "\n" +
                      "Partition: " + recordMetadata.partition() + "\n" +
                      "Offset: " + recordMetadata.offset() + "\n" +
                      "Timestamp: " + recordMetadata.timestamp() + "\n" +
                      "Headers: {Key: " + record.headers().iterator().next().key()+ ", Value: " + new String(record.headers().iterator().next().value())+ "}");
                }
                // ERROR IN RECORD BEING SENT
                else {
                  logger.error("Error while producing", e);
                }
              }
            });
          }
          // CLOSE PRODUCER
          producer.close();
    }
  }
}

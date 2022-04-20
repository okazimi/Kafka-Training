package employee.consumer.kafka;

import employee.consumer.model.Employee;
import employee.consumer.repository.EmployeeRepository;
import employee.consumer.service.EmployeeService;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.avro.EmployeeAvroRecord;

public class Consumer {

  // INITIALIZE REPOSITORY
  private static EmployeeRepository employeeRepository = EmployeeService.employeeRepository;

  // CONFIGURE CONSUMER PROPERTIES
  public static Properties getConsumerProperties() {

    // INITIALIZE PROPERTIES VARIABLE
    Properties properties = new Properties();

    // CONFIGURE CONSUMER PROPERTIES
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaAvroDeserializer.class.getName());
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "EmployeeDataConsumer");
    properties.setProperty("specific.avro.reader", "true");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // RETURN PROPERTIES
    return properties;
  }


  // LISTEN TO TOPIC "EMPLOYEE DATA" FOR MESSAGES
  @PostConstruct
  public static void listen() throws InterruptedException {

    // CREATE KAFKA CONSUMER AND SET PROPERTIES FOR CONSUMER
    KafkaConsumer<String, EmployeeAvroRecord> consumer = new KafkaConsumer<>(
        getConsumerProperties());

    // SUBSCRIBE TO TOPIC
    consumer.subscribe(Collections.singleton("EmployeeData"));

    // WHILE LOOP TO POLL AND OBTAIN RECORDS
    while (true) {
      // POLL FOR 1 SECOND AND OBTAIN RECORDS
      ConsumerRecords<String, EmployeeAvroRecord> records = consumer.poll(Duration.ofMillis(1000));
      if (records.isEmpty()) {
        Thread.sleep(5000);
      } else {
        // LOOP THROUGH EACH RECORD IN RECORDS
        for (ConsumerRecord<String, EmployeeAvroRecord> record : records) {
          // FILTER HEADERS TO DETERMINE OPERATION FOR
          if (new String(record.headers().iterator().next().value()).equals("insert")) {
            // SAVE EMPLOYEE TO MONGODB
            employeeRepository.save(new Employee(record.value().getEmpId(), record.value().getEmpName()));
          } else if (new String(record.headers().iterator().next().value()).equals("delete")) {
            // DELETE EMPLOYEE FROM MONGODB
            employeeRepository.deleteById(record.value().getEmpId());
          } else if (new String(record.headers().iterator().next().value()).equals("update")) {
            // CHECK IF EMPLOYEE EXISTS
            if (employeeRepository.findById(record.value().getEmpId()).isPresent()) {
              // UPDATE EMPLOYEE IN MONGODB
              employeeRepository.save(new Employee(record.value().getEmpId(), record.value().getEmpName()));
            } else {
              System.out.println("\nThe specified Employee does not exist");
              System.out.println("The Employee will now be saved as new entity...");
              employeeRepository
                  .save(new Employee(record.value().getEmpId(), record.value().getEmpName()));
            }
          }

          // PRINT OUT CONSUMER INFO
          System.out.printf(
              "%nConsumer Info %n Topic: %s%n Header: Key = %s, Value = %s%n Partition ID = %s%n Key = %s%n Value = %s%n Offset = %s%n",
              record.topic(), record.headers().iterator().next().key(),
              new String(record.headers().iterator().next().value()), record.partition(),
              record.key(), record.value(), record.offset());
        }
      }
    }
  }
}

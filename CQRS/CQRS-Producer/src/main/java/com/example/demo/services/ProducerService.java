package com.example.demo.services;

import com.example.EmployeeAvroRecord;
import com.example.demo.models.Employee;
import com.example.demo.repositories.ProducerRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.stream.Collectors;

@Service
@EnableScheduling
public class ProducerService {

    // Initialize MongoDB repository
    @Autowired
    ProducerRepository producerRepository;

    // Initialize KafkaTemplate
    @Autowired
    KafkaTemplate<String, EmployeeAvroRecord> kafkaTemplate;

    // Old list represents the state of the MySQL DB before the writeEmployeeData
    // method executes
    private static List<Employee> oldList;

    // Topic to send write operation messages to
    private static final String TOPIC = "CQRSTopic";

    // Set up oldList with the current state of the MySQL DB at startup
    @PostConstruct
    public void setUp(){
        oldList = producerRepository.findAll();
    }

    // **DEPRECATED** Send an employee from the MySQL database to Kafka which will get saved
    // into MongoDB by the consumer in the CQRS-Consumer service
    @Deprecated
    public Employee addEmployeeFromMySql(long empId) {
        Employee employee = producerRepository.findById(empId).get();
        EmployeeAvroRecord employeeAvroRecord = new EmployeeAvroRecord(employee.getEmpId(),employee.getEmpName());
        Message<EmployeeAvroRecord> message = MessageBuilder
                .withPayload(employeeAvroRecord)
                .setHeader(KafkaHeaders.TOPIC, TOPIC).build();
        kafkaTemplate.send(TOPIC,employeeAvroRecord);
        return employee;
    }

    // Create a new Employee
    public Employee addEmployee(Employee employee) {
        Employee savedEmployee = producerRepository.save(employee);
        EmployeeAvroRecord employeeAvroRecord = new EmployeeAvroRecord(savedEmployee.getEmpId(),savedEmployee.getEmpName());
        Message<EmployeeAvroRecord> message = MessageBuilder
                .withPayload(employeeAvroRecord)
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader("Action", "Add").build(); // Used to tell the consumer what to do with the message
        kafkaTemplate.send(message);
        return savedEmployee;
    }

    // Update an existing Employee
    public Employee updateEmployee(Employee employee) {
        Employee savedEmployee = producerRepository.save(employee);
        EmployeeAvroRecord employeeAvroRecord = new EmployeeAvroRecord(savedEmployee.getEmpId(),savedEmployee.getEmpName());
        Message<EmployeeAvroRecord> message = MessageBuilder
                .withPayload(employeeAvroRecord)
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader("Action", "Update").build(); // Used to tell the consumer what to do with the message
        kafkaTemplate.send(message);
        return savedEmployee;
    }

    // Delete an Employee
    public void deleteEmployee(Employee employee) {
        producerRepository.delete(employee);
        EmployeeAvroRecord employeeAvroRecord = new EmployeeAvroRecord(employee.getEmpId(),employee.getEmpName());
        Message<EmployeeAvroRecord> message = MessageBuilder
                .withPayload(employeeAvroRecord)
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader("Action","Delete").build(); // Used to tell the consumer what to do with the message
        kafkaTemplate.send(message);
    }

    // Check for discrepancies between the current state of the MySQL DB and the previous
    // state of the SQL DB, and send Kafka messages to sync
    @Scheduled(cron="0 * * * * ?")
    public void writeEmployeeData(){
        List<Employee> newList = producerRepository.findAll();
        // Tell Kafka about Employees that have been added to or changed in the MySQL DB
        if(!oldList.containsAll(newList)) {
            List<Employee> employeesToAdd = newList.stream().filter(employee -> !oldList.contains(employee)).collect(Collectors.toList());
            for (Employee employee : employeesToAdd) {
                EmployeeAvroRecord employeeAvroRecord = new EmployeeAvroRecord(employee.getEmpId(), employee.getEmpName());
                Message<EmployeeAvroRecord> message = MessageBuilder
                        .withPayload(employeeAvroRecord)
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader("Action", "Update").build();
                kafkaTemplate.send(message);
            }

        }
        // Tell Kafka about Employees that have been deleted from the MySQL DB
        if(!newList.containsAll(oldList)){
            List<Employee> employeesToDelete = oldList.stream().filter(employee -> !newList.contains(employee)).collect(Collectors.toList());
            for(Employee employee: employeesToDelete){
                EmployeeAvroRecord employeeAvroRecord = new EmployeeAvroRecord(employee.getEmpId(), employee.getEmpName());
                Message<EmployeeAvroRecord> message = MessageBuilder
                        .withPayload(employeeAvroRecord)
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader("Action", "Delete").build();
                kafkaTemplate.send(message);
            }
        }
        oldList = newList;
    }
}

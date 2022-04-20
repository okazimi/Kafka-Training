package com.example.demo.services;

import com.example.EmployeeAvroRecord;
import com.example.demo.models.Employee;
import com.example.demo.repositories.ConsumerRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ConsumerService {

    // Topic the the KafkaListener will listen to
    private static final String TOPIC = "CQRSTopic";

    @Autowired
    private ConsumerRepository consumerRepository;

    // Listen for write operations and syncs the MySQL write DB with the MongoDB write DB
    @KafkaListener(topics=TOPIC,groupId = "group")
    public void consume(ConsumerRecord<String, EmployeeAvroRecord> employeeAvroRecord){
        // Create entity that can be saved into MongoDB from a specific AvroRecord
        Employee mongoEmployee = new Employee(employeeAvroRecord.value().getEmpId(),employeeAvroRecord.value().getEmpName());
        // Execute appropriate command based on "Action" header
        Header customHeader = employeeAvroRecord.headers().iterator().next();
        switch (new String(customHeader.value())){
            case "Add":
                consumerRepository.save(mongoEmployee);
                break;
            case "Update":
                consumerRepository.save(mongoEmployee);
                break;
            case "Delete":
                consumerRepository.delete(mongoEmployee);
                break;
        }
        System.out.println(employeeAvroRecord.value().toString());
    }

    // Find all
    public List<Employee> findAllEmployees() {
        return consumerRepository.findAll();
    }

    public Employee findEmployeeById(long empId) {
        return consumerRepository.findById(empId).get();
    }
}

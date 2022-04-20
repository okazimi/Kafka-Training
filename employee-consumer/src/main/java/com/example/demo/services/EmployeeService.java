package com.example.demo.services;

import com.example.EmployeeAvroRecord;
import com.example.demo.configs.ConfigGenerator;
import com.example.demo.entities.Employee;
import com.example.demo.repositories.EmployeeRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

@Service
public class EmployeeService {
    @Autowired
    private EmployeeRepository employeeRepository;


    private List<String> topics = new ArrayList<String>();

    public EmployeeService(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
        topics.add("employee-create");
        topics.add("employee-read");
        topics.add("employee-update");
        topics.add("employee-delete");
    }

    public EmployeeRepository getEmployeeRepository() {
        return employeeRepository;
    }

    public void setEmployeeRepository(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    @KafkaListener(
            topics = {"employee-create", "employee-read", "employee-update", "employee-delete", "employee-sync"},
            groupId = "group_id")
    public void listen(ConsumerRecord<String, EmployeeAvroRecord> record) throws Exception{
        try{
            Employee emp = new Employee(record.value().getEmpid(), record.value().getEmpname());
            // Create Employee
            if (new String(record.topic()).equals("employee-create")) {
                employeeRepository.save(emp);
            }
            // Update
            else if(new String(record.topic()).equals("employee-update")) {
                // CHECK IF EMPLOYEE EXISTS
                if (employeeRepository.findById(record.value().getEmpid()).isPresent()) {
                    // UPDATE EMPLOYEE IN MONGODB
                    employeeRepository.save(emp);
                }
                else {
                    System.out.println("The specified Employee does not exist");
                    System.out.println("The Employee will now be saved as new entity...");
                    employeeRepository.save(emp);
                }

            }
            // Delete
            else if(new String(record.topic()).equals("employee-delete")) {
                // DELETE EMPLOYEE FROM MONGODB
                employeeRepository.deleteById(record.value().getEmpid());
            }
            else if(new String(record.topic()).equals("employee-sync")) {
                try{
                    Employee found = employeeRepository.findById(emp.getEmpId()).get();
                } catch (NoSuchElementException ex){
                    employeeRepository.save(emp);
                    System.out.println("Employee " + emp.getEmpName() +
                                       " with ID " + emp.getEmpId() +
                                       " Added to Database");
                }
            }
        } catch (Exception ex){
            ex.getStackTrace();
        }
        // PRINT OUT CONSUMER INFO
        System.out.printf(
                "\nConsumer Info \n Topic: %s\n Partition ID = %s\n Key = %s\n Value = %s\n Offset = %s\n",
                record.topic(), record.partition(),
                record.key(), record.value(), record.offset());

    }

    public Employee findById(int empId) throws Exception{
        return employeeRepository.findById(empId).get();
    }

    public List<Employee> findAllEmployees() throws Exception{
        List<Employee> employees = employeeRepository.findAll();
        return employees;
    }


}

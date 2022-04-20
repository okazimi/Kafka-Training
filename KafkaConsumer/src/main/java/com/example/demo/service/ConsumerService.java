package com.example.demo.service;

import com.employeeINFO.EmployeeAvroRecord;
import com.example.demo.model.Employee;
import com.example.demo.repository.EmployeeConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class ConsumerService {

    @Autowired
    EmployeeConsumer employeeConsumer;

    @KafkaListener(topics = "EmployeeINFO", groupId = "Employee")
    public void startEmployeeConsumer(ConsumerRecord<String, EmployeeAvroRecord> record) throws InterruptedException
    {

            if(new String(record.headers().iterator().next().value()).equals("add"))
            {
                employeeConsumer.save(new Employee(record.value().getEmpId(), record.value().getEmpName()));
            }
            else if(new String(record.headers().iterator().next().value()).equals("update"))
            {
                if(employeeConsumer.findById(record.value().getEmpId()).isPresent())
                {
                        employeeConsumer.save(new Employee(record.value().getEmpId(), record.value().getEmpName()));
                }
                else
                {
                    System.out.println("That employee ID doesn't exist");
                }
            }
            else if (new String(record.headers().iterator().next().value()).equals("delete"))
            {
                employeeConsumer.deleteById(record.value().getEmpId());
            }
            System.out.printf("Employee INFO \n" +
                    "Topic: %s%n \n" +
                    "Header: %s%n \n" +
                    "Value: %s%n \n" +
                    "Partition: %s%n \n" +
                    "key: %s%n \n" +
                    "Offset: %s%n", record.topic(), record.headers(),record.value(), record.partition(),
                    record.value(), record.key(), record.offset());
        }


    //find all employees
    public List<Employee> findAll() {
        return employeeConsumer.findAll();
    }

    //delete an employee
    public void delete(Employee employee) {
        employeeConsumer.delete(employee);
    }

    //find an employee by Id
    public Employee findById(int empId) {
        return employeeConsumer.findById(empId).get();
    }

    //add an employee
    public Employee addEmployee(Employee employee) {
        return employeeConsumer.save(employee);
    }

    //update an employees info
    public Employee update(Employee employee) {
        Employee foundEmpName = employeeConsumer.findById(employee.getEmpId()).get();
        if(foundEmpName == null){
            System.out.println("That employee doesn't exist");
        }else{
            foundEmpName.setEmpName(employee.getEmpName());
            employeeConsumer.save(foundEmpName);
        }
        return foundEmpName;
    }

}

package com.example.demo.controllers;

import com.example.demo.models.Employee;
import com.example.demo.services.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/employees")
public class ProducerController {

    // Initialize the ProducerService
    @Autowired
    ProducerService producerService;

    // **DEPRECATED** Send an employee from the MySQL database to Kafka which will get saved
    // into MongoDB by the consumer in the CQRS-Consumer service
    @Deprecated
    @PostMapping("/send")
    public Employee addEmployeeFromMySql(@RequestParam long empId){
        return producerService.addEmployeeFromMySql(empId);
    }

    // Create a new Employee
    @PostMapping("/add")
    public Employee addEmployee(@RequestBody Employee employee){
        return producerService.addEmployee(employee);
    }

    // Update an existing employee
    @PutMapping("/update") //localhost:portNum/orders/update
    public Employee updateEmployee(@RequestBody Employee employee) {
        return producerService.updateEmployee(employee);
    }

    // Delete an employee
    @DeleteMapping("/delete")
    public void deleteEmployee(@RequestBody Employee employee){
        producerService.deleteEmployee(employee);
    }
}

package com.example.demo.controllers;

import com.example.demo.models.Employee;
import com.example.demo.services.ConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/employees")
public class ConsumerController {

    // Initialize the ConsumerService
    @Autowired
    ConsumerService consumerService;

    // Find all Employees
    @GetMapping("/findAll")
    public List<Employee> findAllEmployees(){
        return consumerService.findAllEmployees();
    }

    // Find Employee by ID
    @GetMapping("/findById")
    public Employee findEmployeeById(@RequestParam long empId){
        return consumerService.findEmployeeById(empId);
    }

}

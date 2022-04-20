package com.example.demo.controller;

import com.example.demo.model.Employee;
import com.example.demo.service.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/employee")
public class ProducerController {

    @Autowired
    private EmployeeService employeeService;

    public ProducerController(EmployeeService employeeService) {
        this.employeeService = employeeService;
    }

    @PostMapping("/save")
    public Employee saveEmployee(@RequestBody Employee employee) throws ExecutionException, InterruptedException {
        return employeeService.addEmployee(employee);
    }

    @PostMapping("/update")
    public Employee updateEmployee(@RequestBody Employee employee) throws ExecutionException, InterruptedException {
        return employeeService.updateEmployee(employee);
    }

}

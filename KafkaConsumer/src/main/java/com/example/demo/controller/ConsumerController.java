package com.example.demo.controller;

import com.example.demo.model.Employee;
import com.example.demo.service.ConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/employee")
public class ConsumerController {

    @Autowired
    ConsumerService consumerService;

    public ConsumerController(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @GetMapping("/all")
    public List<Employee> getAllEmployeeINFO(){
        return consumerService.findAll();
    }

    @GetMapping("/delete")
    public void deleteEmployee(Employee employee){
        consumerService.delete(employee);
    }

    @GetMapping("/find{id}")
    public Employee findEmployeeById(@PathVariable("id") int empId){
        return consumerService.findById(empId);
    }

    @PostMapping("/add")
    public Employee addEmployee(@RequestBody Employee employee){
        return consumerService.addEmployee(employee);
    }

    @PostMapping("/update")
    public Employee updateEmployee(@RequestBody Employee employee){
        return consumerService.update(employee);
    }


}

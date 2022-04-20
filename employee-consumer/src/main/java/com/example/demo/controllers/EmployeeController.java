package com.example.demo.controllers;

import com.example.EmployeeAvroRecord;
import com.example.demo.entities.Employee;
import com.example.demo.services.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/employee")
public class EmployeeController {
    @Autowired
    private EmployeeService employeeService;

    public EmployeeController(EmployeeService employeeService) {
        this.employeeService = employeeService;
    }

    public EmployeeService getEmployeeService() {
        return employeeService;
    }

    public void setEmployeeService(EmployeeService employeeService) {
        this.employeeService = employeeService;
    }

    @GetMapping("/findById")
    public Employee findByID(@RequestParam int empId) throws Exception{
        Employee emp = null;
        try{
            emp = employeeService.findById(empId);
            if(emp == null){
                System.out.println("Employee with id " + empId + " does not exist in Database.");
            } else{
                System.out.println("Employee " + emp.getEmpName() + " found in Database.");
            }
        }catch(Exception ex) {
            ex.getStackTrace();
        }
        return emp;
    }

    @GetMapping("/all")
    public String findAllEmployees() throws Exception{
        try{
            List<Employee> employees = employeeService.findAllEmployees();

            // RETURN SUCCESS STATEMENT
            return "Successful: Obtained all Employees\n\n"
                    + "Please find Employee Data listed below:\n"
                    + employees.toString()
                    .replace("[","")
                    .replace("]","");
        } catch(Exception ex) {
            return ex.getStackTrace().toString();
        }
    }
}

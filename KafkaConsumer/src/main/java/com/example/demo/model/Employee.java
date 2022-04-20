package com.example.demo.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;



@Document(collection = "EmployeeINFO")
public class Employee {

    @Id
    private int empId;
    private String empName;

    //constructor
    public Employee(int empId, String empName) {
        this.empId = empId;
        this.empName = empName;
    }

    //getters and setters
    public int getEmpId() {
        return empId;
    }

    public void setEmpId(int empId) {
        this.empId = empId;
    }

    public String getEmpName() {
        return empName;
    }

    public void setEmpName(String empName) {
        this.empName = empName;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "empId=" + empId +
                ", empName='" + empName + '\'' +
                '}';
    }
}

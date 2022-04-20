package com.example.demo.models;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "EMPLOYEE")
public class Employee{

    @Id
    private long empId;

    private String empName;

    public long getEmpId() {
        return empId;
    }

    public void setEmpId(long empId) {
        this.empId = empId;
    }

    public String getEmpName() {
        return empName;
    }

    public void setEmpName(String empName) {
        this.empName = empName;
    }

    public Employee(long empId, String empName) {
        this.empId = empId;
        this.empName = empName;
    }

    public Employee(){
        super();
    }
}

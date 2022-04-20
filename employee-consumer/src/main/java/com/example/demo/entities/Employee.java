package com.example.demo.entities;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "EMPLOYEES")
public class Employee {

    @Id
    private int empId;          // Primary Key
    private String empname;

    // Constructors
    public Employee() {
        super();
    }

    public Employee(int empid, String empname) {
        this.empId = empid;
        this.empname = empname;
    }

    // Accessors
    public int getEmpId() {
        return empId;
    }

    public String getEmpName() {
        return empname;
    }

    // Mutators
    public void setEmpId(int empId) {
        this.empId = empId;
    }

    public void setEmpName(String empName) {
        this.empname = empName;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "empId=" + empId +
                ", empName='" + empname + '\'' +
                '}';
    }

}


package com.example.demo.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import javax.persistence.*;

@Entity
@Table(name = "EMPLOYEES")
public class Employee {

    @Id
    @Column(name="EMPID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int empId;          // Primary Key

    @Column(name="EMPNAME")
    private String empname;

    // Constructors
    public Employee() {
        super();
    }

    public Employee(int empId, String empName) {
        this.empId = empId;
        this.empname = empName;
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

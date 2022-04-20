package com.example.demo.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serializer;

import javax.persistence.*;
import java.util.Objects;

@Entity
@Table(name="EMPLOYEE")
//@Data
//@AllArgsConstructor
public class Employee{

    @Id
    @Column(name = "EMPID")
    private long empId;

    @Column(name="EMPNAME")
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee employee = (Employee) o;
        return this.empId == employee.getEmpId() && Objects.equals(this.empName, employee.getEmpName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(empName, empId);
    }
}

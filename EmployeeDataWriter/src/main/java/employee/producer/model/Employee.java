package employee.producer.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "employees")
public class Employee {

  // INITIALIZE TABLE VARIABLES
  @Id @Column(name = "EMPID") private int empId;
  @Column(name = "EMPNAME") private String empName;

  // DEFAULT CONSTRUCTOR
  public Employee() {
    super();
  }

  // CONSTRUCTOR
  public Employee(int empId, String empName) {
    this.empId = empId;
    this.empName = empName;
  }

  // SETTERS AND GETTERS
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
    return "\nEmployee ID: " +empId+ " | Employee Name: " +empName+ "";
  }
}

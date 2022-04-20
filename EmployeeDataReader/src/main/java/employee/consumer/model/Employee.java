package employee.consumer.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


@Document(collection = "EMPLOYEES")
public class Employee {

  // INITIALIZE TABLE VARIABLES
  @Id private int empId;
  private String empName;

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

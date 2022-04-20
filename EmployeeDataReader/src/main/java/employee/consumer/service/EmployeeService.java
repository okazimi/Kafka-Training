package employee.consumer.service;

import employee.consumer.model.Employee;
import employee.consumer.repository.EmployeeRepository;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EmployeeService {

  // INITIALIZE REPOSITORY
  @Autowired
  public static EmployeeRepository employeeRepository;

  // CONSTRUCTOR
  public EmployeeService(EmployeeRepository employeeRepository) {
    this.employeeRepository = employeeRepository;
  }

  // GETTERS AND SETTERS
  public EmployeeRepository getEmployeeRepository() {
    return employeeRepository;
  }

  public void setEmployeeRepository(EmployeeRepository employeeRepository) {
    this.employeeRepository = employeeRepository;
  }

  // FIND ALL EMPLOYEES (READ/QUERY)
  public List<Employee> findAllEmployees() throws InterruptedException {

    // OBTAIN LIST OF EMPLOYEES FROM MYSQL DB
    List<Employee> employees = employeeRepository.findAll();

    // RETURN EMPLOYEE DATA TO PRESENT TO USER
    return employees;
  }
}

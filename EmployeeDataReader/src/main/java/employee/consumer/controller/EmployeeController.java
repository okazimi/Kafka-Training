package employee.consumer.controller;

import employee.consumer.model.Employee;
import employee.consumer.service.EmployeeService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("employees")
public class EmployeeController {

  // INITIALIZE EMPLOYEE SERVICE
  @Autowired
  private EmployeeService employeeService;

  // CONSTRUCTOR
  public EmployeeController(EmployeeService employeeService) {
    super();
    this.employeeService = employeeService;
  }

  // GETTERS AND SETTERS
  public EmployeeService getEmployeeService() {
    return employeeService;
  }

  public void setEmployeeService(EmployeeService employeeService) {
    this.employeeService = employeeService;
  }

  // FIND ALL EMPLOYEES (READ/QUERY)
  @GetMapping("/findAllEmployees")
  public String findAllEmployees() {
    try {

      // FIND ALL EMPLOYEES AND SEND KAFKA MESSAGE FOR EACH EMPLOYEE
      List<Employee> employeeList = employeeService.findAllEmployees();

      // RETURN SUCCESS STATEMENT
      return "Successful: Obtained all Employees\n\n"
          + "Please find Employee Data listed below:\n"
          + employeeList.toString().replace("[","").replace("]","");

    } catch(Exception e) {

      // PRINT STACK TRACE
      e.printStackTrace();

      // UNEXPECTED EXCEPTION
      return "Error: Unexpected Exception | Please refer to console log";
    }
  }
}

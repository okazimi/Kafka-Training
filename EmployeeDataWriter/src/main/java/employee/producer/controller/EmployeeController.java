package employee.producer.controller;

import employee.producer.model.Employee;
import employee.producer.service.EmployeeService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("employees")
@EnableScheduling
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

  // INSERT EMPLOYEE
  @PostMapping("/insertEmployee") // localhost:8070/employees/insertEmployee
  public String insertEmployee(@RequestBody List<Employee> employeeList) {
    try {

      // CALL EMPLOYEE SERVICE TO SEND KAFKA MESSAGE
      employeeService.insertEmployee(employeeList);

      // RETURN SUCCESS STATEMENT
      return "Successful: Insert Employee\n\n"
          + "Forwarded Employee Info to Kafka via Producer\n\n"
          + "Please find Employee Data listed below:\n"
          + employeeList.toString().replace("[","").replace("]","");

    } catch(Exception e) {

      // PRINT STACK TRACE
      e.printStackTrace();

      // UNEXPECTED EXCEPTION
      return "Error: Unexpected Exception | Please refer to console log";
    }
  }

  // DELETE EMPLOYEE
  @DeleteMapping("/deleteEmployee") // localhost:8070/employees/deleteEmployee
  public String deleteEmployee(@RequestBody List<Employee> employeeList) {
    try {

      // CALL EMPLOYEE SERVICE TO SEND KAFKA MESSAGE
      employeeService.deleteEmployee(employeeList);

      // RETURN SUCCESS STATEMENT
      return "Successful: Delete Employee\n\n"
          + "Forwarded Employee Info to Kafka via Producer\n\n"
          + "Please find Employee Data listed below:\n"
          + employeeList.toString().replace("[","").replace("]","");

    } catch(Exception e) {

      // PRINT STACK TRACE
      e.printStackTrace();

      // UNEXPECTED EXCEPTION
      return "Error: Unexpected Exception | Please refer to console log";
    }
  }

  // UPDATE EMPLOYEE
  @PutMapping("/updateEmployee")
  public String updateEmployee(@RequestBody List<Employee> employeeList) {
    try {

      // CALL EMPLOYEE SERVICE TO SEND KAFKA MESSAGE
      employeeService.updateEmployee(employeeList);

      // RETURN SUCCESS STATEMENT
      return "Successful: Updated Employee\n\n"
          + "Forwarded Employee Info to Kafka via Producer\n\n"
          + "Please find Employee Data listed below:\n"
          + employeeList.toString().replace("[","").replace("]","");

    } catch(Exception e) {

      // PRINT STACK TRACE
      e.printStackTrace();

      // UNEXPECTED EXCEPTION
      return "Error: Unexpected Exception | Please refer to console log";
    }
  }

  // READ FROM SQLDB AND FORWARD
  @Scheduled(cron = "0 14 16 * * ?")
  @GetMapping("/sqlToMongoDb")
  public String sqlToMongoDb() {
    try {

      // CALL EMPLOYEE SERVICE TO SEND KAFKA MESSAGE
      List<Employee> employeeList = employeeService.sqlToMongoDb();

      // RETURN SUCCESS STATEMENT
      return "Successful: Forwarded Employee Data from MySQL to MongoDB\n\n"
          + "Forwarded Employee Info to Kafka via Producer\n\n"
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

package employee.producer.service;

import employee.producer.kafka.Producer;
import employee.producer.model.Employee;
import employee.producer.repository.EmployeeRepository;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


@Service
@EnableScheduling
public class EmployeeService {

  // INITIALIZE EMPLOYEE REPOSITORY
  @Autowired
  private EmployeeRepository employeeRepository;

  // INITIALIZE OLD EMPLOYEE LIST
  private List<Employee> oldList = initialSQLDbRead();

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

  // INSERT EMPLOYEE
  public void insertEmployee(List<Employee> employeeList) throws InterruptedException {
    // CALL KAFKA PRODUCER TO INSERT EMPLOYEE
    Producer.sendMessage("EmployeeData", employeeList, "insert");
  }

  // DELETE EMPLOYEE
  public void deleteEmployee(List<Employee> employeeList) throws InterruptedException {
    // CALL KAFKA PRODUCER TO DELETE EMPLOYEE
    Producer.sendMessage("EmployeeData", employeeList, "delete");
  }

  // UPDATE EMPLOYEE
  public void updateEmployee(List<Employee> employeeList) throws InterruptedException {
    // CALL KAFKA PRODUCER TO UPDATE EMPLOYEE
    Producer.sendMessage("EmployeeData", employeeList, "update");
  }

  // READ FROM MYSQL AND PUBLISH TO KAFKA
  public List<Employee> sqlToMongoDb() throws InterruptedException {
    // OBTAIN LIST OF ALL PRESENT EMPLOYEES IN MYSQL DB
    List<Employee> employeeList = employeeRepository.findAll();
    // FORWARD LIST TO KAFKA
    Producer.sendMessage("EmployeeData", employeeList, "update");
    // RETURN EMPLOYEE LIST
    return employeeList;
  }

  // INITIAL READ FROM SQL DB TO COMPARE
  private List<Employee> initialSQLDbRead() {
    return employeeRepository.findAll();
  }

  // CHECK FOR CHANGES IN MYSQLDB AND FORWARD TO MONGODB
  @Scheduled(cron = "5 * * * * ?")
  public void mySQLComparer() throws InterruptedException {
    // FIND CURRENT MYSQL EMPLOYEE LIST
    List<Employee> newList = employeeRepository.findAll();
    // EXTRA EMPLOYEES IN NEW LIST (ADD)
    if(!oldList.containsAll(newList)){
      // FILTER AND OBTAIN EXTRA EMPLOYEES NOT PRESENT IN OLD LIST
      List<Employee> employeesToAdd = newList.stream().filter(employee -> !oldList.contains(employee)).collect(Collectors.toList());
      // FORWARD LIST OF EMPLOYEES TO ADD TO MONGODB
      Producer.sendMessage("EmployeeData", employeesToAdd, "insert");
     }
    // EXTRA EMPLOYEES IN OLD LIST (DELETE)
    else if (!newList.containsAll(oldList)) {
      // FILTER AND OBTAIN EXTRA EMPLOYEES NOT PRESENT IN NEW LIST
      List<Employee> employeesToDelete = oldList.stream().filter(employee -> !newList.contains(employee)).collect(Collectors.toList());
      // FORWARD LIST OF EMPLOYEES TO DELETE FROM MONGODB
      Producer.sendMessage("EmployeeData", employeesToDelete, "delete");
    }
    // FINALLY SET OLD LIST = NEW LIST
    oldList = newList;
  }
}

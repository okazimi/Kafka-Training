package com.example.demo.controllers;

import com.example.demo.entities.Employee;
import com.example.demo.services.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/employee")        //      localhost:8088/employee
public class EmployeeController {
    @Autowired
    private EmployeeService employeeService;

    public EmployeeController(EmployeeService employeeService) {
        this.employeeService = employeeService;
    }

    /********************************** Create Operations *************************************************/
    /** Adds Employee Object to Database
     *
     * @param employee Employee Object
     * @return Employee Object added to database
     * @throws Exception
     */
    @PostMapping("/add")
    public Employee addEmployee(@RequestBody Employee employee) throws Exception{
        return employeeService.addEmployee(employee);
    }

/*********************************** Read Operations **************************************************/
    /** Calls service to send found employee via ID
     *
     * @param empid Employee ID
     * @return found Employee Object associated with (empid)
     * @throws Exception
     */
//    @GetMapping("/findByID")
//    public Employee sendEmployee(@RequestParam int empid) throws Exception{
//        return employeeService.sendEmployee(empid);
//    }

    /** Calls service to send specific found Employee Object
     *
     * @param employee Employee Object
     * @return specified Employee object
     * @throws Exception
     */
    @GetMapping("/send")
    public Employee sendEmployee(@RequestBody Employee employee) throws Exception{
        System.out.println(employee.getEmpName());
        return employeeService.sendEmployee(employee);
    }

    /** Dumps all Database Entries
     *
     * @return List of all available Employee Objects in DB
     * @throws Exception
     */
//    @GetMapping("/")
//    public List<Employee> findAll() throws Exception{
//        return employeeService.findAll();
//    }

/********************************** Update Operations *************************************************/
    /** Updates the Name Attribute of a specified Employee Object in the Database
     *
     * @param empid Employee ID of desired Object to Update
     * @param name Name to update Employee Object
     * @return Updated Employee Object
     * @throws Exception
     */
    @PutMapping("/update")
    public Employee updateEmployee(@RequestParam int empid, @RequestParam String name) throws Exception{

        return employeeService.updateName(empid, name);
    }

/********************************** Delete Operations *************************************************/
    /** Deletes specific employee from Database
     *
     * @param empid Employee ID of desired Employee Object
     * @throws Exception
     */
    @DeleteMapping("/delete")
    public void deleteEmployee(@RequestParam int empid) throws Exception{
        employeeService.deleteEmployee(empid);
    }

/********************************* Sync DB Operations *************************************************/
    /** Updates the Consumer MongoDB with Producer MySQL Data
     *
     * @return String Output confirming completion of task
     * @throws Exception
     */
    @GetMapping("/sync")
    public String syncDatabases() throws Exception{
        employeeService.syncEmployees();
        return "MongoDB Synced Updated with MySQL Data";
    }
}





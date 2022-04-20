package com.example.demo.services;

import com.example.EmployeeAvroRecord;
import com.example.demo.configs.ConfigGenerator;
import com.example.demo.entities.Employee;
import com.example.demo.repositories.EmployeeRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.NoSuchElementException;

@Service
public class EmployeeService {

    @Autowired
    private EmployeeRepository employeeRepository;

    @Autowired
    static KafkaProducer<String, EmployeeAvroRecord> producer = new KafkaProducer<>(ConfigGenerator.getAvroProducerProps());

    private static final String createTopic = "employee-create";
    private static final String readTopic = "employee-read";
    private static final String updateTopic = "employee-update";
    private static final String deleteTopic = "employee-delete";
    private static final String syncTopic = "employee-sync";


    public EmployeeService(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

/*********************************** Setters/Getters **************************************************/
    public static KafkaProducer<String, EmployeeAvroRecord> getProducer() {
        return producer;
    }

    public static void setProducer(KafkaProducer<String, EmployeeAvroRecord> producer) {
        EmployeeService.producer = producer;
    }

    public EmployeeRepository getEmployeeRepository() {
        return employeeRepository;
    }

    public void setEmployeeRepository(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    /** Searches Repository for Employee
     *
     * @param id Employee ID
     * @return valid Employee Object if found, null if not found
     * @throws Exception
     */
    public Employee findEmployee(int id) throws Exception{
        try{
            Employee found = employeeRepository.getById(id);
            return found;
        } catch (NoSuchElementException ex){
            System.out.println("Employee Does Not Exist in Database");
            return null;
        }
    }

/*********************************** CRUD Operations **************************************************/

/********************************** Create Operations *************************************************/
/***************************** Do Not Use This With Consumer ******************************************/
    /** Adds Employee to DB, then sends message to "employee-create" topic
     *
     * @param employee Employee Object to be added to DB
     * @return Employee object added to DB, or Null if already in DB
     * @throws Exception
     */
    public Employee addEmployee(Employee employee) throws Exception{
        Employee found = findEmployee(employee.getEmpId());
        // Employee Not in DB
        if(found == null){
            employeeRepository.save(employee);
            sendAvroMessage(createTopic, employee);
            System.out.printf("Employee Added To Database\n");
            System.out.printf("Employee ID: %d\nEmployee Name: %s\n", employee.getEmpId(), employee.getEmpName());
        }

        return employee;
    }

/*********************************** Read Operations **************************************************/
    /** Finds Employee in DB with ID, then producer sends to "employee-read" topic
     *
     * @param EMPID Employee ID
     * @return Employee object associated with EMPID
     * @throws Exception
     */
    public Employee sendEmployee(int EMPID) throws Exception{
        Employee found = findEmployee(EMPID);
        if(found != null){
            sendAvroMessage(readTopic, found);
        } else{
            System.out.println("Employee Does Not Exist In Database");
        }
        return found;
    }
    public List<Employee> getAllEmployees() throws Exception{
        List<Employee> employees = employeeRepository.findAll();
        sendAvroMessage(readTopic, employees);
        return employees;
    }
    /** Produces a message to "employee-read" topic containing emp Employee Object
     *
     * @param emp Specific Employee Object
     * @return the same object that was Input (emp)
     * @throws Exception
     */
    public Employee sendEmployee(Employee emp) throws Exception{
        sendEmployee(emp.getEmpId());
        return emp;
    }

    /** Sends Contents of MySQL database to Consumer under "employee-sync" topic, which allows
     * mongoDB to update its DB accordingly
     *
     * @throws Exception
     */
    public void syncEmployees() throws Exception{
        List<Employee> employees = employeeRepository.findAll();
        sendAvroMessage(syncTopic, employees);
    }

    /** Returns a list of all employees in DB, then sends messages to "employee-read" topic
     * containing each employee
     *
     * @return list of Employee Objects contained within DB
     * @throws Exception
     */
//    public List<Employee> findAll() throws Exception{
//        // Generate List as local var
//        List<Employee> employees = employeeRepository.findAll();
//        // Send List as message to Producer
//        try{
//            sendAvroMessage(readTopic, employees);
//        } catch (Exception ex){
//            ex.getStackTrace();
//        }
//        // Return List
//        return employees;
//    }

    /** Sends Avro Formatted Message of all Employees in a given List To A Specified Topic
     *
     * @param topic name of topic where data is sent
     * @param employees List of Employee objects to be sent
     * @throws Exception
     */
    public static void sendAvroMessage(String topic, List<Employee> employees) throws Exception{
        try{
            // Check to see if Topic has been created, otherwise create it
            ConfigGenerator.createTopic(topic, 3);
        } catch (Exception ex){
            ex.getStackTrace();
        } finally {
            // Generate Records For Employees in List
            for(Employee employee : employees){
                EmployeeAvroRecord employeeData = new EmployeeAvroRecord(employee.getEmpId(), employee.getEmpName());
                ProducerRecord<String, EmployeeAvroRecord> record = new ProducerRecord<>(topic, employeeData);
                producer.send(record);
            }
        }
    }

    /** Sends Avro Formatted Message of a Specific Employee To A Specified Topic
     *
     * @param topic name of topic where data is sent
     * @param employee Specific Employee object to be sent
     * @throws Exception
     */
    public static void sendAvroMessage(String topic, Employee employee) throws Exception{
        try{
            // Check to see if Topic has been created, otherwise create it
            ConfigGenerator.createTopic(topic, 3);
        } catch (Exception ex){
            ex.getStackTrace();
        } finally {
            // Generate Record For Employee
            EmployeeAvroRecord employeeData = new EmployeeAvroRecord(employee.getEmpId(), employee.getEmpName());
            ProducerRecord<String, EmployeeAvroRecord> record = new ProducerRecord<>(topic, employeeData);
            producer.send(record);
        }
    }


/********************************** Update Operations *************************************************/
/***************************** Do Not Use This With Consumer ******************************************/
    /** Finds specific Employee Object by EMPID, updates its name, then sends message to
     * "employee-update" topic
     *
     * @param EMPID Employee ID
     * @param name Employee Name
     * @return Updated Employee Object, or NULL if does not exist in database
     * @throws Exception
     */
    public Employee updateName(int EMPID, String name) throws Exception{
        Employee found = null;
        try {
            found = employeeRepository.findById(EMPID).get();
            found.setEmpName(name);
            employeeRepository.save(found);
            try{
                ConfigGenerator.createTopic(updateTopic, 3);
            } catch (Exception ex){
                ex.getStackTrace();
            } finally {
                EmployeeAvroRecord employeeData = new EmployeeAvroRecord(found.getEmpId(), found.getEmpName());
                ProducerRecord<String, EmployeeAvroRecord> record = new ProducerRecord<>(updateTopic, employeeData);
                producer.send(record);
            }
        } catch(NoSuchElementException ex){
            System.out.println("Employee Does Not Exist in Database");
        }
        return found;
    }

/********************************** Delete Operations *************************************************/
/***************************** Do Not Use This With Consumer ******************************************/
    /** Finds employee in DB and deletes, then sends message to "employee-delete" topic
     *
     * @param EMPID Employee ID
     * @throws Exception
     */
    public void deleteEmployee(int EMPID) throws Exception{
        try{
            Employee found = employeeRepository.findById(EMPID).get();
            employeeRepository.delete(found);
            try{
                ConfigGenerator.createTopic(deleteTopic, 3);
            } catch (Exception ex){
                ex.getStackTrace();
            } finally {
                EmployeeAvroRecord employeeData = new EmployeeAvroRecord(found.getEmpId(), found.getEmpName());
                ProducerRecord<String, EmployeeAvroRecord> record = new ProducerRecord<>(deleteTopic, employeeData);
                producer.send(record);
            }
        } catch(NoSuchElementException ex){
            System.out.println("Employee Does Not Exist in Database");
        }
    }
}

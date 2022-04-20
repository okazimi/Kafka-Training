package com.example.demo.service;

import com.employeeINFO.EmployeeAvroRecord;
import com.example.demo.configuration.allConfigs;
import com.example.demo.model.Employee;
import com.example.demo.repository.ProducerRepository;
import org.apache.kafka.clients.admin.NewTopic;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

@Service
public class EmployeeService{

    private static final Logger log = LoggerFactory.getLogger(EmployeeService.class.getSimpleName());

    @Autowired
    private ProducerRepository producerRepository;

    /**
     * @param producerRepository
     * constructor
     */
    public EmployeeService(ProducerRepository producerRepository) {
        this.producerRepository = producerRepository;
    }

    /**
     * all getters and setters
     * @return
     */
    public ProducerRepository getProducerRepository() {
        return producerRepository;
    }

    /**
     * @param producerRepository
     */
    public void setProducerRepository(ProducerRepository producerRepository) {
        this.producerRepository = producerRepository;
    }

    /**
     * @param employee
     * @return the saved Employee from sql
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Employee addEmployee(Employee employee) throws ExecutionException, InterruptedException {
        Employee savedEmployee = producerRepository.save(employee);
        sendAddMessage( "EmployeeINFO", "add" ,savedEmployee);
        return savedEmployee;
    }

    /**
     *
     * @param employee
     * @return the updated employee if updated
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Employee updateEmployee(Employee employee) throws ExecutionException, InterruptedException {
        Employee foundEmpName = producerRepository.findById(employee.getEmpId()).get();
        if(foundEmpName == null){
            System.out.println("That employee doesn't exist");
        }else{
            foundEmpName.setEmpName(employee.getEmpName());
            producerRepository.save(foundEmpName);
            sendAddMessage("EmployeeINFO", "update", employee);
        }
        return foundEmpName;
    }

    /**
     *
     * @param employee
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void  deleteEmployee(Employee employee) throws ExecutionException, InterruptedException {
        sendAddMessage("EmployeeINFO", "delete", employee);
        producerRepository.delete(employee);
    }

    /**
     * @param topic
     * @param power
     * @param employee
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void sendAddMessage(String topic, String power, Employee employee) throws ExecutionException, InterruptedException {
        NewTopic employeeTopic = new NewTopic("EmployeeINFO", 3, (short) 1);

        Properties props = allConfigs.getAvroProducerProperties();

        KafkaProducer<String, EmployeeAvroRecord> producer = new KafkaProducer<>(props);

        allConfigs.createTopic(employeeTopic);

        EmployeeAvroRecord avroRecord = EmployeeAvroRecord.newBuilder()
                .setEmpName(employee.getEmpName()).setEmpId(employee.getEmpId()).build();

        ProducerRecord<String, EmployeeAvroRecord> producerRecord =
                new ProducerRecord<>(employeeTopic.name(), avroRecord);

        producerRecord.headers().add("power",power.getBytes());

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    log.info("Employee INFO Bitch \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Value: " + producerRecord.value() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.timestamp() + "\n" +
                            "Header: " + new String(producerRecord.headers().iterator().next().value()));
                }
            }
        });

        //flush and close
        producer.close();

    }
}
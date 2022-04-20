package com.example.demo.repository;

import com.example.demo.model.Employee;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EmployeeConsumer extends MongoRepository<Employee, Integer> {
}

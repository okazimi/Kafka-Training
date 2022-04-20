package com.example.demo.repositories;

import com.example.EmployeeAvroRecord;
import com.example.demo.models.Employee;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConsumerRepository extends MongoRepository<Employee,Long> {
}

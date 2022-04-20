package com.example.demo.serializers;

import com.example.EmployeeAvroRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class EmployeeSerializer implements Serializer<EmployeeAvroRecord> {
    @Override
    public byte[] serialize(String topic, EmployeeAvroRecord employeeAvroRecord) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsBytes(employeeAvroRecord);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}

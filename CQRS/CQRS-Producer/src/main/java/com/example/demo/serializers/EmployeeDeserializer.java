package com.example.demo.serializers;

import com.example.EmployeeAvroRecord;
import com.example.demo.models.Employee;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class EmployeeDeserializer implements Deserializer<EmployeeAvroRecord> {
    @Override
    public EmployeeAvroRecord deserialize(String s, byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(bytes,EmployeeAvroRecord.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

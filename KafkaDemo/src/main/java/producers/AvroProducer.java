package producers;

import configs.ConfigGenerator;
import generatedRecords.AvroRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class AvroProducer {
    public static void main(String args[]) throws Exception {
        Properties properties = ConfigGenerator.getAvroProducerProps();
        // Send 10 Temperature Readings
        for (int i = 0; i < 10; i++) {
            Random rand = new Random();
            int temperature = rand.nextInt(100);

            // Generate Topics if they dont exist
            String topic_1 = "AvroTopic-1";         // High Temp
            String topic_2 = "AvroTopic-2";         // Medium Temp
            String topic_3 = "AvroTopic-3";         // Low Temp

            // Instantiate Producer
            KafkaProducer<String, AvroRecord> producer = new KafkaProducer<>(properties);

            // Generate Current Time
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();
            String tempTime = dtf.format(now);

            // Create Avro Records
            AvroRecord avro_data_1 = AvroRecord.newBuilder()
                    .setTemperatureRecorded("High-Temp: " + temperature + "°C")
                    .setTimeRecorded("Recorded At: " + tempTime)
                    .setTemperature(temperature)
                    .build();

            AvroRecord avro_data_2 = AvroRecord.newBuilder()
                    .setTemperatureRecorded("Medium-Temp: " + temperature + "°C")
                    .setTimeRecorded("Recorded At: " + tempTime)
                    .setTemperature(temperature)
                    .build();

            AvroRecord avro_data_3 = AvroRecord.newBuilder()
                    .setTemperatureRecorded("Low-Temp: " + temperature + "°C")
                    .setTimeRecorded("Recorded At: " + tempTime)
                    .setTemperature(temperature)
                    .build();

            // Create Producer Records using AvroRecords
            ProducerRecord<String, AvroRecord> record_1 = new ProducerRecord<>(topic_1, avro_data_1);
            ProducerRecord<String, AvroRecord> record_2 = new ProducerRecord<>(topic_2, avro_data_2);
            ProducerRecord<String, AvroRecord> record_3 = new ProducerRecord<>(topic_3, avro_data_3);

            // Send Records To Kafka depending on temperature recorded
            if (temperature > 75) {
                producer.send(record_1);
            } else if (temperature <= 75 && temperature > 25) {
                producer.send(record_2);
            } else {
                producer.send(record_3);
            }

            producer.flush();
            producer.close();
        }
    }
}

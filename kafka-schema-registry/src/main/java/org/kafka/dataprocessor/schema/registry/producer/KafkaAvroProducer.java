package org.kafka.dataprocessor.schema.registry.producer;

import lombok.AllArgsConstructor;
import org.checkerframework.checker.units.qual.A;
import org.kafka.dataprocessor.schema.registry.dto.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class KafkaAvroProducer {

    @Value("${topic.name}")
    private String topicName;

    private final KafkaTemplate<String, Employee> kafkaTemplate;

    @Autowired
    public KafkaAvroProducer(KafkaTemplate<String, Employee> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Employee employee) {
        kafkaTemplate.send(topicName, UUID.randomUUID().toString(), employee)
                .whenComplete((result, e) -> {
                    if (e == null) {
                        System.out.println("Sent message=[" + employee +
                                "] with offset=[" + result.getRecordMetadata().offset() + "]");
                    } else {
                        System.out.println("Unable to send message=[" + employee + "] due to " + e.getMessage());
                    }
                });
    }
}


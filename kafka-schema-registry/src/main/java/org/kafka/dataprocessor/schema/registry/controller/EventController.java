package org.kafka.dataprocessor.schema.registry.controller;


import lombok.AllArgsConstructor;
import org.kafka.dataprocessor.schema.registry.dto.Employee;
import org.kafka.dataprocessor.schema.registry.producer.KafkaAvroProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/events")
public class EventController {
    private KafkaAvroProducer kafkaAvroProducer;

    public EventController(KafkaAvroProducer kafkaAvroProducer) {
        this.kafkaAvroProducer = kafkaAvroProducer;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestBody Employee employee){
        kafkaAvroProducer.send(employee);
        return "message published";
    }
}

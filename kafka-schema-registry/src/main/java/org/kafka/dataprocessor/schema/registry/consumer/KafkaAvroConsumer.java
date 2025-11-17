package org.kafka.dataprocessor.schema.registry.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kafka.dataprocessor.schema.registry.dto.Employee;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaAvroConsumer {

    //Logger logger = LoggerFactory.getLogger(KafkaAvroConsumer.class);
    @KafkaListener(topics = "$topic.name")
    public void read(ConsumerRecord<String, Employee> consumerRecord) {
        String key = consumerRecord.key();
        Employee employee = consumerRecord.value();
        System.out.println("Avro message received for key : "+ key +" value : " + employee);
    }
}

package org.kafka.dataprocessor.avro.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafka.dataprocessor.avro.Customer;

import java.util.Properties;

public class KafkaAvroProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 10);

        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());

        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(props);

        String topic = "customer-avro";

        Customer customer = Customer.newBuilder()
                .setFirstName("John")
                .setLastName("Doe")
                .setAge(26)
                .setHeight(185.5f)
                .setWeight(65.5f)
                .setAutomatedEmail(false)
                .build() ;


        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("Successfully sent message to topic: " + recordMetadata.toString());
                } else {
                    e.printStackTrace();
                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}

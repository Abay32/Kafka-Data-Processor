package org.kafka.dataprocessor.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // producer properties
        Properties properties = new Properties();

        //connect to localhost -- unsecured  connection
        properties.setProperty("bootstrap.servers", "localhost:9092");


        // connect to Conduktor playground
//        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"3ZP2D33l5Fr7rb1t6HlWHB\" password=\"79fa840d-e735-4d48-8131-f2a322fed3c8\";" );
//        properties.setProperty("sasl.mechanism", "PLAIN");


        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer recorde
        ProducerRecord<String, String> record = new ProducerRecord<>("first-topic", "key1", "Hello Kafka");

        //send data
        producer.send(record);


        // tell the producer to send lall data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();

    }
}

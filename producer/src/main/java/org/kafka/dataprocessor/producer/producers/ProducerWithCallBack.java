package org.kafka.dataprocessor.producer.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallBack.class.getSimpleName());

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
        properties.setProperty("batch.size", "400");

        // This assigns to a random partitioner and not recommended for performance perpose
        // properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int j=0; j<10; j++){

            for (int i=0; i<40; i++){
                // create a producer recorde
                ProducerRecord<String, String> record = new ProducerRecord<>("java-demo-topic", "key1", "Hello Kafka " + i);

                //send data with Sticky partitioner (Performance improvement)
                producer.send(record, new Callback()  {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // excuted every time a record is successfully sent or an exception is thrown
                        if (e != null) {
                            log.error("Error while sending record to topic", e);
                        } else  {
                            log.info("Received message from topic: \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() );
                        }
                    }
                });

            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        // tell the producer to send lall data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();

    }
}

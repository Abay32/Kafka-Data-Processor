package org.kafka.dataprocessor.avro.producer.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.dataprocessor.avro.Customer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

public class AvroConsumer {
    private static final Logger log = Logger.getLogger(AvroConsumer.class.getSimpleName());
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", "true");

        KafkaConsumer<String, Customer> customerKafkaConsumer = new KafkaConsumer<>(props);

        // Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();
        // add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                customerKafkaConsumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        try {

            customerKafkaConsumer.subscribe(Arrays.asList("customer-avro"));

            while (true) {
                log.info("Polling for new records...");
                ConsumerRecords<String, Customer> consumerRecords = customerKafkaConsumer.poll(Duration.ofSeconds(5));



                for (ConsumerRecord<String, Customer> consumerRecord : consumerRecords) {
                    System.out.println("Consumed --> key=" + consumerRecord.key() + " , value=" + consumerRecord.value());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is a starting to shut down...");
        } catch (Exception e) {
            log.info("Unexpected exception in the consumer thread..."+ e.getMessage());

        } finally {
            customerKafkaConsumer.close();
            log.info("Consumer is shut down...");
        }


    }

}

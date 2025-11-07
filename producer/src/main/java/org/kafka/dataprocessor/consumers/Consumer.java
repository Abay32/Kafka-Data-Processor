package org.kafka.dataprocessor.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

public class Consumer {
    private static final Logger log = Logger.getLogger(Consumer.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Starting Consumer");

        String groupId = "my-java-app";
        String topic = "java-demo-topic";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "groupId");
        properties.setProperty("auto.offset.reset", "earliest");

        //use a cooperativeStickyAssignor for partition balance
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());


        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });



        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
                log.info("Polling ...");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition());
                    log.info("Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is a starting to shut down");
        } catch (Exception e) {
            log.info("Unexpected exception in the consumer: " + e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offset
            log.info("Consumer is now gracefully shutdown");
        }



    }
}

package org.kafka.dataprocessor.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikimediaChangeHandler implements EventHandler {



    KafkaProducer<String, String> kafkaProducer;
    String topic;

    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen()  {

        //nothing here

    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        //

        String eventData = messageEvent.getData();

        // Get the key
        String key = extractKeyFromJson(eventData);

        log.info("Wikimedia change received with Key: " + key + " value " + messageEvent.getData());

        //asynchronous
        kafkaProducer.send( new ProducerRecord<>(topic, key, eventData ));
    }

    @Override
    public void onComment(String s) throws Exception {
        // nothing here

    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in Streaming Reading " + throwable.getMessage());
    }

    private String extractKeyFromJson(String jsonString) {
        try {
            JSONObject jsonObject = new JSONObject(jsonString);
            return jsonObject.optString("user", "default-key");
        } catch (Exception e) {
            return "default-key";
        }
    }
}

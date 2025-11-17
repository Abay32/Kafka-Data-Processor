package org.kafka.dataprocessor.producer.sedes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class JacksonSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper =  new ObjectMapper();


    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception ex) {
            throw new RuntimeException("JSON serializer error", ex);
        }
    }
}

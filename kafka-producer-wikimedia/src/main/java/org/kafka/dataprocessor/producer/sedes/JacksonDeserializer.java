package org.kafka.dataprocessor.producer.sedes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JacksonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> type;

    public JacksonDeserializer(Class<T> type) {
        this.type = type;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            // Log the raw data here before it fails
            String rawJson = new String(data, StandardCharsets.UTF_8);


            // This is likely line 34 where the exception occurs
            return objectMapper.readValue(data, type);
        } catch (IOException e) {
            // Wrap the exception to provide context
            throw new SerializationException("Error deserializing JSON to " + type.getName(), e);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}


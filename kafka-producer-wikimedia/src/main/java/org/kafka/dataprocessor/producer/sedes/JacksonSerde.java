package org.kafka.dataprocessor.producer.sedes;

import org.apache.kafka.common.serialization.Serdes;

public class JacksonSerde<T> extends Serdes.WrapperSerde<T> {
    public JacksonSerde(Class<T> targetClass) {
        super(new JacksonSerializer<>(), new JacksonDeserializer<>(targetClass));
    }
}


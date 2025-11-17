package org.kafka.dataprocessor.producer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Revision {
    public long old;
    public long newValue;
}
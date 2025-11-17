package org.kafka.dataprocessor.producer.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RecentChangeEvent {


    public String schema;

    public Meta meta;
    public long id;
    public String type;
    public int namespace;
    public String title;
    public String title_url;
    public long timestamp;
    public String comment;
    public String user;
    public boolean bot;
    public boolean minor;
    public boolean patrolled;
    public Length length;
    public Revision revision;
    public Integer sizeDelta;

}
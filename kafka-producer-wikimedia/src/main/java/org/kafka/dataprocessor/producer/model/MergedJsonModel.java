package org.kafka.dataprocessor.producer.model;

public class MergedJsonModel {
    public String uri;
    public String id;
    public String domain;
    public String stream;
    public int lengthDiff;
    public long revisionDiff;


    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public int getLengthDiff() {
        return lengthDiff;
    }

    public void setLengthDiff(int lengthDiff) {
        this.lengthDiff = lengthDiff;
    }

    public long getRevisionDiff() {
        return revisionDiff;
    }

    public void setRevisionDiff(long revisionDiff) {
        this.revisionDiff = revisionDiff;
    }




}

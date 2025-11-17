package org.kafka.dataprocessor.producer.model;

public class LengthRevisionDifference {
    public int lengthDiff;
    public long revisionDiff;


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

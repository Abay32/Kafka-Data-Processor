package org.kafka.dataprocessor.producer.transformation;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.kafka.dataprocessor.producer.model.LengthRevisionDifference;
import org.kafka.dataprocessor.producer.model.MergedJsonModel;
import org.kafka.dataprocessor.producer.model.Meta;

public class StreamJoiner {

    public static ValueJoiner<Meta, LengthRevisionDifference, MergedJsonModel> createJoiner() {
        return (metaValue, lengthDiffValue) -> {
            MergedJsonModel result = new MergedJsonModel();

            // Copy fields from the Meta object
            result.uri = metaValue.uri;
            result.id = metaValue.id;
            result.stream = metaValue.stream;
            result.domain = metaValue.domain;
            result.lengthDiff = lengthDiffValue.lengthDiff;
            result.revisionDiff = lengthDiffValue.revisionDiff;

            return result;
        };
    }
}

package org.kafka.dataprocessor.voice.command.parser.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VoiceCommand {

    private String id;
    private String audioCodec;
    private String language;
    private byte[] audio;

}

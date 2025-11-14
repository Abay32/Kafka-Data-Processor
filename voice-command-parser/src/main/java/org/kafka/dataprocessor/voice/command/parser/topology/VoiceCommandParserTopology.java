package org.kafka.dataprocessor.voice.command.parser.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.kafka.dataprocessor.voice.command.parser.SpeechToTextService;
import org.kafka.dataprocessor.voice.command.parser.model.ParsedVoiceCommand;
import org.kafka.dataprocessor.voice.command.parser.model.VoiceCommand;
import org.kafka.dataprocessor.voice.command.parser.serdes.JsonSerde;


public class VoiceCommandParserTopology {

    public static final String VOICE_COMMANDS_TOPIC = "voice-commands";
    public static final String RECOGNIZED_COMMANDS_TOPIC = "recognized-commands";
    private final SpeechToTextService speechToTextService;

    public VoiceCommandParserTopology(SpeechToTextService speechToTextService) {
        this.speechToTextService = speechToTextService;
    }


    public Topology createTopology() {
        var streamsBuilder = new StreamsBuilder();


        var voiceCommandJsonSerde = new JsonSerde<>(VoiceCommand.class);
        var parsedVoiceCommandJsonSerde = new JsonSerde<>(ParsedVoiceCommand.class);

        streamsBuilder.stream(VOICE_COMMANDS_TOPIC, Consumed.with(Serdes.String(), voiceCommandJsonSerde))
                .mapValues(
                        (readOnlyKey, value) -> speechToTextService.speechToText(value)
                )
                .to(RECOGNIZED_COMMANDS_TOPIC, Produced.with(Serdes.String(), parsedVoiceCommandJsonSerde));

        return streamsBuilder.build();
    }
}

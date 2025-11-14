package org.kafka.dataprocessor.voice.command.parser.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kafka.dataprocessor.voice.command.parser.SpeechToTextService;
import org.kafka.dataprocessor.voice.command.parser.model.ParsedVoiceCommand;
import org.kafka.dataprocessor.voice.command.parser.model.VoiceCommand;
import org.kafka.dataprocessor.voice.command.parser.serdes.JsonSerde;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;


@ExtendWith(MockitoExtension.class)
class VoiceCommandParserTopologyTest {

    @Mock
    private SpeechToTextService speechToTextService;

    @InjectMocks
    private VoiceCommandParserTopology voiceCommandParserTopology;
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, VoiceCommand> voiceCommandInputTopic;
    private TestOutputTopic<String, ParsedVoiceCommand> recognizedCommandsOutputTopic;

    @BeforeEach
    void setUp() {

        var voiceCommandParserTopology = this.voiceCommandParserTopology;
        var topology = voiceCommandParserTopology.createTopology();
        var props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        topologyTestDriver = new TopologyTestDriver(topology, props);

        var voiceCommandJsonSerde = new JsonSerde<>(VoiceCommand.class);
        var parsedVoiceCommandJsonSerde = new JsonSerde<>(ParsedVoiceCommand.class);


        voiceCommandInputTopic = topologyTestDriver.createInputTopic(
                                        VoiceCommandParserTopology.VOICE_COMMANDS_TOPIC,
                                        Serdes.String().serializer(),
                                        voiceCommandJsonSerde.serializer()
                                );
        recognizedCommandsOutputTopic = topologyTestDriver.createOutputTopic(
                VoiceCommandParserTopology.RECOGNIZED_COMMANDS_TOPIC,
                Serdes.String().deserializer(),
                parsedVoiceCommandJsonSerde.deserializer()
        );

    }

    @Test
    @DisplayName("Given an English voice command, when processed correctly then I receive a ParsedVoiceCommand in the recognized-commands topic.")
    void testScenario1(){
        // preconditions
        var randomBytes = new byte[20];
        new Random().nextBytes(randomBytes);
        var voiceCommand = VoiceCommand.builder()
                .id(UUID.randomUUID().toString())
                .audio(randomBytes)
                .audioCodec("FLAC")
                .language("en-US")
                .build();

        var parsedVoiceCommand1 = ParsedVoiceCommand.builder()
                .id(voiceCommand.getId())
                .text("call john")
                .build();
        given(speechToTextService.speechToText(voiceCommand)).willReturn(parsedVoiceCommand1);
        //actions (when)
        voiceCommandInputTopic.pipeInput(voiceCommand);

        //verification (then)
        var parsedVoiceCommand = recognizedCommandsOutputTopic.readRecord().value();

        //assertion
        assertEquals(voiceCommand.getId(),parsedVoiceCommand.getId());
        assertEquals("call john", parsedVoiceCommand.getText());

    }
}
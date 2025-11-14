package org.kafka.dataprocessor.voice.command.parser;

import org.kafka.dataprocessor.voice.command.parser.model.ParsedVoiceCommand;
import org.kafka.dataprocessor.voice.command.parser.model.VoiceCommand;

public interface SpeechToTextService {
    ParsedVoiceCommand speechToText(VoiceCommand voiceCommand);

}

package org.example.data;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.*;

public class WordDataDeserializer extends JsonDeserializer<WordData> {

    @Override
    public WordData deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode rootNode = p.getCodec().readTree(p);


        JsonNode wordsNode = rootNode.get("words");
        if (wordsNode == null) {
            throw new IllegalArgumentException("JSON does not contain 'words' key");
        }

        Map<String, List<String>> wordsMap = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> categories = wordsNode.fields();
        while (categories.hasNext()) {
            Map.Entry<String, JsonNode> categoryEntry = categories.next();
            String category = categoryEntry.getKey(); // e.g., "technology"
            JsonNode wordsArray = categoryEntry.getValue();
            List<String> wordsList = new ArrayList<>();
            for (JsonNode wordNode : wordsArray) {
                wordsList.add(wordNode.asText());
            }

            wordsMap.put(category, wordsList);
        }
        WordData wordData = new WordData();
        wordData.setWords(wordsMap);
        return wordData;
    }
}

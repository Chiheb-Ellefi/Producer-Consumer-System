package org.example.data;

import java.util.List;
import java.util.Map;

public class WordData {
    private Map<String, List<String>> words;

    public Map<String, List<String>> getWords() {
        return words;
    }

    public void setWords(Map<String, List<String>> words) {
        this.words = words;
    }
}
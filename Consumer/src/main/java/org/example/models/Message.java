package org.example.models;

import java.util.HashMap;

public class Message {

    String topic;
    String message;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Message [topic=" + topic + ", message=" + message + "]";
    }
}

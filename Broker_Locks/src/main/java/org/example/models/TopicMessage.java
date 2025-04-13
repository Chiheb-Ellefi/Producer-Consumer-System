package org.example.models;

public class TopicMessage {
    String topic;
    long offset;
    byte[] payload;
public TopicMessage(String topic, long offset, byte[] payload) {
    this.topic = topic;
    this.offset = offset;
    this.payload = payload;
}
    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public long getOffset() {
        return offset;
    }
}

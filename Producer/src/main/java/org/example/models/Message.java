package org.example.models;

public class Message {
    private String topic;
    private int size;
    private byte[] content;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.size=content.length;
        this.content = content;
    }

    public int getSize() {
        return size;
    }


}

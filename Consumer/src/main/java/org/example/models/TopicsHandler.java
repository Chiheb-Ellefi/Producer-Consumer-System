package org.example.models;

import java.util.HashMap;

public class TopicsHandler {
    private  final HashMap<String, Long> topics;
    public TopicsHandler() {
        this.topics=new HashMap<>();
    }
    public  void subscribe(String topic)  {
        if(topic==null || topic.isEmpty()) {
            System.out.println("Topic is null or empty");
            return;
        };
        topics.put(topic,0L);

    }
    public  void updateOffset(String topic,long offset) {
        if(topic==null || topic.isEmpty()) {
            System.out.println("Topic is null or empty");
        }
        topics.replace(topic,offset);
    }
    public   void  unsubscribe(String topic) {
        if(topic==null || topic.isEmpty()) {
            System.out.println("Topic is null or empty");
        }
        topics.remove(topic);
    }
    public boolean isEqual(long offset,String topic){
        return topics.containsKey(topic) && topics.get(topic).equals(offset);
    }

    public  HashMap<String, Long> getTopics() {
        return topics;
    }
}

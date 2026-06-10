package com.demo.kafka.model;

import java.time.LocalDateTime;

/**
 * 消息实体
 */
public class Message {

    private String id;
    private String topic;
    private String content;
    private String sendTime;

    public Message() {
    }

    public Message(String id, String topic, String content, String sendTime) {
        this.id = id;
        this.topic = topic;
        this.content = content;
        this.sendTime = sendTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSendTime() {
        return sendTime;
    }

    public void setSendTime(String sendTime) {
        this.sendTime = sendTime;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", topic='" + topic + '\'' +
                ", content='" + content + '\'' +
                ", sendTime='" + sendTime + '\'' +
                '}';
    }
}

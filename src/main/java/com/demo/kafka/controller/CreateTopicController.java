package com.demo.kafka.controller;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/create")
public class CreateTopicController {
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Resource
    private Admin kafkaAdminClient;

    @GetMapping(value = "/topicOne")
    public String createTopic() throws ExecutionException, InterruptedException {
        boolean contains = kafkaAdminClient.listTopics().names().get().contains("topic_one");
        if (contains){
            return "topic_one主题已存在!";
        }
        // 参数：topic名、分区数、副本数（单机副本只能填1）
        NewTopic newTopic = new NewTopic("topic_one", 6, (short) 1);
        // 可选：配置消息留存时间等
        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", "86400000");
        newTopic.configs(configs);
        // 执行创建
        kafkaAdminClient.createTopics(Arrays.asList(newTopic)).all().get();
        return "创建成功，topic：topic_one，分区6，副本1";
    }
}

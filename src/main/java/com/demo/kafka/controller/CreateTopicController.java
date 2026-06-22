package com.demo.kafka.controller;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.apache.kafka.common.TopicPartitionInfo;
import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

    //判断 Topic 是否存在（避免重复创建报错）
    @GetMapping(value = "/topicExist")
    public Boolean topicIsExist() throws ExecutionException, InterruptedException {
        return kafkaAdminClient.listTopics().names().get().contains("topic_one");
    }

    //查询 Topic 分区、副本、配置信息
    @GetMapping(value = "/describeTopicInfo")
    public void describeTopicInfo() throws ExecutionException, InterruptedException {
        String topicName = "topic_one";
        DescribeTopicsResult describeResult = kafkaAdminClient.describeTopics(Arrays.asList(topicName));
        Map<String, TopicDescription> topicMap = describeResult.all().get();
        TopicDescription topicDesc = topicMap.get(topicName);

        // 遍历每一个分区信息
        for (TopicPartitionInfo partitionInfo : topicDesc.partitions()) {
            int partitionId = partitionInfo.partition();
            // leader副本节点
            int leaderBrokerId = partitionInfo.leader().id();
            // 所有副本列表
            String replicas = partitionInfo.replicas().stream()
                    .map(node -> node.id() + "")
                    .reduce((a, b) -> a + "," + b).orElse("");
            // ISR同步副本列表
            String isrList = partitionInfo.isr().stream()
                    .map(node -> node.id() + "")
                    .reduce((a, b) -> a + "," + b).orElse("");
        }
        // 2. 查询Topic自定义配置（留存时间、min.insync.replicas等）
        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config configObj = kafkaAdminClient.describeConfigs(Arrays.asList(topicResource)).all().get().get(topicResource);
        Map<String, String> configMap = new HashMap<>();
        for (ConfigEntry entry : configObj.entries()) {
            // 只保留当前topic单独设置的配置，过滤全局默认配置
            if (!"DEFAULT_CONFIG".equals(entry.source().name())) {
                configMap.put(entry.name(), entry.value());
            }
        }
    }


    //动态扩容分区（只能加不能减）
    @GetMapping(value = "/expandTopicPartition")
    public void expandTopicPartition() throws ExecutionException, InterruptedException {
        String topicName = "topic_one";
        DescribeTopicsResult result = kafkaAdminClient.describeTopics(Arrays.asList(topicName));
        TopicDescription desc = result.all().get().get(topicName);
        int size = desc.partitions().size();

        System.out.println("原分区数量:"+size);

        Map<String, NewPartitions> partitionsMap = new HashMap<>();
        // increaseTo：设置扩容后的总分区数量
        partitionsMap.put(topicName, NewPartitions.increaseTo(size+1));
        // 4. 执行扩容
        kafkaAdminClient.createPartitions(partitionsMap).all().get();

    }

    //删除 Topic、修改 Topic 留存时间等配置
    @GetMapping(value = "/deleteTopic")
    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        kafkaAdminClient.deleteTopics(Arrays.asList(topicName)).all().get();
    }

    //查询集群 Broker 列表、消费者组、消费位点 offset
}

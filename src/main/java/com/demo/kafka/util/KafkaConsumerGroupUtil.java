package com.demo.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Kafka 消费者 & 消费者组创建工具类
 * <p>
 * 提供原生 KafkaConsumer 实例的工厂式创建，
 * 同一 groupId 的消费者自动属于同一个消费者组。
 * 消费逻辑由调用方自行实现。
 */
@Component
public class KafkaConsumerGroupUtil {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ==================== 1. 单个消费者 ====================

    /**
     * 创建一个默认消费者（自动提交，无初始 offset 时从最早消费）
     *
     * @param groupId 消费者组 ID
     */
    public KafkaConsumer<String, String> createConsumer(String groupId) {
        return createConsumer(groupId, true, "earliest");
    }

    /**
     * 创建一个消费者（灵活配置）
     *
     * @param groupId           消费者组 ID
     * @param enableAutoCommit  true=自动提交, false=手动提交
     * @param autoOffsetReset   earliest / latest / none
     */
    public KafkaConsumer<String, String> createConsumer(String groupId,
                                                         boolean enableAutoCommit,
                                                         String autoOffsetReset) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        // 不自动提交时才需要手动设置 auto.offset.reset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    /**
     * 创建批量消费者（每次 poll 最多拉取 maxPollRecords 条）
     */
    public KafkaConsumer<String, String> createBatchConsumer(String groupId,
                                                              int maxPollRecords,
                                                              boolean enableAutoCommit) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    /**
     * 全参数自定义创建消费者
     */
    public KafkaConsumer<String, String> createConsumer(Properties props) {
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    // ==================== 2. 消费者组 ====================

    /**
     * 创建一个消费者组（多个消费者共享同一 groupId）
     * <p>
     * Kafka 消费者组是"声明即创建"——只要指定相同 groupId 的消费者启动，组就自动形成。
     * 建议 consumerNum <= topic 分区数，否则多出来的消费者会空闲。
     *
     * @param groupId     消费者组 ID
     * @param consumerNum 消费者数量
     * @return 消费者列表，订阅 topic 后即可开始消费
     */
    public List<KafkaConsumer<String, String>> createConsumerGroup(String groupId,
                                                                    int consumerNum,
                                                                    boolean enableAutoCommit,
                                                                    String autoOffsetReset) {
        List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
        for (int i = 0; i < consumerNum; i++) {
            consumers.add(createConsumer(groupId, enableAutoCommit, autoOffsetReset));
            System.out.println("创建消费者[" + i + "], groupId=[" + groupId + "]");
        }
        System.out.println("消费者组已创建: groupId=[" + groupId + "], 成员数=" + consumerNum);
        return consumers;
    }

    /**
     * 创建消费者组（简化版，默认自动提交 + earliest）
     */
    public List<KafkaConsumer<String, String>> createConsumerGroup(String groupId, int consumerNum) {
        return createConsumerGroup(groupId, consumerNum, true, "earliest");
    }

    // ==================== 3. 工具方法 ====================

    /**
     * 批量关闭消费者
     */
    public void closeAll(List<KafkaConsumer<String, String>> consumers) {
        for (int i = 0; i < consumers.size(); i++) {
            consumers.get(i).close();
            System.out.println("关闭消费者[" + i + "]");
        }
        System.out.println("已关闭 " + consumers.size() + " 个消费者");
    }
}

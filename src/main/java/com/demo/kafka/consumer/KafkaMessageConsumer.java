package com.demo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka 消费者
 * 监听指定 topic，收到消息后打印到控制台
 */
@Component
public class KafkaMessageConsumer {

    // ==================== 实例1: 监听 test-topic ====================
    /**
     * 监听 test-topic 的所有消息
     * groupId 在 application.yml 中配置: kafka-demo-group
     */
    @KafkaListener(topics = "test-topic", groupId = "kafka-demo-group")
    public void onMessage(ConsumerRecord<String, String> record) {
        System.out.println("============================================");
        System.out.println("[消费者-1] 收到消息:");
        System.out.println("  topic     = " + record.topic());
        System.out.println("  partition = " + record.partition());
        System.out.println("  offset    = " + record.offset());
        System.out.println("  key       = " + record.key());
        System.out.println("  value     = " + record.value());
        System.out.println("  timestamp = " + record.timestamp());
        System.out.println("============================================");
    }

    // ==================== 实例2: 监听另一个 topic ====================
    /**
     * 监听 order-topic，演示多topic场景
     */
    @KafkaListener(topics = "order-topic", groupId = "kafka-demo-group")
    public void onOrderMessage(ConsumerRecord<String, String> record) {
        System.out.println("============================================");
        System.out.println("[消费者-2] 收到订单消息:");
        System.out.println("  topic     = " + record.topic());
        System.out.println("  partition = " + record.partition());
        System.out.println("  offset    = " + record.offset());
        System.out.println("  value     = " + record.value());
        System.out.println("============================================");
    }

    // ==================== 实例3: 批量消费 ====================
    /**
     * 监听 test-topic，一次拉取多条消息批量处理
     * 需要在 application.yml 中配置:
     *   spring.kafka.listener.type: batch
     *   和 max-poll-records
     */
//    @KafkaListener(topics = "test-topic", groupId = "kafka-demo-group-batch")
//    public void onBatchMessage(List<ConsumerRecord<String, String>> records) {
//        System.out.println(">>> 批量消费，本次收到 " + records.size() + " 条消息");
//        for (ConsumerRecord<String, String> record : records) {
//            System.out.println("  offset=" + record.offset() + ", value=" + record.value());
//        }
//    }
}

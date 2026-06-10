package com.demo.kafka.controller;

import com.demo.kafka.model.Message;
import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * 生产者 - REST接口发送消息到Kafka
 *
 * GET  /send?topic=xxx&content=xxx  简单的get方式发送
 * POST /send                         JSON方式发送
 */
@RestController
@RequestMapping("/kafka")
public class MessageController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Gson GSON = new Gson();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 实例1: GET方式发送消息（最简单）
     * 访问: http://localhost:8080/kafka/send?topic=test-topic&content=hello-kafka
     */
    @GetMapping("/send")
    public String sendMessage(@RequestParam(defaultValue = "test-topic") String topic,
                              @RequestParam(defaultValue = "hello-kafka") String content) {

        Message message = new Message(
                UUID.randomUUID().toString().substring(0, 8),
                topic,
                content,
                LocalDateTime.now().format(FORMATTER)
        );

        String jsonMsg = GSON.toJson(message);

        // 异步发送
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, jsonMsg);

        // 注册回调
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("[生产者-成功] 消息发送成功: " + jsonMsg);
                System.out.println("  -> partition=" + result.getRecordMetadata().partition()
                        + ", offset=" + result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                System.err.println("[生产者-失败] 消息发送失败: " + ex.getMessage());
            }
        });

        return "消息已发送到 [" + topic + "]: " + jsonMsg;
    }

    /**
     * 实例2: POST方式发送消息（JSON Body）
     * curl -X POST http://localhost:8080/kafka/send \
     *   -H "Content-Type: application/json" \
     *   -d '{"topic":"test-topic","content":"hello via post"}'
     */
    @PostMapping("/send")
    public String sendMessageByPost(@RequestBody Message body) {
        body.setId(UUID.randomUUID().toString().substring(0, 8));
        body.setSendTime(LocalDateTime.now().format(FORMATTER));

        String jsonMsg = GSON.toJson(body);
        String topic = body.getTopic() != null ? body.getTopic() : "test-topic";

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, jsonMsg);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("[生产者-成功] topic=" + topic
                        + ", partition=" + result.getRecordMetadata().partition()
                        + ", offset=" + result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                System.err.println("[生产者-失败] " + ex.getMessage());
            }
        });

        return "消息已发送: " + jsonMsg;
    }

    /**
     * 实例3: 同步发送（等待结果返回）
     * 访问: http://localhost:8080/kafka/send-sync?topic=test-topic&content=sync-test
     */
    @GetMapping("/send-sync")
    public String sendMessageSync(@RequestParam(defaultValue = "test-topic") String topic,
                                  @RequestParam(defaultValue = "sync-message") String content) throws Exception {

        Message message = new Message(
                UUID.randomUUID().toString().substring(0, 8),
                topic,
                content,
                LocalDateTime.now().format(FORMATTER)
        );

        String jsonMsg = GSON.toJson(message);

        // 同步发送，阻塞等待结果
        SendResult<String, String> result = kafkaTemplate.send(topic, jsonMsg).get();

        return "同步发送成功 -> topic=" + topic
                + ", partition=" + result.getRecordMetadata().partition()
                + ", offset=" + result.getRecordMetadata().offset()
                + ", msg=" + jsonMsg;
    }
}

package com.demo.kafka.controller;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.xml.crypto.Data;
import java.util.Date;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/producer")
public class ProducerController {
    private static final String topic = "topic_one";
    private static final String key = "order_001"; // key 控制分区，相同key进同一个分区

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    //同步发送（阻塞等待，拿到发送结果，简单可靠）
    @GetMapping(value = "/sendSync")
    public void sendSync() throws InterruptedException, ExecutionException {
        for (int i = 0; i < 100; i++) {
            Thread.sleep(100);
            Date date = new Date();
            SendResult<String, String> metadata = kafkaTemplate.send(topic, key, date.toString()).get();
            RecordMetadata recordMetadata = metadata.getRecordMetadata();
            String msgKey = metadata.getProducerRecord().key();
            String msgValue = metadata.getProducerRecord().value();
            // 打印全部信息
            System.out.println("消息key：" + msgKey);
            System.out.println("消息内容：" + msgValue);
            System.out.println("目标分区：" + recordMetadata.partition());
            System.out.println("消息offset：" + recordMetadata.offset());
            System.out.println("broker时间戳：" + recordMetadata.timestamp());
        }
    }

    //异步发送（不阻塞主线程，无回调，火发即走）
    @GetMapping("/sendAsyncSimple")
    public String sendAsyncSimple() {
        String msg = "无回调异步消息";
        kafkaTemplate.send(topic, msg);
        return "异步消息已提交缓冲区，无法确认是否发送成功";
    }

    //异步发送 + 回调函数（最常用！不阻塞，还能捕获成功 / 失败）
    @GetMapping("/sendAsyncCallback")
    public String sendAsyncCallback() {
        String key = "user_002";
        String msg = "带回调的异步消息";
        // 发送并添加回调
        kafkaTemplate.send(topic, key, msg).addCallback(
                // 成功回调
                new ListenableFutureCallback<SendResult<String, String>>() {
                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        RecordMetadata metadata = result.getRecordMetadata();
                        System.out.println("【异步成功】分区=" + metadata.partition() + " offset=" + metadata.offset());
                        // 业务：更新数据库消息发送状态为成功
                    }
                    // 失败回调（网络、权限、topic不存在、kafka宕机都会进这里）
                    @Override
                    public void onFailure(Throwable ex) {
                        ex.printStackTrace();
                        System.err.println("【异步发送失败】异常：" + ex.getMessage());
                        // 业务：记录失败消息到本地/数据库，定时重试
                    }
                }
        );
        return "异步消息已发送，结果看控制台回调日志";
    }

    //批量发送（多条消息一次性发，提升吞吐量）
}

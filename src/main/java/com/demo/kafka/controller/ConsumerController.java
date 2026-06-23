package com.demo.kafka.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/consumer")
public class ConsumerController {
    //基础 poll 拉取消息、循环消费
    //消费者组、分区分配规则（轮询 / 粘性）

    //Offset 手动提交、自动提交区别，丢消息 / 重复消费解决方案

    //指定 offset 从头消费、从尾部消费、指定时间点消费

    //批量消费、消费者参数：max.poll、session.timeout

}

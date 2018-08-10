package com.wl.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @Author:
 * @Description:消费kafka集群1的kafkacluster1test主题消息和kafka集群2的kafkacluster2test主题消息
 * @Date: Created in 下午9:46 2018/8/9
 * @Modified By:
 */
@Component
public class MyConsumer {

    /**
     * 消费kafka集群1的kafkacluster1test主题消息
     * @param records
     */
    @KafkaListener(topics = "kafkacluster1test", containerFactory = "cluster1KafkaListenerContainerFactory")
    private void kafkacluster1testConsumer(List<ConsumerRecord<String, String>> records) {
        System.out.println("消费kafkacluster1test消息:" + records.size() + ">>>" + records.toString());
    }

    /**
     * 消费kafka集群2的kafkacluster2test主题消息
     * @param records
     */
    @KafkaListener(topics = "kafkacluster2test", containerFactory = "cluster2KafkaListenerContainerFactory")
    private void kafkacluster2testConsumer(List<ConsumerRecord<String, String>> records) {
        System.out.println("消费kafkacluster2test消息:" + records.size() + ">>>" + records.toString());
    }
}

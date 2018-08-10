package com.wl.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @Author:
 * @Description: 定时向kafka集群1和kafka集群2发送消息
 * @Date: Created in 下午9:33 2018/8/9
 * @Modified By:
 */
@Component
@EnableScheduling
public class MyProducer {

    @Autowired
    @Qualifier("kafkaCluster1Template")
    private KafkaTemplate kafkaCluster1Template;

    @Autowired
    @Qualifier("kafkaCluster2Template")
    private KafkaTemplate kafkaCluster2Template;

    /**
     * 每5秒向Kafka集群1的topick：kafkacluster1test发送消息
     */
    @Scheduled(cron = "*/5 * * * * ?")
    public void produceMsgToKafkaCluster1(){
        System.out.println("向kafka cluster1 发送消息");
        kafkaCluster1Template.send("kafkacluster1test", "hello cluster1");
    }

    /**
     * 每10秒向Kafka集群2的topick：kafkacluster2test发送消息
     */
    @Scheduled(cron = "*/10 * * * * ?")
    public void produceMsgToKafkaCluster2(){
        System.out.println("向kafka cluster2 发送消息");
        kafkaCluster2Template.send("kafkacluster2test", "hello cluster2");
    }

}

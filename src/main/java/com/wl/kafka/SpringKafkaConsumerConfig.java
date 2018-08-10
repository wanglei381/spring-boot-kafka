package com.wl.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * @Author:
 * @Description: kafka消费者配置，配置多个消费工厂消费不同的kafka集群消息示例
 * @Date: Created in 下午3:20 2018/8/8
 * @Modified By:
 */
@Configuration
@EnableKafka
public class SpringKafkaConsumerConfig {

    /**
     * kafka cluster1配置
     */
    @Value("${kafka.cluster1.consumer.servers}")
    private String kafkaCluster1BootstrapServers;

    /**
     * kafka cluster2配置
     */
    @Value("${kafka.cluster2.consumer.servers}")
    private String kafkaCluster2BootstrapServers;

    /**
     * 公共配置
     */
    @Value("${kafka.consumer.session.timeout}")
    private Integer sessionTimeoutMs;
    @Value("${kafka.consumer.enable.auto.commit}")
    private boolean enableAutoCommit;
    @Value("${kafka.consumer.auto.commit.interval}")
    private Integer autoCommitIntervalMs;
    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;
    @Value("${kafka.consumer.group.id}")
    private String groupId;


    /**
     * kafka cluster1消费工厂
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> cluster1KafkaListenerContainerFactory() {
        Map<String, Object> configProps = configProps();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster1BootstrapServers);
        return getKafkaListenerContainerFactory(configProps);
    }

    /**
     * kafka cluster2消费工厂
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> cluster2KafkaListenerContainerFactory() {
        Map<String, Object> configProps = configProps();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster2BootstrapServers);
        return getKafkaListenerContainerFactory(configProps);
    }

    /**
     * 创建ConcurrentKafkaListenerContainerFactory
     * @param configProps
     * @return
     */
    private ConcurrentKafkaListenerContainerFactory<String, String> getKafkaListenerContainerFactory(Map<String, Object> configProps){
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        ConsumerFactory<String, String> basicConsumerFactory = new DefaultKafkaConsumerFactory<>(configProps);
        factory.setConsumerFactory(basicConsumerFactory);
        //设定为批量消费
        factory.setBatchListener(true);
        return factory;
    }

    /**
     * 公共配置
     * @return
     */
    private Map<String, Object> configProps(){
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,100);
        return configProps;
    }

}

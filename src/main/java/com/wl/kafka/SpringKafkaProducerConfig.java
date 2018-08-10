package com.wl.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author:
 * @Description:kafka生产者配置，配置多个生产模版向多个kafka集群发送消息示例
 * @Date: Created in 下午4:48 2018/8/9
 * @Modified By:
 */
@Configuration
@EnableKafka
public class SpringKafkaProducerConfig {
    /**
     * kafka集群1
     */
    @Value("${kafka.cluster1.producer.servers}")
    private String cluster1Servers;

    /**
     * kafka集群2
     */
    @Value("${kafka.cluster2.producer.servers}")
    private String cluster2Servers;

    @Value("${kafka.producer.retries}")
    private int retries;
    @Value("${kafka.producer.batch.size}")
    private int batchSize;
    @Value("${kafka.producer.linger}")
    private int linger;
    @Value("${kafka.producer.buffer.memory}")
    private int bufferMemory;

    /**
     * kafka cluster1 生产模版
     * @return
     */
    @Bean
    public KafkaTemplate<String, String> kafkaCluster1Template() {
        Map<String, Object> configProps = producerConfigProps();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster1Servers);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configProps));
    }

    /**
     * kafka cluster2 生产模版
     * @return
     */
    @Bean
    public KafkaTemplate<String, String> kafkaCluster2Template() {
        Map<String, Object> configProps = producerConfigProps();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster2Servers);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configProps));
    }

    /**
     * 公共配置
     * @return
     */
    private Map<String, Object> producerConfigProps(){
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        configProps.put(ProducerConfig.RETRIES_CONFIG, retries);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        return configProps;
    }
}

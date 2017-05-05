package net.kafka.config;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * Created by win7 on 2017/5/4.
 */
@Component
public class KafkaConsumerConfig {
    @Bean
    public ConsumerConnector kafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test-consumer-group");
        properties.setProperty("auto-offset-reset", "smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        return Consumer.createJavaConsumerConnector(consumerConfig);
    }
}

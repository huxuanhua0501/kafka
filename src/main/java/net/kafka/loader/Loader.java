package net.kafka.loader;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.kafka.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by win7 on 2017/5/4.
 */
@Component
public class Loader {
    @Autowired
    ConsumerConnector consumerConnector;
    private ExecutorService executorService;

    @PreDestroy
    public void init() {
        ConcurrentHashMap<String, Integer> topicMap = new ConcurrentHashMap<>();
        topicMap.put("test", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicMap);
        executorService = Executors.newFixedThreadPool(topicMap.size());
        int threadNumber = 1;
        for (String topicName : consumerMap.keySet()) {
            for (KafkaStream<byte[], byte[]> stream : consumerMap.get(topicName)) {
                executorService.submit(new KafkaConsumer(stream, topicName, threadNumber));
                threadNumber++;
            }
        }
    }
}

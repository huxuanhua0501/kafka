package net.kafka.loader;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.kafka.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by win7 on 2017/5/4.
 */
@Component
public class Loader {
    @Autowired
    private ConsumerConnector consumer;
    private Map<String, Integer> topicCountMap;
    private int THREAD_COUNT_PER_TOPIC = 1;
    private ExecutorService executorService;

    @PostConstruct
    public void init() throws Exception{

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("test", 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        executorService = Executors.newFixedThreadPool(THREAD_COUNT_PER_TOPIC * topicCountMap.size());

        int threadNumber = 1;
        for (String topicName : consumerMap.keySet()) {
            for (KafkaStream<byte[], byte[]> stream : consumerMap.get(topicName)) {
                executorService.submit(new KafkaConsumer(stream,topicName,threadNumber));
                threadNumber++;
            }
        }
    }
}

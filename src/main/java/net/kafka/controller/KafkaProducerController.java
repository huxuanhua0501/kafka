package net.kafka.controller;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by ykurtulus on 4/15/16.
 */
@RestController
@RequestMapping("/kafka")
public class KafkaProducerController {

    private static final String TOPIC_NAME ="test";

    @Autowired
     KafkaProducer<String,String> producer;

    @GetMapping(value = "/{key}/{value}")
    public void produce(String key, String value){

        ProducerRecord record = new ProducerRecord(TOPIC_NAME,key, value);
        producer.send(record);
    }
}

package com.example.kafkaeventalarm.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.kafkaeventalarm.Constants;
import com.example.kafkaeventalarm.model.Order;

@Service
public class OrderConsumerManualAck {

    private final Logger logger = LoggerFactory.getLogger(OrderConsumerManualAck.class);

    // @KafkaListener(topics = Constants.TOPIC_NAME_ORDER, groupId = Constants.GROUP_ID, containerFactory = "orderKafkaListenerContainerFactoryManualAck")
    // public void consume(Order order) {
    //     logger.info(String.format("Order consumed (manual ack) -> %s", order));
    // }
}

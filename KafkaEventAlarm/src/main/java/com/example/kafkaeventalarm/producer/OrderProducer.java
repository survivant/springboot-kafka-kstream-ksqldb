package com.example.kafkaeventalarm.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.kafkaeventalarm.Constants;
import com.example.kafkaeventalarm.model.Order;

@Service
public class OrderProducer {
    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    public void sendMessage(Order order) {
        logger.debug(String.format("#### -> Producing message -> %s", order));
        this.kafkaTemplate.send(Constants.TOPIC_NAME_ORDER, order.getOrderId(), order);
    }
}

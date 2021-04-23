package com.example.kafkaeventalarm.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.kafkaeventalarm.Constants;
import com.example.kafkaeventalarm.model.Order;

@Service
public class OrderConsumer {

    private final Logger logger = LoggerFactory.getLogger(OrderConsumer.class);

    // private static String typeIdHeader(Headers headers) {
    //     return StreamSupport.stream(headers.spliterator(), false)
    //             .filter(header -> header.key().equals("__TypeId__"))
    //             .findFirst().map(header -> new String(header.value())).orElse("N/A");
    // }
    //
    // @KafkaListener(topics = "orders", clientIdPrefix = "json", containerFactory = "kafkaListenerContainerFactory")
    // public void listenAsObject(ConsumerRecord<String, Order> cr, @Payload Order payload) {
    //     logger.info("Logger 1 [JSON] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
    //             typeIdHeader(cr.headers()), payload, cr.toString());
    // }
    //
    // @KafkaListener(topics = "orders", clientIdPrefix = "string", containerFactory = "kafkaListenerStringContainerFactory")
    // public void listenAsString(ConsumerRecord<String, String> cr, @Payload String payload) {
    //     logger.info("Logger 2 [String] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
    //             typeIdHeader(cr.headers()), payload, cr.toString());
    // }
    //
    // @KafkaListener(topics = "orders", clientIdPrefix = "bytearray", containerFactory = "kafkaListenerByteArrayContainerFactory")
    // public void listenAsByteArray(ConsumerRecord<String, byte[]> cr, @Payload byte[] payload) {
    //     logger.info("Logger 3 [ByteArray] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
    //             typeIdHeader(cr.headers()), payload, cr.toString());
    // }

    @KafkaListener(topics = Constants.TOPIC_NAME_ORDER, groupId = Constants.GROUP_ID, containerFactory = "orderKafkaListenerContainerFactory")
    public void consume(Order order) {
       logger.info(String.format("Order consumed (auto ack)-> %s", order));
   }
}

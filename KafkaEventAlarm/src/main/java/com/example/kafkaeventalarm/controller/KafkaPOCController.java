package com.example.kafkaeventalarm.controller;

import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafkaeventalarm.manager.OrderManager;
import com.example.kafkaeventalarm.model.Order;
import com.example.kafkaeventalarm.model.Return;
import com.example.kafkaeventalarm.producer.OrderProducer;
import com.example.kafkaeventalarm.producer.ReturnProducer;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaPOCController {

    @Autowired
    private OrderProducer orderProducer;

    @Autowired
    private ReturnProducer returnProducer;

    @Autowired
    private OrderManager orderManager;

    @PostMapping(value = "/createOrder")
    public void sendMessageToKafkaTopic(@RequestBody Order order) {
        this.orderProducer.sendMessage(order);
    }

    @PostMapping(value = "/createReturn")
    public void sendMessageToKafkaTopic(@RequestBody Return areturn) {
        this.returnProducer.sendMessage(areturn);
    }

    @GetMapping(value = "/getOrders")
    public List<Order> getOrders() {
        return orderManager.getAllOrders();
    }

    @GetMapping(value = "/getOrders/timestamp")
    public List<Order> getOrders(@RequestParam String timestamp) {
        return orderManager.getAllOrders(timestamp);
    }

    @GetMapping(value = "/getReturns")
    public List<Return> getReturns() {
        return Collections.emptyList();
    }

}
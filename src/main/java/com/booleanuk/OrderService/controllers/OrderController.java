package com.booleanuk.OrderService.controllers;


import com.booleanuk.OrderService.models.Order;
import com.booleanuk.OrderService.repositories.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.tools.jconsole.JConsoleContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
@RestController

@RequestMapping("orders")
public class OrderController {
    private SqsClient sqsClient;
    private SnsClient snsClient;
    private EventBridgeClient eventBridgeClient;
    private ObjectMapper objectMapper;
    private String queueUrl;
    private String topicArn;
    private String eventBusName;

    @Autowired OrderRepository orderRepository;

    public OrderController() {
        this.sqsClient = SqsClient.builder().build();
        this.snsClient = SnsClient.builder().build();
        this.eventBridgeClient = EventBridgeClient.builder().build();

        this.queueUrl = "https://sqs.eu-west-1.amazonaws.com/637423341661/LudwigsOrderQueue";
        this.topicArn = "arn:aws:sns:eu-west-1:637423341661:LudwigsOrderCreatedTopic";
        this.eventBusName = "LudwigsCustomEventBus";

        this.objectMapper = new ObjectMapper();
    }

    @GetMapping
    public ResponseEntity<String> GetAllOrders() {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .build();

        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

        for (Message message : messages) {

            try {
                String extJsondata = this.objectMapper.readTree(message.body()).get("Message").asText();
                Order order = this.objectMapper.readValue(extJsondata, Order.class);

                System.out.println("Order");
                System.out.println(order);

                this.processOrder(order);

                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();

                sqsClient.deleteMessage(deleteRequest);

            } catch (JsonProcessingException e) {
//                e.printStackTrace();
            }
        }
        String status = String.format("%d Orders have been processed", messages.size());
        return ResponseEntity.ok(status);
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody Order order) {
        try {
            String orderJson = objectMapper.writeValueAsString(order);
            System.out.println(orderJson);
            PublishRequest publishRequest = PublishRequest.builder()
                    .topicArn(topicArn)
                    .message(orderJson)
                    .build();
            snsClient.publish(publishRequest);

            PutEventsRequestEntry eventEntry = PutEventsRequestEntry.builder()
                    .source("order.service")
                    .detailType("OrderCreated")
                    .detail(orderJson)
                    .eventBusName(eventBusName)
                    .build();

            PutEventsRequest putEventsRequest = PutEventsRequest.builder()
                    .entries(eventEntry)
                    .build();

            this.eventBridgeClient.putEvents(putEventsRequest);

            String status = "Order created, Message Published to SNS and Event Emitted to EventBridge";
            return ResponseEntity.ok(status);
        } catch (JsonProcessingException e) {
//            e.printStackTrace();
            return ResponseEntity.status(500).body("Failed to create order");
        }
    }

    @PutMapping("{id}")
    public ResponseEntity<Order> updateOrder(@PathVariable int id, @RequestBody Order order){
        Order orderToUpdate = this.orderRepository.findById(id).orElseThrow(()-> new ResponseStatusException(HttpStatus.NOT_FOUND, "Not found"));

        orderToUpdate.setProduct(order.getProduct());
        orderToUpdate.setQuantity(order.getQuantity());
        orderToUpdate.setAmount(order.getAmount());

        return new ResponseEntity<Order>(this.orderRepository.save(orderToUpdate), HttpStatus.CREATED);
    }

    private void processOrder(Order order) {
        int getTotalAmount = order.getAmount() * order.getQuantity();
        order.setProcessed(true);
        order.setTotal(getTotalAmount);

        this.orderRepository.save(order);
    }
}

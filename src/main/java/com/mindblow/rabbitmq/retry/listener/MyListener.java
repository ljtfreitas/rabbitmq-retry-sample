package com.mindblow.rabbitmq.retry.listener;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class MyListener {

	private static final Logger log = LoggerFactory.getLogger(MyListener.class);

	@RabbitListener(queues = "rabbitmq-retry-queue")
	public void onReceive(String message) {
		log.info("Message on retryable queue: {}. Date/time: {}", message, LocalDateTime.now());
		throw new RuntimeException("Message: " + message);
	}

	@RabbitListener(queues = "rabbitmq-dead-queue")
	public void onDeadQueue(String message) {
		log.info("Message on dead queue: {}", message);
	}

}

package com.mindblow.rabbitmq.retry.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class MyListener {

	private static final Logger log = LoggerFactory.getLogger(MyListener.class);

	@RabbitListener(queues = "rabbitmq-retry-queue")
	public void onReceive(String message) {
		log.info("Message on retryable queue: {}", message);
		throw new RuntimeException("Message: " + message);
	}

	@RabbitListener(queues = "rabbitmq-retry-to-delay-queue", containerFactory = "rabbitListenerContainerRetryToDelayFactory")
	public void onReceiveRethrowingToDelay(String message) {
		log.info("Message on retryable queue: {}. Rethrowing to delay exchange...", message);
		throw new RuntimeException("Message: " + message);
	}

	@RabbitListener(queues = "rabbitmq-dead-queue")
	public void onDeadQueue(String message) {
		log.info("Message on dead queue: {}", message);
	}

	@RabbitListener(queues = "rabbitmq-scheduler-queue")
	public void onSchedulerQueue(String message) {
		log.info("Message on scheduled/delayed queue: {}", message);
	}
}

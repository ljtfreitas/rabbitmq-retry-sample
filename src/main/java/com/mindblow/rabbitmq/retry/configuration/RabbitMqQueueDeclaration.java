package com.mindblow.rabbitmq.retry.configuration;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqQueueDeclaration {

	@Bean
	public Queue queue() {
		return QueueBuilder
				.nonDurable("rabbitmq-retry-queue")
					.autoDelete()
						.withArgument("x-dead-letter-exchange", "rabbitmq-dead-exchange")
							.build();
	}

	@Bean
	public Exchange exchange() {
		return ExchangeBuilder.directExchange("rabbitmq-retry-exchange")
				.autoDelete()
					.delayed()
						.build();
	}

	@Bean
	public Binding binding() {
		return BindingBuilder
				.bind(queue())
					.to(exchange())
						.with("retryable")
							.noargs();
	}

	@Bean
	public Queue deadQueue() {
		return QueueBuilder
				.nonDurable("rabbitmq-dead-queue")
					.autoDelete()
							.build();
	}

	@Bean
	public Exchange deadExchange() {
		return ExchangeBuilder.directExchange("rabbitmq-dead-exchange")
				.autoDelete()
					.build();
	}

	@Bean
	public Binding deadBinding() {
		return BindingBuilder
				.bind(deadQueue())
					.to(deadExchange())
						.with("retryable-to-delay")
							.noargs();
	}
}

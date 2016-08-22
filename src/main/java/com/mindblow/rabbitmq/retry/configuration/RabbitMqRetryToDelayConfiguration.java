package com.mindblow.rabbitmq.retry.configuration;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder.StatelessRetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqRetryToDelayConfiguration {

	@Autowired
	private AmqpTemplate amqpTemplate;

	@Bean
	public SimpleRabbitListenerContainerFactory rabbitListenerContainerRetryToDelayFactory(
			SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {

		SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
		configurer.configure(factory, connectionFactory);

		StatelessRetryInterceptorBuilder builder = RetryInterceptorBuilder.stateless();
		builder.maxAttempts(1);
		builder.recoverer(schedulerMessageRecoverer());
		factory.setAdviceChain(builder.build());

		return factory;
	}

	@Bean
	public SchedulerRepublishMessageRecoverer schedulerMessageRecoverer() {
		SchedulerRepublishMessageRecoverer recoverer = new SchedulerRepublishMessageRecoverer(amqpTemplate, "rabbitmq-scheduler-exchange");

		recoverer.errorRoutingKeyPrefix("");

		return recoverer;
	}

	@Bean
	public Queue retryToDelayQueue() {
		return QueueBuilder
				.nonDurable("rabbitmq-retry-to-delay-queue")
					.autoDelete()
						.build();
	}

	@Bean
	public Binding retryToDelayBinding() {
		return BindingBuilder
				.bind(retryToDelayQueue())
					.to(new DirectExchange("rabbitmq-retry-exchange"))
						.with("retryable-to-delay");
	}

	@Bean
	public Queue scheduledQueue() {
		return QueueBuilder
				.nonDurable("rabbitmq-scheduler-queue")
					.autoDelete()
						.build();
	}

	@Bean
	public Exchange scheduledExchange() {
		return ExchangeBuilder.directExchange("rabbitmq-scheduler-exchange")
				.autoDelete()
					.delayed()
						.build();
	}

	@Bean
	public Binding scheduledBinding() {
		return BindingBuilder
				.bind(scheduledQueue())
					.to(scheduledExchange())
						.with("retryable-to-delay")
							.noargs();
	}

	static class SchedulerRepublishMessageRecoverer extends RepublishMessageRecoverer {

		public SchedulerRepublishMessageRecoverer(AmqpTemplate errorTemplate, String errorExchange) {
			super(errorTemplate, errorExchange);
		}

		@Override
		protected Map<? extends String, ? extends Object> additionalHeaders(Message message, Throwable cause) {
			Map<String, Object> properties = new LinkedHashMap<>();
			properties.put("x-delay", 5000);
			return properties;
		}
	}
}

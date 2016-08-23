package com.mindblow.rabbitmq.retry.configuration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.LongStream;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder.StatelessRetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class RabbitMqRetryToDelayConfiguration {

	@Autowired
	private AmqpTemplate amqpTemplate;

	@Bean
	public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
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
		SchedulerRepublishMessageRecoverer recoverer = new SchedulerRepublishMessageRecoverer(amqpTemplate, "rabbitmq-retry-exchange", 10);
		recoverer.errorRoutingKeyPrefix("");
		return recoverer;
	}

	@Bean
	public Binding retryToDelayBinding() {
		return BindingBuilder
				.bind(new Queue("rabbitmq-retry-queue"))
					.to(new DirectExchange("rabbitmq-retry-exchange"))
						.with("retryable-to-delay");
	}

	static class SchedulerRepublishMessageRecoverer extends RepublishMessageRecoverer {

		private final int maxOfAttempts;

		public SchedulerRepublishMessageRecoverer(AmqpTemplate errorTemplate, String errorExchange, int maxOfAttempts) {
			super(errorTemplate, errorExchange);
			this.maxOfAttempts = maxOfAttempts;
		}

		@Override
		public void recover(Message message, Throwable cause) {
			int attempt = currentAttemptOf(message);

			if (attempt > maxOfAttempts) {
				throw new ListenerExecutionFailedException("Retry Policy Exhausted",
						new AmqpRejectAndDontRequeueException(cause), message);
			} else {
				super.recover(message, cause);
			}
		}

		@Override
		protected Map<? extends String, ? extends Object> additionalHeaders(Message message, Throwable cause) {
			Map<String, Object> properties = new LinkedHashMap<>();

			int attempt = currentAttemptOf(message);

			properties.put("x-attempt", attempt + 1);
			properties.put("x-delay", delayOfNext(attempt));

			return properties;
		}

		private int currentAttemptOf(Message message) {
			return (int) message.getMessageProperties().getHeaders().getOrDefault("x-attempt", 1);
		}

		private long delayOfNext(int attempt) {
			BackOffExecution exponential = new ExponentialBackOff(1500, attempt).start();

			long delay = LongStream.generate(() -> exponential.nextBackOff()).limit(attempt).max().getAsLong();

			return delay;
		}
	}
}

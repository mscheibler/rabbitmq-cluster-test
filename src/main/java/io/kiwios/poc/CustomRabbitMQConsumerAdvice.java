package io.kiwios.poc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import javax.annotation.PreDestroy;
import javax.inject.Qualifier;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.RecoverableChannel;
import com.rabbitmq.client.RecoveryDelayHandler;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQConnection;

import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.bind.BoundExecutable;
import io.micronaut.core.bind.DefaultExecutableBinder;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.messaging.Acknowledgement;
import io.micronaut.messaging.exceptions.MessageAcknowledgementException;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.rabbitmq.annotation.Queue;
import io.micronaut.rabbitmq.annotation.RabbitConnection;
import io.micronaut.rabbitmq.annotation.RabbitListener;
import io.micronaut.rabbitmq.annotation.RabbitProperty;
import io.micronaut.rabbitmq.bind.RabbitBinderRegistry;
import io.micronaut.rabbitmq.bind.RabbitConsumerState;
import io.micronaut.rabbitmq.bind.RabbitMessageCloseable;
import io.micronaut.rabbitmq.connect.ChannelPool;
import io.micronaut.rabbitmq.connect.RabbitConnectionFactoryConfig;
import io.micronaut.rabbitmq.exception.RabbitClientException;
import io.micronaut.rabbitmq.exception.RabbitListenerException;
import io.micronaut.rabbitmq.exception.RabbitListenerExceptionHandler;
import io.micronaut.rabbitmq.intercept.DefaultConsumer;
import io.micronaut.rabbitmq.intercept.MutableBasicProperties;
import io.micronaut.rabbitmq.serdes.RabbitMessageSerDes;
import io.micronaut.rabbitmq.serdes.RabbitMessageSerDesRegistry;

/**
 * This overwrites {@link io.micronaut.rabbitmq.intercept.RabbitMQConsumerAdvice} because we are facing issues with
 * connection recovery. This code is also part of a PR for in the Micronaut repo, but I do not have the feeling it is
 * going to be approved soon (https://github.com/micronaut-projects/micronaut-rabbitmq/pull/211).
 *
 * The difference between this solution and the original one is the {@link retryRecovery(java.lang.Exception)} handling,
 * where we forcefully retry the connection if the channel is closed. See the following links for more details:
 * https://github.com/micronaut-projects/micronaut-rabbitmq/issues/210
 * https://github.com/micronaut-projects/micronaut-rabbitmq/pull/211
 *
 * This needs to be checked on Micronaut updates, hopefully this can be deleted when the issue is fixed on Micronaut's
 * side.
 */
@Singleton
@Requires(beans = io.micronaut.rabbitmq.intercept.RabbitMQConsumerAdvice.class)
@Replaces(bean = io.micronaut.rabbitmq.intercept.RabbitMQConsumerAdvice.class)
public class CustomRabbitMQConsumerAdvice implements ExecutableMethodProcessor<RabbitListener>, AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(CustomRabbitMQConsumerAdvice.class);

	private final BeanContext beanContext;
	private final RabbitBinderRegistry binderRegistry;
	private final RabbitListenerExceptionHandler exceptionHandler;
	private final RabbitMessageSerDesRegistry serDesRegistry;
	private final ConversionService conversionService;
	private final Map<Channel, ConsumerState> consumerChannels = new ConcurrentHashMap<>();
	private final RabbitConnectionFactoryConfig connectionFactoryConfig;

	/**
	 * Default constructor.
	 *
	 * @param beanContext             The bean context
	 * @param binderRegistry          The registry to bind arguments to the method
	 * @param exceptionHandler        The exception handler to use if the consumer isn't a handler
	 * @param serDesRegistry          The serialization/deserialization registry
	 * @param conversionService       The service to convert consume argument values
	 * @param connectionFactoryConfig The configuration of the rabbit connection
	 */
	public CustomRabbitMQConsumerAdvice(BeanContext beanContext,
			RabbitBinderRegistry binderRegistry,
			RabbitListenerExceptionHandler exceptionHandler,
			RabbitMessageSerDesRegistry serDesRegistry,
			ConversionService conversionService,
			RabbitConnectionFactoryConfig connectionFactoryConfig) {
		this.beanContext = beanContext;
		this.binderRegistry = binderRegistry;
		this.exceptionHandler = exceptionHandler;
		this.serDesRegistry = serDesRegistry;
		this.conversionService = conversionService;
		this.connectionFactoryConfig = connectionFactoryConfig;
	}

	@Override
	public void process(BeanDefinition<?> beanDefinition, ExecutableMethod<?, ?> method) {

		AnnotationValue<io.micronaut.rabbitmq.annotation.Queue> queueAnn = method.getAnnotation(Queue.class);

		if (queueAnn != null) {
			String queue = queueAnn.getRequiredValue(String.class);

			String clientTag = method.getDeclaringType().getSimpleName() + '#' + method.toString();

			boolean reQueue = queueAnn.getRequiredValue("reQueue", boolean.class);
			boolean exclusive = queueAnn.getRequiredValue("exclusive", boolean.class);

			boolean hasAckArg = Arrays.stream(method.getArguments())
					.anyMatch(arg -> Acknowledgement.class.isAssignableFrom(arg.getType()));

			String connection = method.findAnnotation(RabbitConnection.class)
					.flatMap(conn -> conn.get("connection", String.class))
					.orElse(RabbitConnection.DEFAULT_CONNECTION);

			ChannelPool channelPool;
			try {
				channelPool = beanContext.getBean(ChannelPool.class, Qualifiers.byName(connection));
			} catch (Throwable e) {
				throw new MessageListenerException(String
						.format("Failed to retrieve a channel pool named [%s] to register a listener", connection), e);
			}

			Channel channel = getChannel(channelPool);

			Integer prefetch = queueAnn.get("prefetch", Integer.class).orElse(null);
			try {
				if (prefetch != null) {
					channel.basicQos(prefetch);
				}
			} catch (IOException e) {
				throw new MessageListenerException(
						String.format("Failed to set a prefetch count of [%s] on the channel", prefetch), e);
			}

			ConsumerState state = new ConsumerState();
			state.channelPool = channelPool;
			state.consumerTag = clientTag;
			consumerChannels.put(channel, state);

			Map<String, Object> arguments = new HashMap<>();

			List<AnnotationValue<RabbitProperty>> propertyAnnotations = method
					.getAnnotationValuesByType(RabbitProperty.class);
			Collections.reverse(propertyAnnotations); // set the values in the class first so methods can override
			propertyAnnotations.forEach((prop) -> {
				String name = prop.getRequiredValue("name", String.class);
				String value = prop.getValue(String.class).orElse(null);
				Class type = prop.get("type", Class.class).orElse(null);

				if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(value)) {
					if (type != null && type != Void.class) {
						Optional<Object> converted = conversionService.convert(value, type);
						if (converted.isPresent()) {
							arguments.put(name, converted.get());
						} else {
							throw new MessageListenerException(String.format(
									"Could not convert the argument [%s] to the required type [%s]", name, type));
						}
					} else {
						arguments.put(name, value);
					}
				}
			});

			io.micronaut.context.Qualifier<Object> qualifer = beanDefinition
					.getAnnotationTypeByStereotype(Qualifier.class)
					.map(type -> Qualifiers.byAnnotation(beanDefinition, type))
					.orElse(null);

			Class<Object> beanType = (Class<Object>) beanDefinition.getBeanType();

			Class<?> returnTypeClass = method.getReturnType().getType();
			boolean isVoid = returnTypeClass == Void.class || returnTypeClass == void.class;

			Object bean = beanContext.findBean(beanType, qualifer).orElseThrow(
					() -> new MessageListenerException("Could not find the bean to execute the method " + method));

			try {
				DefaultExecutableBinder<RabbitConsumerState> binder = new DefaultExecutableBinder<>();

				if (LOG.isDebugEnabled()) {
					LOG.debug("Registering a consumer to queue [{}] with client tag [{}]", queue, clientTag);
				}

				Optional<String> executor = method.findAnnotation(RabbitConnection.class)
						.flatMap(conn -> conn.get("executor", String.class));

				ExecutorService executorService = executor
						.flatMap(exec -> beanContext.findBean(ExecutorService.class, Qualifiers.byName(exec)))
						.orElse(null);

				if (executor.isPresent() && executorService == null) {
					throw new MessageListenerException(
							String.format("Could not find the executor service [%s] specified for the method [%s]",
									executor.get(), method));
				}

				ConsumerParams consumerParams = new ConsumerParams();
				consumerParams.clientTag = clientTag;
				consumerParams.exclusive = exclusive;
				consumerParams.connection = connection;
				consumerParams.queue = queue;

				DefaultConsumer consumer = createDefaultConsumer(method,
						consumerParams,
						reQueue,
						hasAckArg,
						channel,
						isVoid,
						bean,
						binder,
						executorService,
						arguments);
				channel.basicConsume(queue, false, clientTag, false, exclusive, arguments, consumer);
			} catch (Throwable e) {
				if (!channel.isOpen()) {
					channelPool.returnChannel(channel);
					consumerChannels.remove(channel);
					if (LOG.isErrorEnabled()) {
						LOG.error(
								"The channel was closed due to an exception. The consumer [{}] will no longer receive messages",
								clientTag);
					}
				}
				handleException(new RabbitListenerException("An error occurred subscribing to a queue", e, bean, null));
			}
		}

	}

	private DefaultConsumer createDefaultConsumer(ExecutableMethod<?, ?> method, ConsumerParams consumerParams,
			boolean reQueue, boolean hasAckArg, Channel channel, boolean isVoid, Object bean,
			DefaultExecutableBinder<RabbitConsumerState> binder, ExecutorService executorService,
			Map<String, Object> arguments) {
		DefaultConsumer consumer = new DefaultConsumer() {
			@Override
			public void handleTerminate(String consumerTag) {
				if (channel instanceof RecoverableChannel) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(
								"The channel was terminated.  Automatic recovery attempt is underway for consumer [{}]",
								consumerParams.clientTag);
					}
					try {
						channel.basicConsume(consumerParams.queue, false, consumerParams.clientTag, false,
								consumerParams.exclusive, arguments, this);
					} catch (IOException | AlreadyClosedException e) {
						retryRecovery(e);
					}
				} else {
					ConsumerState state = consumerChannels.remove(channel);
					if (state != null) {
						state.channelPool.returnChannel(channel);
						if (LOG.isDebugEnabled()) {
							LOG.debug(
									"The channel was terminated. The consumer [{}] will no longer receive messages",
									consumerParams.clientTag);
						}
					}
				}
			}

			private void retryRecovery(Exception e) {
				retryRecovery(e, 0);
				LOG.info("Connection restablished");
			}

			private void retryRecovery(Exception e, int recoveryAttempts) {
				LOG.debug("Retrying network recovery");
				try {
					long delay = getDelay(recoveryAttempts);
					Thread.sleep(delay);
				} catch (InterruptedException interruptedException) {
					interruptedException.printStackTrace();
				}
				ShutdownSignalException cause;
				if (e.getCause() instanceof ShutdownSignalException) {
					cause = (ShutdownSignalException) e.getCause();
				} else {
					cause = new ShutdownSignalException(false, false, null, null);
				}
				AMQConnection amqConnection = (AMQConnection) channel.getConnection();
				try {
					amqConnection.handleIoError(cause);
					LOG.info("Restarting consumer");
					if (!channel.isOpen()) {
						throw new RabbitClientException(
								"Channel still not open, probably a topology issue prevent the recovery");
					}
				} catch (RabbitClientException e2) {
					LOG.debug("Retry failed with exception", e2);
					retryRecovery(e2, recoveryAttempts + 1);
				}
			}

			private Channel createNewChannelAndReturnOldOne() throws IOException, TimeoutException {
				ChannelPool channelPool = beanContext.getBean(ChannelPool.class,
						Qualifiers.byName(consumerParams.connection));
				Channel newChannel = getChannel(channelPool);

				if (channel.isOpen()) {
					LOG.debug("closing channel");
					channel.close();
				}
				ConsumerState oldChannelState = consumerChannels.remove(channel);
				if (oldChannelState != null) {
					channelPool.returnChannel(channel);
				}

				ConsumerState state = new ConsumerState();
				state.channelPool = channelPool;
				state.consumerTag = consumerParams.clientTag;
				consumerChannels.put(newChannel, state);

				return newChannel;
			}

			private long getDelay(int attempts) {
				RecoveryDelayHandler configuredDelayHandler = connectionFactoryConfig.getRecoveryDelayHandler();
				long networkRecoveryInterval = connectionFactoryConfig.getNetworkRecoveryInterval();
				RecoveryDelayHandler delayHandler = configuredDelayHandler == null
						? new RecoveryDelayHandler.DefaultRecoveryDelayHandler(networkRecoveryInterval)
						: configuredDelayHandler;
				return delayHandler.getDelay(attempts);
			}

			public void doHandleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) {
				final RabbitConsumerState state = new RabbitConsumerState(envelope, properties, body, channel);

				BoundExecutable boundExecutable = null;
				try {
					boundExecutable = binder.bind(method, binderRegistry, state);
				} catch (Throwable e) {
					handleException(new RabbitListenerException(
							"An error occurred binding the message to the method",
							e, bean, state));
				}

				try {
					if (boundExecutable != null) {
						try (RabbitMessageCloseable closeable = new RabbitMessageCloseable(state, false,
								reQueue).withAcknowledge(hasAckArg ? null : false)) {
							Object returnedValue = boundExecutable.invoke(bean);

							String replyTo = properties.getReplyTo();
							if (!isVoid && StringUtils.isNotEmpty(replyTo)) {
								MutableBasicProperties replyProps = new MutableBasicProperties();
								replyProps.setCorrelationId(properties.getCorrelationId());

								byte[] converted = null;
								if (returnedValue != null) {
									RabbitMessageSerDes serDes = serDesRegistry
											.findSerdes(method.getReturnType().asArgument())
											.map(RabbitMessageSerDes.class::cast)
											.orElseThrow(() -> new RabbitListenerException(String.format(
													"Could not find a serializer for the body argument of type [%s]",
													returnedValue.getClass().getName()), bean, state));

									converted = serDes.serialize(returnedValue, replyProps);
								}

								channel.basicPublish("", replyTo, replyProps.toBasicProperties(), converted);
							}

							if (!hasAckArg) {
								closeable.withAcknowledge(true);
							}
						} catch (MessageAcknowledgementException e) {
							throw e;
						} catch (Throwable e) {
							if (e instanceof RabbitListenerException) {
								handleException((RabbitListenerException) e);
							} else {
								handleException(new RabbitListenerException(
										"An error occurred executing the listener",
										e, bean, state));
							}
						}
					} else {
						new RabbitMessageCloseable(state, false, reQueue).withAcknowledge(false).close();
					}
				} catch (MessageAcknowledgementException e) {
					if (!channel.isOpen()) {
						ConsumerState consumerState = consumerChannels.remove(channel);
						if (consumerState != null) {
							consumerState.channelPool.returnChannel(channel);
						}
						if (LOG.isErrorEnabled()) {
							LOG.error(
									"The channel was closed due to an exception. The consumer [{}] will no longer receive messages",
									consumerParams.clientTag);
						}
					}
					handleException(new RabbitListenerException(e.getMessage(), e, bean, null));
				} finally {
					consumerChannels.get(channel).inProgress = false;
				}
			}

			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				consumerChannels.get(channel).inProgress = true;

				if (executorService != null) {
					executorService.submit(() -> doHandleDelivery(consumerTag, envelope, properties, body));
				} else {
					doHandleDelivery(consumerTag, envelope, properties, body);
				}
			}
		};
		return consumer;
	}

	@PreDestroy
	@Override
	public void close() throws Exception {
		while (!consumerChannels.entrySet().isEmpty()) {
			Iterator<Map.Entry<Channel, ConsumerState>> it = consumerChannels.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Channel, ConsumerState> entry = it.next();
				Channel channel = entry.getKey();
				ConsumerState state = entry.getValue();
				try {
					channel.basicCancel(state.consumerTag);
				} catch (IOException | AlreadyClosedException e) {
					// ignore
				}
				if (!state.inProgress) {
					state.channelPool.returnChannel(channel);
					it.remove();
				}
			}
		}
	}

	/**
	 * Retrieves a channel to use for consuming messages.
	 *
	 * @param channelPool The channel pool to retrieve the channel from
	 * @return A channel to publish with
	 */
	protected Channel getChannel(ChannelPool channelPool) {
		try {
			return channelPool.getChannel();
		} catch (IOException e) {
			throw new MessageListenerException("Could not retrieve a channel", e);
		}
	}

	private void handleException(RabbitListenerException exception) {
		Object bean = exception.getListener();
		if (bean instanceof RabbitListenerExceptionHandler) {
			((RabbitListenerExceptionHandler) bean).handle(exception);
		} else {
			exceptionHandler.handle(exception);
		}
	}

	/**
	 * Consumer state.
	 */
	private static class ConsumerState {
		private String consumerTag;
		private ChannelPool channelPool;
		private volatile boolean inProgress;
	}

	/**
	 * Connection params.
	 */
	private static class ConsumerParams {
		private String clientTag;
		private String connection;
		private boolean exclusive;
		private String queue;
	}
}
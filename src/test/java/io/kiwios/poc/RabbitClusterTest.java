package io.kiwios.poc;

import static java.lang.Math.abs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.HealthCheck;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoverableConnection;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownSignalException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RabbitClusterTest {
	private static final String CONSUMER_TAG = "myConsumerTag";
	private static final DockerImageName RABBIT_IMAGE = DockerImageName.parse("rabbitmq:3-management");
	private static final String CLUSTER_COOKIE = "testcluster";
	private static final String RABBIT_CONFIG_PATH;
	private static final String RABBIT_DEFINITIONS_PATH;
	private static final int AMQP_PORT = 5672;
	private static final String NODE1 = "rabbit@rabbitmq1";
	private static final String NODE2 = "rabbit@rabbitmq2";
	private static final String NODE3 = "rabbit@rabbitmq3";
	private static final int MANAGEMENTUI_PORT = 15672;
	private static final String EXCHANGE = "test-exchange";
	private static final String QUEUE = "test-durable-queue";
	private static final DockerClient DOCKER_CLIENT = DockerClientFactory.lazyClient();
	static {
		RABBIT_CONFIG_PATH = ClassLoader.getSystemResource("rabbit/rabbitmq.conf").getPath();
		RABBIT_DEFINITIONS_PATH = ClassLoader.getSystemResource("rabbit/definitions.json").getPath();
	}

	private static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
	private static Network mqClusterNet = Network.newNetwork();
	private static GenericContainer<?> mq1 = new GenericContainer<>(RABBIT_IMAGE);
	private static GenericContainer<?> mq2 = new GenericContainer<>(RABBIT_IMAGE);
	private static GenericContainer<?> mq3 = new GenericContainer<>(RABBIT_IMAGE);
	private static Map<String, Integer> NODE_PORTS = Map.of(
			NODE1, AMQP_PORT, NODE2, AMQP_PORT + 1, NODE3, AMQP_PORT + 2);
	private static Map<String, GenericContainer<?>> NODE_CONTAINERS = Map.of(NODE1, mq1, NODE2, mq2, NODE3, mq3);
	private static AtomicInteger publishCounter;
	private Connection con;
	private AtomicInteger receiveCounter;

	@BeforeAll
	static void setup() throws Exception {
		configureContainer(mq1, "rabbitmq1", NODE1);
		// first node must boot up completely to that the other nodes can join the cluster
		mq1.waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(4)));
		mq1.start();
		log.info("first node startup complete");

		configureContainer(mq2, "rabbitmq2", NODE2);
		configureContainer(mq3, "rabbitmq3", NODE3);
		// get the management UI always on the same port
		addPortBinding(mq3, MANAGEMENTUI_PORT, MANAGEMENTUI_PORT);
		// node 2 and 3 may startup in parallel as they can join the already existing cluster
		mq2.waitingFor(new DoNotWaitStrategy());
		mq3.waitingFor(new DoNotWaitStrategy());
		mq2.start();
		mq3.start();
		Awaitility.await("other cluster nodes start")
				.atMost(Duration.ofMinutes(4)).pollInterval(Duration.ofSeconds(1))
				.until(() -> mq2.isHealthy() && mq3.isHealthy());
		log.info("cluster startup complete");

		startPublisher(NODE3);
	}

	@AfterAll
	static void cleanup() {
		executorService.shutdown();
	}

	@BeforeEach
	void prepareTest() throws IOException, TimeoutException {
		// clean out possible leftovers from previous tests
		try (Connection con = createConnection(NODE3)) {
			con.createChannel().queuePurge(QUEUE);
		}

		publishCounter = new AtomicInteger();
		receiveCounter = new AtomicInteger();
	}

	@AfterEach
	void cleanupTest() throws IOException {
		con.close();
	}

	/**
	 * Demonstrate the various combinations of client to cluster node connection and the consequences of the inability
	 * of the client to handle certain situations by default.
	 *
	 * @param connectedNode        the node where the client will connect to
	 * @param restartedNode        the node that will be restarted during the test
	 * @param withConsumerRecovery defines whether a custom consumer recovery should be executed or just the client
	 *                             default recovery should be used
	 */
	@ParameterizedTest(name = "connect to {0} with consumer recovery  {2}, {1} will be restarted")
	@MethodSource("getParametersforTestRestartAndConsumerRecovery")
	@DisplayName("test combinations of client connections, cluster node restarts and consumer recovery")
	void testRestartAndConsumerRecovery(String connectedNode, String restartedNode, boolean withConsumerRecovery)
			throws Exception {
		con = createConnection(connectedNode);
		addConsumer(con, withConsumerRecovery);
		Thread.sleep(Duration.ofSeconds(10).toMillis());

		restartContainer(restartedNode);
		Thread.sleep(Duration.ofSeconds(10).toMillis());

		assertPublishConsumeCount();
	}

	/**
	 * Argument provider for {@link #testRestartAndConsumerRecovery(String, String, boolean)}.
	 * <p>
	 * Most of the test cases are handled by the default recovery mechanisms of the RabbitMQ client. The two marked (by
	 * comment '<- ') tests demonstrate where the default is not sufficient and that a custom recovery method can safely
	 * detect and recover that situation
	 */
	static List<Arguments> getParametersforTestRestartAndConsumerRecovery() {
		return List.of(
				Arguments.of(NODE1, NODE1, false),
				Arguments.of(NODE1, NODE1, true),
				Arguments.of(NODE1, NODE2, false),
				Arguments.of(NODE1, NODE2, true),
				Arguments.of(NODE2, NODE1, false), // <- this will fail due to no consumer recovery by default
				Arguments.of(NODE2, NODE1, true), // <- this demonstrates that an automatic recovery is possible
				Arguments.of(NODE2, NODE2, false),
				Arguments.of(NODE2, NODE2, true));
	}

	@Test
	void testConsumerCancelationDoesNotTriggerConsumerRecovery() throws Exception {
		con = createConnection(NODE2);
		// setup with consumer recovery
		Channel channel = addConsumer(con, true);
		Thread.sleep(Duration.ofSeconds(10).toMillis());

		// regular cancel of the consumer
		channel.basicCancel(CONSUMER_TAG);
		int cancelCounter = receiveCounter.get();

		// sleep for some time to verify that the automatic recovery does not kick in on regularly canceled consumers
		Thread.sleep(Duration.ofSeconds(10).toMillis());
		assertEquals(cancelCounter, receiveCounter.get(), "no messages must be received after cancelation");
	}

	private static void configureContainer(GenericContainer<?> mqContainer, String hostname, String nodeName) {
		mqContainer
				.withEnv("RABBITMQ_ERLANG_COOKIE", CLUSTER_COOKIE)
				.withFileSystemBind(RABBIT_CONFIG_PATH, "/etc/rabbitmq/rabbitmq.conf", BindMode.READ_ONLY)
				.withFileSystemBind(RABBIT_DEFINITIONS_PATH, "/etc/rabbitmq/definitions.json", BindMode.READ_ONLY)
				.withNetwork(mqClusterNet)
				.withLogConsumer(new Slf4jLogConsumer(log).withPrefix(hostname))
				.withCreateContainerCmdModifier(cmd -> cmd
						.withHostName(hostname)
						.withHealthcheck(new HealthCheck()
								.withTest(List.of("CMD-SHELL", "rabbitmqctl status"))
								.withStartPeriod(Duration.ofMinutes(4).toNanos())
								.withInterval(Duration.ofSeconds(5).toNanos())
								.withRetries(10)
								.withTimeout(Duration.ofSeconds(5).toNanos())));
		// Use fixed port binding, because the dynamic port binding would use different port on each container start.
		// These changing ports would make any reconnect attempt impossible, as the client assumes that the broker
		// address does not change.
		addPortBinding(mqContainer, NODE_PORTS.get(nodeName), AMQP_PORT);
	}

	private static void addPortBinding(GenericContainer<?> cont, int hostPort, int contPort) {
		cont.getPortBindings().add(String.format("%d:%d/%s",
				hostPort, contPort, InternetProtocol.TCP.toDockerNotation()));
	}

	private void assertPublishConsumeCount() {
		int pub = publishCounter.get();
		int con = receiveCounter.get();
		String message = "published (" + pub + ") and consumed (" + con + ") are equal";
		// the difference of 1 is acceptable due to the time gap between publish and consume
		assertTrue(abs(pub - con) <= 1, message);
	}

	private Channel addConsumer(Connection con, boolean withRecovery) throws IOException {
		Channel channel = con.createChannel();
		channel.basicConsume(QUEUE, false, CONSUMER_TAG, false, false, Map.of(), new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					AMQP.BasicProperties properties, byte[] body) throws IOException {
				long deliveryTag = envelope.getDeliveryTag();
				log.info("received: {}", new String(body));
				channel.basicAck(deliveryTag, false);
				receiveCounter.incrementAndGet();
			}

			@Override
			public void handleShutdownSignal(String consumerTag,
					ShutdownSignalException sig) {
				log.warn("handleShutdownSignal for {}", consumerTag, sig);
			}

			@Override
			public void handleCancel(String consumerTag) throws IOException {
				log.warn("handleCancel for {}", consumerTag);
				if (withRecovery) {
					try {
						getChannel().close(AMQP.REPLY_SUCCESS, "closing channel of canceled consumer");
					} catch (TimeoutException e) {
						throw new IOException("close timeout", e);
					}
					log.info("start re-creation of consumer");
					Channel recreated = null;
					while (recreated == null) {
						try {
							Thread.sleep(2000);
							recreated = addConsumer(con, withRecovery);
						} catch (InterruptedException | IOException e) {
							log.warn("consumer re-creation failed, will retry: {}", e.getMessage());
						}
					}
					log.info("consumer re-created");
				}
			}

			@Override
			public void handleCancelOk(String consumerTag) {
				log.info("handleCancelOk for {}", consumerTag);
			}

			@Override
			public void handleRecoverOk(String consumerTag) {
				log.info("handleRecoverOk for {}", consumerTag);
			}

			@Override
			public void handleConsumeOk(String consumerTag) {
				super.handleConsumeOk(consumerTag);
				log.info("handleConsumeOk for {}", consumerTag);
			}
		});
		return channel;
	}

	private void restartContainer(String nodeName) throws InterruptedException {
		GenericContainer<?> cont = NODE_CONTAINERS.get(nodeName);
		log.info("stopping container: {}", cont.getContainerId());
		DOCKER_CLIENT.stopContainerCmd(cont.getContainerId()).exec();
		log.info("re-starting container: {}", cont.getContainerId());
		DOCKER_CLIENT.startContainerCmd(cont.getContainerId()).exec();
		Awaitility.await("other cluster nodes start")
				.atMost(Duration.ofMinutes(4)).pollInterval(Duration.ofSeconds(1))
				.until(() -> cont.isHealthy());
		log.info("started container: {}", cont.getContainerId());
	}

	private static void startPublisher(String nodeName) throws IOException, TimeoutException {
		log.info("start publishing on  {}", nodeName);
		createConnection(nodeName).openChannel()
				.map(ch -> {
					try {
						ch.confirmSelect();
						// returned messages must not count as published
						ch.addReturnListener((r) -> publishCounter.decrementAndGet());
					} catch (IOException e) {
						log.error("failed to set confirmSelect", e);
					}
					return ch;
				})
				.ifPresentOrElse(
						ch -> executorService.scheduleWithFixedDelay(() -> {
							try {
								ch.basicPublish(EXCHANGE, "", true, new AMQP.BasicProperties.Builder()
										.deliveryMode(2)
										.build(),
										"test".getBytes());
								if (ch.waitForConfirms(1000)) {
									log.info("publish ack");
									publishCounter.incrementAndGet();
								}
							} catch (IOException | RuntimeException | InterruptedException | TimeoutException e) {
								log.error("failed to publish: {}", e.getMessage());
							}
						}, 500, 500, TimeUnit.MILLISECONDS),
						() -> fail("no channel available"));
	}

	private static Connection createConnection(String nodeName) throws IOException, TimeoutException {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setPort(NODE_PORTS.get(nodeName));
		Connection con = connectionFactory.newConnection();
		((RecoverableConnection) con).addRecoveryListener(new RecoveryListener() {

			@Override
			public void handleRecoveryStarted(Recoverable recoverable) {
				log.info("handleRecoveryStarted: {}", recoverable);
			}

			@Override
			public void handleRecovery(Recoverable recoverable) {
				log.info("handleRecovery: {}", recoverable);
			}

			@Override
			public void handleTopologyRecoveryStarted(Recoverable recoverable) {
				log.info("handleTopologyRecoveryStarted: {}", recoverable);
			}
		});
		return con;
	}

	private static class DoNotWaitStrategy extends AbstractWaitStrategy {
		@Override
		protected void waitUntilReady() {
			// NOOP - do not wait
		}
	}
}

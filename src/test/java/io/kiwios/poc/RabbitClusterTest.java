package io.kiwios.poc;

import java.time.Duration;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.HealthCheck;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RabbitClusterTest {
	private static final DockerImageName RABBIT_IMAGE = DockerImageName.parse("rabbitmq:3-management");
	private static final String CLUSTER_COOKIE = "testcluster";
	private static final String RABBIT_CONFIG_PATH;
	private static final String RABBIT_DEFINITIONS_PATH;
	private static final int AMQP_PORT = 5672;
	private static final int MANAGEMENTUI_PORT = 15672;
	static {
		RABBIT_CONFIG_PATH = ClassLoader.getSystemResource("rabbit/rabbitmq.conf").getPath();
		RABBIT_DEFINITIONS_PATH = ClassLoader.getSystemResource("rabbit/definitions.json").getPath();
	}

	private Network mqClusterNet = Network.newNetwork();
	private GenericContainer<?> mq1 = new GenericContainer<>(RABBIT_IMAGE);
	private GenericContainer<?> mq2 = new GenericContainer<>(RABBIT_IMAGE);
	private GenericContainer<?> mq3 = new GenericContainer<>(RABBIT_IMAGE);

	@BeforeEach
	void setup() {
		configureContainer(mq1, "rabbitmq1");
		// first node must boot up completely to that the other nodes can join the cluster
		mq1.waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(4)));
		mq1.start();
		log.info("first node startup complete");

		configureContainer(mq2, "rabbitmq2");
		configureContainer(mq3, "rabbitmq3");
		// get the management UI always on the same port
		mq3.getPortBindings().add(String.format("%d:%d/%s",
				MANAGEMENTUI_PORT, MANAGEMENTUI_PORT, InternetProtocol.TCP.toDockerNotation()));
		// node 2 and 3 may startup in parallel as they can join node 1
		mq2.waitingFor(new DummyWaitStrategy());
		mq3.waitingFor(new DummyWaitStrategy());
		mq2.start();
		mq3.start();
		Awaitility.await("other cluster nodes start")
				.atMost(Duration.ofMinutes(4)).pollInterval(Duration.ofSeconds(2))
				.until(() -> mq2.isHealthy() && mq3.isHealthy());
		log.info("cluster startup complete");
	}

	private static class DummyWaitStrategy extends AbstractWaitStrategy {
		@Override
		protected void waitUntilReady() {
			// NOOP - do not wait
		}
	}

	@AfterEach
	void cleanup() {
		mq1.stop();
		mq2.stop();
		mq3.stop();
	}

	private void configureContainer(GenericContainer<?> mqContainer, String hostname) {
		mqContainer
				.withEnv("RABBITMQ_ERLANG_COOKIE", CLUSTER_COOKIE)
				.withFileSystemBind(RABBIT_CONFIG_PATH, "/etc/rabbitmq/rabbitmq.conf", BindMode.READ_ONLY)
				.withFileSystemBind(RABBIT_DEFINITIONS_PATH, "/etc/rabbitmq/definitions.json", BindMode.READ_ONLY)
				.withNetwork(mqClusterNet)
				.withExposedPorts(AMQP_PORT)
				.withLogConsumer(new Slf4jLogConsumer(log).withPrefix(hostname))
				.withCreateContainerCmdModifier(cmd -> cmd
						.withHostName(hostname)
						.withHealthcheck(new HealthCheck()
								.withTest(List.of("CMD-SHELL", "rabbitmqctl status"))
								.withStartPeriod(Duration.ofMinutes(4).toNanos())
								.withInterval(Duration.ofSeconds(5).toNanos())
								.withRetries(10)
								.withTimeout(Duration.ofSeconds(5).toNanos())));
	}

	@Test
	void createClusterTest() throws InterruptedException {
		log.info("rabbitmq1 AMQP port: {}", mq1.getFirstMappedPort());
		log.info("rabbitmq2 AMQP port: {}", mq2.getFirstMappedPort());
		log.info("rabbitmq3 AMQP port: {}", mq3.getFirstMappedPort());
		Thread.currentThread();
		Thread.sleep(120000);
	}
}

package io.micronaut.configuration.kafka

import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.util.environment.OperatingSystem

abstract class AbstractEmbeddedServerSpec extends AbstractKafkaSpec {

    // The confluent kafka image doesn't run on M1 mac at present, so use this (old) image for local dev
    private static boolean isM1Mac = OperatingSystem.current.macOs && System.getProperty("os.arch") == 'aarch64'
    private static DockerImageName dockerImageName = isM1Mac ?
            DockerImageName.parse("kymeric/cp-kafka").asCompatibleSubstituteFor("confluentinc/cp-kafka") :
            DockerImageName.parse("confluentinc/cp-kafka")

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer(dockerImageName)
    @Shared @AutoCleanup EmbeddedServer embeddedServer
    @Shared @AutoCleanup ApplicationContext context

    void setupSpec() {
        startKafka()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                getConfiguration() +
                        ['kafka.bootstrap.servers': kafkaContainer.bootstrapServers]
        )
        context = embeddedServer.applicationContext
    }

    void startKafka() {
        kafkaContainer.start()
    }

    void createTopic(String name, int numPartitions, int replicationFactor) {
        try (def admin = AdminClient.create([(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG): kafkaContainer.bootstrapServers])) {
            admin.createTopics([new NewTopic(name, numPartitions, (short) replicationFactor)]).all().get()
        }
    }
}

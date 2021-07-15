package org.kafka.workshop.kafkaworkshop

import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.support.TestPropertySourceUtils
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

class CustomKafkaContainer {
    companion object {
        val container: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
        fun start() {
            container.withExposedPorts(9092, 9093)
            container.start()
            container.execInContainer("kafka-topics " +
                    "            --create " +
                    "            --zookeeper 127.0.0.1:2181 " +
                    "            --partitions 3 " +
                    "            --replication-factor 1 " +
                    "            --config cleanup.policy=compact " +
                    "            --config retention.bytes=-1 " +
                    "            --config retention.ms=2592000000 " +
                    "            --topic proAnimation.tracking.event")

        }
    }

    class Initializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(applicationContext: ConfigurableApplicationContext) {
            start()
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                    applicationContext,
                    "kafka.bootstrapserver=${container.bootstrapServers}"
            )
        }
    }
}
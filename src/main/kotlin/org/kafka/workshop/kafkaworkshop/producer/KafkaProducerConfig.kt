package org.kafka.workshop.kafkaworkshop.producer

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class KafkaProducerConfig {
    companion object{
        const val SCHEMA_URL = "http://localhost:8081"
        const val BOOTSTRAP_SERVER = "localhost:9092"

    }

    fun stringProducerFactory(): ProducerFactory<String, String> {
        val configProps: MutableMap<String, Any> = HashMap()
        configProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVER
        configProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        return DefaultKafkaProducerFactory(configProps)
    }
    fun jsonProducerFactory(): ProducerFactory<String, Any> {
        val jsonProps: MutableMap<String, Any> = HashMap()
        jsonProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVER
        jsonProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java
        jsonProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = JsonSerializer::class.java
        return DefaultKafkaProducerFactory(jsonProps)
    }

    fun avroProducerFactory(): ProducerFactory<String, GenericRecord> {
        val avroProps: MutableMap<String, Any> = HashMap()
        avroProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVER
        avroProps["schema.registry.url"] = SCHEMA_URL
        avroProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        avroProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
        return DefaultKafkaProducerFactory(avroProps)
    }

    @Bean
    fun kafkaStringProducer(): KafkaTemplate<String, String> {
        return KafkaTemplate(stringProducerFactory())
    }

    @Bean
    fun kafkaJsonProducer(): KafkaTemplate<String, Any> {
        return KafkaTemplate(jsonProducerFactory())
    }

    @Bean
    fun kafkaAvroProducer(): KafkaTemplate<String, GenericRecord> {
        return KafkaTemplate(avroProducerFactory())
    }
}
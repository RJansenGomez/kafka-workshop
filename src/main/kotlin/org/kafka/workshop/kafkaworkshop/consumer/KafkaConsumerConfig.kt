package org.kafka.workshop.kafkaworkshop.consumer

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.kafka.workshop.kafkaworkshop.producer.KafkaProducerConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer

@Configuration
class KafkaConsumerConfig {

    fun stringConsumerConfig(): ConsumerFactory<String, String> {
        val configProps: MutableMap<String, Any> = HashMap()
        configProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaProducerConfig.BOOTSTRAP_SERVER
        configProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        configProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        return DefaultKafkaConsumerFactory(configProps)
    }

    fun jsonConsumerConfig(): ConsumerFactory<String, Any> {
        val jsonProps: MutableMap<String, Any> = HashMap()
        jsonProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaProducerConfig.BOOTSTRAP_SERVER
        jsonProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = JsonDeserializer::class.java
        jsonProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = JsonDeserializer::class.java
        jsonProps[JsonDeserializer.TRUSTED_PACKAGES] = "*"
        return DefaultKafkaConsumerFactory(jsonProps)
    }

    fun avroConsumerConfig(): ConsumerFactory<String, GenericRecord> {
        val avroProps: MutableMap<String, Any> = HashMap()
        avroProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaProducerConfig.BOOTSTRAP_SERVER
        avroProps["schema.registry.url"] = KafkaProducerConfig.SCHEMA_URL
        avroProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] =  "earliest"
        avroProps["specific.avro.reader"] = "true"
        avroProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        avroProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java
        return DefaultKafkaConsumerFactory(avroProps)
    }

    @Bean
    fun stringConsumerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = stringConsumerConfig();
        return factory;
    }

    @Bean
    fun jsonConsumerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
        factory.consumerFactory = jsonConsumerConfig();
        return factory;
    }

    @Bean
    fun avroConsumerFactory(): ConcurrentKafkaListenerContainerFactory<String, GenericRecord> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, GenericRecord>()
        factory.consumerFactory = avroConsumerConfig();
        return factory;
    }
}
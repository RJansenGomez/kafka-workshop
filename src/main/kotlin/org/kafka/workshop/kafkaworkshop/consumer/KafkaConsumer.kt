package org.kafka.workshop.kafkaworkshop.consumer

import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class KafkaConsumer {
    @KafkaListener(
            topics = ["kafka.workshop.string"],
            groupId = "string-group-consumer",
            containerFactory = "stringConsumerFactory"
    )
    fun stringConsumer(@Payload message: String,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int) {
        println("String message $message received from $partition")
    }

    @KafkaListener(
            topics = ["kafka.workshop.any"],
            groupId = "json-group-consumer",
            containerFactory = "jsonConsumerFactory"
    )
    fun jsonConsumer(@Payload message: Any,
                     @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int) {
        println("Json message $message received from $partition")
    }

    @KafkaListener(
            topics = ["kafka.workshop.avro"],
            groupId = "avro-group-consumer",
            containerFactory = "avroConsumerFactory"
    )
    fun stringConsumer(@Payload message: GenericRecord,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int) {
        println("Avro message $message received from $partition")
    }
}
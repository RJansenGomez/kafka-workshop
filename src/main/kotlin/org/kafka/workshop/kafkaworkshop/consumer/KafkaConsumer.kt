package org.kafka.workshop.kafkaworkshop.consumer

import org.apache.avro.generic.GenericRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import workshop.kafka.avro.AvroTest
import java.time.LocalDateTime

@Component
class KafkaConsumer {

    companion object{
        var sPartitions = mutableSetOf<Int>()
        var jPartitions = mutableSetOf<Int>()
        var aPartitions = mutableSetOf<Int>()
        var first = true
        var jsonFirst = true
        var avroFirst = true
    }

    @KafkaListener(
            topics = ["kafka.workshop.string"],
            groupId = "string-group-consumerB",
            containerFactory = "stringConsumerFactory"
    )
    fun stringConsumer(@Payload message: String,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int) {
//        println("String message $message received from $partition")

        var previousSize = sPartitions.size
        sPartitions.add(partition)
        if(sPartitions.size!=previousSize){
            println("string Partitions subscribed $sPartitions")

        }
    }

    @KafkaListener(
            topics = ["kafka.workshop.any"],
            groupId = "json-group-consumerB",
            containerFactory = "jsonConsumerFactory"
    )
    fun jsonConsumer(@Payload message: Any,
                     @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int) {
//        println("Json message $message received from $partition")

        var previousSize = jPartitions.size
        jPartitions.add(partition)
        if(jPartitions.size!=previousSize){
            println("Json Partitions subscribed $jPartitions")
        }
    }

    @KafkaListener(
            topics = ["kafka.workshop.avro"],
            groupId = "avro-group-consumer-2",
            containerFactory = "avroConsumerFactory"
    )
    fun avroConsumer(@Payload message: AvroTest,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int) {
//        println("Avro message $message received from $partition")
        if(message.getSource().isNullOrBlank()){
            println("OLD SHEMA")
        }
        var previousSize = aPartitions.size
        aPartitions.add(partition)
        if(aPartitions.size!=previousSize){
            println("Avro Partitions subscribed $aPartitions")
        }
    }
}
package org.kafka.workshop.kafkaworkshop.controller

import org.kafka.workshop.kafkaworkshop.producer.KafkaProducer
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import workshop.kafka.avro.AvroTest
import workshop.kafka.avro.AvroTestContent

@RestController
@RequestMapping("load")
class LoadController(val kafkaProducer: KafkaProducer) {

    @GetMapping
    fun executeLoad() {
        executeStringPublisher()
        executeJsonPublisher()
        executeAvroPublisher()
    }

    companion object {
        const val MAX_MESSAGES = 1000
    }

    private fun executeStringPublisher() {
        var count = 0
        while (count < MAX_MESSAGES) {
            kafkaProducer.publishMessage("Random message number $count")
            count++;
        }
    }

    private fun executeJsonPublisher() {
        var count = 0
        while (count < MAX_MESSAGES) {
            kafkaProducer.publishMessage(
                    JsonMessageContent(
                            field1 = "Field1",
                            field2 = "Field2",
                            field3 = "Field3",
                            field4 = "Field4",
                            field5 = "Field5")
            )
            count++;
        }
    }

    private fun executeAvroPublisher() {
        var count = 0
        while (count < MAX_MESSAGES) {
            kafkaProducer.publishMessage(
                    AvroTest("Event-$count",
                            "market",
                            "platformId",
                            "source",
                            AvroTestContent("EventContent-$count",
                                    "marketContent",
                                    "platformIdContent",
                                    "sourceContent"))
            )
            count++;
        }
    }
}

data class JsonMessageContent(val field1: String?, val field2: String?, val field3: String?, val field4: String?, val field5: String?) {
    constructor() : this("", "", "", "", "")
}
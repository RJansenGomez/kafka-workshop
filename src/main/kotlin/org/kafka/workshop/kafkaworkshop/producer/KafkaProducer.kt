package org.kafka.workshop.kafkaworkshop.producer

import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import org.springframework.util.concurrent.ListenableFuture
import org.springframework.util.concurrent.ListenableFutureCallback
import kotlin.reflect.KClass

@Component
class KafkaProducer(
        @Qualifier("kafkaStringProducer") val stringProducer: KafkaTemplate<String, String>,
        @Qualifier("kafkaJsonProducer") val jsonProducer: KafkaTemplate<String, Any>,
        @Qualifier("kafkaAvroProducer") val avroProducer: KafkaTemplate<String, GenericRecord>) {

    fun publishMessage(message: String) {
        sendMessageWithCallback<String>(message)
    }

    fun publishMessage(message: Any) {
        sendMessageWithCallback<Any>(message)
    }

    fun publishMessage(message: GenericRecord) {
        sendMessageWithCallback<GenericRecord>(message)
    }

    fun <T> sendMessageWithCallback(message: Any) {
        val future = when (message) {
            is String -> {
                stringProducer.send("kafka.workshop.string", message) as ListenableFuture<SendResult<String, T>>
            }
            is GenericRecord -> {
                avroProducer.send("kafka.workshop.avro", message) as ListenableFuture<SendResult<String, T>>
            }
            else -> {
                jsonProducer.send("kafka.workshop.any", message) as ListenableFuture<SendResult<String, T>>
            }
        }
        future.addCallback(KafkaProcessCallback(message))
    }
}

class KafkaProcessCallback<T>(
        private val message: Any,
) : ListenableFutureCallback<SendResult<String, T>> {
    private val log = LoggerFactory.getLogger(KafkaProcessCallback::class.java)
    override fun onSuccess(result: SendResult<String, T>?) {
        log.info(
                "Message [{}] delivered with offset {} and partition {}",
                message,
                result!!.recordMetadata.offset(),
                result.recordMetadata.partition()
        )
    }

    override fun onFailure(ex: Throwable) {
        ex.printStackTrace()
        log.warn(
                "Unable to deliver message [{}]. {}",
                message,
                ex.message
        )
    }
}

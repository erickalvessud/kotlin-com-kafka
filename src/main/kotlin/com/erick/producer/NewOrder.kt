package com.erick.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.util.*

fun main() {
    var kafkaProducer = createProducer()
    var value = "132132, 67523, 74545745"
    val record = ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value)

    kafkaProducer.send(record) { data, ex ->
        if (ex != null) {
            ex.printStackTrace()
            return@send;
        }
        print("${data.topic()} :::partition ${data.partition()} / offset ${data.offset()} / timestamp ${data.timestamp()}")
    }.get()
}

fun createProducer(): KafkaProducer<String, String> {
    return KafkaProducer<String, String>(createProperties())
}

fun createProperties(): Properties = Properties().apply() {
    setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
}

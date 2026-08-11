package com.roomdoor.kafkalab

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class KafkaLabApplication

fun main(args: Array<String>) {
	runApplication<KafkaLabApplication>(*args)
}

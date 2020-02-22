package com.nxhuy.bitbrew.kafka.demo

import java.nio.charset.StandardCharsets
import java.util.{Properties, UUID}

import com.rabbitmq.client._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object BitbrewDataProducer {
  val host = "rabbitmq-production.kubernetes.bitbrew.com"
  val username = "huy@road-b-score.com"
  val pass = "nguyenxuanhuy1996"
  val port = 5671
  val vhost = "tenantroadbscore"
  val QUEUE_NAME = "kafka-test"

  def main(args: Array[String]): Unit = {
    val brokers = util.Try(args(0)).getOrElse("localhost:9092")
    val topic = util.Try(args(1)).getOrElse("bitbrew-data")
    val events = util.Try(args(2)).getOrElse("0").toInt
    val clientId = UUID.randomUUID().toString

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", clientId)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    println("==========================BEGIN=======================")

    val factory = new ConnectionFactory()
    factory.setUsername(username)
    factory.setPassword(pass)
    factory.setPort(port)
    factory.setHost(host)
    factory.setVirtualHost(vhost)
    factory.setRequestedHeartbeat(30)
    factory.useSslProtocol("TLSv1.2")

    val connection = factory.newConnection()
    val channel = connection.createChannel()
    channel.queueDeclare(QUEUE_NAME, true, false, false, null)
    println(" [*] Waiting for messages. To exit press CTRL+C")

    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String,
                                  envelope: Envelope,
                                  properties: AMQP.BasicProperties,
                                  body: Array[Byte]): Unit = {
        val message = new String(body, StandardCharsets.UTF_8)
        val key = UUID.randomUUID().toString().split("-")(0)
        val data = new ProducerRecord[String, String](topic, key, message)

        println("----topic: " + topic + " ----")
        println("key: " + data.key())
        println("value: " + data.value())
        producer.send(data)
      }
    }
    channel.basicConsume(QUEUE_NAME, true, consumer)
  }
}

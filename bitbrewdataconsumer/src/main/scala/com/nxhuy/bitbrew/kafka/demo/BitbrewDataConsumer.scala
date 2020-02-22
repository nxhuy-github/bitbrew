package com.nxhuy.bitbrew.kafka.demo

import java.util.UUID

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonParser}

object BitbrewDataConsumer {
  var brokers = ""

  var region = "eu-west-3"
  var output_dynamo_table = "Kafka-Test"
  var aws_access_key_id = "AKIAZD4IIA6BD6OIIKHT"
  var aws_secret_access_key = "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"
  final var credentials = new BasicAWSCredentials(aws_access_key_id, aws_secret_access_key)
  final var credentialsProvider = new AWSStaticCredentialsProvider(credentials)

  def main(args : Array[String]) : Unit = {
    brokers = util.Try(args(0)).getOrElse("localhost:9092")
    val outTopic = util.Try(args(1)).getOrElse("bitbrew-data")
    val batchDuration = util.Try(args(2)).getOrElse("30").toInt

    val sparkConf = new SparkConf().setAppName("BitbrewDataConsumer").setMaster("local[*]")
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val streamCtx = new StreamingContext(sparkSession.sparkContext, Seconds(batchDuration))

    val outTopicSet = Set(outTopic)
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "test"
    )
    val preferredHost = LocationStrategies.PreferConsistent

    val data = KafkaUtils.createDirectStream[String, String](
      streamCtx,
      preferredHost,
      ConsumerStrategies.Subscribe[String, String](outTopicSet, kafkaParams)
    )

    val value = data.map(_.value())
    value.print()
    /*----Code for process received data here---*/
    value.foreachRDD(rddRaw => {
      val rdd = rddRaw.map(_.toString)
      val df = sparkSession.read.json(rdd.toDS())

      df.printSchema()
      df.show(5, false)

      val dfJsonArr = df.toJSON
      dfJsonArr.foreachPartition(pushDataInDynamoDB(_))
    })

    //Start the stream
    streamCtx.start()
    streamCtx.awaitTermination()
  }

  def pushDataInDynamoDB(items: Iterator[String]): Unit = {
    val dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(credentialsProvider)
      .withRegion(region)
      .build()
    val dynamoDBConn = new DynamoDB(dynamoDBClient)
    val dynamoTable = dynamoDBConn.getTable(output_dynamo_table)

    items.foreach(obj => {
      val parser = new JsonParser()
      val _t = parser.parse(obj).getAsJsonObject()

      val header = _t.getAsJsonObject("header")
      val body = _t.getAsJsonObject("body")
      val deviceId = header.get("deviceId")
      val eventId = header.get("eventId")
      val messageId = header.get("messageId")
      val tags = header.get("tags")
      val timestamp = body.get("timestamp")
      //val timestamp = getKeyRecursivelyInJson(body, "timestamp")

      val item = new Item()
        .withPrimaryKey("deviceId", deviceId.toString) //+ UUID.randomUUID().toString
        .withString("eventId", eventId.toString)
        .withJSON("message", obj)
        .withString("tags", tags.toString)
        .withString("timestamp", timestamp.toString)
        .withString("messageId", messageId.toString)
      dynamoTable.putItem(item)

      /*val jsonObj = JSON.parseFull(obj)
      jsonObj match {
        case Some(e:Map[String, Map[String, Any]]) => {
          val deviceId = e.get("header").get("deviceId")
          val eventId = e.get("header").get("eventId")
          val messageId = e.get("header").get("messageId")
          val tags = e.get("header").get("tags")
          val timestamp = e.get("body").get("timestamp")

          val item = new Item()
            .withPrimaryKey("deviceId", deviceId.toString + UUID.randomUUID().toString)
            .withString("eventId", eventId.toString)
            .withJSON("message", obj)
            .withString("tags", tags.toString)
            .withString("timestamp", timestamp.toString)
            .withString("messageId", messageId.toString)

          dynamoTable.putItem(item)
        }
      }*/
    })
  }

}

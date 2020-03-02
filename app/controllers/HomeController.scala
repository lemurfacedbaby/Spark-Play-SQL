package controllers

import java.util.Collections
import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import javax.inject.Inject
import play.api.mvc._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SQLContext
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import spark.SparkTest

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  def index = Action { implicit request =>
    Ok(views.html.index())
  }
  // change group id to the machine id

  def kafkaTest = Action { implicit request =>
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "0")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    //val conf = new SparkConf().setAppName("IBM").setMaster("local[2]")
    //val ssc = new StreamingContext(conf, Seconds(1))
    //}
    var topics = Array("test")
    // Kafka Consumer Approach
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(topics(0)))
    val records: ConsumerRecords[String, String] = consumer.poll(100)
    val iterator = records.records(topics(0)).iterator()
    val messageList = ArrayBuffer[String]()
    while(iterator.hasNext()) {
      messageList += iterator.next().value()
    }
    Ok(views.html.test_args(s"A call to Spark, with result: $messageList"))
  }
}

package controllers

import javax.inject.Inject
import play.api.mvc._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SQLContext
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import spark.SparkTest

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
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
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // External, local orchestration
    val conf = new SparkConf().setMaster("local[2]").setAppName("IBM")
    val ssc = new StreamingContext(conf, Seconds(1))
    val topics = Array("test")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val sql = new SQLContext(ssc.sparkContext)
    import sql.implicits._
    //val message = "Yo"
    System.out.println(stream.print())
    Ok(views.html.test_args(s"A message from Kafka topic test with message: ${stream.print()}"))
    stream.foreachRDD { rdd => 
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      //Ok(views.html.test_args(s"A message from Kafka topic test with message: ${sql}"))
      val wordsDataFrame = sql.createDataFrame(rdd)
      wordsDataFrame.createOrReplaceTempView("words")
      val wordCountsDataFrame = 
        sql("select word, count(*) as total from words group by word")
      Ok(views.html.test_args(s"A message from Kafka topic test with message: ${wordCountsDataFrame.show()}"))*/

    }
    //stream.map(record => (record.key, record.value))


    //val inputStream = stream.map(record => (record.key, record.value))
    // Usually would then convert to an RDD, then DF
    // In this case, print the first message consumed from the topic
    //val message = inputStream.print(1)
  }

  // A simple example to call Apache Spark
  // A
  def test = Action { implicit request =>

  	val sum = SparkTest.Example
    Ok(views.html.test_args(s"A call to Spark, with result: $sum"))
  }

  // A non-blocking call to Apache Spark 
  def testAsync = Action.async{
  	val futureSum = Future{SparkTest.Example}
    futureSum.map{ s => Ok(views.html.test_args(s"A non-blocking call to Spark with result: ${s + 1000}"))}
  }

}

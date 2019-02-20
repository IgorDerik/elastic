package com.app

import java.time.{Instant, LocalDateTime}
import java.util.TimeZone
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.elasticsearch.spark.sql._

object StreamUtils {

  /**
    * Method to get date and time in "yyyy-mm-ddThh:mm:ss.ms" format
    * It will be used to timestamp for Kibana app
    * @param timestamp in milliseconds (for example from Kafka)
    * @return date and time in needed format as String
    */
  def getDateTime(timestamp: Long): String = {
    val localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), TimeZone.getDefault.toZoneId)
    localDateTime.toString
  }

  /**
    * Method reading input stream and write data to Elastic
    * @param stream to read from
    * @param esIndex Elastic index where data will be saved
    */
  def publishRDDs(stream: InputDStream[ConsumerRecord[String, String]], esIndex: String): Unit = {
    val sparkSession = SparkSession.builder.getOrCreate

    val structType = new StructType()
      .add("date", DataTypes.StringType)
      .add("offset", DataTypes.LongType)
      .add("value", DataTypes.StringType)

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        val targetDataRDD = rdd.map(rec => (getDateTime(rec.timestamp), rec.offset, rec.value))
        val targetDataRowRDD: RDD[Row] = targetDataRDD.map(data => Row(data._1, data._2, data._3))
        val targetDataDF: DataFrame = sparkSession.createDataFrame(targetDataRowRDD, structType)
        targetDataDF.saveToEs(esIndex)
      }
      else println("RDD IS EMPTY")
    }
  }

  /**
    * Method for getting stream from Kafka
    * @param streamingContext streaming context
    * @param topic is kafka topic to read from
    * @param kafkaParams Map contains Kafka parameters
    * @param fromOffsets offset from which data should start reading
    * @return input direct stream of consumer records from Kafka
    */
  def getStream(streamingContext: StreamingContext, topic: String, kafkaParams: Map[String, AnyRef], fromOffsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    val topicList = List(topic)
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicList, kafkaParams,fromOffsets))
  }

  /**
    * Method for getting stream from Kafka
    * It doesn't contain fromOffsets parameter
    * It will call the previous method with default fromOffsets parameter
    * @return input direct stream of consumer records from Kafka
    */
  def getStream(streamingContext: StreamingContext, topic: String, kafkaParams: Map[String, AnyRef]): InputDStream[ConsumerRecord[String, String]] = {
    val defaultFromOffsets = Map(new TopicPartition(topic, 0) -> 0L)
    getStream(streamingContext, topic, kafkaParams, defaultFromOffsets)
  }

}
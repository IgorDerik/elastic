package com.app

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

object Main extends App {

  val topic = args(0)
  val elasticIndex = args(1)
  val duration = Duration(args(2).toLong)

  val conf = new SparkConf().setAppName("SparkWithElastic")
                            .setMaster("local[*]")
                            .set("es.index.auto.create", "true")
                            .set("es.nodes", "localhost")
                            .set("es.port", "9200")
                            .set("es.http.timeout", "1m")
                            .set("es.scroll.size", "50")

  val streamingContext = new StreamingContext(conf, duration)

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "stream-hw",
    "kafka.consumer.id" -> "kafka-consumer-01",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val stream = StreamUtils.getStream(streamingContext, topic, kafkaParams)

  StreamUtils.publishRDDs(stream, elasticIndex)

  streamingContext.start()
  streamingContext.awaitTermination()

}

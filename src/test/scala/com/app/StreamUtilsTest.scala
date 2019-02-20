package com.app

import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}
import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}

class StreamUtilsTest extends FlatSpec with BeforeAndAfter {

  private var sparkConf: SparkConf = _
  private var ssc: StreamingContext = _
  private var embeddedElastic: EmbeddedElastic = _

  private val topic: String = "hotels10"

  private val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "stream-hw",
    "kafka.consumer.id" -> "kafka-consumer-01",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  before {
    val kafkaProps = new Properties()
    for ( (k,v) <- kafkaParams ) kafkaProps.put(k,v)

    try {
      AdminClient.create(kafkaProps)
        .listTopics(new ListTopicsOptions()
          .timeoutMs(3000)).listings().get()
    } catch {
      case e: Exception =>
        println("Kafka is not available: "+e.getMessage)
        println("Please install and configure it locally before testing, you can find all details in the README file.")
        sys.exit
    }

    embeddedElastic = EmbeddedElastic.builder()
      .withElasticVersion("5.5.3")
      .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
      .withSetting(PopularProperties.CLUSTER_NAME, "cluster")
      .build()
      .start()

    sparkConf = new SparkConf().setAppName("Elastic Test")
      .setMaster("local[*]")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "localhost")
      .set("es.port", embeddedElastic.getHttpPort.toString)
      .set("es.http.timeout", "1m")
      .set("es.scroll.size", "50")

    ssc = new StreamingContext(sparkConf, Seconds(1))
  }

  after {
    Thread.sleep(1000)
    ssc.stop()
    embeddedElastic.stop()
  }

  behavior of "StreamUtils.publishRDDs method"
  it should "save records to the Elastic" in {

    val stream = StreamUtils.getStream(ssc, topic, kafkaParams)

    StreamUtils.publishRDDs(stream, "test/docs")

    var expectedDocs: Array[String] = null
    stream.foreachRDD( rdd=>
      if (!rdd.isEmpty) {
        val targetDataRDD = rdd.map(rec => (StreamUtils.getDateTime(rec.timestamp), rec.offset, rec.value))
        expectedDocs = targetDataRDD.collect().map {
          case(time, offset, value) =>
            ("{\"date\":" + "\"" + time + "\"", "\"offset\":" + offset, "\"value\":" + "\"" + value + "\"}").toString
        }
      }
    )

    ssc.start()
    ssc.awaitTerminationOrTimeout(2000)

    Thread.sleep(3000)

    val actualDocs = embeddedElastic.fetchAllDocuments("test").toArray.map(doc => "(" + doc + ")")

    assert(expectedDocs.length==actualDocs.length)

    expectedDocs.foreach( doc => assert(actualDocs.contains(doc)) )

  }
}

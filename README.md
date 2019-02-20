# Spark Job Application that reads Kafka topic and publish data to Elastic

## Getting Started

Please be sure to configure your Kafka and Elastic properties in Main class.
After that, the recommend way of use is to build jar using maven plugin and run it with parameters using java command.

* Build project with maven plugin:
```
mvn package
```
* Run jar with 3 parameters using java:
```
java -jar pathToJarFile.jar topic elasticIndex duration

pathToJarFile.jar: Path to the generated jar file
topic: kafka topic name that should be read
elasticIndex: Elastic index where data should be saved
duration: batch duration of Spark Streaming
```

## StreamUtils methods

* def getDateTime(timestamp: Long): String
  * Method to get date and time in "yyyy-mm-ddThh:mm:ss.ms" format
    * It will be used to timestamp for Kibana app
    * @param timestamp in milliseconds (for example from Kafka)
    * @return date and time in needed format as String

* def publishRDDs(stream: InputDStream[ConsumerRecord[String, String]], esIndex: String)
  * Method reading input stream and write data to Elastic
    * @param stream to read from
    * @param esIndex Elastic index where data will be saved
    
* def getStream(streamingContext: StreamingContext, topic: String, kafkaParams: Map[String, AnyRef], fromOffsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]]
  * Method for getting stream from Kafka
    * @param streamingContext streaming context
    * @param topic is kafka topic to read from
    * @param kafkaParams Map contains Kafka parameters
    * @param fromOffsets offset from which data should start reading
    * @return input direct stream of consumer records from Kafka
    
* def getStream(streamingContext: StreamingContext, topic: String, kafkaParams: Map[String, AnyRef]): InputDStream[ConsumerRecord[String, String]]
  * Method for getting stream from Kafka
    * It does not contain fromOffsets parameter
    * It will call the previous method with default fromOffsets parameter
    * @return input direct stream of consumer records from Kafka

## Running the tests

Please install Kafka on your local machine to be able to test the application.
By default, Kafka's bootstrap server should be running on <localhost:9092>, but you can change it by editing the class with tests.
If you want to test on Windows OS, here is an article which describe how to do this: <https://dzone.com/articles/running-apache-kafka-on-windows-os>
Also, please create a topic with name: `hotels10` and send some sample text messages to it, for example:
```
0,195,2,0,8803,0,3,151,1236,82
1,66,3,0,12009,,1,0,9,6/26/2015
2,66,2,0,12009,1,2,50,368,95
3,3,2,3,66,0,6,105,35,25
4,3,2,3,23,0,6,105,35,82
5,3,2,3,3,0,6,105,35,8
6,3,2,0,3,1,6,105,35,15
7,23,2,1,23,0,3,151,1236,30
8,23,2,1,8278,0,2,50,368,91
9,23,2,1,8278,0,2,50,368,0
```
How to create a topic and send messages to it you can read in the same article above.
After that you can run the following class to test the application behaviour:
```
src/test/scala/com/app/StreamUtilsTest.scala
```
Please note that application uses embedded Elastic for testing, so don't worry about installing it locally.

##### Enjoy!
package com.mcd.spark.kafka

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import scala.util.Random
object SparkKafkaProducer {
  
       //C:\Program Files\DataStax-DDC\apache-cassandra\bin>cassandra.bat
      //C:\Program Files\DataStax-DDC\apache-cassandra\bin>cqlsh.bat
      //C:\kafka\bin\windows> zookeeper-server-start.bat C:\kafka\config\zookeeper.properties//
      //C:\kafka\bin\windows>kafka-server-start.bat C:\kafka\config\server.properties
      // No need of prodcer and consumer as you are doing from scala applications. You can see output in eclipse console only for consumer.
  

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark-Kafka-Producer").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val Array(zkQuorum, topic) = args //localhost:9092 SparkKafkaProducer
    println("In above statement args is array of aguments so we are passing broker list and topic name as localhost:9092 SparkKafkaProducer")
    val props: Properties = new Properties()
    // props.put("metadata.broker.list", "10.220.11.171:9092")
    props.put("metadata.broker.list", zkQuorum)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    var events = 0;
    var totalEvents = 10;
    // for loop execution with a range
    for (index <- 1 to totalEvents) {
      val salary = Random.nextInt(500000);
      val empId = Random.nextInt(1000);
      val empName = "Revanth-" + empId
      val msg = empId + "|" + empName + "|" + salary;
      println("Topic "+topic +": is sending message "+msg)
      producer.send(new KeyedMessage[String, String](topic, msg))
    }
  }
}
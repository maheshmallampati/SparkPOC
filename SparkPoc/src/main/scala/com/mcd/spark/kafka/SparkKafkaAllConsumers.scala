package com.mcd.spark.kafka

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._
import com.mcd.sparksql.util._
import org.apache.spark.sql._
import com.mcd.sparksql.datahub._
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.StringReader
import org.apache.spark._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVReader
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import com.mcd.json.parsing.Person
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import java.util.Properties
import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream

import com.datastax.spark.connector.cql.CassandraConnector
import scala.reflect.runtime.universe
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import kafka.serializer.StringDecoder
import org.apache.spark.sql.cassandra.CassandraSQLContext

object SparkKafkaAllConsumer
 {
  def main(args: Array[String]): Unit = {
      //%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    if (args.length < 2) { //localhost:9092 SparkKafkaProducer
      //C:\Program Files\DataStax-DDC\apache-cassandra\bin>cassandra.bat
      //C:\Program Files\DataStax-DDC\apache-cassandra\bin>cqlsh.bat
      //C:\kafka\bin\windows> zookeeper-server-start.bat C:\kafka\config\zookeeper.properties//
      //C:\kafka\bin\windows>kafka-server-start.bat C:\kafka\config\server.properties
      // No need of prodcer and consumer as you are doing from scala applications. You can see output in eclipse console only for consumer.
      println("Please specify <Broker> <TopicName> like localhost:9092 SparkKafkaProducer")
      System.exit(-1)
    }
     //%%%%%%%%%%%% STREAMING CONTEXTS%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    try {
    val mapProps = DaasUtil.getConfig("Daas.properties")
    val master=DaasUtil.getValue(mapProps, "Master")
    val driverMemory=DaasUtil.getValue(mapProps, "Driver.Memory")
    val executorMemory=DaasUtil.getValue(mapProps, "Executor.Memory")
    val cassandraHost=DaasUtil.getValue(mapProps, "Cassandra.Host")
    val jobName="JDBCSparkPropsFile"
    val conf = DaasUtil.getJobConfForCassandra(jobName, master, executorMemory, driverMemory,cassandraHost);
              
      //val conf = DaasUtil.getJobConfForCassandra("CassandraSparkJobs", "local[2]", "1g", "1g", "127.0.0.1");
      val sparkContext = new SparkContext(conf)
      val sqlContext = new HiveContext(sparkContext)
      val ssc = new StreamingContext(sparkContext, Seconds(5)) 

      val logger = LoggerFactory.getLogger("JSONFileReaderWriter")
     // val cassandraHost="127.0.0.1";
      val hdfsOrLocalURI="Input"

      //%%%%%%%%%%%%% KAFKA BROKERS AND TOPICS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
      val Array(brokerList, topics) = args //localhost:9092 SparkKafkaProducer
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
      
      //%%%%%%%%%%%%%% KAFKA DIRECT DSTREAM FOR CONSUMERS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
      println("*** ---> --> --> Producer should always be up and running and if you want to send messages to consumer run the KafkaProducer program once again*****")
      
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)
      
      //%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
      
      
      printKafkaConsumerMessagesToConsole(messages);
      saveKafkaConsumerMessagesToCassandra(messages.map(line => line.split('|')), cassandraHost)
      saveKafkaConsumerMessagesToHdfs(messages,hdfsOrLocalURI)
      ssc.start()
      ssc.awaitTermination()
        
    } catch {
      case ex: Exception => {
        println(ex.printStackTrace())
      }
    }
  }


  
  def printKafkaConsumerMessagesToConsole(messages:DStream[String]) {
    
      messages.foreachRDD(x => {
        if (!x.isEmpty()) {
          x.foreach { x => println(x) }
          println("--------------------------------------------------------")
          println(x.first())
        }else{
          println("Data is not received from the producer")
        }
      })
}

 
  
   def saveKafkaConsumerMessagesToCassandra(messages:DStream[Array[String]],cassandraHost:String) {
     
     val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
      //Creating Session object
      val session = cluster.connect()
      session.execute("CREATE KEYSPACE IF NOT EXISTS spark_kafka_cassandra WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
      val query = "CREATE TABLE IF NOT EXISTS spark_kafka_cassandra.employee (id int PRIMARY KEY,name VARCHAR, salary int);"
      //Executing the query
      session.execute(query)
      
     messages.foreachRDD(
        rdd => {
          if (!rdd.isEmpty()) {
            println(rdd.first())
            println("rdd count  " + rdd.count())
            val resRDD = rdd.map(line => (line(0), line(1), line(2)))
              .saveToCassandra("spark_kafka_cassandra", "employee", SomeColumns("id", "name", "salary"))
          } else {
            println("Data is not yet recevied from the producer....")
          }
        })
        
        println("Run query and see if data is present..select * from spark_kafka_cassandra.employee;")
}
   
   def saveKafkaConsumerMessagesToHdfs(messages:DStream[String],hdfsOrLocalURI:String) {
    
     messages.foreachRDD(
        rdd => {
          if (!rdd.isEmpty()) {
            println(rdd.first())
            println("rdd count  " + rdd.count())
            println("Hadoop URI or Local URI= " + hdfsOrLocalURI)
            val hdfsPath = hdfsOrLocalURI + "/user/data/" 
            println("HDFS Path = " + hdfsPath)
            rdd.saveAsTextFile(hdfsPath)
          } else {
            println("Data is not yet recevied from the producer....")
          }
        })
        println("Check if data is present in folder Input/user/data")
}
   
  
}// end of object
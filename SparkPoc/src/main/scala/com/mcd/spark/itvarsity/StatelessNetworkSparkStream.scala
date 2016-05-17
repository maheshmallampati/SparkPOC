package com.mcd.spark.itvarsity
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
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
//http://spark.apache.org/docs/latest/streaming-programming-guide.html#migration-guide-from-091-or-below-to-1x
object StatelessNetworkSparkStream {
  def main(args: Array[String]): Unit = {

    if (args.length < 0) {
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    val logger = LoggerFactory.getLogger("NetworkSparkStream")
    
    val conf = DaasUtil.getJobConf("SparkNetworkStream", "local[2]", "1g", "1g"); // ***** 2 cores should be at least given here for spark streaming to prevent from a starvation scenario.
    val sparkContext = new SparkContext(conf)
    val sqlContext = new HiveContext(sparkContext)
    
    
    val sparkStreamContext=new StreamingContext(conf, Seconds(1))
    //*******Discretized Stream or DStream is the basic abstraction provided by Spark Streaming. It represents a continuous stream of data
    println("Above will create streamig context for every 1 seconds whichi is micro batch size in spark")
    
    
    println("Example from ---> http://spark.apache.org/docs/latest/streaming-programming-guide.html#migration-guide-from-091-or-below-to-1x")
    
    // Create a DStream that will connect to hostname:port, like localhost:9999
    println("Network spark streaming will use ssc.socketTextTream to pull records from network on that socket which is port")
    val lines = sparkStreamContext.socketTextStream("localhost", 9999,StorageLevel.MEMORY_AND_DISK_SER_2) //** IP address of cloudera VM
    //nc -lk 9999 in Cloudera virtual box.
    
    lines.flatMap (x => x.split(" ") ).map ( x => (x,1)).reduceByKey(_+_).print()
    sparkStreamContext.start()
    sparkStreamContext.awaitTermination()

    
    
    
}
}
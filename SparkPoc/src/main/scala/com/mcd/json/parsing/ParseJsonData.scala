package com.mcd.json.parsing
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
//import com.mcd.sparksql.datahub.CalDt

import org.slf4j.LoggerFactory

/*
 * 
 * 
 * http://wpcertification.blogspot.com/2015/12/i-wanted-to-build-spark-program-that.html
 * Limitations: Each line should be Json
 * 
 * 
 * 
 * 
 * 
 * */

class Person extends java.io.Serializable{ // Asmath Fixed it as I was getting serializable exception so added extends clause
  @JsonProperty var first: String = null
  @JsonProperty var last: String = null
  @JsonProperty var address: Address = null

  override def toString = s"Person(first=$first, last=$last, address=$address)"
}

class Address extends java.io.Serializable {// Asmath Fixed it as I was getting serializable exception so added extends clause
  @JsonProperty var line1: String = null
  @JsonProperty var line2: String = null
  @JsonProperty var city: String = null
  @JsonProperty var state: String = null
  @JsonProperty var zip: String = null

  override def toString = s"Address(line1=$line1, line2=$line2, city=$city, state=$state, zip=$zip)"
}

object ParseJsonData {
   def main(args: Array[String]): Unit = {
     
   
    
    if(args.length < 2){
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
     val inputFile = args(0)  // Input/Json
    val output= args(1)  // Output/Json
    
    println(inputFile)
    val conf = DaasUtil.getJobConf("JsonFileReadWrite", "local[2]", "1g", "1g");
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    
    val logger = LoggerFactory.getLogger("JSONFileReaderWriter")
    val mapper = new ObjectMapper 
    

    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
    
    try { hdfs.delete(new org.apache.hadoop.fs.Path(output), true) } catch { case _ : Throwable => { } }

    logger.debug(s"Read json from $inputFile and write to $output")


    val errorRecords = sparkContext.accumulator(0)
    val records = sparkContext.textFile(inputFile)
    println(records.take(10).foreach { println })
    
    var resultsRDD = records.flatMap { record =>
      try {
        Some(mapper.readValue(record, classOf[Person]))
      } catch {
        case e: Exception => {
          errorRecords += 1
          None
        }
      }
    }
    println(resultsRDD.take(10).foreach { println })

    var results = records.flatMap { record =>
      try {
        Some(mapper.readValue(record, classOf[Person]))
      } catch {
        case e: Exception => {
          errorRecords += 1
          None
        }
      }
    }.filter(person => person.address.city.equals("mumbai"))
    
    results.saveAsTextFile(output)
    
    println("Number of bad records " + errorRecords)

  }
   
  
}

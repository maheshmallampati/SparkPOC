package com.mcd.sparksql.datahub

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import scala.collection.JavaConversions._
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import com.mcd.sparksql.util.DaasUtil
import org.apache.spark.sql.SaveMode

object Spark_CSVToParaquet {
   def main(args: Array[String]): Unit = {

 
    if (args.length < 0) {
      println("Please specify >Input Path> <outputPath>")
      System.exit(-1)
    }
    val inputPath=args(0) //src/test/resources/sales.csv
    val outputPath=args(1) //output/paraquet
    val mapProps = DaasUtil.getConfig("Daas.properties")
    val master=DaasUtil.getValue(mapProps, "Master")
    val driverMemory=DaasUtil.getValue(mapProps, "Driver.Memory")
    val executorMemory=DaasUtil.getValue(mapProps, "Executor.Memory")
    val jobName="PartitionExample"
    val conf = DaasUtil.getJobConf(jobName, master, executorMemory, driverMemory);
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> inputPath, "header" -> "false"))
    df.printSchema();
    df.save("org.apache.spark.sql.parquet",SaveMode.ErrorIfExists, Map("path" -> outputPath))
   }
}
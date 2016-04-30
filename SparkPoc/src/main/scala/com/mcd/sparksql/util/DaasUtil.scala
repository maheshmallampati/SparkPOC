package com.mcd.sparksql.util
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType };
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._

object DaasUtil {
  def getJobConf(jobName: String, masters: String, execMemory: String, driverMemory: String) : SparkConf= {
    new SparkConf().setAppName(jobName).setMaster(masters).set("spark.executor.memory", execMemory).set("spark.driver.memory", driverMemory);
  }

  def getJobConf(jobName: String, masters: String, execMemory: String): SparkConf ={
    new SparkConf().setAppName(jobName).setMaster(masters).set("spark.executor.memory", execMemory)
  }

}
package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.spark.SparkContext
import com.mcd.sparksql.util.DaasUtil

/**
 * Created by madhu on 11/3/15.
 */
object MapPartitionExample02{

  def main(args: Array[String]) {

   
    if (args.length < 0) {
      println("Please specify Input Path")
      System.exit(-1)
    }
    val mapProps = DaasUtil.getConfig("Daas.properties")
    val master=DaasUtil.getValue(mapProps, "Master")
    val driverMemory=DaasUtil.getValue(mapProps, "Driver.Memory")
    val executorMemory=DaasUtil.getValue(mapProps, "Executor.Memory")
    val jobName="PartitionExample"
    val conf = DaasUtil.getJobConf(jobName, master, executorMemory, driverMemory);
    val sc = new SparkContext(conf)
    val salesData = sc.textFile(args(0)) // src/main/resources/sales.csv
    
    
    /******************************************************************************************
     Accessing Partitions Slides:
     
     You can access whole partition at a time instead of accessing it row by row.This will return iterator. foldleft is like accumulator
     where we can use that is your business case like reducer logic
     mapPartitions is used to pick whole partition instead of row/row. 
     Ex: Min and Max in one partition and then I find out min and max partitons of whole file.
    
     $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$*/
    
    
    val (min,max)=salesData.mapPartitions(iterator => {
      val (min,max) = iterator.foldLeft((Double.MaxValue,Double.MinValue))((acc,salesRecord) => {
        val itemValue = salesRecord.split(",")(3).toDouble
        (acc._1 min itemValue , acc._2 max itemValue)
      })
      List((min,max)).iterator
    }).reduce((a,b)=> (a._1 min b._1 , a._2 max b._2))
    println("min = "+min + " max ="+max)

  }
  

}

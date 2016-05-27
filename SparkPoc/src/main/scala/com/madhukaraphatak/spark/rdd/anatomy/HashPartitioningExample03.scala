package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.mcd.sparksql.util.DaasUtil

/**
 * Created by madhu on 11/3/15.
 */
object HashPartitioningExample03 {

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

    val salesData = sc.textFile(args(0)) //// src/main/resources/sales.csv

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1),colValues(3).toDouble)
    })
    
    
    // 
    println("Default number of partitons coming frm the developer")
    val groupedData = salesByCustomer.groupByKey()

    println("default partitions are "+groupedData.partitions.length)


    //increase partitions
    // with custom partitoner, keys are distributed evenly across partiioners but not with hash partitoner
    println("Increasing the partitons as it is done programatically for rdd to rdd transformations.see next example to get more details")
    val groupedDataWithMultiplePartition = salesByCustomer.groupByKey(2)
    println("increased partitions are " + groupedDataWithMultiplePartition.partitions.length)

  
  }
}

package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.mcd.sparksql.util.DaasUtil

/**
 * Created by madhu on 11/3/15.
 */
object LookUpExample {

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
    
    // instead of filter we use lookup o make faster

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1), colValues(3).toDouble)
    })


    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner)

    val groupedDataWithPartitionData = groupedData.mapPartitionsWithIndex((partitionNo, iterator) => {
      println(" accessed partition " + partitionNo)
      iterator
    }, true)


    println("for accessing customer id 1")
    groupedDataWithPartitionData.lookup("1")


  }


}

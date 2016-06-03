package com.madhukaraphatak.spark.rdd.anatomy
 import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{SparkContext, SparkEnv}
import com.mcd.sparksql.util.DaasUtil

/**
 * Created by madhu on 24/3/15.
 */
object CacheExample08{

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
    val salesData = sc.textFile(args(0),1)

    val salesByCustomer = salesData.map(value => {
      println("computed")
      val colValues = value.split(",")
      (colValues(1), colValues(3).toDouble)
    })

    println("salesData rdd id is " + salesData.id)
    println("salesBy customer id is " + salesByCustomer.id)



    val firstPartition = salesData.partitions.head

    salesData.cache()

    println(" the persisted RDD's " + sc.getPersistentRDDs)

    //check whether its in cache
    val blockManager = SparkEnv.get.blockManager
    val key = RDDBlockId(salesData.id, firstPartition.index)

    println("before evaluation " +blockManager.get(key))

    // after execution

    salesData.count()


    println("after evaluation " +blockManager.get(key))






















  }


}

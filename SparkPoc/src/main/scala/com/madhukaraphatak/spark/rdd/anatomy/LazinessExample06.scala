package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.spark.SparkContext
import com.mcd.sparksql.util.DaasUtil

/**
 * Created by madhu on 27/3/15.
 */
object LazinessExample06 {

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

    val dataRDD = sc.textFile(args(0))
    /*
     ********************************************************************************
    
    Hadoopconf and set the block size will increase it or if you have hadoop configuration files in class path then also it 
    picks and changes block size.
    
    actions will internally call Runjob api which will make sure to run the job. that were computation starts
    
    lineage is saved in disk through persistance so if anything goes wrong it's recomputed
    to avoid recomputation, use cache or persist
    
    action when calls save(), will load output of data into memeory and saves into disk if needed or just put it in memory
    
    persist will check for storage level like memory only, memmory and disk ect.
    
    //********************************************************************************
     * */
     */

    val flatMapRDD = dataRDD.flatMap(value => value.split(""))

    println("type of flatMapRDD is "+ flatMapRDD.getClass.getSimpleName +" and parent is " + flatMapRDD.dependencies.head.rdd.getClass.getSimpleName)

    val hadoopRDD = dataRDD.dependencies.head.rdd

    println("type of  Data RDD is " +dataRDD.getClass.getSimpleName+"  and the parent is "+ hadoopRDD.getClass.getSimpleName)

    println("parent of hadoopRDD is " + hadoopRDD.dependencies)


  }

}

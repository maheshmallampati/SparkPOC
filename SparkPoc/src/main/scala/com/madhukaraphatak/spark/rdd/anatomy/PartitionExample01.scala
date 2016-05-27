package com.madhukaraphatak.spark.rdd.anatomy

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import com.mcd.sparksql.util.DaasUtil

/**
 * Created by madhu on 11/3/15.
 */
object PartitionExample01{

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
    val inputPath="Input/person.txt"
    
    
    /* $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
    Number of partitons = number of executors  ---> which means threads...and that are in JVM process under workder nodes.
     actual textFile api converts to the following code.
     when you call sc.textfile, it internally calls sc.hadoopFile with defulat number of partitions.
     Since we called sc.textfile, format is picked up as TextInputFormat with Input key and Input value as LongWritable and Text.     
     It takes default number of partitions but you can give your own partitions. Example less than 64MB file also as 3 partitions.
     
     Final output is value of line and it wont send byteoffset(.map(pair => pair._2.toString)
     
     Conclusion: sc.textFile("Asmath.txt") == sc.hadoopFile(args(1), classOf[TextInputFormat], classOf[LongWritable], classOf[Text], sc.defaultMinPartitions).map(pair => pair._2.toString)
    
    local means only one thread in executor. In case of local there is only one executor.(Asmath--> Executor means machines and workers means process inside machines)
    local[2] means 2 threads in single executor. Spark context will assign 2 threads to the executors even in single node machine.
    local[*] will give threads based on cluster but it is bounded by cores.
    Maximun number of partition is handled by spark(may be 3 or 4) but minimum can be done from driver.
    
    Driver is responsible for partitions based on the file size and from that it will know the offset of the partitions.
    Actual work will be done by workers on the partitons like which partition should be processed by which worker. all these is handled by spark internally.
    
    RDD's are immutable because partitions are immutable and on are based on hadoop file system. underneath data cannot be changed.
    
    content of file is read at lazy evaulation. In this example only size is read.
    
    RDD's are partitioned and distributed as they internally use hadoop api file system to read which is again partitioned and distributed.
    
     ******************************************************************************************
     Accessing Partitions Slides:
     
     You can access whole partiton at a time instead of accessing it row by row.  where we can use that is your business case like reducer logic
     mapPartitions is used to pick whole partition instead of row/row. 
     Ex: Min and Max in one partition and then I find out min and max partitons of whole file.
    
     $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$*/
    val dataRDD = sc.hadoopFile(inputPath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      sc.defaultMinPartitions).map(pair => pair._2.toString)

    println("local[*] will give 2 and local will give only 1 as output ---->"+dataRDD.partitions.length)

  }


}
package com.mcd.hadoop.inputformats

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}
import com.mcd.sparksql.util.DaasUtil
import org.apache.spark.sql.SQLContext

/**
  * Created by KhajaAsmath on 05/05/16.
  */
object FileReader {

  def main(args:Array[String]): Unit ={
    if(args.length < 1){
      println("Please specify <Input Path> <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    val inputPath=args(0);
    
    val conf = DaasUtil.getJobConf("FileHadoopReader", "local[*]", "1g", "1g");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val directoryPath = args(0)
    println(s"Reading data from $directoryPath")
   
    // Set custom delimiter for text input format
    //sc.hadoopConfiguration.set("textinputformat.record.delimiter","\t")
    val sentences = sc.textFile(directoryPath)
    println("Number of lines " + sentences.count())
    sentences.take(10).foreach(println)
  }

}
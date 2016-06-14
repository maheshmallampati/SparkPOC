package itvarsity.class8

import java.io._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils



class ReadSaveFiles08 {
  
}
object ReadSaveFiles08
{
  
  def main(args: Array[String]): Unit = {
    
    var inputFileLocation = "Input/departments";
    var outputLocation = "OutputFile";
    var appName="ReadSaveFiles08"
    var master="local"

    if (args.length >= 4) {
      println("Inside arguments list")
      inputFileLocation = args(0);
      outputLocation = args(1);
      appName=args(2)
      master=args(3)
    }
    FileUtils.deleteDirectory(new File(outputLocation));

    val sparkConf = new SparkConf().setAppName(appName).setMaster(master);
    val sc = new SparkContext(sparkConf);

    //val lineRdd = sc.textFile("Food.txt")// .count() for number of records and try new variable with cache also

    val lineRdd = sc.textFile(inputFileLocation)//cache will make RDD not to recompile every time we use RDD in next steps.

    lineRdd.collect().foreach(println)
    // Output is printed on Console.
    
       
    //ClouderaVM --> spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local sparkexamples_2.10-1.0.jar
    
    //Windows -> spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     /* spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     */
  
  }
}
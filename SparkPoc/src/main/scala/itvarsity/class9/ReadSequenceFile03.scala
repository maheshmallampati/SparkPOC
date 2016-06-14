package itvarsity.class9
import java.io._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io._

class ReadSequenceFile {

}
//*** For reading sequence files, we need to give Type of class that we are reading for key and value. You will get error if you don’t give that type.
object ReadSequenceFile {
  def main(args: Array[String]): Unit = {
    var inputFileLocation = "/user/cloudera/departmentsseqout"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var outputLocation = "/user/cloudera/scalaspark/departmentsSeqWithKey"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/scalaspark/departmentsTesting"
    var appName = "SequenceFileOutputFormatWithKey"
    var master = "local"

    if (args.length >= 4) {
      println("Inside arguments list")
      inputFileLocation = args(0);
      outputLocation = args(1);
      appName = args(2)
      master = args(3)
    }

    val sparkConf = new SparkConf().setAppName(appName).setMaster(master);
    val sc = new SparkContext(sparkConf);

    // val lineRdd = sc.textFile("Input/departments") //cache will make RDD not to recompile every time we use RDD in next steps.
    
    
    //*** For reading sequence files, we need to give Type of class that we are reading for key and value. You will get error if you don’t give that type.
       
    //sc.sequenceFile("OutputOfSequenceWrite", classOf[IntWritable], classOf[Text]).map(rec => rec.toString()).collect().foreach(println)
    //inputFileLocation="OutputOfSequenceWrite";
    sc.sequenceFile(inputFileLocation, classOf[IntWritable], classOf[Text]).map(rec => rec.toString()).collect().foreach(println)
   
    //Output on console.
       
    

    /*    
    
    //ClouderaVM --> spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local sparkexamples_2.10-1.0.jar
    
    //Windows -> spark-submit --class itvarsity.class8.WriteObjectFile --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     * spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     */
  }
}
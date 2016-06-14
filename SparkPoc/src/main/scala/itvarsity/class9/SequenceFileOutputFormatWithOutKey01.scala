package itvarsity.class9
import java.io._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.NullWritable

class SequenceFileOutputFormatWithoutKey {

}

object SequenceFileOutputFormatWithoutKey {
  def main(args: Array[String]): Unit = {
    var inputFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
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

    //val lineRdd = sc.textFile("Food.txt")// .count() for number of records and try new variable with cache also
    //val lineRdd = sc.textFile("Input/departments") //cache will make RDD not to recompile every time we use RDD in next steps.
    val lineRdd = sc.textFile(inputFileLocation) //cache will make RDD not to recompile every time we use RDD in next steps.

    //Save as sequence file is not present on RDD but present on other RDD. Lets google out.
    //Sequence file key and value so we need to have key and value before saving it as sequence file.
    
   // (lineRdd.map { x => (NullWritable.get(),x)}).saveAsSequenceFile("OutputOfSequenceWrite")  // --> Here Key is Null and vlaue is value
      (lineRdd.map { x => (NullWritable.get(),x)}).saveAsSequenceFile(outputLocation)  // --> Here Key is Null and vlaue is value
    
    // Here key is 1st value of x and value is 2nd value of x.
    
    
    

    /*    
    
    //ClouderaVM --> spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local sparkexamples_2.10-1.0.jar
    
    //Windows -> spark-submit --class itvarsity.class8.WriteObjectFile --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     * spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     */
  }
}
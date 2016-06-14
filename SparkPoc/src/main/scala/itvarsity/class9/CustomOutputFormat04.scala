package itvarsity.class9
import java.io._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io._

import org.apache.hadoop.mapreduce.lib.output._
class CustomOutputFormat {

}

object CustomOutputFormat {

  def main(args: Array[String]): Unit = {

    /*    
•	You can save the file in your own output format(i.e your own key and own value ) by extending the output package of Hadoop and implementing the call to saveAsNewAPIHadoopFile
•	You have to import the import org.apache.hadoop.mapreduce.lib.output._ package also
•	This can be done by using and calling saveAsNewAPIHadoopFile . 

    Here we are using Text as key and Text as value. Custom outputformat is saved as SequenceFile  in this example. 
    You can use any Hadoop format here only we are changing he keys and values based on the custom format.
*/

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

   /* inputFileLocation = "Input/departments"
    outputLocation = "Output";*/
    FileUtils.deleteDirectory(new File(outputLocation));
    val lineRdd = sc.textFile(inputFileLocation)

    lineRdd.map(x => (new Text(x.split(",")(0)), new Text(x.split(",")(1) + x.split(",")(0)))).saveAsNewAPIHadoopFile(outputLocation, classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text, Text]])
  FileUtils.deleteDirectory(new File(outputLocation));
   lineRdd.map(x => (new Text(x.split(",")(0)), new Text(x.split(",")(1) + x.split(",")(0)))).saveAsNewAPIHadoopFile(outputLocation, classOf[LongWritable], classOf[Text], classOf[TextOutputFormat[LongWritable, Text]])
     FileUtils.deleteDirectory(new File(outputLocation));
   lineRdd.map(x => (new Text(x.split(",")(0)), new Text(x.split(",")(1) + x.split(",")(0)))).saveAsNewAPIHadoopFile(outputLocation, classOf[Text], classOf[Text], classOf[TextOutputFormat[Text, Text]])

  }

}
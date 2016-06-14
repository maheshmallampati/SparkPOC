package edureka.streaming
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import StreamingContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, Writable, Text}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level,Logger}


object WordCountStreaming {
  
  def main(args: Array[String]): Unit = {
    var inputFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var outputLocation = "/user/cloudera/scalaspark/departmentsSeqWithKey"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/scalaspark/departmentsTesting"
    var appName = "SparkStreaming"
    var master = "local"
    
    var portNumber=7777
    var host="localhost";

    if (args.length >= 4) {
      println("Inside arguments list")
      inputFileLocation = args(0);
      outputLocation = args(1);
      appName = args(2)
      master = args(3)
    }
    
    inputFileLocation = "Input/departments"
    outputLocation = "Output";
    println("Issue while running programs in windows --> http://stackoverflow.com/questions/31632824/connection-refused-error-while-running-spark-streaming-on-local-machine")
    FileUtils.deleteDirectory(new File(outputLocation));
    println("master -->"+master)
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master);
    
    /* -------------------------------------------------------------------------------------------------
    
    if nc is not available install it using command , sudo m install nc . nc -lk 9999 to lock that terminal and port
    
    Set of data coming in form of batches in spark streaming is called as Dstreams. There are nothing but sequence of RDD's
    
    You can set the batch size of 15 seconds which creating spark conf. In case of rolling window, it should be multiple of batch size
    
    
    //add logger levels by mutgus
     * 
     * if proceess more than batch size, lock out issues..
    
    
    
    -------------------------------------------------------------------------------------------------*/
    
    
    
    //********************************************** IMP Information Here **********************************************
    //always check API http://spark.apache.org/docs/latest/api/scala/index.html#package  2. Socket use nc -lk 9999 3. Go through slides of eudreka, see persistence mechanism also here
    // Scala API also http://www.scala-lang.org/api/current/index.html#package
    
    println("Install NC in centos using the command sudo yum install nc")
    println("sbin/start-masters.sh and sbin/start-slaves.sh not slave.sh")
    println("spark-submit --class edureka.streaming.WordCountStreaming --master spark://localhost.localdomain:7077 /home/edurekadaasspark.jar")
    
    println("-------------- SETTING LOGGER TO ERROR-------------------------------------")
    val rootLogger=Logger.getRootLogger;
    rootLogger.setLevel(Level.ERROR)
    println("---------------------------------------------------")
    
    val ssc = new StreamingContext(sparkConf, Seconds(15))

    val lineRdd = ssc.socketTextStream(host, portNumber,StorageLevel.MEMORY_AND_DISK_SER); // You can get these details in Programming guide by searching as Context. See Muthu Video

    

    //Flat map converts the lines into List of words instead of list of listofwords.
    val wordsRdd = lineRdd.flatMap(line => line.split(" "));
    val wordCount =wordsRdd.map { x => (x,1) }.reduceByKey(_+_)
    wordCount.print();
    
    //Start the socket and wait for its termination.
    
    ssc.start();
    ssc.awaitTermination();
    
    

    
    
    

    /*    
    
    //EdurekaVM -->  spark-submit --class edureka.streaming.WordCountStreaming --master spark://localhost.localdomain:7077 /home/edureka/daasspark.jar /user/edureka /user/edureka/out streaming spark://localhost.localdomain:7077
     * 
    
    //Windows -> spark-submit --class itvarsity.class8.WriteObjectFile --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     * spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     */
  }
}
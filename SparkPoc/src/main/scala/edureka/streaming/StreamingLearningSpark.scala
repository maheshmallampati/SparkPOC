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
import org.apache.spark.streaming.dstream._

object StreamingLearningSpark {

  
   def main(args: Array[String]) {
    val master = args(0)
    val conf = new SparkConf().setMaster(master).setAppName("StreamingLogInput")
    // Create a StreamingContext with a 1 second batch size
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream from all the input on port 7777
    val lines = ssc.socketTextStream("localhost", 7777)
    val errorLines = processLines(lines)
    // Print out the lines with errors, which causes this DStream to be evaluated
    errorLines.print()
    // start our streaming context and wait for it to "finish"
    ssc.start()
    // Wait for 10 seconds then exit. To run forever call without a timeout
    ssc.awaitTermination(10000)
    ssc.stop()
  }
   
   // calling methods in scala.
   //def processLines(lines: DStream[String]): DStream[String] = {
    def processLines(lines: DStream[String]) = {
    // Filter our DStream for lines with "error"
    lines.filter(_.contains("error"))
  
}
    
     //EdurekaVM -->  spark-submit --class edureka.streaming.StreamingLearningSpark --master spark://localhost.localdomain:7077 /home/edureka/daasspark.jar /user/edureka /user/edureka/out streaming spark://localhost.localdomain:7077
    
}
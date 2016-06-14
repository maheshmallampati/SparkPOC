package itvarsity.class12
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;

class JoinDataSets01 {
  
}

//You get Mapped RDD and Joined RDD in this example
object WordCountScala{
  def main(args: Array[String]): Unit = {
    var inputOrderFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var inputOrderItemsFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var outputLocation = "/user/cloudera/scalaspark/departmentsSeqWithKey"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/scalaspark/departmentsTesting"
    var appName = "SequenceFileOutputFormatWithKey"
    var master = "local"

    if (args.length >= 4) {
      println("Inside arguments list")
      inputOrderFileLocation = args(0);
      inputOrderItemsFileLocation =args(1);
      outputLocation = args(2);
      appName = args(3)
      master = args(4)
    }
    
    inputOrderFileLocation = "Input/orders"
    inputOrderItemsFileLocation = "Input/order_items"
    outputLocation = "Output";
    
    FileUtils.deleteDirectory(new File(outputLocation));
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master);
    val sc = new SparkContext(sparkConf);


    
    
    //**************************************************************************
    val ordersRdd = sc.textFile(inputOrderFileLocation)
    val ordersItemsRdd=sc.textFile(inputOrderItemsFileLocation)
    

    val ordersParsedRdd=ordersRdd.map { record => (record.split(",")(0),record) } //*** You get Mapped RDD Here
    val ordersItemsParsedRdd=ordersItemsRdd.map { record => (record.split(",")(1),record) } // You get Mapped RDD here
    
    println("Checking if above two are mappedRdd -->"+ordersItemsParsedRdd)
    
    
    val joinOrderAndOrderItemsRdd=ordersParsedRdd.join(ordersItemsParsedRdd); // Here we get joinedRDD by equiJoin on Key of tuple i.e orderid in this case
    
    println("Checking if above is joinedRdd -->"+joinOrderAndOrderItemsRdd)
    
    //joinOrderAndOrderItemsRdd.foreach(println)
    
    
    
    //(45007,(45007,2014-04-29 00:00:00.0,5748,COMPLETE,112383,45007,1004,1,399.98,399.98))  // Orderid is key and rest is columns of table1+table2.
    // 1st tuple is 45007 and 2nd tuple is (45007,2014-04-29 00:00:00.0,5748,COMPLETE,112383,45007,1004,1,399.98,399.98) and inside second tuple we have two more 
    
    
    // Accessing values after joining the key   OrderId+ Orderid + OrderItemSubTotal 
    joinOrderAndOrderItemsRdd.map(record => ("First Tuple OrderId="+record._1 +" Second Tuple(First Tuple of Second Tuple from Join) OrderId-->"+record._2._1.charAt(0)+" Second Tuple(Second Tuple of  Join) OrderId-->"+record._2._2.charAt(4))).foreach { println }
    
    
      
   
    
    

    /*    
    
    //ClouderaVM --> spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local sparkexamples_2.10-1.0.jar
    
    //Windows -> spark-submit --class itvarsity.class8.WriteObjectFile --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     * spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     */
  }
}
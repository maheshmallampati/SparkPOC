package itvarsity.class15
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;

class AggregateByKey {
  
}

object AggregateByKey
{
  
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


    
    
    //***********************************************************************************************************
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
   // joinOrderAndOrderItemsRdd.map(record => ("First Tuple OrderId="+record._1 +" Second Tuple(First Tuple of Second Tuple from Join) OrderId-->"+record._2._1.charAt(0)+" Second Tuple(Second Tuple of  Join) OrderId-->"+record._2._2.charAt(4))).foreach { println }
    
    
    // *********************************************Program Starts Here ******************************************
    
    println("**** Joining Data Sets here *****************")
    joinOrderAndOrderItemsRdd.take(5).foreach(println)
    //(25128,(25128,2013-12-27 00:00:00.0,9212,CLOSED,62962,25128,191,2,199.98,99.99))
    //rec._1 = 25128
    //rec._2 = (25128,2013-12-27 00:00:00.0,9212,CLOSED,62962,25128,191,2,199.98,99.99)
    //rec._2._1=25128,2013-12-27 00:00:00.0,9212,CLOSED
    //rec._2._2=62962,25128,191,2,199.98,99.99
    
    
     //(34731,(34731,2014-02-24 00:00:00.0,2598,COMPLETE,86745,34731,627,2,79.98,39.99))
 //(34731,(34731,2014-02-24 00:00:00.0,2598,COMPLETE,86746,34731,1014,1,49.98,49.98))
    
    println("**** Create tuple with ((date+orderid),(rev)) *****************")
    val dateOrderRevMap=joinOrderAndOrderItemsRdd.map(rec => ((rec._2._1.split(",")(1),rec._2._2.split(",")(0)),(rec._2._2.split(",")(4))))
        
    dateOrderRevMap.take(5).foreach(println)  
    //((2013-12-27 00:00:00.0,62962),199.98)
    //((2013-12-27 00:00:00.0,62962),299.98)
    
    println("****** Orders Tuple is still having line items but not the entire basket value so we have to add totals to get reveneue per order")
    


    val dateOrderRevTotalsMap=dateOrderRevMap.reduceByKey((x,y)=>(x+y)); // Input type and output type is float so no need of aggretate yet. use aggrgrate when input type is diff to output types
    
     dateOrderRevTotalsMap.take(5).foreach(println)
    //((2014-05-17 00:00:00.0,119757),100.0)  you got total for one order here
    
      
    println("********************* Removing OrderId from tuple now to get Revenues/Day **************")
    
    val orderRevTotalsPerDayMap=dateOrderRevTotalsMap.map(rec => (rec._1._1, rec._2.toDouble))
    
    orderRevTotalsPerDayMap.sortByKey().foreach(println);
           
      /*(2014-07-24 00:00:00.0,39.99)
      (2014-07-24 00:00:00.0,199.99)
      (2014-07-24 00:00:00.0,399.98)*/
     
     
    println("********************* We need to get output as (date,(total,orderscount)) so reducebykey doesnt fit our need**************")
    println("AggByKey takes 3 Parms. First: Initial Vaues for output, Second --> Combiner Logic , Third --> Reducer logic ")
    println("First param= Initial value for output value -->(total,orderscount) --> 0.0,0 since total is float(0.0) and count is number(0)")
    val revenuePerDay = orderRevTotalsPerDayMap.aggregateByKey((0.0, 0))((acc, revenue) => (acc._1 + revenue, acc._2 + 1),
      (total1, total2) => (total1._1 + total2._1, total1._2 + total2._2) 
)

       revenuePerDay.collect().foreach(println)
    

    /*    
    
    //ClouderaVM --> spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local sparkexamples_2.10-1.0.jar
    
    //Windows -> spark-submit --class itvarsity.class8.WriteObjectFile --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     * spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     */
  }
}
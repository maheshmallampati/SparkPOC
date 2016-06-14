package itvarsity.class16
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;
class MaxByValueRegularFunct{

}

object MaxByValue1 {
  def main(args: Array[String]): Unit = {
    var inputOrderFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var inputOrderItemsFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var outputLocation = "/user/cloudera/scalaspark/departmentsSeqWithKey"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/scalaspark/departmentsTesting"
    var appName = "SequenceFileOutputFormatWithKey"
    var master = "local"

    if (args.length >= 4) {
      println("Inside arguments list")
      inputOrderFileLocation = args(0);
      inputOrderItemsFileLocation = args(1);
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
    val ordersItemsRdd = sc.textFile(inputOrderItemsFileLocation)

    val ordersParsedRdd = ordersRdd.map { record => (record.split(",")(0), record) } //*** You get Mapped RDD Here
    val ordersItemsParsedRdd = ordersItemsRdd.map { record => (record.split(",")(1), record) } // You get Mapped RDD here

    println("Checking if above two are mappedRdd -->" + ordersItemsParsedRdd)

    val joinOrderAndOrderItemsRdd = ordersParsedRdd.join(ordersItemsParsedRdd); // Here we get joinedRDD by equiJoin on Key of tuple i.e orderid in this case

    println("Checking if above is joinedRdd -->" + joinOrderAndOrderItemsRdd)

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

    println("**** Create tuple with ((date+customerid),(rev)) *****************")
    println("rec._2._1 --> is second tuple, first element --> in this case it is 34731,2014-02-24 00:00:00.0,2598 so we are using split")
    val dateCustRevMap = joinOrderAndOrderItemsRdd.map(rec => ((rec._2._1.split(",")(1), rec._2._1.split(",")(2)), rec._2._2.split(",")(4).toFloat))
    // val ordersPerDayPerCustomer = ordersJoinOrderItems.map(rec => ((rec._2._2.split(",")(1), rec._2._2.split(",")(2)), rec._2._1.split(",")(4).toFloat))

    dateCustRevMap.take(5).foreach(println)
    //((2013-12-27 00:00:00.0,62962),199.98)
    //((2013-12-27 00:00:00.0,62962),299.98)

    println("****** We have to move customerid to value tuple using map now")

    println("rec._1._1 --> is First tuple, first element --> in this case it is 2013-12-27 00:00:00.0,62962 so rec._1._1=2013-12-27 00:00:00.0 and rec._1._2=62962")

    val rddwithCustRevValue = dateCustRevMap.map(rec => (rec._1._1, (rec._1._2, rec._2)))
    rddwithCustRevValue.take(5).foreach(println)

    // Asmath 0106 223.46
    //Asmath 0106  344.56
    //Sammu 0106   223.18  --> In this case Asmath has 2 two trsnactions but I have to get output as Asmath withg 344.56 as top transaction
    // We are intersted only to find out max transaction done by customer in a day (customer can have multiple transcactions) 

    /*    (2013-12-27 00:00:00.0,(9212,199.98))
          (2013-12-27 00:00:00.0,(9212,299.98))
          (2013-12-27 00:00:00.0,(9212,159.96))*/

    println("(2013-12-27 00:00:00.0,(9212,299.98)) --> (9212,199.98) is value which is again a tuple of two values. acc._1=0(This is for first value in tuple i.e. 9212) and acc._2 is 0(This is for second value in tuple i.e. 199.98)")
    println("Value is tuple here (9212,299.98) --> FirstElement in Tuple value._1=9212  SecondElement in Tuple value._2=299.98")
    println("acc._1=0, acc._2=0, value._1=9212 and value._2=299.98")
   
    
    //val topCustomerPerDaybyRevenue = rddwithCustRevValue.reduceByKey((acc, value) => (if (acc._2 >= value._2) acc else value)) // Printing tuple value here to know the customerid assocaited with it.
    
    def findMax(x: (String, Float), y: (String, Float)): (String, Float) = {
      if (x._2 >= y._2)
        return x
      else
        return y
    }
    
    
    val topCustomerPerDaybyRevenue = rddwithCustRevValue.reduceByKey((x, y) => findMax(x, y)) // Function should be written before accessing. Weird in scala .. Please try it out asmath.

    topCustomerPerDaybyRevenue.take(5).foreach(println)
    
    
    
    //
    // (9212,299.98) --> we are passing tuple here so x,y should have datatype of tuple. Return Type here is Tuple so we have : (String, Float) here
    
    /*    
    
    //ClouderaVM --> spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local sparkexamples_2.10-1.0.jar
    
    //Windows -> spark-submit --class itvarsity.class8.WriteObjectFile --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     * spark-submit --class itvarsity.class8.ReadHdfsFileAndSaveHdfs01 --master local C:\Users\mc41946\git\MySparkExamples\sparkcode\target\scala-2.10\sparkexamples_2.10-1.0.jar
     */
  }

}
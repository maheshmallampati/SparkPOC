package itvarsity.class13
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;
class TotalByOrderAndMaxAmount {
  
}

object TotalByOrderAndMaxAmount{
def main(args: Array[String]): Unit = {
    var inputOrderFileLocation = "/user/cloudera/sqoop_import/orders"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var inputOrderItemsFileLocation = "/user/cloudera/sqoop_import/order_items"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
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
    
    val ordersRdd = sc.textFile(inputOrderFileLocation).cache()
    val ordersItemsRdd=sc.textFile(inputOrderItemsFileLocation).cache()
    
    println("Orders -->"+ordersRdd.first());
    println("Order Items -->"+ordersItemsRdd.first());
    
    
    
    //**************************************************************************
    println("-------------------To get total based on order, use reducebykey similar to workcount--------------------------")
    
    ordersItemsRdd.map { record => (record.split(",")(1).toInt,record.split(",")(4).toDouble)}.take(5).foreach(println);
    
    val ordersItemsParsedRdd=ordersItemsRdd.map { record => (record.split(",")(1).toInt,record.split(",")(4).toDouble) }
    
    
    val totalByOrderKey=ordersItemsParsedRdd.reduceByKey((acc,value)=> acc+value)
    
    //Reduceby key also returns tuple.
    
   println("-------printing values just to see how the data looks like by limiting to 5------")
   
   totalByOrderKey.take(5).foreach(println)
   
   
     //**************************************************************************
   
   println("............................Finding maximum transaction of orders.....................")
 
/*(18624,199.99)
(20484,439.95000000000005)
(62544,929.9200000000001)
(20904,649.86)
(57720,579.9200000000001)*/
   
 /*  In the above case it is tuple , value of first accumlator is 0 by default and value of first value is 199.99 in this case
   so you are comparing, 2nd element of tuple with accumulator value i.e. acc._2 with val._2
in above case acc._1, tuple always starts with 1 and accessed by _. Acc_1=0 and val_1=18624  acc_2=199.9 and val_2=929. */

   
   val maxOrderByKey=totalByOrderKey.reduce((acc,value)=> ( if (acc._2 > value._2) acc else value));
   
    
    
    //----------------------- Asmath one more approach to get max value directly if there is no key based(max value of single line item ). In this case it is wrong because we have multiple orderId's 
    
    val totalAmountRdds=ordersItemsRdd.map { record => (record.split(",")(4).toDouble) }
    val maxTxnAmountOfAllOrders=totalAmountRdds.max();
    println("Max value of orders"+maxTxnAmountOfAllOrders)
    // If it is more than one value then it is tuple. In above case it is never tuple
   
     
   
}
}
package itvarsity.class13
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;

class CountTotalAvg {
  
}
object CountTotalAvg{
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
    println("-------------------Count (distinct count is obtained by using transformation distinct and total sum by reduce if required)--------------------------")
    ordersItemsRdd.map { record => record.split(",")(1).toDouble}.take(5).foreach(println);
    val ordersItemsParsedRdd=ordersItemsRdd.map { record => record.split(",")(1).toInt }
   
    val orderCount=ordersItemsParsedRdd.distinct().count();
    
    println("Asmath ---> I was getting error while average because I didnt use count() and used only distinct() which is RDD from transformation not the action")
    println("OrdersCount -->"+orderCount)
    
     //**************************************************************************
    
    println("-------------------Total (Is obtained by reduce action )--------------------------")
    //val ordersAllItemsParsedRdd=ordersItemsRdd.map { record => (record.split(",")(0),record.split(",")(1),record.split(",")(2),record.split(",")(3),record.split(",")(4)) }
    ordersItemsRdd.map { record => record.split(",")(4).toDouble}.take(5).foreach(println);
    val totalSumOfValues=ordersItemsRdd.map { record => record.split(",")(4).toDouble}.reduce((acc,value) => (acc+value)); // See Value changed to Double here
    printf("%f",totalSumOfValues) // printf will format the data.
    
     //**************************************************************************
    
    println("-------------------Average (Calculating average on the orders. Since the tables having multiple orderId's (Sub Items inside order), we got total of sub items and distinct orders to calculate avg)--------------------------")
    
    val avg=totalSumOfValues/orderCount;
     printf("%f",avg) // printf will format the data.
}
}
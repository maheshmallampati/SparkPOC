package itvarsity.class17
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;
import org.apache.spark.sql._;
import org.apache.spark.sql.hive.HiveContext
class FilterDatasets {

}

object FilterDatasets {
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

    // Filter data into a smaller dataset using Spark
    val ordersRDD = sc.textFile(inputOrderFileLocation)
    ordersRDD.filter(line => line.split(",")(3).equals("COMPLETE")).take(5).foreach(println)
    ordersRDD.filter(line => line.split(",")(3).contains("PENDING")).take(5).foreach(println)
    ordersRDD.filter(line => line.split(",")(0).toInt > 100).take(5).foreach(println)
    ordersRDD.filter(line => line.split(",")(0).toInt > 100 || line.split(",")(3).contains("PENDING")).take(5).foreach(println)
    ordersRDD.filter(line => line.split(",")(0).toInt > 1000 &&
      (line.split(",")(3).contains("PENDING") || line.split(",")(3).equals("CANCELLED"))).
      take(5).
      foreach(println)
    ordersRDD.filter(line => line.split(",")(0).toInt > 1000 &&
      !line.split(",")(3).equals("COMPLETE")).
      take(5).
      foreach(println)

    /*#Check if there are any cancelled orders with amount greater than 1000$
#Get only cancelled orders
#Join orders and order items
#Generate sum(order_item_subtotal) per order
#Filter data which amount to greater than 1000$*/

    //val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
    val orderItemsRDD = sc.textFile(inputOrderItemsFileLocation)

    val ordersParsedRDD = ordersRDD.filter(rec => rec.split(",")(3).contains("CANCELED")).
      map(rec => (rec.split(",")(0).toInt, rec))
      
      ordersParsedRDD.take(5).foreach(println)
      
    val orderItemsParsedRDD = orderItemsRDD.
      map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
      
      orderItemsParsedRDD.take(5).foreach(println)
      
    val orderItemsAgg = orderItemsParsedRDD.reduceByKey((acc, value) => (acc + value))

    orderItemsAgg.take(5).foreach(println)
    
    val ordersJoinOrderItems = orderItemsAgg.join(ordersParsedRDD)

    ordersJoinOrderItems.filter(rec => rec._2._1 >= 1000).take(5).foreach(println)

    /*#Using SQL
import org.apache.spark.sql.hive.HiveContext*/
    val sqlContext = new HiveContext(sc)

   // sqlContext.sql("select * from (select o.order_id, sum(oi.order_item_subtotal) as order_item_revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id where o.order_status = 'CANCELED' group by o.order_id) q where order_item_revenue >= 1000").count()

  }
}
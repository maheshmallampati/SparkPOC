package itvarsity.class18
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;
import org.apache.spark.sql._;
import org.apache.spark.sql.hive.HiveContext

class SortBykeyRdd {
}

object SortBykeyRdd {
  def main(args: Array[String]): Unit = {

    var inputDepartmentFileLocation = "/user/cloudera/sqoop_import/products"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var inputOrderItemsFileLocation = "/user/cloudera/sqoop_import/products"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var outputLocation = "/user/cloudera/scalaspark/departmentsSeqWithKey"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/scalaspark/departmentsTesting"
    var appName = "SequenceFileOutputFormatWithKey"
    var master = "local"

    if (args.length >= 4) {
      println("Inside arguments list")
      inputDepartmentFileLocation = args(0);
      inputOrderItemsFileLocation = args(1);
      outputLocation = args(2);
      appName = args(3)
      master = args(4)
    }

    inputDepartmentFileLocation = "Input/products"
    inputOrderItemsFileLocation = "Input/order_items"
    outputLocation = "Output";

    FileUtils.deleteDirectory(new File(outputLocation));
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master);
    val sc = new SparkContext(sparkConf);

    // Filter data into a smaller dataset using Spark
    val productsRDD = sc.textFile(inputDepartmentFileLocation)

    // $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

    println("************************* GLOBAL SORTING ******************************* SORT BY KEY ***************************")

    println(" --> Data is sorted in assecnding order by default with sort by key---> used collect to see all values here")

    productsRDD.map { rec => (rec.split(",")(4), rec) }.sortByKey().take(5).foreach(println); // 5th i.e. 4 subscript is productid
    //(999.99,695,32,Callaway Women's Solaire Gems 20-Piece Comple,,999.99,http://images.acmesports.sports/Callaway+Women%27s+Solaire+Gems+20-Piece+Complete+Set+-...)

    println("Type casted to appropriate data type to get sorting. Ealier case it was string so it is sorted based on strings .. Data is wrong so not type casting here at (4).toFloat")
    productsRDD.map { rec => (rec.split(",")(4), rec) }.sortByKey(false).collect().foreach(println);

    println("-----------SORTING DATA IN DESCENDING ORDER ----------- BY SETTING FLAG AS FALSE -----------------------------------")

    productsRDD.map { rec => (rec.split(",")(4), rec) }.sortByKey(false).collect().foreach(println);

    // $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$   

    println("*************************  RANKING ******************************* by TOP action ***************************")

    println("For ranking you have to sort first by key and then apply the top function ---> ")

    productsRDD.map { rec => (rec.split(",")(4), rec) }.top(5).foreach(println);

    // $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ 

    println("----------------------  TAKEOrdered is equal to  sortbykey+ take functions together  and also useful while doing secondary sort also-------------------------------------")

    productsRDD.map(rec => (rec.split(",")(0).toInt, rec)).
      takeOrdered(5).
      foreach(println)

    println("Ordering is scala class to perform ordering")

    println("Printing in ascending order as I didnt use revere on --->")
    productsRDD.takeOrdered(5)(Ordering[Int].on(x => x.split(",")(0).toInt)).foreach(println)
    /*1,2,Quest Q64 10 FT. x 10 FT.
      2,2,Under Armour Men's*/

    println("Printing in descending order as I used revere on --->")
    productsRDD.takeOrdered(5)(Ordering[Int].reverse.on(x => x.split(",")(0).toInt)).foreach(println)

    /*1345,59,Nike Men's
      1344,59,Nike Men's*/

  }
}

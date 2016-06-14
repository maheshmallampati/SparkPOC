package itvarsity.class20
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;
import org.apache.spark.sql._;
class GroupByRanking {

}

object GroupByRanking {



    def main(args: Array[String]): Unit = {

      var inputDepartmentFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
      var inputOrderItemsFileLocation = "/user/cloudera/sqoop_import/departments"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
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

      println("************************* Group by --> For sorting and ranking as we are not using aggregartion here ***************************")

      val products = sc.textFile(inputDepartmentFileLocation)
      val productsMap = products.map(rec => (rec.split(",")(1), rec))     
      productsMap.take(5).foreach(println)
      
      val productsGroupBy = productsMap.groupByKey() 
      productsGroupBy.collect().foreach(println)
       
      
      println("ProductCategoryId is 1 and ProductCategories is 5 ------------------>")
      println("Group By Key returns key with Iterable of values ")
      println("Sorting the data based on the key and product categorties like secondary sort")
      println("Changing the Iterable to flat map and getting the data that should be sorted out")
      //(29,CompactBuffer(621,29,Under Armour Men's Performance Chino Straight,,79.99)
      
/*      #Get data sorted by product price per category
#You can use map or flatMap, if you want to see one record per line you need to use flatMap
#Map will return the list*/

  productsGroupBy.map(rec => (rec._2.toList.sortBy(k => k.split(",")(4).toFloat))).
  take(100).
  foreach(println)

println("- before key return the records in descending order")
  productsGroupBy.map(rec => (rec._2.toList.sortBy(k => -k.split(",")(4).toFloat))).
  take(100).
  foreach(println)


  productsGroupBy.flatMap(rec => (rec._2.toList.sortBy(k => -k.split(",")(4).toFloat))).
  take(100).
  foreach(println)

    }

  

}
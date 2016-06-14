package itvarsity.class14
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import java.io._;
class GroupByCountByReduceByCombineBy {

}
object GroupByCountByReduceByCombineBy {
  def main(args: Array[String]): Unit = {
    var inputOrderFileLocation = "/user/cloudera/sqoop_import/orders"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
    var inputOrderItemsFileLocation = "/user/cloudera/sqoop_import/order_items"; //or "hdfs://quickstart.cloudera:8022/user/cloudera/sqoop_import/departments"
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

    /**
     * ** Interview questions: Group by doesn’t not use combiner internally in spark
     * Group by Key creates tuple of String and Iterable. It returns key with list of values(Iterable)
     * With combiner, aggregations happens at map side but here it is not so that is disadvantage.
     * You can also use reduceBYkey..
     * groupbyKey  are used for sorting and other things .
     *
     * Best example is workdcount, if lakh or keys are there then you have to pass all those values when combiner is not present (1 Lakh keys + their values)
     * In case of combiner only (1 Lakh keys + their aggregated count) to the reducer.
     *
     * •	CombineByKey will pay (Keys+ someofthecount)  that came in mapper (like part files) finally the result is same.
     * •	Use reducebykey when sum, min,max as the value is same for groubbykey and combine by key. Use only when combine and reduce logic is same. Like wordcount in reducer and combiner.
     * Reduce by key can be used only for aggregations but group by key can be used for other purposes too.
     */

    //**************************************************************************

    val ordersRdd = sc.textFile(inputOrderFileLocation).cache()
    // val ordersItemsRdd=sc.textFile(inputOrderItemsFileLocation).cache()

    println("Orders -->" + ordersRdd.first());
    // println("Order Items -->"+ordersItemsRdd.first());

    //**************************************************************************
    println("-------------------CountByKey similar to wordcount --------------------------")

    ordersRdd.map { record => (record.split(",")(3), 0) }.take(5).foreach(println);

    val ordersParsedRdd = ordersRdd.map { record => (record.split(",")(3), 1) }.cache();
    ordersParsedRdd.take(5).foreach(println)
    ordersParsedRdd.countByKey().take(5).foreach(println)
    //Reduceby key also returns tuple.
    println("-------printing values just to see how the data looks like by limiting to 5------")

    //**************************************************************************
    //#groupByKey is not very efficient for aggregations. It does not use combiner
    println("-------------------GroupByKey. It returns String and Iterable of values, String is key and iterator is list of values--------------------------")

    ordersParsedRdd.groupByKey().take(5).foreach(println)
    println("VVVIMP ----> Asmath --> Group by is printing tuples of string and Iterable of values.see above")
    //Reduceby key also returns tuple.
    println("GroubBy is returning the Tuple of Int and Iterable here so we have to  on keys")
    println("Asmath ---> we are getting key from group of tuple. Second one is list from tuple and list has function called as sum")
    ordersParsedRdd.groupByKey().map(rec => (rec._1, rec._2.sum)).collect().foreach(println);

    //**************************************************************************
    
    
    
     //**************************************************************************
    //#reduceByKey uses combiner - both reducer logic and combiner logic are same
   // #Both reduceByKey and combineByKey expects type of input data and output data are same
    println("-------------------ReduceByKey. It returns Tuples--------------------------")
    ordersParsedRdd.take(5).foreach(println)
    ordersParsedRdd.reduceByKey((x,y) => (x+y))
    println("VVVIMP ----> Asmath --> Reduceby is printing tuples of string and Iterable of values.see above")
    //Reduceby key also returns tuple.
    
    ordersParsedRdd.reduceByKey((x,y) => (x+y)).collect().foreach(println);

    //*************************************     Combine By Key  *****************************************************************
    
    
     println("-------------------CombineByKey aslo returns Tuples. It takes 3 args createCombiner, mergeValue, mergeCombiners:  --------------------------")
      println("-------------------createCombiner=Initial Value, mergeValue= Partfiles output, mergeCombiners: Sum of MergeValues  --------------------------")
    
     ordersParsedRdd.combineByKey(value =>1, (acc:Int,value:Int) => (acc+value), (acc:Int,value:Int) => (acc+value)).take(5).foreach(println);
    
    
     //*************************************     Aggregatee By Key  *****************************************************************
     println("It takes two parms. first parm is a vlaue. Second param again takes two parms i.e one for map phase and other for reduce phase")
     println("During Map Phase it takes key and value so accumlator")
     println("During red phase it sums up all values so it is value at that point of time. Compare with WC in Haoop")
    val ordersMap = ordersRdd.map(rec =>  (rec.split(",")(3), rec))
    val ordersByStatus =  ordersMap.aggregateByKey(0)((acc, value) => acc+1, (acc, value) => acc+value) // see two parms here and no comma
    ordersByStatus.collect().foreach(println)

    
  }
}
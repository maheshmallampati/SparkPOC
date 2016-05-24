package com.mcd.sparksql.datahub
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType };
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._
import com.mcd.sparksql.util._
import org.apache.spark.sql._
import com.mcd.sparksql.datahub._
import org.slf4j.LoggerFactory
//import com.mcd.sparksql.datahub.CalDt
object Spark_Json_Reader {
  def main(args: Array[String]) {
    val conf = DaasUtil.getJobConf("Generate Datahub With Spark SQL Dataframe", "local[2]", "1g", "1g");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val path = "Input/sales.json"
    println(path)
    val salesDF = sqlContext.read.json(path)
    salesDF.registerTempTable("sales")
    val aggDF = sqlContext.sql("select sum(amountPaid) from sales")
    println(aggDF.collectAsList())
    
    val results = sqlContext.sql("SELECT customerId,itemName FROM sales ORDER BY itemName")
    // display dataframe in a tabular format
    results.show()
  }
}
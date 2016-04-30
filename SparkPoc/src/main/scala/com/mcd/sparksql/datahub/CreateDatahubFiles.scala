package com.mcd.sparksql.datahub
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType };
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.mcd.sparksql.datahub._
import com.mcd.sparksql.util._
import org.apache.spark.sql._

object CreateDatahubFiles {
  def main(args: Array[String]): Unit = {

    val conf = DaasUtil.getJobConf("Generate Datahub With Spark SQL Dataframe", "local[2]", "1g", "1g");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //Defining Input Paths : Should be parameterized
    val inputPath = "datahub/data,datahub/data2"

    //Getting Schema of Datahub through Struct Type instead of Case Classes
    val customDatahubSchema = DataHubUtil.getSchemaforDatahub();

    //Creating DataFrame with header and delimiter
    val datahubDataFrame = sqlContext.load(
      "com.databricks.spark.csv",
      schema = customDatahubSchema,
      Map("path" -> inputPath, "header" -> "true", "delimiter" -> "\t"))
      
    //Regsitering DF as temp table
    datahubDataFrame.registerTempTable("tld_datahub")

    val results = sqlContext.sql("SELECT  posbusndt,pos_itm_lvl_nu,last_updt_ts FROM tld_datahub")
    // results.save("newcars.csv", "com.databricks.spark.csv")
    results.map(t => "Posbusndt: " + t(0)).collect().foreach(println)

  }
}
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

object CreateDatahubFiles {

  def main(args: Array[String]): Unit = {

    val conf = DaasUtil.getJobConf("Generate Datahub With Spark SQL Dataframe", "local[2]", "1g", "1g");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // ***************************** You can use below code directly ********************************************************************************************************************

    /*//Defining Input Paths : Should be parameterized
    val datahubInputPath = "datahub/data,datahub/data2"

    //Getting Schema of Datahub through Struct Type instead of Case Classes
    val customDatahubSchema = DataHubUtil.getSchemaforDatahub();

    //Creating DataFrame with header and delimiter
    val datahubDataFrame = sqlContext.load(
      "com.databricks.spark.csv",
      schema = customDatahubSchema,
      Map("path" -> datahubInputPath, "header" -> "true", "delimiter" -> "\t")).cache()

    //Regsitering DF as temp table
    datahubDataFrame.registerTempTable("tld_datahub")

    val results = sqlContext.sql("SELECT  posbusndt,pos_itm_lvl_nu,last_updt_ts FROM tld_datahub")
    // results.save("newcars.csv", "com.databricks.spark.csv")
    results.map(t => "Posbusndt: " + t(0)).collect().foreach(println)

    // this is used to implicitly convert an RDD to a DataFrame. (import sqlContext.implicits._)
    import sqlContext.implicits._

    //Defining Input Paths : Should be parameterized
    val calDtInputPath = "datahub/caldt"
    val calDt = sc.textFile("datahub/caldt/CAL_DT.txt").map(_.split("\t")).map(p => CalDt(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9))).toDF()

    val calDtDF = calDt.toDF()
    calDtDF.registerTempTable("CAL_DT")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val calDate = sqlContext.sql("SELECT cal_dt from CAL_DT")*/

    // ***************************** You can use above code directly ********************************************************************************************************************

    
    val datahubInputPath = "datahub/data,datahub/data2"
    val datahubTableName="tld_datahub";
    val datahubDataFrame = getDataHubDF(sc, sqlContext, datahubInputPath, datahubTableName)
    datahubDataFrame.registerTempTable(datahubTableName)
    val results = sqlContext.sql("SELECT  posbusndt,pos_itm_lvl_nu,last_updt_ts FROM tld_datahub")
    // results.save("newcars.csv", "com.databricks.spark.csv")
    results.map(t => "Posbusndt: " + t(0)).collect().foreach(println)
    
    
    val calDtInputPath = "datahub/caldt"
    val calDTableName="CAL_DT";
    val calDtDataFrame = getCalDtDF(sc, sqlContext, calDtInputPath, calDTableName)
    calDtDataFrame.registerTempTable(calDTableName)
    val calDtResults = sqlContext.sql("SELECT yr_nu from CAL_DT")
    // results.save("newcars.csv", "com.databricks.spark.csv")
    calDtResults.map(t => "yr_nu: " + t(0)).collect().foreach(println)
    

  }

  def getDataHubDF(sc: SparkContext, sqlContext: SQLContext, datahubInputPath: String, tableName: String): DataFrame =
    {
      //Getting Schema of Datahub through Struct Type instead of Case Classes
      val customDatahubSchema = DataHubUtil.getSchemaforDatahub();

      //Creating DataFrame with header and delimiter
      val datahubDataFrame = sqlContext.load(
        "com.databricks.spark.csv",
        schema = customDatahubSchema,
        Map("path" -> datahubInputPath, "header" -> "true", "delimiter" -> "\t")).cache()
      return datahubDataFrame;
    }

  def getCalDtDF(sc: SparkContext, sqlContext: SQLContext, datahubInputPath: String, tableName: String): DataFrame =
    {
      // this is used to implicitly convert an RDD to a DataFrame. (import sqlContext.implicits._). This is mandatory import when case statement is implemented
      import sqlContext.implicits._

      //Defining Input Paths : Should be parameterized
      val calDtInputPath = "datahub/caldt"
      val calDt = sc.textFile("datahub/caldt/CAL_DT.txt").map(_.split("\t")).map(p => CalDt(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9))).toDF().cache()

      return calDt.toDF()

    }
}
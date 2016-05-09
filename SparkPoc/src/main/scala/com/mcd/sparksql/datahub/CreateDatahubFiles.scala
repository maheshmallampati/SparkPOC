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

object CreateDatahubFiles {

  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    
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
    //results.map(t => "Posbusndt: " + t(0)).collect().foreach(println)
    
    
    val calDtInputPath = "datahub/caldt"
    val calDTableName="cal_dt";
    val calDtDataFrame = getCalDtDF(sc, sqlContext, calDtInputPath, calDTableName)
    calDtDataFrame.registerTempTable(calDTableName)
    val calDtResults = sqlContext.sql("SELECT yr_nu from cal_dt")
    // results.save("newcars.csv", "com.databricks.spark.csv")
    //calDtResults.map(t => "yr_nu: " + t(0)).collect().foreach(println)
    
    val dyptInputPath = "datahub/dypt"
    val dyptTableName="dypt";
    val dyptDataFrame = getDyptDF(sc, sqlContext, dyptInputPath, dyptTableName)
    dyptDataFrame.registerTempTable(dyptTableName)
    val dyptResults = sqlContext.sql("SELECT * FROM dypt")
    // results.save("newcars.csv", "com.databricks.spark.csv")
    //dyptResults.map(t => "dypt_id_nu: " + t(0)).collect().foreach(println)
    dyptResults.take(5).foreach { println }
    
    
    val offrsIncListInputPath = "datahub/offrsIncludeList"
    val offrsIncListTableName="offers_include_list";
    val offrsIncListDataFrame = getOffrsIncludeListDF(sc, sqlContext, offrsIncListInputPath, offrsIncListTableName)
    offrsIncListDataFrame.registerTempTable(offrsIncListTableName)
    val offrsIncListResults = sqlContext.sql("SELECT  pos_rest_id FROM offers_include_list")
    // results.save("newcars.csv", "com.databricks.spark.csv")
    //offrsIncListResults.map(t => "pos_rest_id: " + t(0)).collect().foreach(println)
    
    //var datamartQuery="select hub.TERR_CD, hub.POS_ORD_KEY_ID, hub.MCD_GBAL_LCAT_ID_NU, hub.POS_BUSN_DT, hub.DYPT_ID_NU, hub.LGCY_LCL_RFR_DEF_CD, hub.POS_TRN_STRT_TS, hub.POS_AREA_TYP_SHRT_DS, hub.POS_PRD_DLVR_METH_CD, cldt.DY_OF_CAL_WK_DS, dypt.DYPT_DS,S_TOT_NET_TRN_AM,hub.POS_TOT_NET_TRN_AM,0), coalesce(hub.POS_TOT_NET_TRN_AM+hub.POS_TOT_TAX_AM,hub.POS_TOT_NET_TRN_AM+hub.POS_TOT_TAX_AM,0) as POS_TOT_GRSS_TRN_AM, coalesce(hub.POS_TOT_ITM_QT,hub.POS_TOT_ITM_QT,0), coalesce(hub.TOT_ORD_TM_SC_QT,hub.TOT_ORD_TM_SC_QT,0), coalesce(hub.CUST_OFFR_DIGL_OFFR_ID,hub.CUST_OFFR_DIGL_OFFR_ID,0), coalesce(hub.CUST_ID,hub.CUST_ID,hub.CUST_ID,hub.CUST_ID,0), coalesce(hub.CUST_OFFR_ID,hub.CUST_OFFR_ID,0), 840 FROM tld_datahub hub join offers_include_list offr on hub.terr_cd=offr.terr_cd and cast(hub.LGCY_LCL_RFR_DEF_CD as int)=cast(offr.pos_rest_id as int) left outer join  dypt dypt on hub.dypt_id_nu = dypt.dypt_id_nu left outer join cal_dt cldt on cldt.cal_dt = hub.pos_busn_dt WHERE hub.TERR_CD=840 and hub.POS_ITM_LINE_SEQ_NU=1 and hub.POS_EVNT_TYP_CD='TRX_Sale' and hub.ORD_DIGL_OFFR_APPD_FL=1";
    var datamartQuery="select hub.TERR_CD, hub.POS_ORD_KEY_ID, hub.MCD_GBAL_LCAT_ID_NU, hub.POS_BUSN_DT, hub.DYPT_ID_NU, hub.LGCY_LCL_RFR_DEF_CD, hub.POS_TRN_STRT_TS, hub.POS_AREA_TYP_SHRT_DS, hub.POS_PRD_DLVR_METH_CD,840 FROM tld_datahub hub"; 
    //join offers_include_list offr on hub.terr_cd=offr.terr_cd and cast(hub.LGCY_LCL_RFR_DEF_CD as int)=cast(offr.pos_rest_id as int) left outer join  dypt dypt on hub.dypt_id_nu = dypt.dypt_id_nu left outer join cal_dt cldt on cldt.cal_dt = hub.pos_busn_dt WHERE hub.TERR_CD=840 and hub.POS_ITM_LINE_SEQ_NU=1 and hub.POS_EVNT_TYP_CD='TRX_Sale' and hub.ORD_DIGL_OFFR_APPD_FL=1";
    datamartQuery=datamartQuery.toLowerCase();
    val datamart= sqlContext.sql(datamartQuery).cache();
   datamart.take(5).foreach { println }

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

  def getCalDtDF(sc: SparkContext, sqlContext: SQLContext, calDtInputPath: String, tableName: String): DataFrame =
    {
      // this is used to implicitly convert an RDD to a DataFrame. (import sqlContext.implicits._). This is mandatory import when case statement is implemented
      import sqlContext.implicits._

      //Defining Input Paths : Should be parameterized
      val calDtInputPath = "datahub/caldt"
      val calDt= sc.textFile("datahub/caldt/CAL_DT.txt").map(_.split("\t")).map(p => CalDt(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9))).toDF().cache()

      return calDt.toDF()

    }
  
  def getDyptDF(sc: SparkContext, sqlContext: SQLContext, dyptInputPath: String, tableName: String): DataFrame =
    {
      //Getting Schema of Datahub through Struct Type instead of Case Classes
      val customDatahubSchema = DataHubUtil.getSchemaforDyPt();
      //Creating DataFrame with header and delimiter
      val dyptDataFrame = sqlContext.load(
        "com.databricks.spark.csv",
        schema = customDatahubSchema,
        Map("path" -> dyptInputPath, "header" -> "true", "delimiter" -> "\t")).cache()
       return dyptDataFrame;

    }
  
  def getOffrsIncludeListDF(sc: SparkContext, sqlContext: SQLContext, offrsListInputPath: String, tableName: String): DataFrame =
    {
      //Getting Schema of Datahub through Struct Type instead of Case Classes
      val customDatahubSchema = DataHubUtil.getSchemaforOffrsIncList();
      //Creating DataFrame with header and delimiter
      val offrsListDataFrame = sqlContext.load(
        "com.databricks.spark.csv",
        schema = customDatahubSchema,
        Map("path" -> offrsListInputPath, "header" -> "true", "delimiter" -> "\t")).cache()
       return offrsListDataFrame;

    }
  
}
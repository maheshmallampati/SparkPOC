package com.mcd.spark.jdbc

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import scala.collection.JavaConversions._
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import com.mcd.sparksql.util.DaasUtil


object JDBCSparkPropsFile  extends Serializable{

  def main(args: Array[String]): Unit = {

    if (args.length < 0) {
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    //val mapProps = DaasUtil.getConfig("Daas.properties")
    val mapProps = DaasUtil.getConfig("Daas.properties")
    val master=DaasUtil.getValue(mapProps, "Master")
    val driverMemory=DaasUtil.getValue(mapProps, "Driver.Memory")
    val executorMemory=DaasUtil.getValue(mapProps, "Executor.Memory")
    val jobName="PartitionExample"
    val conf = DaasUtil.getJobConf(jobName, master, executorMemory, driverMemory);
    val sparkContext = new SparkContext(conf)
    //val sqlContext = new HiveContext(sparkContext)
    val logger = LoggerFactory.getLogger("JDBCSparkSqlWithPropertiesFile03")

    val url = DaasUtil.getValue(mapProps, "DB.SQLServer.URL")
    val username = DaasUtil.getValue(mapProps, "DB.SQLServer.UserName")
    val password = DaasUtil.getValue(mapProps, "DB.SQLServer.Password")
    val driver=DaasUtil.getValue(mapProps, "DB.SQLServer.Driver")
    val databaseName = DaasUtil.getValue(mapProps, "DB.SQLServer.Database")
    //val url = “jdbc:sqlserver://wcarroll3:1433;database=mydb;user=ReportUser;password=ReportUser”
    //var sqlServerConnectionString=url+";database="+databaseName+";user="+username+";password="+password;
    //println(sqlServerConnectionString)
    getDBConnectionWithJDBCRDD(url,username,password,driver,databaseName,sparkContext)
    

  }

  def getDBConnectionWithJDBCRDD(url:String,username:String,password:String,driver:String,databaseName:String,sparkContext:SparkContext) =
    {
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance
      println("Connecting to Connection")
      val connection=DriverManager.getConnection(url,username,password)
      
      println("Established Connection "+connection)
      
      var query="select CTRY_NA,CTRY_SHRT_NA from "+databaseName+"."+"CTRY where ? <= CTRY_ISO_NU and CTRY_ISO_NU <= ?"
      println("*** Issues with upper and lower bound .. some how worked for this but not sure for others ***")
     // query="select CTRY_NA,CTRY_SHRT_NA from "+databaseName+"."+"CTRY limit ? "
      println(query)
      
      println("not seraializable exceptions are thrown if you declare getconnection outside of the anonymous function")
      
      val myRDD = new JdbcRDD( sparkContext, () => DriverManager.getConnection(url,username,password) ,query,2, 5, 1, r => r.getString("CTRY_NA") + ", " + r.getString("CTRY_SHRT_NA"))  
        println("RDD --> "+myRDD)
        myRDD.take(5).foreach(println)
       //myRDD.saveAsTextFile("C:\\jdbcrddexample")
      
    }

}
package com.mcd.spark.jdbc

import java.sql.DriverManager
import java.sql.Connection
import com.mcd.sparksql.util.DaasUtil
import org.apache.spark.SparkContext
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

object ScalaJdbcConnectSelect {

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    if (args.length < 0) {
      println("Please specify <JobName> <Master> <Exec Memort> <Driver Memory>")
      System.exit(-1)
    }
    //val mapProps = DaasUtil.getConfig("Daas.properties")
    val mapProps = DaasUtil.getConfig("Daas.properties")
    val master = DaasUtil.getValue(mapProps, "Master")
    val driverMemory = DaasUtil.getValue(mapProps, "Driver.Memory")
    val executorMemory = DaasUtil.getValue(mapProps, "Executor.Memory")
    val jobName = "PartitionExample"
    val conf = DaasUtil.getJobConf(jobName, master, executorMemory, driverMemory);
    val sparkContext = new SparkContext(conf)
    //val sqlContext = new HiveContext(sparkContext)
    val logger = LoggerFactory.getLogger("JDBCSparkSqlWithPropertiesFile03")

    val url = DaasUtil.getValue(mapProps, "DB.SQLServer.URL")
    val username = DaasUtil.getValue(mapProps, "DB.SQLServer.UserName")
    val password = DaasUtil.getValue(mapProps, "DB.SQLServer.Password")
    val driver = DaasUtil.getValue(mapProps, "DB.SQLServer.Driver")
    val databaseName = DaasUtil.getValue(mapProps, "DB.SQLServer.Database")

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      var query = "select CTRY_NA,CTRY_SHRT_NA from " + databaseName + "." + "CTRY"
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(query)
      val colCount = resultSet.getMetaData().getColumnCount();
      val builder = StringBuilder.newBuilder

      while (resultSet.next()) {
        for (i <- 0 to colCount - 1) {
          //println("Value of i: " + i);
          if (i > 0) {
            //out.write("\t");
            var value = resultSet.getObject(i + 1);
            if (value == null || resultSet.wasNull()) {
              builder.append("NULL");
            } else {
              builder.append(value.toString());
            }
            if(i!=(colCount-1))
            			  {
            				  builder.append("|");
            			  }
          }
         
        }
        println(builder.toString())
      }
    } catch {
      case e: Throwable => e.printStackTrace
    }
    connection.close()
  }

}
package com.mcd.spark.jdbc

/**
 * Illustrates writing data over JDBC
 */


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.JdbcRDD
import java.sql.{PreparedStatement, DriverManager, ResultSet}
import org.apache.hadoop.mapred.lib.db._
import org.apache.hadoop.mapred.JobConf

object WriteSimpleDB04{
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: [sparkmaster]")
      //exit(1)
    }
    val master = "local[*]";
    val sc = new SparkContext(master, "WriteSimpleJdbc")
    val data = sc.parallelize(List(("cat1", 1)))
    // foreach partition method
    data.foreachPartition{records =>
      records.foreach(record => println("fake db write"))
    }
    // DBOutputFormat approach
    val records = data.map(e => (catRecord(e._1, e._2), null))
    val tableName = "CTRY"
    val fields = Array("CTRY_NA", "CTRY_SHRT_NA")
    val jobConf = new JobConf()
    DBConfiguration.configureDB(jobConf, "com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://usaecducc1ew1.ccgacol62mak.us-east-1.rds.amazonaws.com/ABaCProdAWS.dbo?user=sa&password=?s3iY2mvJmK5y6.H")
    DBOutputFormat.setOutput(jobConf, tableName, fields:_*)
    records.saveAsHadoopDataset(jobConf)
  }
  case class catRecord(name: String, age: Int) extends DBWritable {
    override def write(s: PreparedStatement) {
      s.setString(1, name)
      s.setInt(2, age)
    }
    override def readFields(r: ResultSet) = {
      // blank since only used for writing
    }
  }

}

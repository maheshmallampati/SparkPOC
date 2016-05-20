package com.mcd.spark.cassandra
import org.apache.spark.{ SparkContext, SparkConf }
import com.datastax.spark.connector._
import org.apache.spark._
import java.util.UUID
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql._
import com.mcd.sparksql.util.DaasUtil

object CassandraSQLToRdd01 {

  case class Greetings(user: String, id: String, greet: String, creation_date: String)
  //INSERT INTO example.greetings (user , id , greet , creation_date ) VALUES ('H01474777', now(),'hello','2014-05-21 07:32:16');

  def main(args: Array[String]): Unit = {
      //C:\Program Files\DataStax-DDC\apache-cassandra\bin>cassandra.bat
      //C:\Program Files\DataStax-DDC\apache-cassandra\bin>cqlsh.bat
      //C:\kafka\bin\windows> zookeeper-server-start.bat C:\kafka\config\zookeeper.properties//
      //C:\kafka\bin\windows>kafka-server-start.bat C:\kafka\config\server.properties
      // No need of prodcer and consumer as you are doing from scala applications. You can see output in eclipse console only for consumer.
    val conf = DaasUtil.getJobConfForCassandra("CassandraSparkJobs", "local[2]", "1g", "1g", "127.0.0.1");

    //val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setAppName("CassandraCQL").setMaster("local[2]")  
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val cc = new CassandraSQLContext(sc)

    println("------------------------------ To use cassandra we need to have cassandra sql connector as jar that can be added in maven ------------------------")
    println("Follow link //http://rustyrazorblade.com/2015/01/introduction-to-spark-cassandra/ ")

    println("*** Querying Cassandra Table using Spark Context ---> *************************************")

    queryUsingSCCassandraTable(sc);

    println("*** Querying Cassandra Table using SQL Context ---> ****************************************")

    queryUsingSQLContext(sqlContext);

    println("*** Querying Cassandra Table using Cassandra SQL Context ---> ******************************")

    queryUsingCassandraSQLContext(cc);

    println("*** Querying Cassandra Table using SparkContext and Case Classes---> ************************")
    querySCCassandraTableWithCaseClass(sc);

  }

  def queryUsingCassandraSQLContext(cc: CassandraSQLContext): DataFrame =
    {
      val rdd = cc.sql("select * from example.greetings");
      rdd.collect().foreach(println)
      return rdd;
    }
  def queryUsingSQLContext(sqlContext: SQLContext): DataFrame =
    {
      //val user_table = sc.cassandraTable("tutorial", "user");
      val df = sqlContext
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "greetings", "keyspace" -> "example"))
        .load()
      df.registerTempTable("greetings")
      val results = sqlContext.sql("SELECT * FROM greetings");
      results.collect().foreach(println)
      return df;

    }
  def queryUsingSCCassandraTable(sc: SparkContext) = {
    val table = sc.cassandraTable("example", "greetings") // tutorial is keyspace and greetings is table
    val results = table.select("user", "id"); // user and id are columns
    results.collect().foreach(println)

  }
  def querySCCassandraTableWithCaseClass(sc: SparkContext) = {
    try {
      val rows = sc.cassandraTable[Greetings]("example", "greetings") // tutorial is keyspace and greetings is table
      println("rows:" + rows.first())
      rows.toArray.foreach(println)

      val row = sc.cassandraTable[Greetings]("example", "greetings").select("user", "id", "greet","creation_date")
      if (!row.isEmpty()) {
        println("row:" + row.first())
      } else {
        println("Records does not exist !")
      }

    } catch {
      case e: Exception =>
        println(e)
    }

  }

}
package com.mcd.sparksql.datahub

import scala.io.Source
import com.mcd.sparksql.util.DaasUtil

//This is small utility function in Scala, that takes fully qualified path of the properties file, and converts it into Map and returns. 
//I use it for taking path of the properties file in my standalone Scala program and load it into Map
object ReadPropertiesFile {
  def main(args: Array[String]): Unit = {
    val mapProps=DaasUtil.getConfig("Daas.properties")
    mapProps.get("Name");
    println("value of Key Name:"+DaasUtil.getValue(mapProps, "Name"))// we can use mapProps.get but it returns some datatype.
  }
  
  
  def getConfig(filePath: String)= {
    Source.fromFile(filePath).getLines().filter(line => line.contains("=")).map{ line =>
      println("Values from property file --> "+line)
      val tokens = line.split("=")
      ( tokens(0) -> tokens(1))
    }.toMap
  }
}
//This is small utility function in Scala, that takes fully qualified path of the properties file, and converts it into Map and returns. I use it for taking path of the properties file in my standalone Scala program and load it into Map
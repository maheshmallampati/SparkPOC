package com.spark.sprinklr
import scala.io.Source
import scala.sys.process._
object CurlWithScala {
  def main(args: Array[String]): Unit = {
    //val cmd = Seq("curl", "-L", "-X", "POST", "-H", "'Content-Type: application/json'", "-d " + jsonHash,  args.chronosHost + "/scheduler/" + jobType)
   // val cmd = Seq("find", "/dev/null", "-name", "null") // works
// does not work: val cmd = Seq("find", "/dev/null", "-name null")
    //curl -X POST -H "Authorization: Bearer 1w0HQ5jwhs6NzLmhzySCRROpXk6UDYCi7EdXta9QR5M4YjMzNTljOTkzMWU3Y2MwOTA5MWZiYzMxNzRjNDM0ZQ==" -H "Key: rgvqzwb63r3rmcutnyecn87b" -H "Content-Type: application/json" -d @reqJsonPayloadAccountInsights2 "https://api2.sprinklr.com/sandbox/api/v1/reports/query"
    
    //val cmd = Seq("curl -X POST -H ","/Authorization: Bearer 1w0HQ5jwhs6NzLmhzySCRROpXk6UDYCi7EdXta9QR5M4YjMzNTljOTkzMWU3Y2MwOTA5MWZiYzMxNzRjNDM0ZQ==/"" -H "Key: rgvqzwb63r3rmcutnyecn87b" -H "Content-Type: application/json" -d @reqJsonPayloadAccountInsights2 "https://api2.sprinklr.com/sandbox/api/v1/reports/query"")

    val cmd= Seq("curl", " -X", " POST", " -H", " 'Authorization: Bearer 1w0HQ5jwhs6NzLmhzySCRROpXk6UDYCi7EdXta9QR5M4YjMzNTljOTkzMWU3Y2MwOTA5MWZiYzMxNzRjNDM0ZQ=='",
        " -H", " 'Key: rgvqzwb63r3rmcutnyecn87b'", " -H", " 'Content-Type: application/json'", " -d @C:\\Asmath_DontDelete\\SparkPoc\\src\\main\\scala\\com\\spark\\sprinklr\\reqJsonPayloadAccountInsights2", " https://api2.sprinklr.com/sandbox/api/v1/reports/query")

        println(cmd.foreach { print })

   val result = cmd.!!
   println(result)
        
  }
}
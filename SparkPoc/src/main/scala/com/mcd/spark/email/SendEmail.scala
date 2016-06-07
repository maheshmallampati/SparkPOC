package com.mcd.spark.email
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
import javax.mail._
import javax.mail.internet._
import java.util.Date
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.commons.net.smtp.SMTP

class MailAgent(to: String,
                cc: String,
                bcc: String,
                from: String,
                subject: String,
                content: String,
                smtpHost: String, smtpPort: String) {
  var message: Message = null

  println("Inside class")

  def sendEmail {
    message = createMessage
    message.setFrom(new InternetAddress(from))
    setToCcBccRecipients
    message.setSentDate(new Date())
    message.setSubject(subject)
    message.setText(content)
    sendMessage

  }

  // throws MessagingException
  def sendMessage {
    Transport.send(message)
  }

  def createMessage: Message = {
    val properties = new Properties()
    properties.put("mail.smtp.host", smtpHost)
    properties.put("mail.smtp.port", smtpPort);
    val session = Session.getDefaultInstance(properties, null)
    return new MimeMessage(session)
  }

  // throws AddressException, MessagingException
  def setToCcBccRecipients {
    setMessageRecipients(to, Message.RecipientType.TO)
    if (cc != null) {
      setMessageRecipients(cc, Message.RecipientType.CC)
    }
    if (bcc != null) {
      setMessageRecipients(bcc, Message.RecipientType.BCC)
    }
  }

  // throws AddressException, MessagingException
  def setMessageRecipients(recipient: String, recipientType: Message.RecipientType) {
    // had to do the asInstanceOf[...] call here to make scala happy
    val addressArray = buildInternetAddressArray(recipient).asInstanceOf[Array[Address]]
    if ((addressArray != null) && (addressArray.length > 0)) {
      message.setRecipients(recipientType, addressArray)
    }
  }

  // throws AddressException
  def buildInternetAddressArray(address: String): Array[InternetAddress] = {
    // could test for a null or blank String but I'm letting parse just throw an exception
    return InternetAddress.parse(address)
  }

}

object SendEmail {
  def main(args: Array[String]): Unit = {
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

    val SMTPHost = DaasUtil.getValue(mapProps, "SMTP.Host")
    val SMTPPort = DaasUtil.getValue(mapProps, "SMTP.Port")
    val fromEmailAddress = DaasUtil.getValue(mapProps, "FromEmailAddress")
    val toEmailAddress = DaasUtil.getValue(mapProps, "ToEmailAddress")
    val ccEmailAddress = DaasUtil.getValue(mapProps, "CCEmailAddress")
    val bccEmailAddress = DaasUtil.getValue(mapProps, "BCCEmailAddress")

    val mail = new MailAgent(toEmailAddress, ccEmailAddress, bccEmailAddress, fromEmailAddress, "Hi From Scala", "HI", SMTPHost, SMTPPort);
    mail.sendEmail
  }

}
package com.mcd.spark.email

import javax.mail._
import javax.mail.internet._
import java.util.Date
import java.util.Properties
import scala.collection.JavaConversions._

object SendEmailViaGmail {
  def main(args: Array[String]): Unit = {
    
    try {
    // your scala code here
    val mail = new MailAgentForGmail("mdkhajaasmath@gmail.com", "mdkhajaasmath@gmail.com", "mdkhajaasmath@gmail.com", "mdkhajaasmath@gmail.com", "Hi From Scala", "HI", "smtp.gmail.com");
    mail.sendEmail
    }
    catch {
    case _: Throwable => println("Got some other kind of exception")
  } finally {
    println("Inside finaly block")
    // your scala code here, such as to close a database connection
  }
  }
}
class MailAgentForGmail(to: String,
                        cc: String,
                        bcc: String,
                        from: String,
                        subject: String,
                        content: String,
                        var smtpHost: String) {
  var message: Message = null
  val username = "mdkhajaasmath@gmail.com"
  val password = "siddiqueakthar"
  var session:javax.mail.Session=null;
  
  
  def sendEmail{
  
  try {
    // your scala code here

    println("Inside class1")
    message = createMessage
    message.setFrom(new InternetAddress(from))
    setToCcBccRecipients

    message.setSentDate(new Date())
    message.setSubject(subject)
    message.setText(content)
    println("before sending message class")
    sendMessage(session)
     
    // throws MessagingException
  } catch {
     case e : Exception => println("Got some other kind of exception"+e.printStackTrace())
  } finally {
    // your scala code here, such as to close a database connection
  }
  }
  

  def sendMessage(session: Session) {
    //Transport.send(message)
    
    val t: Transport = session.getTransport("smtps")
try {
  t.connect(smtpHost, username, password)
  t.sendMessage(message, message.getAllRecipients)
} finally {
  t.close()
}
  }

  def createMessage: Message = {
    println("before sending message class")
    val properties = new Properties()
    println("before sending message class")
    smtpHost = if (smtpHost == null) "smtp.gmail.com" else smtpHost;
    println("before sending message class")
    properties.put("mail.smtp.host", smtpHost)
    properties.put("mail.smtp.auth", "true");
    properties.put("mail.smtp.starttls.enable", "true");
    properties.put("mail.smtp.port", "587");
    println("before sending message class")
    //session = Session.getDefaultInstance(properties, null)
    session=Session.getInstance(properties,null);
    return new MimeMessage(session)
  }

  // throws AddressException, MessagingException
  def setToCcBccRecipients {
    println("Inside BCC ")
    setMessageRecipients(to, Message.RecipientType.TO)
    if (cc != null && cc.trim().equalsIgnoreCase("")) {
      setMessageRecipients(cc, Message.RecipientType.CC)
    }
    if (bcc != null && cc.trim().equalsIgnoreCase("")) {
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
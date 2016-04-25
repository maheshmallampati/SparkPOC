package com.mcd.gdw.daas.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.mail.HtmlEmail;
import org.apache.commons.mail.SimpleEmail;
import org.codehaus.jackson.map.module.SimpleModule;



public class SendMail
{

	public String SMTP_HOST 	    = "DALSMTPRelay.mcd.com";
	public String SMTP_PORT 		= "25";
	public String SMTP_AUTH 	    = "false";
	public String SMTP_PROTOCOL     = "smtp";
	  
	public SendMail(){
		
	}
	
	public SendMail(DaaSConfig daasconfig){
		
		System.out.println("host : " +daasconfig.smtpHost());
		if(daasconfig.smtpHost() != null)
			SMTP_HOST = daasconfig.smtpHost();
		if(daasconfig.smtpPort() != null)
			SMTP_PORT = daasconfig.smtpPort();
		
	}
	public static void main(String [] args)
   {    
		String configXMLFile = args[0];
		
		DaaSConfig daasConfig = new DaaSConfig(configXMLFile);
		SendMail sendMail = new SendMail(daasConfig);
		
		
		String fromAddress = args[1];
		String commadelimitedtoAddresses = args[2];
		
		String subject = args[3];
		String body    = args[4];
		
		
		sendMail.SendEmail(fromAddress, commadelimitedtoAddresses, subject, body);
		
     
   }
   
	public void SendEmail(String fromAddress,String commadelimitedtoAddresses,String subject,String body){
		
		String[] toAddresses = commadelimitedtoAddresses.split(",");
		SendEmail(fromAddress, toAddresses, subject, body);
		
	}
   public void SendEmail(String fromAddress,String[] toAddresses,String subject,String body){
	      try{    
		      // Get system properties
		      Properties properties = System.getProperties();
	
		      if(properties == null)
		    	  properties = new Properties();
		      // Setup mail server
		      properties.setProperty("mail.smtp.host", SMTP_HOST);
		      properties.setProperty("mail.smtp.port", SMTP_PORT);
		      properties.setProperty("mail.smtp.auth", SMTP_AUTH);
		      properties.setProperty("mail.transport.protocol",SMTP_PROTOCOL);
		 
		      HashSet<InternetAddress> toAddSet = new HashSet<InternetAddress>();
		      for(String toaddress:toAddresses){
		    	  toAddSet.add(new InternetAddress(toaddress));
		      }
		      
//		      HtmlEmail he = new HtmlEmail();
//		      he.setHostName(SMTP_HOST);
//		      he.setSmtpPort(Integer.parseInt(SMTP_PORT));
//		      he.setFrom(fromAddress);
//		      he.setTo(toAddSet);
//		      
//		      StringBuffer msg = new StringBuffer();
//		      msg.append("<html><body>");
//		      msg.append(body.replaceAll("<","&lt;\n").replaceAll(">","&gt;\n"));
//		      msg.append("</body></html>");
//		      		
//		      he.setHtmlMsg(msg.toString());
//		      he.send();
		      
		      
		      
//		      SimpleEmail se = new SimpleEmail();
//		      se.setHostName(SMTP_HOST);
//		      se.setSmtpPort(Integer.parseInt(SMTP_PORT));
		      
//		      se.setHostName("smtp.gmail.com");
//		      se.setSmtpPort(Integer.parseInt("465"));
//		      se.setAuthentication("", "");
//		      se.setSSL(true);
		      
//		      se.setFrom(fromAddress);
//		      se.setTo(toAddSet);
//		      se.setContent(body, "text/html");
//		      se.send();
////	
////		      // Get the default Session object.
		      Session session = Session.getDefaultInstance(properties);

	
	         // Create a default MimeMessage object.
	         MimeMessage message = new MimeMessage(session);

	         // Set From: header field of the header.
	         message.setFrom(new InternetAddress(fromAddress));

	         // Set To: header field of the header.
//	         String[] toaddresses = commadelimitedtoAddresses.split(",");
	         for(String toaddress:toAddresses){
	        	 message.addRecipient(Message.RecipientType.TO,
	                                  new InternetAddress(toaddress));
	         }

	         // Set Subject: header field
	         message.setSubject(subject);
	         
	         

	         // Now set the actual message
//	         message.setText(body);
	       
	

	         message.setContent(new String(body.toString().getBytes(), "iso-8859-1"), "text/html; charset=\"iso-8859-1\"");
	         
	         message.saveChanges();

	         // Send message
	         Transport.send(message);
	         System.out.println("Sent message successfully....");
	      }catch (MessagingException mex) {
	         mex.printStackTrace();
	      }catch(Exception ex){
	    	  ex.printStackTrace();
	      }
   }
}
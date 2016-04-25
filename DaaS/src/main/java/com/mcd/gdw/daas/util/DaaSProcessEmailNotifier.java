package com.mcd.gdw.daas.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DaaSProcessEmailNotifier {

	public static void main(String[] args){
		try{
			String configXMLFile = args[0];
			
			DaaSConfig daasConfig = new DaaSConfig(configXMLFile);
			SendMail sendMail = new SendMail(daasConfig);
			
			
			String fromAddress = args[1];
			String commadelimitedtoAddresses = args[2];
			
			String subject = args[3];
			String body    = args[4];
			
		
			
			StringBuffer sbf = new StringBuffer(body);
			
			
//			String hdfsOutputPath = args[5];
			
//			Configuration conf = new Configuration();
//
//			
//			FileSystem fileSystem = FileSystem.get(conf);
//			
//			FileStatus[] fstats = fileSystem.listStatus(new Path(hdfsOutputPath));
//			
//			sbf.append("\n\nThe process generated the following sales and pmix extracts");
//			
//			for(FileStatus fstat : fstats){
//				sbf.append("\n\n");
//				sbf.append(fstat.getPath().toUri().toString());
//			
//			}
			
			sendMail.SendEmail(fromAddress, commadelimitedtoAddresses, subject, sbf.toString());
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}

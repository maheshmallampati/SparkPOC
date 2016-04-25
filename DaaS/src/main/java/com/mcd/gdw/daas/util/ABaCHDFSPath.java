package com.mcd.gdw.daas.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.fs.Path;

public class ABaCHDFSPath {

	public static void main(String[] args){
	 String configXmlFile = "/config.xml";
	 String outputfilePath = "";
	 
	 
	  
  	   
  	   for ( int idx=0; idx < args.length; idx++ ) {
  		   
  		   System.out.println("indx " + idx + " - " +args[idx]);
	   			if ( args[idx].equalsIgnoreCase("-c") && (idx+1) < args.length ) {
	   				configXmlFile = args[idx+1];
	   			}
	   			if ( args[idx].equalsIgnoreCase("-o") && (idx+1) < args.length ) {
	   				outputfilePath = args[idx+1];
	   			}
	   			
  	   }
  	   
  	   
  	 BufferedWriter bw  = null;
		try{
		
			
			File file = new File(outputfilePath);
	        bw = new BufferedWriter(new FileWriter(file));
	        
	       DaaSConfig daasConfig = new DaaSConfig(configXmlFile);
	       String destpath = daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.hdfsLandingZoneArrivalSubDir();
	       
	       bw.write(destpath);
	       
	       bw.flush();
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
			
				if(bw != null)
					bw.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
}


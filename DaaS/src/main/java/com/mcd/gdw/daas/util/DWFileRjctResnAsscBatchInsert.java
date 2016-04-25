package com.mcd.gdw.daas.util;

import java.io.BufferedReader;
import java.io.FileReader;

public class DWFileRjctResnAsscBatchInsert {
	
	static DaaSConfig daasConfig;
	
	public static void main(String[] args){
	   
	  String configXmlFile = "/config.xml";
	
	   String dwfileDatafilePath = null;
	   
	   for ( int idx=0; idx < args.length; idx++ ) {
  			if ( args[idx].equalsIgnoreCase("-c") && (idx+1) < args.length ) {
  				configXmlFile = args[idx+1];
  			}else 
  			if ( args[idx].equalsIgnoreCase("-i") && (idx+1) < args.length ) {
  				dwfileDatafilePath = args[idx+1].trim();
  			}
	   }
   	   daasConfig = new DaaSConfig(configXmlFile);
   	   
   	DWFileRjctResnAsscBatchInsert  dwFileRjctResnAsscBatchInsert = new DWFileRjctResnAsscBatchInsert();
   	dwFileRjctResnAsscBatchInsert.readFileandInsertRowstoDWFile(configXmlFile, dwfileDatafilePath);
	}
	
    public int readFileandInsertRowstoDWFile(String configXmlFile,String dwfileDatafilePath){
 	   
 	   BufferedReader br = null;
 	   RDBMS rdbmsutil = null;
 	   int count = -1;
 	    try {
 	    	
 	    	br =  new BufferedReader(new FileReader(dwfileDatafilePath));
 	        StringBuilder sb = new StringBuilder();
 	        String line = null;
 	        String[] lineparts;
 	        
 	        DaaSConfig daasConfig = new DaaSConfig(configXmlFile);
 	       
 
     		rdbmsutil = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());
     		
     		 
			String insertIntoDWFileRjctResnAsscQuery = "insert into " + daasConfig.abacSqlServerDb() + ".dw_file_rjct_resn_assc "+
													   "(DW_FILE_ID,DW_AUDT_RJCT_RESN_ID) values (?,?)";
			
			
			rdbmsutil.setPreparedStatement(insertIntoDWFileRjctResnAsscQuery);
			
			
			
	        while ((line = br.readLine()) != null) {
	        	
	        	lineparts = line.split("\\|");
	        	
	        	
	        	rdbmsutil.addBatch(Integer.parseInt(lineparts[0]), (short)Integer.parseInt(lineparts[1]));
	        }
     		
	        
	        count = rdbmsutil.finalizeBatch();
	        
 	    }catch(Exception ex){
 	    	ex.printStackTrace();
 	    }finally{
 	    	try{
 	    		if(br != null)
 	    			br.close();
 	    		if(rdbmsutil != null)
 	    			rdbmsutil.dispose();
 	    	}catch(Exception ex){
 	    		ex.printStackTrace();
 	    	}
 	    }
 	    
 	    return count;
    }
	
}


package com.mcd.gdw.daas.util;

public class ABaCTestConnection {

	public static void main(String[] args){
		
		  String configXmlFile = "/config.xml";
		  RDBMS rdbmsutil = null;
		  
	   	   
	   	   for ( int idx=0; idx < args.length; idx++ ) {
	   		   if ( args[idx].equalsIgnoreCase("-c") && (idx+1) < args.length ) {
	   				configXmlFile = args[idx+1];
	   			}
		   	}
	   	   
	   	   try{
	   		   
	   		 DaaSConfig daasConfig = new DaaSConfig(configXmlFile);
	   		rdbmsutil = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());
	   		   
	   	   }catch(Exception ex){
	   		  ex.printStackTrace();
	   		   System.exit(10);
	   	   }finally{
	   		rdbmsutil.dispose();
	   	   }
	   	System.out.println(" exiting with 0 ");
	   	   System.exit(0);
	}
}

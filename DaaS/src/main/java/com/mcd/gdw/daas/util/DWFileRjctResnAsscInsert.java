package com.mcd.gdw.daas.util;

public class DWFileRjctResnAsscInsert {
	
	static DaaSConfig daasConfig;
	
	public static void main(String[] args){
	   
	  String configXmlFile = "/config.xml";
	  String fileId = "";
	  String rjctRsnId = "";
	  
   	   
   	   for ( int idx=0; idx < args.length; idx++ ) {
	   			if ( args[idx].equalsIgnoreCase("-c") && (idx+1) < args.length ) {
	   				configXmlFile = args[idx+1];
	   			}
	   			if ( args[idx].equalsIgnoreCase("-f") && (idx+1) < args.length ) {
	   				fileId = args[idx+1];
	   			}
	   			if ( args[idx].equalsIgnoreCase("-r") && (idx+1) < args.length ) {
	   				rjctRsnId = args[idx+1];
	   			}
   	   }
   	   
   	   daasConfig = new DaaSConfig(configXmlFile);
   	   
   	   DWFileRjctResnAsscInsert  dwFileRjctResnAsscInsert = new DWFileRjctResnAsscInsert();
   	   dwFileRjctResnAsscInsert.insertIntoDWFileRjctResnAssc(fileId, rjctRsnId);
	}
	
	
	public int insertIntoDWFileRjctResnAssc(String fileIdStr,String rjctRsnIdStr){
		
		RDBMS rdbmsutil = null;
		try{
			int fileId = Integer.parseInt(fileIdStr);
			int rjctRsnId = Integer.parseInt(rjctRsnIdStr);
			
			 
			 
			String insertIntoDWFileRjctResnAsscQuery = "insert into " + daasConfig.abacSqlServerDb() + ".dw_file_rjct_resn_assc "+
													   "(DW_FILE_ID,DW_AUDT_RJCT_RESN_ID) values ("+ fileId+","+rjctRsnId+")";
			
			System.out.println(daasConfig.abacSqlServerServerName() + " - " + daasConfig.abacSqlServerUserId() );
			rdbmsutil = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());
			
			rdbmsutil.executeUpdate(insertIntoDWFileRjctResnAsscQuery);
			
			rdbmsutil.commit();
			
			System.out.println(" query executed successfully");
			
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			if(rdbmsutil != null){
				try{
					rdbmsutil.dispose();
				}catch(Exception ex){
					ex.printStackTrace();
				}
			}
		}
		
		return 0;
	}
}


package com.mcd.gdw.daas.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;

public class DWExecuteQuery {
	
	static DaaSConfig daasConfig;
	
	public static void main(String[] args){
	   
	  String configXmlFile = "/config.xml";
	  String query = "";
	  String outputfilePath = "";
	  String delimiter = "|";
	 
	  
   	   
   	   for ( int idx=0; idx < args.length; idx++ ) {
   		   
   		   System.out.println("indx " + idx + " - " +args[idx]);
	   			if ( args[idx].equalsIgnoreCase("-c") && (idx+1) < args.length ) {
	   				configXmlFile = args[idx+1];
	   			}
	   			if ( args[idx].equalsIgnoreCase("-q") && (idx+1) < args.length ) {
	   				query = args[idx+1];
	   			}
	   			if ( args[idx].equalsIgnoreCase("-o") && (idx+1) < args.length ) {
	   				outputfilePath = args[idx+1];
	   			}
	   			if ( args[idx].equalsIgnoreCase("-s") && (idx+1) < args.length ) {
	   				delimiter = args[idx+1];
	   			}
	   			
	   			
   	   }
   	   
   	   daasConfig = new DaaSConfig(configXmlFile);
   	   
   	   
   	   DWExecuteQuery  dwExecuteQuery = new DWExecuteQuery();
   	   dwExecuteQuery.executeQuery(dwExecuteQuery.replaceChars(query),outputfilePath,delimiter);
	}
	
	
	private String replaceChars(String query){	
		query = query.replaceAll("%SQ%","'");
		
		return query;
	}
	public int executeQuery(String query,String filePath,String delimiter){
		
		RDBMS rdbmsutil = null;
		ResultSet rs = null;
		BufferedWriter bw  = null;
		try{
		
			System.out.println(daasConfig.abacSqlServerServerName() + " - " + daasConfig.abacSqlServerUserId() );
			rdbmsutil = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());
			
			System.out.println( " query  " + query );
			rs = rdbmsutil.resultSet(query);
//			int colIndx =1;
			
			File file = new File(filePath);
	        bw = new BufferedWriter(new FileWriter(file));
	         
	       
	        int rowCnt = 0;
	        StringBuffer sbf = new StringBuffer();
			while(rs.next()){
		
//				sbf.setLength(0);
//				for(int colIndx=1;colIndx <= rs.getMetaData().getColumnCount();colIndx++){
//					if(rowCnt == 0){
//						if(colIndx > 1)
//							sbf.append("|");
//						sbf.append(rs.getMetaData().getColumnName(colIndx));
//						
//						
//					}
//				}
//				
//				
//				bw.write(sbf.toString());
//				rowCnt++;
				
				sbf.setLength(0);
				for(int colIndx=1;colIndx <= rs.getMetaData().getColumnCount();colIndx++){
					
					if(colIndx > 1)
						sbf.append(delimiter);
					
					sbf.append(rs.getString(colIndx));
				}
				
				if(rowCnt > 0)
					bw.write("\n");
				bw.write(sbf.toString());
				
				
				 rowCnt ++;
				
			}
			bw.flush();
			
			rdbmsutil.commit();
			
			System.out.println(" query executed successfully");
			
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				if(rs != null)
					rs.close();
				if(rdbmsutil != null){
					rdbmsutil.dispose();
				}
				if(bw != null)
					bw.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
		return 0;
	}
}

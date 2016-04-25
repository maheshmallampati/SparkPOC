package com.mcd.gdw.daas.util;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.ResultSet;

public class DWDataTypeUtil {

	public static void main(String[] args){
		
		String dbServerName		= "dbservername";
		String userName			= "dbusername";
		String password			= "dbpassword";
		
		DWDataTypeUtil dwDataTypeUtil = new DWDataTypeUtil();
		
		dwDataTypeUtil.createDWDataTypeCacheFile ("DW_DATA_TYP",dbServerName,userName,password);
	}
	
	
	
	public void createDWDataTypeCacheFile(String destPath,String dbServerName,String userName,String password){
		RDBMS rdbmsUtil = null;
		ResultSet rs = null;
		
		FileOutputStream fout = null;
		String query = "select * from ABaC.dbo.DW_DATA_TYP";
		try{
			rdbmsUtil = new RDBMS(RDBMS.ConnectionType.SQLServer,dbServerName,userName,password);
			
			rs = rdbmsUtil.resultSet(query);
			if(rs != null ){
				fout = new FileOutputStream(new File(destPath));
				StringBuffer sbf = new StringBuffer();
				while(rs.next()){
					
					sbf.setLength(0);
					
					sbf.append(rs.getString(1)).append("|");
					sbf.append(rs.getString(2)).append("|");
					sbf.append(rs.getString(3)).append("|");
					sbf.append(rs.getString(4)).append("|");
					sbf.append(rs.getString(5)).append("|");
					sbf.append(rs.getString(6)).append("|");
					sbf.append(rs.getString(7)).append("|");
					sbf.append(rs.getString(8)).append("|");
					sbf.append(rs.getString(9));
					
					fout.write(sbf.toString().getBytes());
					
					
				}
				fout.flush();
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				if(fout != null)
					fout.close();
				
				if(rs != null)
					rs.close();
				if(rdbmsUtil != null)
					rdbmsUtil.dispose();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
}

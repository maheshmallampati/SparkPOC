package com.mcd.gdw.daas.util;

import java.io.BufferedReader;
import java.io.FileReader;

public class SQLScript {

	public static void main(String[] args) {
		
		/*
		 * @param args[0] SQLServer server name
		 * @param args[1] user id
		 * @param args[2] password
		 * @param args[3] sql script file
		 * @param args[4-n] optional sql script substitution key value pairs.  For example, @DB=AWSDB  
		 */
		
		SQLScript instance = new SQLScript();
		
		instance.run(args);
		
		System.exit(0);
		
	}
	
	private void run(String[] args) {
		
		String[] parts;
		String line;
		StringBuffer sql = new StringBuffer();
		RDBMS sqlServer;
		BufferedReader br; 
		
		try {
			sqlServer = new RDBMS(RDBMS.ConnectionType.SQLServer,args[0],args[1],args[2]);
			
			br = new BufferedReader(new FileReader(args[3]));

			
			while ((line = br.readLine()) != null) {
				if ( sql.length() == 0 ) {
					System.out.println("--------------------\n");
				}
				
				System.out.println(line);
				
				for (int idx=4; idx < args.length; idx++) {
					parts = args[idx].split("=");
					line = line.replaceAll(parts[0], parts[1]).trim();
				}
				
				sql.append(line);
				sql.append("\n");
				
				if ( line.endsWith(";")) {
					sqlServer.executeUpdate(sql.toString());
					sql.setLength(0);
				}
			}
			
			br.close();
			
			sqlServer.dispose();
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
}

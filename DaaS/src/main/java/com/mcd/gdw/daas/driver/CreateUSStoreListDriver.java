/**
 * 
 */
package com.mcd.gdw.daas.driver;

/**
 * @author mc41946
 *
 */
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.RDBMS;
import com.mcd.gdw.daas.util.SendMail;


public class CreateUSStoreListDriver extends Configured implements Tool {

	String selectSubTypes = "";
	String fileType = "";
	String configXmlFile = "";
	String inputPath = "";
	String[] args;
	String outputPath = "";
	FileSystem fileSystem;
	SendMail sendMail;
	
	String fromAddress = "";
	String toAddress = "";	
	String subject = "";
	String emailText    = "";

	public static void main(String[] args) throws Exception {

		int retval = ToolRunner.run(new Configuration(),
				new CreateUSStoreListDriver(), args);

		System.out.println(" return value : " + retval);
	}

	@Override
	public int run(String[] argsall) throws Exception {

		GenericOptionsParser gop = new GenericOptionsParser(argsall);

		args = gop.getRemainingArgs();

		for (int idx = 0; idx < args.length; idx++) {
			if (args[idx].equals("-c") && (idx + 1) < args.length) {
				configXmlFile = args[idx + 1];
			}
						
			if (args[idx].equals("-fromAddress") && (idx + 1) < args.length) {
				fromAddress = args[idx + 1];
			}
			if (args[idx].equals("-toAddress") && (idx + 1) < args.length) {
				toAddress = args[idx + 1];
			}
			if (args[idx].equals("-subject") && (idx + 1) < args.length) {
				subject = args[idx + 1];
			}
			if (args[idx].equals("-emailText") && (idx + 1) < args.length) {
				emailText = args[idx + 1];
			}
		}

		//if (configXmlFile.length() == 0 || fromAddress.length()==0 || toAddress.length()==0 || subject.length()==0 || emailText.length()==0) {
			
			if (configXmlFile.length() == 0) {
			System.err.println("Invalid parameters");
			System.err
					.println("Usage: CreateUSStoreListDriver -c config.xml -fromAddress fromAddress -toAddress toAddress -subject subject -emailText emailText");
			System.exit(8);
		}

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile);
		//AWS START
		//fileSystem = FileSystem.get(getConf());
		fileSystem = HDFSUtil.getFileSystem(daasConfig, getConf());
		//AWS END

		if (daasConfig.configValid()) {
			if (daasConfig.displayMsgs()) {
				System.out.println(daasConfig.toString());
			}
			sendMail = new SendMail(daasConfig);
			createListDistCache(daasConfig, fileSystem);
		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.exit(8);
		}

		return (0);

	}

	

	public void createListDistCache(DaaSConfig daasConfig, FileSystem fileSystem) throws InterruptedException {
		
		try {
			
			//FsPermission fspermission = new FsPermission(FsAction.ALL,FsAction.ALL, FsAction.ALL);
			Path locationList = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR  + "distcachefiles" + Path.SEPARATOR + "USTLDStoreList.txt");
            //Path locationList = new Path("Input" + Path.SEPARATOR + "USStoresList.txt");
			
			if ( fileSystem.exists(locationList) ) {
				fileSystem.delete(locationList, false);
			}
		
			Connection connection = null;
			Statement stmt = null;
			StringBuffer sql = new StringBuffer();
			ResultSet rset=null;
	 
			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");				
				connection = DriverManager.getConnection(daasConfig.tranpolConnectionString(), daasConfig.tranpolUserId(),daasConfig.tranpolPassword());
				stmt=connection.createStatement();
				sql.setLength(0);
				sql.append("select\n");
				sql.append("'840' as TERR_CD\n");
				sql.append(",NATL_STR_NU as STORE_ID\n"); 
				sql.append("from TLDUSR.V1TRNPOL \n");
				sql.append("where TMIT_INSL_STUS_CD = 'P'");
				rset = getResultSet(sql.toString(),stmt);

				BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(locationList,true)));

				while ( rset.next() ) {
					bw.write(rset.getString("TERR_CD") + "|" + rset.getString("STORE_ID"));
					bw.write("\n");
				}

				rset.close();				
				bw.close();			
	 
			} catch (SQLException e) {
	 
				System.out.println("Exception occured in getting connection to the database"+e.toString());
				return;
	 
			}
			finally {
				if (rset!=null)
				{
					rset.close();
				}
				if (stmt!=null)
				{
					stmt.close();
				}
				if (connection!=null)
				{
					connection.close();
				}
			}
	 
			} catch (Exception ex) {
			// TODO Auto-generated catch block
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			ex.printStackTrace();
			throw new InterruptedException("Error occured in Moving Files:"+ex.toString());
		}
	}
	public void sendEmailToSupport(String fromAddress,String toAddress,String subject,String body)
	{
		sendMail.SendEmail(fromAddress, toAddress, subject, body);
	}
	public ResultSet getResultSet(String sql,Statement stmt) throws Exception {

		ResultSet retResults = null; 

		try {
			retResults = stmt.executeQuery(sql);
			
		
		} catch (Exception ex) {
			throw new InterruptedException("Error occured in getting result set:"+ex.toString());
		}
			
		return(retResults);
	
	}
}

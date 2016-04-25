/**
 * 
 */
package com.mcd.gdw.daas.util;

/**
 * @author mc41946
 *
 */
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

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
import com.mcd.gdw.daas.util.SendMail;
import com.mcd.gdw.test.daas.mapreduce.GenericMoveMapper;

public class CreateFileUtil extends Configured implements Tool {

	String selectSubTypes = "";
	String fileType = "";
	String configXmlFile = "";
	String[] args;
	String filePath = "";
	FileSystem fileSystem;
	SendMail sendMail;
	
	String fromAddress = "";
	String toAddress = "";	
	String subject = "";
	String emailText    = "";
	private Configuration hdfsConfig = null;

	public static void main(String[] args) throws Exception {

		int retval = ToolRunner.run(new Configuration(),
				new CreateFileUtil(), args);

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

			
			if (args[idx].equals("-o") && (idx + 1) < args.length) {
				filePath = args[idx + 1];
			}
			if (args[idx].equals("-t") && (idx + 1) < args.length) {
				fileType = args[idx + 1];
			}
			if (args[idx].equals("-selectsubtypes") && (idx + 1) < args.length) {
				selectSubTypes = args[idx + 1];
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

		if (configXmlFile.length() == 0 || fileType.length() == 0 || filePath.length() == 0 || fromAddress.length()==0 || toAddress.length()==0 || subject.length()==0 || emailText.length()==0) {
			System.err.println("Invalid parameters");
			System.err
					.println("Usage: CreateFileUtil -c config.xml -i inputPath -o outputPath -t fileType -fromAddress fromAddress -toAddress toAddress -subject subject -emailText emailText");
			System.exit(8);
		}

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		fileSystem = FileSystem.get(getConf());

		if (daasConfig.configValid()) {
			if (daasConfig.displayMsgs()) {
				System.out.println(daasConfig.toString());
			}
			sendMail = new SendMail(daasConfig);
			
		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.exit(8);
		}
		
		runJob(daasConfig,fileType);
		
		return (0);

	}
	private void runJob(DaaSConfig daasConfig
            ,String fileType) {
		try{
       Path fileLocation = new Path(filePath);
			
			if ( fileSystem.exists(fileLocation) ) {
				fileSystem.delete(fileLocation, false);
			}
		BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(fileLocation,true)));
		bw.write("Temporary File created for oozie workflow");
		bw.close();
		}catch(Exception ex)
		{
			ex.printStackTrace();
			sendEmailToSupport(fromAddress, toAddress, subject, " Error occured while creating temporary file"+ex.getMessage());
		}
	}
	
	public void sendEmailToSupport(String fromAddress,String toAddress,String subject,String body)
	{
		sendMail.SendEmail(fromAddress, toAddress, subject, body);
	}
}

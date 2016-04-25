package com.mcd.gdw.test.daas.driver;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.VerifyGZipFormatMapper;
import com.mcd.gdw.test.daas.mapreduce.VerifyXMLMapper;

public class VerifyGZipFormat extends Configured implements Tool {

	String inputpath = "";
	String outputpath = "";
	public static void main(String[] args) throws Exception  {
		
		Configuration hdfsConfig = new Configuration();
		
		int retval = ToolRunner.run(hdfsConfig,new VerifyGZipFormat(), args);

		System.out.println(" return value : " + retval);

	}
	
	public int run(String[] args) throws Exception {
			
			inputpath = args[0];
			String outputpath = args[1];

			runJob();
		
	
		return(0);
	}
	
	private void runJob() throws Exception {
		
		Job job;
		ArrayList<Path> requestedPaths;

		job = Job.getInstance(getConf(), "Verify GZip");
		
		

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setJarByClass(VerifyGZipFormat.class);
		job.setMapperClass(VerifyGZipFormatMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//job.setOutputKeyClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		TextOutputFormat.setOutputPath(job,new Path(outputpath));

		
		job.setNumReduceTasks(0);
		
		if ( ! job.waitForCompletion(true) ) {
			System.err.println("Error occured in MapReduce process, stopping");
			System.exit(8);
		}
		
	}
	}

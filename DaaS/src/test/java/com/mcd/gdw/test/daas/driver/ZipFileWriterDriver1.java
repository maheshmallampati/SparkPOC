package com.mcd.gdw.test.daas.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.driver.MergeToFinal;
import com.mcd.gdw.daas.mapreduce.UnzipInputFileMapper;
import com.mcd.gdw.daas.mapreduce.UnzipInputFileReducer;
import com.mcd.gdw.daas.mapreduce.ZipFileInputFormat;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.ZipFileOutputFormat;
import com.mcd.gdw.test.daas.mapreduce.ZipFileWriterMapper;
import com.mcd.gdw.test.daas.mapreduce.ZipFileWriterReducer;
/**
 * 
 * @author Sateesh Pula
 *
 */

public class ZipFileWriterDriver1 extends Configured implements Tool {
	
	
	public static void main(String[] argsAll) throws Exception{
		
		GenericOptionsParser gop = new GenericOptionsParser(argsAll);
		
		String[] args = gop.getRemainingArgs();
		
		ToolRunner.run(new Configuration(), new ZipFileWriterDriver1(), args);
	}

	@Override
	public int run(String[] argsAll) throws Exception {

		Job job = new Job(getConf(),"Test");
		
		FileSystem fs = FileSystem.get(getConf());
	
		ZipFileInputFormat.setLenient(true);
		ZipFileInputFormat.setInputPaths(job, new Path("/daastest/lz/abac" ));
		
		job.setJarByClass(ZipFileWriterDriver1.class);
		job.setMapperClass(ZipFileWriterMapper.class);
		job.setReducerClass(ZipFileWriterReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		job.setInputFormatClass(ZipFileInputFormat.class);
		
//		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		HDFSUtil.removeHdfsSubDirIfExists(fs, new Path("/daastest/work/np_xml/test"), true);
		
//		job.setOutputFormatClass(ZipFileOutputFormat.class);
		
		job.setOutputFormatClass(ZipFileOutputFormat.class);
		ZipFileOutputFormat.setOutputPath(job, new Path("/daastest/work/np_xml/test"));
	
//		TextOutputFormat.setOutputPath(job, new Path("/daastest/work/np_xml/test"));
		
		MultipleOutputs.addNamedOutput(job, "POS_XML_AU_0002_20141210".replace("_",""), ZipFileOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "POS_XML_US_10139_20130317".replace("_",""), ZipFileOutputFormat.class, Text.class, Text.class);
		job.waitForCompletion(true);
		
		return 0;
	}
	
	
	
	

}

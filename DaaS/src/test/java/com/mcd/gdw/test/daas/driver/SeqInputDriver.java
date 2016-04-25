package com.mcd.gdw.test.daas.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.SeqInputMapper;

public class SeqInputDriver extends Configured implements Tool{

	public static void main(String[] args){
		try{
			ToolRunner.run(new Configuration(), new SeqInputDriver(),args);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration hdfsConfig = getConf();
		
		hdfsConfig.set("mapreduce.map.output.compress", "true");
		hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
		hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
//		hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
		hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
//		hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
		
		Job job = Job.getInstance(getConf());
		job.setJobName("Test");
		
		
		job.setJarByClass(SeqInputDriver.class);
		
		job.setMapperClass(SeqInputMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		HDFSUtil.removeHdfsSubDirIfExists(FileSystem.get(getConf()), new Path("/daastest/snappytest"), true);
		SequenceFileInputFormat.addInputPath(job, new Path("/daas/work/compress/Store-Db*"));
		FileOutputFormat.setOutputPath(job, new Path("/daastest/snappytest"));
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);  
		
		job.waitForCompletion(true);
		
		return 0;
	}

}

package com.mcd.gdw.daas.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MoveFilesinHDFSDriver extends Configured implements Tool{

	
	public static void main(String[] argsAll) throws Exception{
		try{
			int ret = ToolRunner.run(new Configuration(), new MoveFilesinHDFSDriver(), argsAll);
		}catch(Exception ex){
			ex.printStackTrace();
			System.exit(1);
		}
	}
	@Override
	public int run(String[] argsAll) throws Exception {
		// TODO Auto-generated method stub
		
		GenericOptionsParser gop = new GenericOptionsParser(argsAll);
		
		String[] args = gop.getRemainingArgs();
		
		Configuration conf = getConf();
		
		Job job  = new Job(conf,"MoveFilesinHDFS");
		
		
		job.setJarByClass(MoveFilesinHDFSDriver.class);
		
		job.setMapperClass(MoveFilesinHDFSMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);
		
		job.waitForCompletion(true);
		
		
		return 0;
	}
	
	

}

package com.mcd.gdw.daas.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.mapreduce.MenuItemExtractMapper;

public class MenuItemExtractDriver extends Configured implements Tool{

	
	public static void main(String[] argsAll) throws Exception{
		
		int retCode = ToolRunner.run(new Configuration(),new MenuItemExtractDriver(),argsAll);
	}

	@Override
	public int run(String[] argsAll) throws Exception {
		
		String[] args = new GenericOptionsParser(argsAll).getRemainingArgs();
		
		
		Job job = new Job(getConf(),"TestExtract");
		
		
		job.setJarByClass(MenuItemExtractDriver.class);
		
		job.setMapperClass(MenuItemExtractMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.waitForCompletion(true);
	
		
		return 0;
	}
	
	
}

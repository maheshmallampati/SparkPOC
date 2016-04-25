package com.mcd.gdw.daas.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.mapreduce.ZipFileInputFormat;

public class GenerateTdaFormatSTLDPOS35 extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		
		
		int retval = ToolRunner.run(new Configuration(), new GenerateTdaFormatSTLDPOS35(),args);
	}
	@Override
	public int run(String[] argsall) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		String[] args = gop.getRemainingArgs();
		Job job = new Job(getConf(),"GenerateTdaFormatSTLDPOS35");
//		ZipFileInputFormat.setLenient(true);
//		ZipFileInputFormat.addInputPath(job, new Path(args[0]));
		
		
		
		job.setMapperClass(com.mcd.gdw.daas.mapreduce.POS35TDAExtractMapper.class);
		job.setJarByClass(com.mcd.gdw.daas.mapreduce.POS35TDAExtractMapper.class);
		
		job.setMapOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
		job.setMapOutputValueClass(org.apache.hadoop.io.Text.class);
		
//		job.setInputFormatClass(ZipFileInputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setNumReduceTasks(0);
		
		MultipleOutputs.addNamedOutput(job, "TDASALES", TextOutputFormat.class,
				NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "TDAPMIX", TextOutputFormat.class,
				NullWritable.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		boolean retvalue = job.waitForCompletion(true);
		
		
		
		return 0;
	}
	
	

}

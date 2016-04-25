package com.mcd.gdw.daas.mapreduce.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MoveFilesinHDFSMapper extends Mapper<LongWritable,Text,NullWritable,Text>{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		try{
			String[] parts = value.toString().split("\t");
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			context.getCounter("Count", "Total").increment(1);
			
			if(fs.rename(new Path(parts[0]), new Path(parts[1]))){
				context.getCounter("Count", "Success").increment(1);
			}else{
				context.getCounter("Count", "Failure").increment(1);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
	
	

}

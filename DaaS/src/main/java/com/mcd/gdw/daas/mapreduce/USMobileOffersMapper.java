package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USMobileOffersMapper extends Mapper<LongWritable,Text,Text,Text>{
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
	}

	
	
	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}
	
	
	

}

package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class SeqInputMapper extends Mapper<NullWritable,Writable,NullWritable,Text>{

	@Override
	protected void map(NullWritable key, Writable value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		context.write(NullWritable.get(),value);
	}

	
}

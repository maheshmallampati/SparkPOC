package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CompressUsingGZipCodec extends Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		context.write(NullWritable.get(), value);
	}
}

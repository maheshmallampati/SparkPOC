package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AUExtractReducer2 extends Reducer<Text, Text, NullWritable, Text> {

	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	    for (Text value : values ) {
	    	context.write(NullWritable.get(), value);
	    }
	}
}

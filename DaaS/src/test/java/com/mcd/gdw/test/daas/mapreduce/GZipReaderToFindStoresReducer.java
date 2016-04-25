package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GZipReaderToFindStoresReducer extends Reducer<Text,Text,NullWritable,Text> {

	@Override
	protected void reduce(Text arg0, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		
		
	}

	

	
}

package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.util.HDFSUtil;

public class CompressGoldLayerReducer extends Reducer<Text, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> mos;
	private static String[] parts = null;
	private Iterator<Text> valuesIt;
	private Text value;
	private String fileName;

	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	  
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		parts = key.toString().split("\\t");
		
		fileName = parts[0] + HDFSUtil.FILE_PART_SEPARATOR + parts[1] + HDFSUtil.FILE_PART_SEPARATOR + parts[2];
		
		valuesIt = values.iterator();
		
		while ( valuesIt.hasNext() ) {
			value = valuesIt.next();
			mos.write(NullWritable.get(), value, fileName);
			value = new Text();
			value.set(fileName);
		}
	}
	
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
}

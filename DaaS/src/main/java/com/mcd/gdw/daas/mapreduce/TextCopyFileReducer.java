package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.util.HDFSUtil;

public class TextCopyFileReducer extends Reducer<Text, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> mos;
	private static String[] parts = null;

	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	  
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	     for (Text value : values ) {

	    	 parts = key.toString().split("_");
		       
	    	 mos.write(NullWritable.get(), value, HDFSUtil.replaceMultiOutSpecialChars(parts[0] + HDFSUtil.FILE_PART_SEPARATOR + parts[1]));
	    	 
	    	 //context.getCounter("Debug", parts[0]).increment(1);
	     }
	}

	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}

}

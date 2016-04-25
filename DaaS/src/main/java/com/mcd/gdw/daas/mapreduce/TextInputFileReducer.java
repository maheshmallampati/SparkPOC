package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.util.HDFSUtil;

public class TextInputFileReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;
	private static String currKey = "";
	private static String fileName = "";

	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	  
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		currKey = key.toString() + "_";
		fileName = HDFSUtil.replaceMultiOutSpecialChars(currKey.substring(0, currKey.indexOf("_")));

	    for (Text value : values ) {
    		mos.write(NullWritable.get(),value,fileName);
	    }
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}

}

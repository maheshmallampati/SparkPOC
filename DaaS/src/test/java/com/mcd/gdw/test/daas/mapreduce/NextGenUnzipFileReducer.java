package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NextGenUnzipFileReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;
	private static String[] parts = null;
	
	private Text outValue = new Text();
	
	private String fileName;

	private Iterator<Text> valIt;
	
	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	  
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		parts = key.toString().split("\t");
		
		if ( parts[0].equals("INVALID") ) {
			fileName = "INVALID";
		} else {
			fileName = parts[0] + "~" + parts[1] + "~" + parts[2];
		}
		
		valIt = values.iterator();
		
		while ( valIt.hasNext() ) {
			outValue = valIt.next();
			mos.write(NullWritable.get(), outValue,fileName );
		}
	}
	
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
	
	
}

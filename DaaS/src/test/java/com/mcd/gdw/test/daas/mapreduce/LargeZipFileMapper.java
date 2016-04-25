package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class LargeZipFileMapper extends Mapper<Text, BytesWritable, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;
	private Text valueText = new Text();
	
	@Override
	public void setup(Context context) {
		
		String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
		
		System.err.println("FILE NAME=" + filePathString);
		
		mos = new MultipleOutputs<NullWritable, Text>(context);
		
	}
	
	
	@Override
	public void map(Text key
			       ,BytesWritable value
			       ,Context context) throws IOException, InterruptedException {

		try {
			
			System.err.println("KEY=" + key.toString());
			
			byte[] valuebytes = value.getBytes();
			
			System.err.println("Value Length=" + valuebytes.length);
			
			//String valText = new String(valuebytes,"UTF-8");
			
			valueText.clear();
			//valueText.set(valText);
			valueText.set(valuebytes);
			
			mos.write(NullWritable.get(),valueText,key.toString());
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}	
}

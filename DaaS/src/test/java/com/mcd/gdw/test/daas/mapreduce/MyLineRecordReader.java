package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class MyLineRecordReader extends LineRecordReader{
	Text currentValue = new Text();
	
	String fileName = "";
	
	public MyLineRecordReader(){
		super();
	}
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		// TODO Auto-generated method stub
		fileName = ((FileSplit)genericSplit).getPath().toString();
		
		super.initialize(genericSplit, context);
	}
	


	@Override
	public boolean nextKeyValue() throws IOException {
		
		try{
			super.nextKeyValue();
		}catch(IOException ex){
			ex.printStackTrace();
			currentValue.clear();
			currentValue.set(fileName);
			return false;
		}
		
		return true;
	}

	

}

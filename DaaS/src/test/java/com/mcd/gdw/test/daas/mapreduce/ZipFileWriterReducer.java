package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ZipFileWriterReducer extends Reducer<Text,Text,Text,Text>{
	
	
	private MultipleOutputs<Text, Text> mos;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
			mos.close();
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	Text valText;

	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		
		System.out.println(" Key " + key.toString() );
		
		String[] valArr ;
		Text newKey = new Text();
		Text newVal = new Text();
		
		
		
 		Iterator<Text> valit = values.iterator();
 		
 		while(valit.hasNext()){
 			valText  = valit.next();
 		}
		
				
		for(Text value :values){
 			
 			valArr = value.toString().split("\t");
 			
 			newKey.clear();
 			newKey.set(valArr[0]);
 			
 			newVal.clear();
 			newVal.set(valArr[1]);
 			
 			mos.write(key.toString().split("\\.")[0].replace("_", ""), newKey, newVal, key.toString());
//			context.write(newKey, newVal);
		}
	}


}

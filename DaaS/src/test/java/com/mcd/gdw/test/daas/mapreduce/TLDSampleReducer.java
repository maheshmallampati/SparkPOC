package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mcd.gdw.test.daas.driver.GenerateTLDSample;

public class TLDSampleReducer extends Reducer<Text, Text, NullWritable, Text> {

	private HashMap<String,String> sosMap = new HashMap<String,String>();
	private ArrayList<String> tldValueList = new ArrayList<String>();
	private String valueText;
	private String[] parts;
	private String keyText;
	
	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputValue = new StringBuffer();
	
	private Text outputText = new Text();
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		keyText = key.toString();
		
		outputKey.setLength(0);
		outputKey.append(Integer.parseInt(keyText.substring(0, 3)));
		outputKey.append(GenerateTLDSample.SEPARATOR_CHARACTER);
		outputKey.append(keyText.substring(3, 13));
		outputKey.append(GenerateTLDSample.SEPARATOR_CHARACTER);
		outputKey.append(keyText.substring(13));
		outputKey.append(GenerateTLDSample.SEPARATOR_CHARACTER);
		
		tldValueList.clear();
		sosMap.clear();
		
		for (Text value : values ) {
			valueText = value.toString();
			
			if ( valueText.startsWith(GenerateTLDSample.REC_PRD) ) {
				outputValue.setLength(0);
				outputValue.append("PRODUCT");
				outputValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
				outputValue.append(outputKey);
				outputValue.append(valueText.substring(4));
				
				outputText.clear();
				outputText.set(outputValue.toString());
				
				context.write(NullWritable.get(), outputText);
			
			}
			
			/*
			if ( valueText.startsWith(GenerateTLDSample.REC_TRN) ) {
				tldValueList.add(valueText.substring(4));
			} else if ( valueText.startsWith(GenerateTLDSample.REC_SOS) ) {
				parts = valueText.split("\\t",-1);
				
				sosMap.put(parts[1], parts[2] + GenerateTLDSample.SEPARATOR_CHARACTER + parts[3]);
			}
			*/
		}
		
		/*
		for (String value : tldValueList) {
			outputValue.setLength(0);
			outputValue.append(outputKey);
			outputValue.append(value);
			outputValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
			
			parts = value.split("\\t",-1);
			
			if ( sosMap.containsKey(parts[1]) ) {
				outputValue.append(sosMap.get(parts[1]));
			} else {
				outputValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
			}
			
			outputText.clear();
			outputText.set(outputValue.toString());
			
			context.write(NullWritable.get(), outputText);
		}
		*/
	}
}

package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TLDVoicePreReducer extends Reducer<Text, Text, NullWritable, Text> {

	private ArrayList<String> voiceHeaderList = new ArrayList<String>();
	private HashMap<String,String> voiceDetailMap = new HashMap<String,String>();

	private String[] parts;
	private String keyText;
	private String valueText;
	
	private Text outputValue = new Text();

	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		voiceHeaderList.clear();
		voiceDetailMap.clear();
		
		for (Text value : values ) {
			valueText = value.toString();
			if ( valueText.startsWith("HDR") ) {
				voiceHeaderList.add(valueText);
			} else {
				parts = valueText.split("\t");
				
				keyText = parts[1];
				valueText = parts[2];
				
				voiceDetailMap.put(keyText, valueText);
			}
		}

		for ( String value : voiceHeaderList ) {
			parts = value.split("\t");
			
			if ( voiceDetailMap.containsKey(parts[4]) ) {
				outputValue.clear();
				outputValue.set(parts[1] + "\t" + parts[2] + "\t" + parts[3] + "|" + parts[5] + "\t" + parts[6] + "\t" + parts[7] + "\t" + parts[8] + "\t" + parts[4] + "\t" + parts[9]);
				
				context.write(NullWritable.get(), outputValue);
				context.getCounter("DaaS","Output").increment(1);
				
			}
		}
	}
}

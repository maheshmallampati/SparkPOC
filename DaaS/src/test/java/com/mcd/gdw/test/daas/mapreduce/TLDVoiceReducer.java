package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TLDVoiceReducer extends Reducer<Text, Text, NullWritable, Text> {

	private String valueText;
	private String[] parts;
	private String[] parts2;
	
	private String tldMapKey;
	private String tldMapValue;
	
	private String findKey;
	
	private ArrayList<String> voiceValueList = new ArrayList<String>();
	private HashMap<String,String> tldPriceValueMap = new HashMap<String,String>();
	private HashMap<String,String> tldOrderNumValueMap = new HashMap<String,String>();
	
	private String keyPrefix;
	private String foundValues;

	private Text outputValue = new Text();
	
	private boolean includeFl; 

	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		parts = key.toString().split("\t");
		
		keyPrefix = parts[0] + "\t" + parts[1];
		
		voiceValueList.clear();
		tldPriceValueMap.clear();
		tldOrderNumValueMap.clear();
		
		for (Text value : values ) {
			valueText = value.toString();
			if ( valueText.startsWith("VOICE") ) {
				voiceValueList.add(valueText);
			} else {
				parts = valueText.split("\t");
				
				tldMapKey = parts[1] + "\t" + parts[2] + "\t" + parts[6];
				tldMapValue = parts[4] + "\t" + parts[5] + "\t" + parts[3];
				
				tldPriceValueMap.put(tldMapKey, tldMapValue);
				
				tldMapKey = parts[1] + "\t" + parts[2] + "\t" + parts[3];
				tldMapValue = parts[4] + "\t" + parts[5] + "\t" + parts[6];
				
				tldOrderNumValueMap.put(tldMapKey, tldMapValue);
			}
		}
		
		
		for (String voiceValueText : voiceValueList ) {
			parts = voiceValueText.split("\t");
			
			findKey = parts[1] + "\t" + parts[2] + "\t" + parts[4];
			
			if ( tldPriceValueMap.containsKey(findKey) ) {
				parts2 = tldPriceValueMap.get(findKey).split("\t");
				foundValues = parts2[0] + "\t" + parts2[1] + "\t" + parts2[2];
				includeFl = true;
			} else {
				findKey = parts[1] + "\t" + parts[2] + "\t" + parts[3];
				if ( tldOrderNumValueMap.containsKey(findKey) ) {
					parts2 = tldOrderNumValueMap.get(findKey).split("\t");
					foundValues = parts2[0] + "\t" + parts2[1] + "\t" + parts2[2];
					includeFl = true;
				} else {
					foundValues = "NULL\tNULL\tNULL";
					includeFl = false;
				}
			}

			if ( includeFl ) {
				outputValue.clear();
				outputValue.set(keyPrefix + "\t" + findKey + "\t" + parts[3] + "\t" + parts[5] + "\t" + parts[6] + "\t" + foundValues + "\tEND");
				
				context.write(NullWritable.get(), outputValue);
				context.getCounter("DaaS","Output").increment(1);
			} else {
				context.getCounter("DaaS","Output Skipped").increment(1);
			}
		}
	}
}

package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mcd.gdw.test.daas.driver.GenerateExtractICRawXMLFormat;

public class ExtractICRawXMLFormatReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private HashMap<String,Integer> fileTypesMap = new HashMap<String,Integer>();
	private String[] parts;
	private String[] parts2;
	private String fileTypesValue;
	private String[] files = new String[4];
	
	private Iterator<Text> valueIter;

	private String keyValue;
	private Text outValue = new Text();
	private StringBuffer outValueText = new StringBuffer();
	
	@Override
	public void setup(Context context) {
		
		fileTypesValue = context.getConfiguration().get(GenerateExtractICRawXMLFormat.CONFIG_SETTING_FILE_TYPES);
		
		parts = fileTypesValue.split(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
		
		for ( int idx=0; idx < parts.length; idx++ ) {
			parts2 = parts[idx].split(GenerateExtractICRawXMLFormat.ALT_SEPARATOR_CHARACTER);
			
			fileTypesMap.put(parts2[0], Integer.parseInt(parts2[1]));
		}
	}
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		keyValue = key.toString();

		for ( int idx=0; idx < files.length; idx++ ) {
			files[idx] = "";
		}
		
		valueIter = values.iterator();
		
		while ( valueIter.hasNext() ) {

			outValue = valueIter.next();
			parts = outValue.toString().split(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
			
			if ( fileTypesMap.containsKey(parts[0]) ) {
				context.getCounter("DaaS Reduce","Output XML File: " + parts[0]).increment(1);
				files[fileTypesMap.get(parts[0])] = parts[1]; 
			} else {
				context.getCounter("DaaS Reduce","Output ERROR XML File").increment(1);
			}
		}

		context.getCounter("DaaS Reduce","Output Location,Date").increment(1);
		
		outValueText.setLength(0);
		outValueText.append(keyValue);
		outValueText.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
		outValueText.append(files[0]);
		outValueText.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
		outValueText.append(files[1]);
		outValueText.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
		outValueText.append(files[2]);
		outValueText.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
		outValueText.append(files[3]);

		outValue.clear();
		outValue.set(outValueText.toString());
		
		context.write(NullWritable.get(), outValue);
	}
	
}

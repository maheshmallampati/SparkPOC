package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CostsReducer extends Reducer<Text, Text, NullWritable, Text> {

	private ArrayList<String> transList = new ArrayList<String>();
	private HashMap<String,String> costMap = new HashMap<String,String>();

	private String[] parts;
	private String keyText;
	private String valueText;
	
	private StringBuffer outValue = new StringBuffer();
	private Text outText = new Text();

	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		try {
			transList.clear();
			costMap.clear();
			
			keyText = key.toString();
			
			for (Text value : values ) {
				valueText = value.toString();
				
				if ( valueText.startsWith("C") ) {
					parts = valueText.split("\\|");
					costMap.put(parts[0].substring(1), parts[1]);
				} else {
					transList.add(valueText);
				}
			}
			
			for ( String value : transList ) {
				parts = value.split("\t");

				outValue.setLength(0);
				outValue.append(keyText.substring(6));
				outValue.append("\t");
				outValue.append(value.substring(1));
				outValue.append("\t");
				
				if ( costMap.containsKey(parts[5]) ) {
					outValue.append(costMap.get(parts[5]));
				} else {
					outValue.append("NULL\tNULL\tNULL\tNULL");
				}

				outText.clear();
				outText.set(outValue.toString());
				
				context.write(NullWritable.get(), outText);
				context.getCounter("DaaS","Output").increment(1);
			}
		} catch(Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
}

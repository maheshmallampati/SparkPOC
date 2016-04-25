package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GDWFileFormatPreReducer extends Reducer<Text, Text, Text, Text> {

	private Text newValue;
	private Integer newCount;
	private HashMap<String,Integer> componentMap = new HashMap<String,Integer>();
	private String componentValue;
	private int rowCnt;
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		componentMap.clear();
		
		Iterator<Text> nextValue = values.iterator();
		while ( nextValue.hasNext() ) {
			newValue = nextValue.next();
		
			if ( componentMap.containsKey(newValue.toString()) ) {
				newCount = componentMap.get(newValue.toString()).intValue() + 1;
				componentMap.put(newValue.toString(), newCount);
			} else { 
				componentMap.put(newValue.toString(), new Integer(1));
			}
		}
		
		rowCnt = -1;
		componentValue = "";
		
		for (Map.Entry<String,Integer> componentEntry : componentMap.entrySet()) {
			if ( componentEntry.getValue().intValue() > rowCnt ) {
				rowCnt = componentEntry.getValue().intValue();
				componentValue = componentEntry.getKey().toString();
			}
		}
		
		if ( rowCnt > 0 ) {
			newValue.clear();
			newValue.set(componentValue);
			context.write(key, newValue);
		}

	}
}

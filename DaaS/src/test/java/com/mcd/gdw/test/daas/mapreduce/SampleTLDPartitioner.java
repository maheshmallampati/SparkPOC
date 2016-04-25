package com.mcd.gdw.test.daas.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SampleTLDPartitioner extends Partitioner<Text, Text> {

	private String keyText;

	@Override
	public int getPartition(Text key, Text value, int numpartitions) {
	
		int lastDigit;
		
		try {
			keyText = key.toString();
			
			lastDigit = Integer.parseInt(keyText.substring(keyText.length()-1));
		} catch (Exception ex) {
			lastDigit = 0;
		}
		
		if ( lastDigit > numpartitions) {
			lastDigit = 0;
		}
		
		return(lastDigit);
	}
}

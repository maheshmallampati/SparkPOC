package com.mcd.gdw.test.daas.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaskXMLPartitioner extends Partitioner<Text, Text> {

	private String keyText;

	@Override
	public int getPartition(Text key, Text value, int numpartitions) {
	
		int partId;
		
		try {
			keyText = key.toString();
			
			partId = Integer.parseInt(keyText.substring(keyText.length()-1));
		} catch (Exception ex) {
			partId = 0;
		}
		
		if ( partId > numpartitions) {
			partId = 0;
		}
		
		return(partId);
	}

}

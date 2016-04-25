package com.mcd.gdw.test.daas.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NextGenUnzipFilePartitioner extends Partitioner<Text, Text> {

	private int partValue;
	private String[] keyParts;
	
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
	
		
		if ( numReduceTasks > 0 ) {
			keyParts = key.toString().split("\t");
			//System.err.println(key.toString() + " -- " + keyParts.length);
			if ( keyParts.length >= 4 ) {
				//System.err.println(key.toString() + " -- " + keyParts.length + " -- " + keyParts[3]);
				try {
					partValue = Integer.parseInt(keyParts[3]) % numReduceTasks;
					
				} catch (Exception ex ) {
					partValue = 0; 
				}
			}
			
		} else {
			partValue = 0;
		}
		
		return(partValue);
	}
}

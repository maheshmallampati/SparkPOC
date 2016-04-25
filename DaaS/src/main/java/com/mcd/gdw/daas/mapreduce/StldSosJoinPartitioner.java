package com.mcd.gdw.daas.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StldSosJoinPartitioner  extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numpartitions) {
		String[] keyvals =key.toString().split("\t");
		
		
		String compKey = keyvals[1];
//		System.out.println(   keyvals[1] + " - " + (Math.abs(compKey.hashCode())%numpartitions));
		
		return (compKey.hashCode() & Integer.MAX_VALUE)%numpartitions;
	}

}

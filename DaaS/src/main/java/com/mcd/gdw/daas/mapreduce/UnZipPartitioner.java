package com.mcd.gdw.daas.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author Sateesh Pula
 * Partition by File Type, Last digit of Legacy Ref Cd and TerrCd only. Ignore BusinessDay 
 */
public class UnZipPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numpartitions) {
		
//		if("FileStatus".equals(key.toString()))
//			 return Math.abs(key.toString().hashCode())%numpartitions;
//	
//		System.out.println( " key " + key.toString());
		
		String[] keyvals =key.toString().split("_");
		
						//FileType	Last digit of LgcyRefCd	TerrCd
		String compKey = keyvals[0]+"\t"+keyvals[1]+"\t"+keyvals[2];
		
		return Math.abs(compKey.hashCode())%numpartitions;
	}

	
}

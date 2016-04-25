package com.mcd.gdw.daas.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author Sateesh Pula
 * Group Comparator that uses only the File Type, the Last digit of the legacy local ref cd and terr for comparison and ignores
 * Business Dt
 *
 */
public class UnZipGroupComparator extends WritableComparator{

	protected UnZipGroupComparator() {
		super(Text.class,true);
	}
	
	
	@Override
	public int compare(WritableComparable key1,WritableComparable key2){
		
		
	
		
		
		String[] key1Parts = ((Text)key1).toString().split("_");
		String[] key2Parts = ((Text)key2).toString().split("_");
		
		
						  //output type      last digit        terr cd
		String key1part = key1Parts[0]+"\t"+key1Parts[1] +"\t"+key1Parts[2];
		
		String key2part = key2Parts[0]+"\t"+key2Parts[1] +"\t"+key2Parts[2];
		
		return key1part.compareTo(key2part);
	}
	
 
}

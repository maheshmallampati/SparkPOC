package com.mcd.gdw.daas.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NpSalesSummaryCompKeyComparator extends WritableComparator {

	public NpSalesSummaryCompKeyComparator() {

		super(Text.class,true);

	}

	public NpSalesSummaryCompKeyComparator(Text keyClass) {

		super(Text.class,true);

	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		
		Text key1 = (Text)w1;
		Text key2 = (Text)w2;
		
		String[] key1Parts = key1.toString().split("\\|");
		String[] key2Parts = key2.toString().split("\\|");
		
		int comp = key1Parts[0].compareTo(key2Parts[0]); //date
		
		if(comp == 0){
			comp = key1Parts[1].compareTo(key2Parts[1]); // terr cd
			if(comp == 0){
				return key1Parts[2].compareTo(key2Parts[2]); // location id
			}else{
				return comp;
			}
		}else{
			return(comp*-1); // changed to decending on date
		}
	}
}
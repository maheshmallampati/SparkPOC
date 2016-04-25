package com.mcd.gdw.daas.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author Sateesh Pula
 * use the whole key for comparison
 */
public class UnZipCompositeKeyComparator extends WritableComparator{

	protected UnZipCompositeKeyComparator() {
		
		super(Text.class,true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		
//		if(1 ==1 )
			return ((Text)w1).toString().compareTo(((Text)w2).toString());
//		Text key1 = (Text)w1;
//		Text key2 = (Text)w2;
//		
//		String[] recValues1 = key1.toString().split("_");
//		String[] recValues2 = key2.toString().split("_");
//		
//		String outputType1  = recValues1[0];
//		String lgcyCdDigit1 = recValues1[1];
//		String terrCd1		= recValues1[1];
//		
//		String outputType2  = recValues2[0];
//		String lgcyCdDigit2 = recValues2[1];
//		String terrCd2 		= recValues2[2];
//		
//		int cmp = outputType1.compareTo(outputType2);
//		
//		if(cmp != 0){
////			System.out.println(" returning 1");
//			return cmp;
//		}
//		
//		cmp = lgcyCdDigit1.compareTo(lgcyCdDigit2);
//		
//		if(cmp != 0){
//			return cmp;
//		}
//		
//		cmp = terrCd1.compareTo(terrCd2);
//		
////		System.out.println(" returning 2");
//		
//		return cmp;
	}
	
	
	
	

}

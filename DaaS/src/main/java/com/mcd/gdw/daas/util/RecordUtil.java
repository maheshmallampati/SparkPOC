package com.mcd.gdw.daas.util;

public class RecordUtil {
	
	
	public static String createRecord(String[] recArr){
		return createRecord( recArr,"\t");
	}
	/*
	 * create a string out of any array with \t as delimiter
	 */
	public static String createRecord(String[] recArr,String delimiter){
		StringBuffer sb = new StringBuffer();
		for (int i=0;i<recArr.length;i++){
			if (i>0){
				sb.append(delimiter);
			}
			if (recArr[i] != null){
				sb.append(recArr[i].trim());
			}else {
				sb.append("");
					
			}
		}
		return sb.toString();
	}
	
	public static void clearContents(String[] recArr){
		for (int i=0;i<recArr.length;i++){
			recArr[i] = "";
		}
	}
}

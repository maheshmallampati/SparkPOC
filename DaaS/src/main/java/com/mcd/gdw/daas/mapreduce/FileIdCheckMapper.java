package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mcd.gdw.daas.DaaSConstants;

public class FileIdCheckMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static String[] parts = null;
	private static Text currentKey = new Text();
	private static Text currentValue = new Text();
	private String lcatKey;
	private static HashMap<String,String> fileIdList = new HashMap<String,String>();

	@Override
	public void setup(Context context) {

		fileIdList.put("1830220", "1830220");
		fileIdList.put("1830474", "1830474");
		fileIdList.put("1830628", "1830628");
		fileIdList.put("1833443", "1833443");
		fileIdList.put("1834032", "1834032");
		fileIdList.put("1834043", "1834043");
		fileIdList.put("1834524", "1834524");
		fileIdList.put("1835695", "1835695");
		fileIdList.put("1835997", "1835997");
		fileIdList.put("1836410", "1836410");
		fileIdList.put("1836608", "1836608");
		fileIdList.put("1838777", "1838777");
		fileIdList.put("1839960", "1839960");
		fileIdList.put("1840053", "1840053");
		fileIdList.put("1840935", "1840935");
		fileIdList.put("1842097", "1842097");
		fileIdList.put("1844687", "1844687");
		fileIdList.put("1845197", "1845197");
		
	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		try {
	    	parts = value.toString().split("\\t");
	    	
	    	if ( fileIdList.containsKey(parts[DaaSConstants.XML_REC_DW_FILE_ID_POS]) ) {
	    		currentKey.clear();
	    		lcatKey = "00"+parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
	    		currentKey.set(lcatKey.substring(lcatKey.length()-2,lcatKey.length()));
	    	
	    		currentValue.clear();
	    		currentValue.set(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + "\t" + 
	    			         	parts[DaaSConstants.XML_REC_TERR_CD_POS] + "\t" + 
	    			         	parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS] + "\t" + 
	    			         	parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0,4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4,6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6,8) + "\t" +
	    			         	parts[DaaSConstants.XML_REC_DW_FILE_ID_POS]);
	    		context.write(currentKey,currentValue);
	    	} else {
	    		return;
	    	}
	    	
	    } catch (Exception ex) {
	    	System.err.println("Error occured in FileIdCheckMapper:");
	    	ex.printStackTrace(System.err);
	    }

	}
	
}

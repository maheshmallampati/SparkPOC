package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.test.daas.driver.CompressGoldLayer;

public class CompressGoldLayerMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String[] parts;
	private Text keyOut = new Text();
	//private Text valueOut = new Text();
	private StringBuffer keyText = new StringBuffer();
	private String digits;
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
	    	parts = value.toString().split("\\t");

	    	if ( parts.length >= 7 ) {
	    		keyText.setLength(0);
	    		keyText.append(parts[DaaSConstants.XML_REC_FILE_TYPE_POS]);
	    		keyText.append(CompressGoldLayer.SEPARATOR_CHARACTER);
	    		keyText.append(parts[DaaSConstants.XML_REC_TERR_CD_POS]);
	    		keyText.append(CompressGoldLayer.SEPARATOR_CHARACTER);
	    		keyText.append(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
	    		keyText.append(CompressGoldLayer.SEPARATOR_CHARACTER);
	    		
	    		if ( parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].length() >=2 ) {
	    			digits = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].substring(parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].length()-2);
	    		} else if ( parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].length() >= 1 ) {
	    			digits = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].substring(parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].length()-1);
	    		} else {
	    			digits = "00";
	    		}
	    		
	    		keyText.append(digits);
	    		keyOut.set(keyText.toString());
	    		//valueOut.set(parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].substring(parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].length()-1) + "|" + parts[DaaSConstants.XML_REC_TERR_CD_POS] + "|" + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS] + "|" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
	    		
	    		context.write(keyOut, value);
	    	}

		} catch (Exception ex) {
	    	System.err.println("Error occured in CompressGoldLayerMapper:");
	    	ex.printStackTrace(System.err);			
		}
		
	}
}

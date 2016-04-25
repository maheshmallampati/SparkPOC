package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.util.HDFSUtil;

public class NewTLDDataHubReducer extends Reducer<Text, Text, NullWritable, Text> {

	private final static String SEPARATOR_CHARACTER = "\t";
	private final static String REC_POSTRN          = "TRN";
	private final static String REC_POSTRNOFFR      = "OFR";
	private final static String REC_POSTRNITM       = "ITM";
	private final static String REC_POSTRNITMOFFR   = "IOF";
	
	//private final static int LEN_REC_POS_TRN_TYP_TYPE_1     = 26;
	//private final static int LEN_REC_POS_TRN_ITM_TYP_TYPE_1 = 40;
	private final static int LEN_REC_TYPE_2                 = 16;
	
	private MultipleOutputs<NullWritable, Text> mos;
	
	//private String[] parts = null;
	//private String[] parts2 = null;

	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputPart1 = new StringBuffer();
	private StringBuffer outputPart2 = new StringBuffer();
		
	private Text outputValue = new Text();
	
	private String recPrefix;
	private String prefix;
	private String fileName;
	
	private String keyText;
	
	private String valueText;
	private int valueTextIdx;
	private boolean continueFl;
	
	private ArrayList<String> valueList = new ArrayList<String>();
	String multioutBaseOutputPath = "";
	
	
	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);
		
		multioutBaseOutputPath = context.getConfiguration().get("MULTIOUT_BASE_OUTPUT_PATH");
		

	}
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try{
		
		valueList.clear();
		
		for (Text value : values ) {
			valueList.add(value.toString());
		}
		
		
		outputValue.clear();
		outputKey.setLength(0);
		outputPart2.setLength(0);

		keyText = key.toString();
		
		recPrefix = keyText.substring(0,3);
		
		if ( recPrefix.equals(REC_POSTRN) ) {
			prefix = "POSTRN";
		} else if ( recPrefix.equals(REC_POSTRNOFFR) ) {
			prefix = "POSTRNOFFR";
		} else if ( recPrefix.equals(REC_POSTRNITM) ) {
		    prefix = "POSTRNITM";
		} else {
		    prefix = "POSTRNITMOFFR";
		}

		
//		fileName = String.valueOf(Integer.parseInt(keyText.substring(3,6))) + "_" + keyText.substring(6,16) + "_" + prefix;
		
		fileName = String.valueOf(Integer.parseInt(keyText.substring(3,6))) + "RxD126" + keyText.substring(6,16) ;
		
		outputKey.append(keyText.substring(16));
		outputKey.append(SEPARATOR_CHARACTER);
	
		if ( recPrefix.equals(REC_POSTRN) || recPrefix.equals(REC_POSTRNITM) ) {
			valueTextIdx = 0;
			continueFl = true;
			
			while ( valueTextIdx < valueList.size() && continueFl ) {
				valueText = valueList.get(valueTextIdx);
				if ( valueText.startsWith("2") ) {
					continueFl = false;
					outputPart2.append(SEPARATOR_CHARACTER);
					outputPart2.append(valueText.substring(2));
//					
//					if(key.toString().contains("POS0001:1001435871")){
//						System.out.println(" valueText " + valueText.toString());
//						System.out.println(" substring(3) " + valueText.substring(2));
//					}
				}
				valueTextIdx++;
			}
			
			if ( outputPart2.length() == 0 ) {
				for ( int fillIdx = 2; fillIdx <= LEN_REC_TYPE_2; fillIdx++ ) {
					outputPart2.append(SEPARATOR_CHARACTER);
				}
			}
		}
		
		
		valueTextIdx = 0;
		String[] temp;
		
		while ( valueTextIdx < valueList.size() ) {
			valueText = valueList.get(valueTextIdx);
	    	
			if ( (recPrefix.equals(REC_POSTRN) || recPrefix.equals(REC_POSTRNITM)) && valueText.startsWith("1") ) {
				
				outputPart1.setLength(0);
//				outputPart1.append(outputKey);
				outputPart1.append(valueText.substring(2));
				outputPart1.append(outputPart2);
				
//				System.out.println(" outpart2 "  + outputPart2.toString());
				
				outputValue.clear();
				outputValue.set(outputPart1.toString());
				
				
				temp =fileName.toString().split("RxD126");
				
				
//	    		mos.write(NullWritable.get(), outputValue, fileName);
	    		mos.write(HDFSUtil.replaceMultiOutSpecialChars(fileName),NullWritable.get(),outputValue,multioutBaseOutputPath+"/terr_cd="+temp[0]+"/pos_busn_dt="+temp[1]+"/"+HDFSUtil.replaceMultiOutSpecialChars(fileName));
				context.getCounter("COUNT", prefix).increment(1);
				
				
		    	
			} 
			
			valueTextIdx++;
		}	
	}catch(Exception ex){
		ex.printStackTrace();
	}
		}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
	
}
 
package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.test.daas.driver.GenerateSampleTLD;

public class VoiceDataReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;
	
	private final static String PIPE_DELIMITER		= "|";
	private final static String REC_HEADER 			= "HDR";
	private final static String REC_DETAIL 			= "DTL";
	
	private String keyText;
	private String valueText;
	private String[] parts;
	private String[] parts2;
	
	private String prefix;
	private String recType;
	
	private String voiceHdrTerrCd;
	private String voiceHdrStoreId;
	private String voiceHdrResponseId;
	private String voiceHdrVisitDate;
	private String voiceHdrVisitDateTime;
	private String voiceHdrAmountSpent;
	private String voiceHdrVendor;
	private String voiceHdrPOSArea;
	
	private String voiceDtlTerrCd;
	private String voiceDtlResponseId;
	private String voiceDtlQuestionId;
	private String voiceDtlAnswerId;
	private String voiceDtlComment;
	
	private ArrayList<String> voiceHeaderList = new ArrayList<String>();
	private ArrayList<String> voiceDetailList = new ArrayList<String>();
	private HashMap<String,String> voiceHeaderMap = new HashMap<String,String>();
	private HashMap<String,String> voiceDetailMap = new HashMap<String,String>();

	private Text outputValue = new Text();
	
	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		voiceHeaderList.clear();
		voiceDetailList.clear();
		voiceHeaderMap.clear();
		voiceDetailMap.clear();
		
		for (Text value : values ) {
			valueText = value.toString();
			
			recType = valueText.substring(0, 3);
			
			if ( recType.equals(REC_HEADER) ) {
				
				parts = valueText.split("\\|", -1);
				
				voiceHdrTerrCd			= parts[1];
				voiceHdrStoreId 		= parts[2];
				voiceHdrResponseId 		= parts[3];
				voiceHdrVisitDate 		= parts[4];
				voiceHdrVisitDateTime 	= parts[5];
				voiceHdrPOSArea 		= parts[6];
				voiceHdrVendor 			= parts[7];
				voiceHdrAmountSpent 	= parts[8];
				
				keyText = voiceHdrResponseId;
				valueText = voiceHdrTerrCd + PIPE_DELIMITER + voiceHdrStoreId + PIPE_DELIMITER + voiceHdrResponseId + PIPE_DELIMITER + voiceHdrVisitDate + PIPE_DELIMITER + voiceHdrVisitDateTime + PIPE_DELIMITER + voiceHdrPOSArea + PIPE_DELIMITER + voiceHdrVendor + PIPE_DELIMITER + voiceHdrAmountSpent;
				
				voiceHeaderMap.put(keyText, valueText);
				voiceHeaderList.add(valueText);
				
			} else if ( recType.equals(REC_DETAIL) ) {

				parts = valueText.split("\\|", -1);
				
				voiceDtlTerrCd 		= parts[1];
				voiceDtlResponseId 	= parts[2];
				voiceDtlQuestionId 	= parts[3];
				voiceDtlAnswerId 	= parts[4];
				voiceDtlComment 	= parts[5];
				
				if ( voiceDtlQuestionId.equalsIgnoreCase("R004000")) {
					keyText = voiceDtlResponseId;
					
					if (voiceDtlAnswerId.equalsIgnoreCase("1")) {
						valueText = "3";
					} else {
						valueText = voiceDtlAnswerId;
					}

					voiceDetailMap.put(keyText, valueText);
				
				} else {
				
					keyText = voiceDtlResponseId;
					valueText = voiceDtlTerrCd + PIPE_DELIMITER + voiceDtlResponseId + PIPE_DELIMITER + voiceDtlQuestionId + PIPE_DELIMITER + voiceDtlAnswerId + PIPE_DELIMITER + voiceDtlComment;
				
					voiceDetailList.add(valueText);
				}
			}
		}
		
		for ( String value : voiceHeaderList ) {
			prefix = "HEADER";
			
			parts = value.split("\\|", -1);
			
			voiceHdrTerrCd			= parts[0];
			voiceHdrStoreId 		= parts[1];
			voiceHdrResponseId 		= parts[2];
			voiceHdrVisitDate 		= parts[3];
			voiceHdrVisitDateTime 	= parts[4];
			voiceHdrPOSArea 		= parts[5];
			voiceHdrVendor 			= parts[6];
			voiceHdrAmountSpent 	= parts[7];
			
			if ( voiceDetailMap.containsKey(voiceHdrResponseId) ) {
				parts2 = voiceDetailMap.get(voiceHdrResponseId).split("\\|", -1);
				
				outputValue.clear();
				outputValue.set(voiceHdrTerrCd + PIPE_DELIMITER + voiceHdrStoreId + PIPE_DELIMITER + voiceHdrResponseId + PIPE_DELIMITER + voiceHdrVisitDate + PIPE_DELIMITER + voiceHdrVisitDateTime + PIPE_DELIMITER + parts2[0] + PIPE_DELIMITER + voiceHdrVendor + PIPE_DELIMITER + voiceHdrAmountSpent);
					
			    mos.write(prefix, NullWritable.get(), outputValue, prefix);

			    context.getCounter("DaaS","Header Out").increment(1);
			}
		}
		
		for ( String value : voiceDetailList ) {
			prefix = "DETAIL";
			
			parts = value.split("\\|", -1);
			
			voiceDtlTerrCd		= parts[0];
			voiceDtlResponseId	= parts[1];
			voiceDtlQuestionId	= parts[2];
			voiceDtlAnswerId	= parts[3];
			voiceDtlComment		= parts[4];
			
			if ( voiceHeaderMap.containsKey(voiceDtlResponseId) ) {
				parts2 = voiceHeaderMap.get(voiceDtlResponseId).split("\\|", -1);
				
				outputValue.clear();
				outputValue.set(voiceDtlTerrCd + PIPE_DELIMITER + parts2[1] + PIPE_DELIMITER + voiceDtlResponseId + PIPE_DELIMITER + parts2[3] + PIPE_DELIMITER + parts2[4] + PIPE_DELIMITER + voiceDtlQuestionId + PIPE_DELIMITER + voiceDtlAnswerId + PIPE_DELIMITER + voiceDtlComment);
				
				mos.write(prefix, NullWritable.get(), outputValue, prefix);

			    context.getCounter("DaaS","Detail Out").increment(1);
			}
		}
	}
	
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {
		mos.close();
	}
}

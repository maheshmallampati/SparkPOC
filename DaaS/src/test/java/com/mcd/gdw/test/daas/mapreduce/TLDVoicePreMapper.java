package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TLDVoicePreMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String[] parts;
	private Text mapKey = new Text();
	private Text mapValue = new Text();
	
	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputValue = new StringBuffer();
	
	private String terrCd;
	private String calDt;
	private String orderTs; 
	private String regNum;
	private String orderNum;
	private String amt;
	
	private String splitFileName; 
	
	@Override
	public void setup(Context context) {

		Path path;
		
		try {
	    	path = ((FileSplit) context.getInputSplit()).getPath();
	    	splitFileName = path.getName();
	    	
	    	parts = splitFileName.split("\\_");
	    	
	    	if ( parts[1].equals("US") ) {
	    		terrCd = "840";
	    	} else {
	    		terrCd = "0";
	    	}

	    	calDt = parts[2].substring(0, 4) + "-" + parts[2].substring(4, 6) + "-" + parts[2].substring(6,8);
	    	
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		parts = value.toString().split("\\|");
		
		if ( splitFileName.toUpperCase().contains("HEADER") && parts.length >= 35 ) {
			getVoiceHeaderData(context);
		} else if ( splitFileName.toUpperCase().contains("DETAIL") && parts.length >= 3 ) {
			getVoiceDetailData(context);
		}

	}

	private void getVoiceHeaderData(Context context) {
		
		try {
			orderTs = timestampFromSMGFormat(parts[34]);

			if ( orderTs.substring(0, 7).equals("2015-09") ) {
				try {
					String[] tmpParts = ("0" + parts[1].trim() + ".00").split("\\.");
					String dollars = String.valueOf(Integer.parseInt(tmpParts[0]));
					String cents = String.format("%02d",(Integer.parseInt(tmpParts[1])));
				
					amt = dollars + "." + cents;
				} catch (Exception ex) {
					amt = "0.00";
				}
			
				try {
					regNum = String.valueOf(Integer.parseInt(parts[30]));
				
				} catch (Exception ex ) {
					regNum = "0";
				}
			
				try {
					orderNum = String.valueOf(Integer.parseInt(parts[32]));
					
					if ( orderNum.length() > 2 ) {
						orderNum = String.valueOf(Integer.parseInt(orderNum.substring(orderNum.length()-2)));
					}
				} catch (Exception ex) {
					orderNum = "0";
				}
			
				outputKey.setLength(0);
				outputKey.append(calDt);
			
				outputValue.setLength(0);
				outputValue.append("HDR");
				outputValue.append("\t");
				outputValue.append(terrCd);
				outputValue.append("\t");
				outputValue.append(parts[2]); //Store
				outputValue.append("\t");
				outputValue.append(orderTs.substring(0, 10));
				outputValue.append("\t");
				outputValue.append(parts[0]);  //Response ID
				outputValue.append("\t");
				outputValue.append(orderTs);
				outputValue.append("\t");
				outputValue.append(regNum);
				outputValue.append("\t");
				outputValue.append(orderNum);
				outputValue.append("\t");
				outputValue.append(amt);
				outputValue.append("\t");
				outputValue.append(timestampFromSMGFormat(parts[3])); //Survey Date

				mapKey.clear();
				mapKey.set(outputKey.toString());

				mapValue.clear();
				mapValue.set(outputValue.toString());

				context.write(mapKey, mapValue);	
				context.getCounter("DaaS","VOICE HEADER").increment(1);
			} else {
				context.getCounter("DaaS","VOICE HEADER SKIPPED").increment(1);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void getVoiceDetailData(Context context) {

		try {
		
			if ( parts.length >=3  && parts[1].equals("R007000") && (parts[2].equals("1") || parts[2].equals("2") )) {
				outputKey.setLength(0);
				outputKey.append(calDt);
		
				outputValue.setLength(0);
				outputValue.append("DTL");
				outputValue.append("\t");
				outputValue.append(parts[0]);  //Response ID
				outputValue.append("\t");
				outputValue.append(parts[2]);

				mapKey.clear();
				mapKey.set(outputKey.toString());

				mapValue.clear();
				mapValue.set(outputValue.toString());

				context.write(mapKey, mapValue);	
				context.getCounter("DaaS","VOICE DETAIL").increment(1);
			} else {
				context.getCounter("DaaS","VOICE DETAIL SKIPPED").increment(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private String timestampFromSMGFormat(String timestampIn) {

		String[] dateParts1;
		String[] dateParts2;
		int hour;
		String retTimestamp = "";
		
		try {
			dateParts1 = timestampIn.split(" ");
			dateParts2 = dateParts1[0].split("/");
		
			retTimestamp = dateParts2[2] + "-" + String.format("%02d", Integer.parseInt(dateParts2[0])) + "-" + String.format("%02d", Integer.parseInt(dateParts2[1]));
			
			dateParts2 = dateParts1[1].split(":");
		
			hour = Integer.parseInt(dateParts2[0]);
			if ( dateParts1[2].equalsIgnoreCase("PM") && hour < 12 ) {
				hour += 12;
			}

			retTimestamp += " " + String.format("%02d", hour) + ":" + String.format("%02d", Integer.parseInt(dateParts2[1])) + ":" + String.format("%02d", Integer.parseInt(dateParts2[2]));
		} catch (Exception ex ) {
			retTimestamp = "1955-04-15 00:00:00";
		}
		
		return(retTimestamp);
	}
}

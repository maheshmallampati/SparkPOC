package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.test.daas.driver.GenerateSampleReport;

public class SampleReportFormatReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private final static int LEN_REC_TYPE_2                 = 16;
	
	private MultipleOutputs<NullWritable, Text> mos;

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
	
	private String nullSub;
	
	private ArrayList<String> valueList = new ArrayList<String>();

	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

		nullSub = context.getConfiguration().get(GenerateSampleReport.NULL_SUB_CHARACTER);
		
		if ( nullSub.equals(GenerateSampleReport.NULL_SUB_CHARACTER_BLANK_VALUE) ) {
			nullSub = "";
		}

	}
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		valueList.clear();
		
		for (Text value : values ) {
			valueList.add(value.toString());
		}
		
		
		outputValue.clear();
		outputKey.setLength(0);
		outputPart2.setLength(0);

		keyText = key.toString();
		
		recPrefix = keyText.substring(0,3);
		
		if ( recPrefix.equals(GenerateSampleReport.REC_POSTRN) ) {
			prefix = "POSTRN";
		} else if ( recPrefix.equals(GenerateSampleReport.REC_POSTRNOFFR) ) {
			prefix = "POSTRNOFFR";
		} else if ( recPrefix.equals(GenerateSampleReport.REC_POSTRNITM) ) {
		    prefix = "POSTRNITM";
		} else {
		    prefix = "POSTRNITMOFFR";
		}

		fileName = String.valueOf(Integer.parseInt(keyText.substring(3,6))) + "_" + keyText.substring(6,16) + "_" + prefix;

		outputKey.append(String.valueOf(Integer.parseInt(keyText.substring(3,6))));
		outputKey.append(GenerateSampleReport.SEPARATOR_CHARACTER);
		outputKey.append(keyText.substring(6,16));
		outputKey.append(GenerateSampleReport.SEPARATOR_CHARACTER);
		outputKey.append(keyText.substring(16));
		outputKey.append(GenerateSampleReport.SEPARATOR_CHARACTER);
	
		if ( recPrefix.equals(GenerateSampleReport.REC_POSTRN) || recPrefix.equals(GenerateSampleReport.REC_POSTRNITM) ) {
			valueTextIdx = 0;
			continueFl = true;
			
			while ( valueTextIdx < valueList.size() && continueFl ) {
				valueText = valueList.get(valueTextIdx);
				if ( valueText.startsWith("2") ) {
					continueFl = false;
					outputPart2.append(GenerateSampleReport.SEPARATOR_CHARACTER);
					outputPart2.append(valueText.substring(2)); //changed from 3 to 2 because the first digit was getting truncated
				}
				valueTextIdx++;
			}
			
			if ( outputPart2.length() == 0 ) {
				for ( int fillIdx = 1; fillIdx <= LEN_REC_TYPE_2; fillIdx++ ) {
					outputPart2.append(GenerateSampleReport.SEPARATOR_CHARACTER);
					outputPart2.append(nullSub);
				}
			}
		}
		
		valueTextIdx = 0;
		while ( valueTextIdx < valueList.size() ) {
			valueText = valueList.get(valueTextIdx);
	    	
			if ( (recPrefix.equals(GenerateSampleReport.REC_POSTRN) || recPrefix.equals(GenerateSampleReport.REC_POSTRNITM)) && valueText.startsWith("1") ) {
				
				outputPart1.setLength(0);
				outputPart1.append(outputKey);
				outputPart1.append(valueText.substring(2));
				outputPart1.append(outputPart2);
				
				outputValue.clear();
				outputValue.set(outputPart1.toString());
	    		mos.write(NullWritable.get(), outputValue, fileName);
				context.getCounter("COUNT", prefix).increment(1);
		    	
			} else if ( recPrefix.equals(GenerateSampleReport.REC_POSTRNOFFR) || recPrefix.equals(GenerateSampleReport.REC_POSTRNITMOFFR) ) {
 
				outputPart1.setLength(0);
				outputPart1.append(outputKey);
				outputPart1.append(valueText);
				
				outputValue.clear();
		    	outputValue.set(outputPart1.toString());
			    mos.write(NullWritable.get(), outputValue, fileName);
				context.getCounter("COUNT",prefix).increment(1);
			}
			
			valueTextIdx++;
		}
	    
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}

}

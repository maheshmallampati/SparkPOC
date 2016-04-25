package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TLDFormatReducer extends Reducer<Text, Text, NullWritable, Text> {

	private final static String REC_TLD_ITM = "ITM";
	private final static String REC_MENU    = "MNU";
	private final static String PREFIX_TLDTRN_ITM  = "TLDTRNITM";
	private final static String PREFIX_MENU        = "MENUITM";
	
	private MultipleOutputs<NullWritable, Text> mos;
	
	//private static String[] parts = null;
	
	private String prefix;
	private String dt;
	private String terrCd;
	private String fileName;
	
	private String keyValue;
	
	private Text outputValue = new Text();

	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputTextValue = new StringBuffer();
	
	private int storeCount;
	
	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		try {

			outputKey.setLength(0);

			//parts = key.toString().split("\\|");
			
			keyValue = key.toString();
			terrCd = keyValue.substring(3, 6) + "_";
			
			if ( keyValue.startsWith(REC_TLD_ITM) ) {
				prefix = PREFIX_TLDTRN_ITM;
				dt = keyValue.substring(6, 16) + "_";
				fileName = terrCd + dt + prefix;
				
				outputKey.append(keyValue.substring(16));
				outputKey.append("\t");
				
				for (Text value : values ) {
					
					outputTextValue.setLength(0);
					outputTextValue.append(outputKey);
					outputTextValue.append(value.toString());
					outputValue.clear();
					outputValue.set(outputTextValue.toString());
					mos.write(NullWritable.get(), outputValue, fileName);

				}
				
			} else if ( keyValue.startsWith(REC_MENU) ) {
				prefix = PREFIX_MENU;
				dt = "dt_";
				fileName = terrCd + prefix;

				outputKey.append(keyValue.substring(6));
				outputKey.append("\t");
				
				storeCount=0;

				for (Text value : values ) {
					storeCount+= Integer.parseInt(value.toString());
				}	
			    outputKey.append(String.valueOf(storeCount));
				outputValue.clear();
				outputValue.set(outputKey.toString());
				mos.write(NullWritable.get(), outputValue, fileName);
			}
			
			/*
			prefix = parts[0];
			terrCd = parts[1] + "_";
			
			if ( prefix.equalsIgnoreCase("TLDTRNITM") ) {
				outputKey.append(parts[3]);
				outputKey.append("\t");
				
				dt = parts[2].substring(0, 10) + "_";
				
			} else if ( prefix.equalsIgnoreCase("MENUITM") ) { 
				outputKey.append(parts[2]);
				outputKey.append("\t");
				outputKey.append(parts[3]);
				outputKey.append("\t");

				dt = "dt_";
			}
			*/

			/*
			if ( prefix.equalsIgnoreCase("TLDTRNITM") ) {
				
				for (Text value : values ) {
			    	parts = value.toString().split("\\|",-1);

			    	outputTextValue.setLength(0);
			    	
			    	for ( int idx2=0; idx2 < parts.length; idx2++ ) {
			    		if ( idx2 > 0 ) {
			    			outputTextValue.append("\t");
			    		}
			    		outputTextValue.append(parts[idx2]);
			    	}
			    	
			    	outputValue.set(outputKey.toString() + outputTextValue.toString() );
		    		mos.write(NullWritable.get(), outputValue, terrCd + dt + prefix);

				}
				
			} else if ( prefix.equalsIgnoreCase("MENUITM") ) {
				
				storeCount=0;

				for (Text value : values ) {
					storeCount+= Integer.parseInt(value.toString());
				}	
			    
				outputValue.set(outputKey.toString() + String.valueOf(storeCount) );
		    	mos.write(NullWritable.get(), outputValue, terrCd + prefix);			
			}
			*/
			
		} catch(Exception ex) {
			System.err.println("outputKey=" + key.toString());
		}
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
}

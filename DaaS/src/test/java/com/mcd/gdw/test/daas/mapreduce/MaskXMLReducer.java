package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MaskXMLReducer extends Reducer<Text, Text, Text, Text> {
	
	private String zipOutputFileName;
	private String[] keyParts;
	private Text newKey = new Text();
	private Text newValue;
	private String timestamp = null;
	private int idx;
	private String fileSubType;
	
	private MultipleOutputs<Text, Text> mos;

	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
			mos.close();
			
	}

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);
		
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		timestamp = sdf.format(calendar.getTime());

	}
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		keyParts = key.toString().split("\\t");
		
		zipOutputFileName = "POS_XML_" + keyParts[0] + "_" + keyParts[1] + "_" + keyParts[2] + "." + timestamp + ".zip"; 

		Iterator<Text> nextValue = values.iterator();
		while ( nextValue.hasNext() ) {
			newValue = nextValue.next();
			
			idx = 1;
			while ( newValue.charAt(idx) != '<' ) {
				idx++;
			}
			
			if ( newValue.charAt(idx+1) == 'T' ) {
				fileSubType = "STLD";

			} else if ( newValue.charAt(idx+1) == 'D' && newValue.charAt(idx+2) == 'e' ) {
				fileSubType = "DetailedSOS";

			} else if ( newValue.charAt(idx+1) == 'M' ) {
				fileSubType = "MenuItem";

			} else if ( newValue.charAt(idx+1) == 'S' ) {
				fileSubType = "SecurityData";

			} else if ( newValue.charAt(idx+1) == 'D' && newValue.charAt(idx+2) == 'o' ) {
				fileSubType = "Store-Db";

			} else if ( newValue.charAt(idx+1) == 'P' ) {
				fileSubType = "Product-Db";
			} else {
				fileSubType = "ERR";
			}
			
			newKey.clear();
			newKey.set("POS_" + fileSubType + "_" + keyParts[0] + "_" + keyParts[1] + "_" + keyParts[2] + "." + timestamp + ".xml");
			
			mos.write("ZIPOUTPUT", newKey, newValue,  zipOutputFileName);

		}
		
		context.getCounter("DaaS Counters", "Reduce Rows").increment(1);
		
		
	}
}

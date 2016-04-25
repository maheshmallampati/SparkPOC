package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.test.daas.driver.GenerateSampleTLD;

public class SampleTLDReducer extends Reducer<Text, Text, NullWritable, Text>{

	private MultipleOutputs<NullWritable, Text> mos;
	
	private StringBuffer outputText = new StringBuffer();
	private Text outputValue = new Text();
	private ArrayList<String> headerList = new ArrayList<String>();
	private ArrayList<String> detailList = new ArrayList<String>();
	private HashMap<String,String> menuMap = new HashMap<String,String>();
	
	private String[] parts;
	private String keyText;
	private String valueText;
	
	private String menuItemPrice;
	
	private StringBuffer keyTextOut = new StringBuffer();
	
	private String recType;
	private String terrCd;
	private String posBusnDt;
	private String lgcyLclRfrDefCd;

	private String fileName;
	
	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		menuMap.clear();
		headerList.clear();
		detailList.clear();
		
		keyText = key.toString();
		terrCd = Integer.toString(Integer.parseInt(keyText.substring(0, 3)));
		posBusnDt = keyText.substring(3, 13);
		lgcyLclRfrDefCd = keyText.substring(13);
		
		keyTextOut.setLength(0);
		keyTextOut.append(terrCd);
		keyTextOut.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
		keyTextOut.append(lgcyLclRfrDefCd);
		keyTextOut.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
		keyTextOut.append(posBusnDt);
		keyTextOut.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
		
		
		for (Text value : values ) {
			valueText = value.toString();
			
			recType = valueText.substring(0, 3);
			
			if ( recType.equals(GenerateSampleTLD.REC_TYPE_MENU) ) {
				parts = valueText.split(GenerateSampleTLD.SEPARATOR_CHARACTER);
				menuMap.put(parts[1], parts[2]);
			} else if ( recType.equals(GenerateSampleTLD.REC_TYPE_HDR) ) {
				headerList.add(keyTextOut.toString() + valueText.substring(3));
			} else if ( recType.equals(GenerateSampleTLD.REC_TYPE_DTL) ) {
				detailList.add(valueText.substring(3));
			}
		}

		fileName = "HDR_" + posBusnDt;
		
		for ( String value : headerList ) {
			outputValue.clear();
			outputValue.set(value);
			
		    mos.write(NullWritable.get(), outputValue, fileName);

		    context.getCounter("DaaS","Header Out").increment(1);
		}

		fileName = "DTL_" + posBusnDt;
		
		for ( String value : detailList ) {
			parts = value.split(GenerateSampleTLD.SEPARATOR_CHARACTER);
			
			if ( menuMap.containsKey(parts[4]) ) {
				menuItemPrice = menuMap.get(parts[4]);
			} else {
				menuItemPrice = "***";
			}
			
			outputValue.clear();
			outputValue.set(keyTextOut.toString() + value + GenerateSampleTLD.SEPARATOR_CHARACTER + menuItemPrice);
			
		    mos.write(NullWritable.get(), outputValue, fileName);
		    
			context.getCounter("DaaS","Detail Out").increment(1);
		}
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
	
}

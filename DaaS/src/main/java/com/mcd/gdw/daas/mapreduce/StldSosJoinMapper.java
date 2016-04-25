package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * 
 * @author Sateesh Pula
 *
 * Mapper class to join STLD/HDR* records and SOS records to get the final TDA format sales data.
 * The Mapper uses GDWLGCYLCLRFRDEFCD, BUS_DT, POS_TYPE, ORDER_NBR as key. It prepends each STLD record 
 * with 'STLD' and SOS records with 'SOS'. This field is used to distinguish between STLD and SOS records
 * on the reducer side. The reducer receives both STLD and SOS records for a given key. 
 */
public class StldSosJoinMapper extends Mapper<LongWritable, Text,Text,Text> {

	//indexes for STLD/HDR keys
	public static int STLD_GDWLGCYLCLRFRDEFCD_INDX 		= 1;
	public static int STLD_BUS_DT_INDX 					= 4;
	public static int STLD_POS_TYPE_INDX				= 7;
	public static int STLD_ORDER_NBR_INDX 				= 3;
	public static int STLD_ISOCODE_INDX 				= 15;
	
	//indexes for SOS keys
	public static int SOS_GDWLGCYLCLRFRDEFCD_INDX 		= 0;
	public static int SOS_BUS_DT_INDX 					= 1;
	public static int SOS_POS_TYPE_INDX					= 3;
	public static int SOS_ORDER_NBR_INDX 				= 4;
	public static int SOS_ISOCODE_INDX 					= 7;
	
	//make STLD as the left table in the join
	private String STLD_FILE_NM  = "STLD";
	
	private String INPUT_SPLIT_PATH;
	
	Text newKey   = new Text();
	Text newValue = new Text();
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		InputSplit split = context.getInputSplit();
		INPUT_SPLIT_PATH = ((FileSplit) split).getPath().toString();
		System.out.println(" file name " + ((FileSplit) split).getPath().getName());
	}



	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		
		StringBuffer keybf = new StringBuffer();
		
		String[] values = value.toString().split("\\|");
		
		newKey.clear();
		newValue.clear();
		
		String newValuestr = "";
		
		//processing stld data
		if(INPUT_SPLIT_PATH.toUpperCase().contains(STLD_FILE_NM)){

			//prepare HDR record key
			keybf.append(values[STLD_GDWLGCYLCLRFRDEFCD_INDX]).append("\t");
			keybf.append(values[STLD_BUS_DT_INDX]).append("\t");
			keybf.append(values[STLD_POS_TYPE_INDX]).append("\t");
			keybf.append(values[STLD_ORDER_NBR_INDX]).append("\t");
			keybf.append(values[STLD_ISOCODE_INDX]);
			
			
			//prepend STLD to the value of the record. this will help identify the record in the reducer
			newValuestr  =  "STLD"+"|"+value.toString();
//			if(keybf.toString().split("\t")[3].equals("999228952")){
			//increment the count of number of STLD/HDR records
			context.getCounter("Count", "NumofSTLD").increment(1);
//			}
			
		
			
		}else{//processing sos data
			
			//prepare SOS record key
			keybf.append(values[SOS_GDWLGCYLCLRFRDEFCD_INDX]).append("\t");
			keybf.append(values[SOS_BUS_DT_INDX]).append("\t");
			keybf.append(values[SOS_POS_TYPE_INDX]).append("\t");
			keybf.append(values[SOS_ORDER_NBR_INDX]).append("\t");
			keybf.append(values[SOS_ISOCODE_INDX]);
			
					
			//prepent SOS to the value. this will help identify the record in the reducer
			newValuestr  =  "SOS"+"|"+value.toString();
//			if(keybf.toString().split("\t")[3].equals("999228952")){
			//increament the count of number of SOS records
			context.getCounter("Count", "NumofSOS").increment(1);
//			}
			
			
		}
		newKey.set(keybf.toString());
		newValue.set(newValuestr);
		
//		if(keybf.toString().split("\t")[3].equals("999228952")){
//			System.out.println( " keybf : "+keybf.toString());
			//output new key and new value
			context.write(newKey, newValue);
			
//		}
				
		
		
		
	}
	
	

}

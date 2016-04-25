package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.RecordUtil;

/**
 * 
 * @author Sateesh Pula
 * Reducer to join the HDR and SOS records. The reducer receives STLD and SOS records for a given key.
 * It distinguishes each value by the first column which is either 'STLD' or 'SOS'. As it reads the values,
 * it creates two lists. One to hold STLD records and one to hold SOS records. Once all the records are read,
 * it loops thru the STLD records and create a final output record with STLD fields and SOS fields. 
 * If there are no SOS records, it uses default values for SOS.
 */
public class StldSosJoinReducer extends Reducer<Text,Text,NullWritable,Text>{
	
	//HDR file indexes
	public static int STLD_STORENBR_INDX 		= 0;
	public static int STLD_REG_INDX 			= 1;
	public static int STLD_LCAT_INDX 			= 2;
	public static int STLD_ORDER_NUM_INDX 		= 3;
	public static int STLD_BUS_DT_INDX 			= 4;
	public static int STLD_ORDER_DT_INDX 		= 5;
	public static int STLD_ORDER_TIME_INDX 		= 6;
	public static int STLD_POS_TYPE_INDX		= 7;
	public static int STLD_DLVR_METH_INDX 		= 8;
	public static int STLD_TRAN_TYPE_INDX 		= 9;
	public static int STLD_DISC_AMT_INDX 		= 10;
	public static int STLD_PROMO_AMT_INDX 		= 11;
	public static int STLD_TOTAL_AMT_INDX 		= 12;
	public static int STLD_MENUPRICEBASIS_INDX 	= 13;
	public static int STLD_PYMT_METH_INDX 		= 14;
	public static int STLD_ISOCD_INDX 			= 15;
	
	
	
	//SOS file indexes
	public static int SOS_LCAT_INDX 		= 0;
	public static int SOS_BUS_DT_INDX 		= 1;
	public static int SOS_POS_TYPE_CD_INDX	= 2;
	public static int SOS_POS_TYPE_INDX		= 3;
	public static int SOS_ORDER_KEY_INDX 	= 4;
	public static int SOS_TOTAL_TIME_INDX	= 5;
	public static int SOS_HELD_TIME_INDX 	= 6;
	
	//list to hold HDR records
	private List<String[]> tableRecordsOne;
	//list to hold SOS records
	private List<String[]> tableRecordsTwo;
	

	  private MultipleOutputs<NullWritable, Text> mos;
	  

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		  mos.close();
	}


	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	String ts = DaaSConstants.SDF_yyyyMMddHHmmssSSSnodashes.format(new Date(System.currentTimeMillis()));

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		tableRecordsOne = new ArrayList<String[]>();
		tableRecordsTwo = new ArrayList<String[]>();
		
//		System.out.println(" key : " + key.toString());
		
		String[] valuesarray;
		String[] stldvaluesarraynew = null;
		String[] sosvaluesarraynew	= null;
		
		Text joinoutput = new Text();
		
		StringBuffer moskey = new StringBuffer();
		try{
			for (Text valueText :values) {// Create table one table two records lists
				if (valueText != null) {
					valuesarray = valueText.toString().split("\\|");
					
					//check if the first record value start with STLD at index 0. This identifies the record as HDR record. 
					//otherwise, it is SOS record
					if("STLD".equalsIgnoreCase(valuesarray[0])){
						//initialize new array that will hold the actual values after removing STLD
						if(stldvaluesarraynew == null)
							stldvaluesarraynew = new String[valuesarray.length - 1];
						else
							RecordUtil.clearContents(stldvaluesarraynew);
						
						//copy the values to the new array starting from index 1
						System.arraycopy(valuesarray, 1, stldvaluesarraynew, 0,valuesarray.length - 1);
						
						tableRecordsOne.add(stldvaluesarraynew); // HDR records
					}else{
						//initialize new array that will hold the actual values after removing SOS
						if(sosvaluesarraynew == null)
							sosvaluesarraynew = new String[valuesarray.length - 1];
						else
							RecordUtil.clearContents(sosvaluesarraynew);
						
						//copy the values to the new array starting from index 1
						System.arraycopy(valuesarray, 1, sosvaluesarraynew, 0,valuesarray.length - 1);
						
						tableRecordsTwo.add(sosvaluesarraynew);//SOS records
					}
				}
			}
			
			//array to hold the output values after the join
			String[] outputarray = new String[17];
			
			if(tableRecordsOne == null || tableRecordsOne.isEmpty() ){
				context.getCounter("Count", "tableRecordsOneEmpty").increment(1);
			}
			if(tableRecordsOne != null && !tableRecordsOne.isEmpty() ){ // check if HDR list has entries
				
				context.getCounter("Count", "tableRecordsOneNotEmpty").increment(1);
				
				if(tableRecordsTwo != null && !tableRecordsTwo.isEmpty()){
					context.getCounter("Count", "tableRecordsTwoEmpty").increment(1);
				}
				if(tableRecordsTwo != null && !tableRecordsTwo.isEmpty()){//check if SOS list has entries
					
					context.getCounter("Count", "tableRecordsTwoNotEmpty").increment(1);
					
					for(String[] recordone:tableRecordsOne){//for every HDR record, find if there is/are SOS record(s)
						for(String[] recordtwo:tableRecordsTwo){//loop thru SOS records
							//when a match is found, populate the output array
//							outputarray[0]  = recordone[0]; //HDR:store id
							outputarray[0]  = recordone[1]; //HDR:gdwLgcyLclRfrDefCd
							outputarray[1]  = recordone[2]; //HDR:reg
							outputarray[2]  = recordone[3]; //HDR:ordernum
							outputarray[3]  = recordone[4]; //HDR:busndt
							outputarray[4]  = recordone[5]; //HDR:orderdt
							outputarray[5]  = recordone[6]; //HDR:ordertime
							outputarray[6]  = recordone[7]; //HDR:postype
							outputarray[7]  = recordone[8]; //HDR:dlvrmeth
							outputarray[8]  = recordone[9]; //HDR:trantype
							outputarray[9]  = recordone[10]; //HDR:discamt
							outputarray[10] = recordone[11];//HDR:promoamt		
							outputarray[11] = recordone[12];//HDR:totalamt
							outputarray[12] = recordtwo[6]; //SOS:heldtime
							outputarray[13] = recordtwo[5]; //SOS:totaltime
							outputarray[14] = recordone[13];//HDR:menupricebasis
							outputarray[15] = recordone[14];//HDR:pymtmeth
							outputarray[16] = recordone[15];//HDR:ISO Code
//							outputarray[17] = "MF";//Match Found
							
							joinoutput.clear();
							//set the output
							joinoutput.set(RecordUtil.createRecord(outputarray,"|"));
							
							String lastDigit = recordone[1].substring(recordone[1].length()-1);
							String terr_cd   =  recordone[1];
							
							moskey.setLength(0);
							moskey.append("SALES").append(DaaSConstants.SPLCHARTILDE_DELIMITER);
							moskey.append(recordone[15]).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
							moskey.append(recordone[4]).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
//							if(Integer.parseInt(terr_cd) ==  156){
//								moskey.append(terr_cd);
//							}else{
								moskey.append(lastDigit);
//							}
							
							//write the output
//							context.write(NullWritable.get(), joinoutput);
//							mos.write("SALES"+recordone[4]+lastDigit,NullWritable.get(),joinoutput);
							if(Integer.parseInt(terr_cd) != 156){
								mos.write(moskey.toString(),NullWritable.get(),joinoutput);
							}else{
								mos.write(moskey.toString(),NullWritable.get(),joinoutput,"/daas/tdaextracts/salesandpmix_CHINAAPT/GTDA_Dly_Sls_CN_"+recordone[0]+"_"+recordone[4]+"."+ts+".psv");
							}
//							
//							//write the output
////							context.write(NullWritable.get(), joinoutput);
//							mos.write("SALES"+recordone[4],NullWritable.get(),joinoutput);
//							
							//increment the match count
							context.getCounter("Count", "MatchFound").increment(1);
							
						}
					}
				}else{
					for(String[] recordone:tableRecordsOne){
						
						
						//when a match is not found, populate the output array with empty for sos valuess
//						outputarray[0]  = recordone[0]; //HDR:store id
						outputarray[0]  = recordone[1]; //HDR:gdwLgcyLclRfrDefCd
						outputarray[1]  = recordone[2]; //HDR:reg
						outputarray[2]  = recordone[3]; //HDR:ordernum
						outputarray[3]  = recordone[4]; //HDR:busndt
						outputarray[4]  = recordone[5]; //HDR:orderdt
						outputarray[5]  = recordone[6]; //HDR:ordertime
						outputarray[6]  = recordone[7]; //HDR:postype
						outputarray[7]  = recordone[8]; //HDR:dlvrmeth
						outputarray[8]  = recordone[9]; //HDR:trantype
						outputarray[9]  = recordone[10]; //HDR:discamt
						outputarray[10] = recordone[11];//HDR:promoamt		
						outputarray[11] = recordone[12];//HDR:totalamt
						outputarray[12] = "0000"; //SOS:heldtime -- empty missing
						outputarray[13] = "0000"; //SOS:totaltime -- empty missing
						outputarray[14] = recordone[13];//HDR:menupricebasis
						outputarray[15] = recordone[14];//HDR:pymtmeth
						outputarray[16] = recordone[15];//HDR:ISO Code
//						outputarray[17] = "NMF";//No Match Found
						
						joinoutput.clear();
						//set the output
						joinoutput.set(RecordUtil.createRecord(outputarray,"|"));
						
						
						
						
						String lastDigit = recordone[1].substring(recordone[1].length()-1);
						
						moskey.setLength(0);
						moskey.append("SALES").append(DaaSConstants.SPLCHARTILDE_DELIMITER);
						moskey.append(recordone[15]).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
						moskey.append(recordone[4]).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
						moskey.append(lastDigit);
						//write the output
//						context.write(NullWritable.get(), joinoutput);
//						mos.write("SALES"+recordone[4]+lastDigit,NullWritable.get(),joinoutput);
//						mos.write(moskey.toString(),NullWritable.get(),joinoutput);
						if(Integer.parseInt(recordone[15]) != 156){
							mos.write(moskey.toString(),NullWritable.get(),joinoutput);
						}else{
							mos.write("SALES"+DaaSConstants.SPLCHARTILDE_DELIMITER+recordone[15]+DaaSConstants.SPLCHARTILDE_DELIMITER+recordone[4],NullWritable.get(),joinoutput,"/daas/tdaextracts/salesandpmix_CHINAAPT/"+recordone[4]+"/GTDA_Dly_Sls_CN_"+recordone[0]+"_"+recordone[4]+".201503227160015.psv");
						}
						
						
//						mos.write("HDRSOSNOMatch",NullWritable.get(), RecordUtil.createRecord(recordone));
						//increment counter for HDR records with no matching SOS records
						context.getCounter("Count", "MatchNotFound").increment(1);
					}
						
				}
				
			}
		}catch(Exception ex){
			ex.printStackTrace();
			System.out.println("exception ex "+ ex.getMessage());
			context.getCounter("Count", "Exception").increment(1);
			 throw new InterruptedException(ex.toString());
		}
		
		
		
	}
	

	
	

}

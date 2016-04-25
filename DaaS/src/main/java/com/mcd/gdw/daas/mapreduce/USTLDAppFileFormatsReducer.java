package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class USTLDAppFileFormatsReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;
	
	public void setup(Context context) {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}
	
	private final static String COMMA_DELIMITER  	= ",";
	private final static String REC_MENU 			= "MNU";
	private final static String REC_TRANS 			= "TRN";
	private final static String REC_TRANSITEM 		= "ITM";
	
	private String keyText;
	private String recPrefix;
	private String prefix;
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//Menu
		String lgcyLclRfrDefCd = "";
		String posBusnDt = "";
		String posItmMenuItmDesc = "";
		String posItmMenuItmUntPrcAm = "";
		String posItmMenuItmId = "";
		String posItmMenuItmIndex = "";
		
		//Transaction
		String posTrnDscntTypCd = "";
		String posPrdDlvrMethCd = "";
		String posTrnSeqNu = "";
		String posTrnOrdTs = "";
		String posOrdKeyCd = "";
		String posTrnNumber = "";
		String posTrnTotQt = "";
		String posTrnTotTaxAm = "";
		String posTrnTotNetAm = "";
		String posPymtType = "";
		String posTrnTotBDAmt = "";
		String dyptIdNu = "";
		String dyOfCalWkDs = "";
		
		//Transaction Item
		String posItmQtyText = "";
		String posItmPrmoQtyText = "";
		String posItmPrmo = "";
		String posTrnItmIndex = "";
		
		String parts[] = null;
		
		boolean isMenu = false;
		boolean isTrans = false;
		boolean isTransItem = false;
		
		for (Text value : values ) {
			
			isMenu = false;
			isTrans = false;
			isTransItem = false;
			
			parts = value.toString().split(COMMA_DELIMITER);
			
			keyText = key.toString();
			
			recPrefix = keyText.substring(0,3);
			
			if (recPrefix.equals(REC_MENU) ) {
				prefix = "MENU";
				isMenu = true;
			} else if ( recPrefix.equals(REC_TRANS) ) {
				prefix = "TRANS";
				isTrans = true;
			} else if ( recPrefix.equals(REC_TRANSITEM) ) {
			    prefix = "TRANSITEM";
			    isTransItem = true;
			}
			
			if (isMenu) {		
				lgcyLclRfrDefCd = parts[0];
				posBusnDt = parts[1];
				posItmMenuItmDesc = parts[2];
				posItmMenuItmUntPrcAm = parts[3];
				posItmMenuItmId = parts[4];
				posItmMenuItmIndex = parts[5];
			}
			
			if (isTrans) {
				
				lgcyLclRfrDefCd = parts[0];
				posBusnDt = parts[1];
				posTrnDscntTypCd = parts[2];
				posPrdDlvrMethCd = parts[3];
				posTrnSeqNu = parts[4];
				posTrnOrdTs = parts[5];
				posOrdKeyCd = parts[6];
				posTrnNumber = parts[7];
				posTrnTotQt = parts[8];
				posTrnTotTaxAm = parts[9];
				posTrnTotNetAm = parts[10];
				posPymtType = parts[11];
				posTrnTotBDAmt = parts[12];
				dyptIdNu = parts[13];
				dyOfCalWkDs = parts[14];
			}
			
			if (isTransItem) {

				lgcyLclRfrDefCd = parts[0];
				posBusnDt = parts[1];
				posTrnSeqNu = parts[2];
				posTrnOrdTs = parts[3];
				posOrdKeyCd = parts[4];
				posTrnNumber = parts[5];
				posTrnTotQt = parts[6];
				posTrnTotNetAm = parts[7];
				posPrdDlvrMethCd = parts[8];
				posItmMenuItmIndex = parts[9];
				posItmQtyText = parts[10];
				posItmMenuItmId = parts[11];
				posItmPrmoQtyText = parts[12];
				posItmPrmo = parts[13];
				posItmMenuItmId = parts[14];
				posItmMenuItmDesc = parts[15];
				posPymtType = parts[16];
				dyptIdNu = parts[17];
				dyOfCalWkDs = parts[18];
				posTrnDscntTypCd = parts[19];
				posTrnItmIndex = parts[20];
			}
			
			if (isMenu) {
				mos.write(
						prefix, 
						NullWritable.get(), 
						new Text((new StringBuffer(lgcyLclRfrDefCd)
						.append(COMMA_DELIMITER)
						.append(posBusnDt)
						.append(COMMA_DELIMITER)
						.append(posItmMenuItmDesc)
						.append(COMMA_DELIMITER)
						.append(posItmMenuItmUntPrcAm)
						.append(COMMA_DELIMITER)
						.append(posItmMenuItmId)
						.append(COMMA_DELIMITER)
						.append(posItmMenuItmIndex)).toString()),
					prefix);
			}
			
			if (isTrans) {
				mos.write(
						prefix, 
						NullWritable.get(), 
						new Text((new StringBuffer(lgcyLclRfrDefCd)
						.append(COMMA_DELIMITER)
						.append(posBusnDt)
						.append(COMMA_DELIMITER)
						.append(posTrnDscntTypCd)
						.append(COMMA_DELIMITER)
						.append(posPrdDlvrMethCd)
						.append(COMMA_DELIMITER)
						.append(posTrnSeqNu)
						.append(COMMA_DELIMITER)
						.append(posTrnOrdTs)
						.append(COMMA_DELIMITER)
						.append(posOrdKeyCd)
						.append(COMMA_DELIMITER)
						.append(posTrnNumber)
						.append(COMMA_DELIMITER)
						.append(posTrnTotQt)
						.append(COMMA_DELIMITER)
						.append(posTrnTotTaxAm)
						.append(COMMA_DELIMITER)
						.append(posTrnTotNetAm)
						.append(COMMA_DELIMITER)
						.append(posPymtType)
						.append(COMMA_DELIMITER)
						.append(posTrnTotBDAmt)
						.append(COMMA_DELIMITER)
						.append(dyptIdNu)
						.append(COMMA_DELIMITER)
						.append(dyOfCalWkDs)).toString()),
						prefix);
			}
			
			if (isTransItem) {
				mos.write(
						prefix, 
						NullWritable.get(), 
						new Text((new StringBuffer(lgcyLclRfrDefCd)
						.append(COMMA_DELIMITER)
						.append(posBusnDt)
						.append(COMMA_DELIMITER)
						.append(posTrnSeqNu)
						.append(COMMA_DELIMITER)
						.append(posTrnOrdTs)
						.append(COMMA_DELIMITER)
						.append(posOrdKeyCd)
						.append(COMMA_DELIMITER)
						.append(posTrnNumber)
						.append(COMMA_DELIMITER)
						.append(posTrnTotQt)
						.append(COMMA_DELIMITER)
						.append(posTrnTotNetAm)
						.append(COMMA_DELIMITER)
						.append(posPrdDlvrMethCd)
						.append(COMMA_DELIMITER)
						.append(posItmMenuItmIndex)
						.append(COMMA_DELIMITER)
						.append(posItmQtyText)
						.append(COMMA_DELIMITER)
						.append(posItmMenuItmId)
						.append(COMMA_DELIMITER)
						.append(posItmPrmoQtyText)
						.append(COMMA_DELIMITER)
						.append(posItmPrmo)
						.append(COMMA_DELIMITER)
						.append(posItmMenuItmId)
						.append(COMMA_DELIMITER)
						.append(posItmMenuItmDesc)
						.append(COMMA_DELIMITER)
						.append(posPymtType)
						.append(COMMA_DELIMITER)
						.append(dyptIdNu)
						.append(COMMA_DELIMITER)
						.append(dyOfCalWkDs)
						.append(COMMA_DELIMITER)
						.append(posTrnDscntTypCd)
						.append(COMMA_DELIMITER)
						.append(posTrnItmIndex)).toString()),
						prefix);
			}
		}  
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {
		mos.close();
	}
}
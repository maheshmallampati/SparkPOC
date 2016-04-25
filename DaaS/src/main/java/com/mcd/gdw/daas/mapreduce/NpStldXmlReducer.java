package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NpStldXmlReducer  extends Reducer<Text,Text,NullWritable,Text>{
	

	private MultipleOutputs<NullWritable, Text> mos;
	  


	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String pmixHeader  = "LGCY_LCL_RFR_DEF_CD|REG_ID|POS_BUSN_DT|ORD_NUM|MENU_ITM_ID|ITEM_PRICE|ITEM_QTY|ITEM_QTY_PROMO|ITEM_TAX_RATE|VAT_ROUNDING|BP_UNIT_PRICE|BP_TAX_PER_UNIT|BD_UNIT_PRICE|BD_TAX_PER_UNIT|CTRY_ISO_NU";
		String salesHeader = "LGCY_LCL_RFR_DEF_CD|REG_ID|ORD_NUM|POS_BUSN_DT|ORD_DT|ORD_TM|POD|SALE_TYPE|ORD_KIND|BD_TOT_AM|BP_TOT_AM|ORD_TOT_AM|HELD_TIME|TOTAL_TIME|MENUPRICEBASIS|TENDER_NAME|CTRY_ISO_NU";
	    int i = 0;
		
		for(Text value:values){
//			if(i == 0){
//				if(key.toString().startsWith("PMIX")){
//					mos.write(key.toString(), NullWritable.get(), new Text(pmixHeader));
//				}else{
//					mos.write(key.toString(), NullWritable.get(), new Text(salesHeader));
//				}
//			}
			mos.write(key.toString(), NullWritable.get(), value);
			
			i++;
		}
		
	}
	
	

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		  mos.close();
	}


}

package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NpSalesSummaryReducer extends Reducer<Text, Text, NullWritable, Text> {

	private static final BigDecimal DECIMAL_ZERO = new BigDecimal("0.00");

	//private static final int BUSN_DT_POS = 0;
	//private static final int TERR_CD_POS = 1;
	//private static final int LCAT_POS = 2;
	private static final int DAILY_SLS_NET_AMT_POS = 3;
	private static final int DAILY_SLS_GROSS_AMT_POS = 4;
	private static final int DAILY_SLS_TC_POS = 5;
	private static final int XML_AMT_POS = 6;
	private static final int XML_TC_POS = 7;
	private static final int XML_CNT_POS = 8;
	private static final int NET_GROSS_FL_POS = 9;

	private static Text sumaryValue = new Text();

	private static BigDecimal xmlAmt = DECIMAL_ZERO;
	private static int xmlTc = 0;
	private static BigDecimal dailySalesNetAmt = DECIMAL_ZERO;
	private static BigDecimal dailySalesGrossAmt = DECIMAL_ZERO;
	private static int dailySalesTc = 0;
	private static int xmlCnt = 0;
	private static String menuPriceBasis="";
	
	private static String[] parts = null;
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		dailySalesNetAmt = DECIMAL_ZERO;
		dailySalesGrossAmt = DECIMAL_ZERO;
		dailySalesTc = 0;
		xmlAmt = DECIMAL_ZERO;
		xmlTc = 0;
		xmlCnt = 0;
		menuPriceBasis="N";

	    for (Text value : values ) {
	    	parts = value.toString().split("\\|");
	    	
	    	//System.err.println(value.toString());
	    	
	    	dailySalesNetAmt = dailySalesNetAmt.add(new BigDecimal(parts[DAILY_SLS_NET_AMT_POS]));
	    	dailySalesGrossAmt = dailySalesGrossAmt.add(new BigDecimal(parts[DAILY_SLS_GROSS_AMT_POS]));
	    	dailySalesTc += Integer.parseInt(parts[DAILY_SLS_TC_POS]);
	    			
	    	xmlAmt = xmlAmt.add(new BigDecimal(parts[XML_AMT_POS]));
	    	xmlTc += Integer.parseInt(parts[XML_TC_POS]);

	    	xmlCnt += Integer.parseInt(parts[XML_CNT_POS]);

	    	if ( parts[NET_GROSS_FL_POS].trim().length() > 0 ) {
	    		menuPriceBasis=parts[NET_GROSS_FL_POS];
	    	}
	    }
	    
	    if ( xmlCnt > 0 ) {
		    sumaryValue.clear();
		    sumaryValue.set(key.toString() + "|" + dailySalesNetAmt.toString() + "|" + dailySalesGrossAmt.toString() + "|" + dailySalesTc + "|" + xmlAmt.toString() + "|" + xmlTc + "|" + menuPriceBasis);
		    context.write(NullWritable.get(), sumaryValue);
	    }
	}
}

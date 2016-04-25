package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.compress.archivers.dump.DumpArchiveEntry.TYPE;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;

public class QuarterHourDaypartPMIXReducer  extends Reducer<Text,Text,NullWritable,Text>{
	
	private MultipleOutputs<NullWritable, Text> mos;

	private HashMap<String,String> timeSegmentStartTimeMap = new HashMap<String,String>();
	private HashMap<String,String> timeSegmentEndTimeMap = new HashMap<String,String>();
	private HashMap<String,String> daypartIdMap = new HashMap<String,String>();
	private HashMap<String,String> orderSaleTypeMap = new HashMap<String,String>();
	private HashMap<String,String> orderKindMap = new HashMap<String,String>();
	private HashMap<String,String> trxSalePodMap = new HashMap<String,String>();
	private HashMap<String,HashSet<Integer>> orderNumberMap = new HashMap<String,HashSet<Integer>>();
	private HashMap<String,BigDecimal> qtyMap = new HashMap<String,BigDecimal>();
	private HashMap<String,BigDecimal> qtyPromoMap = new HashMap<String,BigDecimal>();
	private HashMap<String,BigDecimal> taxUnitPriceMap = new HashMap<String,BigDecimal>();
	
	private HashMap<String,BigDecimal> netSalesAmtMap 			= new HashMap<String,BigDecimal>();
	private HashMap<String,BigDecimal> netAmtbeforeDiscountMap 	= new HashMap<String,BigDecimal>();
	private HashMap<String,String> typeMap 	= new HashMap<String,String>();
	

	private static final int TM_SEGMENT_ID_NU = 6;
	private static final int TM_SEGMENT_START_TIME = 7;
	private static final int TM_SEGMENT_END_TIME = 8;
	private static final int DAYPART_ID = 9;
	private static final int ORDER_SALE_TYPE = 10;
	private static final int ORDER_KIND = 11;
	private static final int TRX_SALE_POD = 12;
	private static final int QTY = 13;
	private static final int QTY_PROMO = 14;
	private static final int TAX_UNIT_PRICE = 15;
	private static final int ORDER_NUMBER = 16;
	
	private static final int NET_SALES_AMT = 18;
	private static final int NET_AMT_BEFORE_DISCOUNT = 19;
	private static final int TYPE = 20;
	
	
	private static Text summaryValue = new Text();
	private static String timeSegmentIdNu = "";
	
	private static String[] parts = null;
	  
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String currencyIsoNu = new String("");
		String timeSegmentStartTime = new String("");
		String timeSegmentEndTime = new String("");
		String daypartId = new String("");
		String orderSaleType = new String("");
		String orderKind = new String("");
		String trxSalePod = new String("");
		String code = new String("");
		String gdwLgcyLclRfrDefCd = new String("");
		
		Integer numberOfTransctions = new Integer(0);
		
		BigDecimal qty = new BigDecimal("0.00");
		BigDecimal qtyPromo = new BigDecimal("0.00");
		BigDecimal taxUnitPrice = new BigDecimal("0.00");
		BigDecimal netUnitPrice = new BigDecimal("0.00");
		
		BigDecimal netSalesAmt = new BigDecimal("0.00");
		BigDecimal netAmtBeforeDiscount = new BigDecimal("0.00");
		
		String type = "";
		
				
		String[] keyParts = key.toString().split("\\|");
				
		gdwLgcyLclRfrDefCd = new String(keyParts[2]);
		code = new String(keyParts[3]);
		netUnitPrice = new BigDecimal(keyParts[4]);
		currencyIsoNu = new String(keyParts[5]);
		
		
		
		timeSegmentStartTimeMap = new HashMap<String,String>();
		timeSegmentEndTimeMap = new HashMap<String,String>();
		daypartIdMap = new HashMap<String,String>();
		orderSaleTypeMap = new HashMap<String,String>();
		orderKindMap = new HashMap<String,String>();
		trxSalePodMap = new HashMap<String,String>();
		orderNumberMap = new HashMap<String,HashSet<Integer>>();
		qtyMap = new HashMap<String,BigDecimal>();
		qtyPromoMap = new HashMap<String,BigDecimal>();
		taxUnitPriceMap = new HashMap<String,BigDecimal>();
		
		netSalesAmtMap 			= new HashMap<String,BigDecimal>();
		netAmtbeforeDiscountMap 	= new HashMap<String,BigDecimal>();
		typeMap 	= new HashMap<String,String>();
		
		
		
		for (Text value : values ) {
	    	parts = value.toString().split("\\|");
	    	
	    	timeSegmentIdNu = parts[TM_SEGMENT_ID_NU];
	    	
	    	timeSegmentStartTime = new String(parts[TM_SEGMENT_START_TIME]);
	    	timeSegmentStartTimeMap.put(timeSegmentIdNu, timeSegmentStartTime);
	    	
	    	timeSegmentEndTime = new String(parts[TM_SEGMENT_END_TIME]);
	    	timeSegmentEndTimeMap.put(timeSegmentIdNu, timeSegmentEndTime);
	    	
	    	daypartId = new String(parts[DAYPART_ID]);
	    	daypartIdMap.put(timeSegmentIdNu, daypartId);
	    	
	    	orderSaleType = new String(parts[ORDER_SALE_TYPE]);
	    	orderSaleTypeMap.put(timeSegmentIdNu, orderSaleType);
	    	
	    	orderKind = new String(parts[ORDER_KIND]);
	    	orderKindMap.put(timeSegmentIdNu, orderKind);
	    	
	    	trxSalePod = new String(parts[TRX_SALE_POD]);
	    	trxSalePodMap.put(timeSegmentIdNu, trxSalePod);
			
			qty = new BigDecimal(parts[QTY]);
			
			if (qtyMap.containsKey(timeSegmentIdNu)){
				qty = qty.add(qtyMap.get(timeSegmentIdNu));
			}
			
			qtyMap.put(timeSegmentIdNu, qty);
			
			qtyPromo = new BigDecimal(parts[QTY_PROMO]);
			
			if (qtyPromoMap.containsKey(timeSegmentIdNu)){
				qtyPromo = qtyPromo.add(qtyPromoMap.get(timeSegmentIdNu));
			}
			
			qtyPromoMap.put(timeSegmentIdNu, qtyPromo);
			
			taxUnitPrice = new BigDecimal(parts[TAX_UNIT_PRICE]);
			
			if (taxUnitPriceMap.containsKey(timeSegmentIdNu)){
				taxUnitPrice = taxUnitPrice.add(taxUnitPriceMap.get(timeSegmentIdNu));
			}
			
			taxUnitPriceMap.put(timeSegmentIdNu, taxUnitPrice);
			
			try{
				numberOfTransctions = new Integer(parts[ORDER_NUMBER]);
			}catch(NumberFormatException nfex){
				numberOfTransctions = 0;
				context.getCounter("Count", "OrderNumberException").increment(1); 
						
			}
	    	if (!orderNumberMap.containsKey(timeSegmentIdNu)) {
	    		orderNumberMap.put(timeSegmentIdNu, new HashSet<Integer>());
	    	}
	    	
	    	orderNumberMap.get(timeSegmentIdNu).add(numberOfTransctions);
	    	
	    	netSalesAmt = new BigDecimal(parts[NET_SALES_AMT]);
	    	
	    	if(netSalesAmtMap.containsKey(timeSegmentIdNu)){
	    		netSalesAmt = netSalesAmt.add(netSalesAmtMap.get(timeSegmentIdNu));
	    	}
	    	
	    	netSalesAmtMap.put(timeSegmentIdNu, netSalesAmt);
	    	
	    	netAmtBeforeDiscount = new BigDecimal(parts[NET_AMT_BEFORE_DISCOUNT]);
	    	if(netAmtbeforeDiscountMap.containsKey(timeSegmentIdNu)){
	    		netAmtBeforeDiscount = netAmtBeforeDiscount.add(netAmtbeforeDiscountMap.get(timeSegmentIdNu));
	    	}
	    	
	    	netAmtbeforeDiscountMap.put(timeSegmentIdNu, netAmtBeforeDiscount);
	    	
	    	type = parts[TYPE];
	    	typeMap.put(timeSegmentIdNu,type);
	    	
	    }
		
		Set<String> segmentSet = qtyMap.keySet();
		
		Iterator<String> segit = segmentSet.iterator();
		
		StringBuffer outfileNmBf = new StringBuffer();
		StringBuffer outValBf = new StringBuffer();
		while(segit.hasNext()){
			String segment = segit.next();
			
			timeSegmentStartTime 	= timeSegmentStartTimeMap.get(segment);
			timeSegmentEndTime 		= timeSegmentEndTimeMap.get(segment);
	    	daypartId 				= daypartIdMap.get(segment);
	    	orderSaleType 			= orderSaleTypeMap.get(segment);
	    	orderKind 			 	= orderKindMap.get(segment);
	    	trxSalePod 				= trxSalePodMap.get(segment);
	    	qty 					= qtyMap.get(segment);
	    	qtyPromo 				= qtyPromoMap.get(segment);
			taxUnitPrice 			= taxUnitPriceMap.get(segment);

			numberOfTransctions 	= orderNumberMap.get(segment).size();
			
			netSalesAmt 			= netSalesAmtMap.get(segment);
			netAmtBeforeDiscount	= netAmtbeforeDiscountMap.get(segment);
			type					= typeMap.get(segment);
			
			outValBf.setLength(0);
			outValBf.append(gdwLgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(code).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(netUnitPrice).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(currencyIsoNu).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(timeSegmentStartTime).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(timeSegmentEndTime).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(daypartId).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(orderSaleType).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(orderKind).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(trxSalePod).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(qty).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(qtyPromo).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(taxUnitPrice).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(numberOfTransctions).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(netSalesAmt).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(netAmtBeforeDiscount).append(DaaSConstants.PIPE_DELIMITER);
			outValBf.append(type);
			
			
		
			summaryValue.clear();
//			sumaryValue.set(gdwLgcyLclRfrDefCd + "|" + code + "|" + netUnitPrice + "|" + currencyIsoNu + "|" + timeSegmentStartTime + "|" + timeSegmentEndTime + "|" + daypartId + "|" + orderSaleType + "|" + orderKind + "|" + trxSalePod + "|" + qty + "|" + qtyPromo + "|" + taxUnitPrice + "|" + orderNumber);
//			context.write(NullWritable.get(), sumaryValue);
			summaryValue.set(outValBf.toString());
			
			outfileNmBf.setLength(0);
			outfileNmBf.append("15MINDAYPART").append(DaaSConstants.SPLCHARTILDE_DELIMITER).append(keyParts[1]).append(DaaSConstants.SPLCHARTILDE_DELIMITER).append(keyParts[0].replaceAll("-", "")); //15MINDAYPARTRxD126840RxD12620141001 15MINDAYPAT+TERRCD+BUSDT RxD126 delimiter
			mos.write(outfileNmBf.toString(),NullWritable.get(),summaryValue);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		  mos.close();
	}


}

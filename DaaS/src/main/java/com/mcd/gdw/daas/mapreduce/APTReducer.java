package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.APTDriver;
import com.mcd.gdw.daas.util.APTUtil;
import com.mcd.gdw.daas.util.PmixData;

public class APTReducer extends Reducer<Text, Text, Text, Text>{

	private MultipleOutputs<Text, Text> multipleOutputs;
	
	private Map<String, String> itemPriceLookup = null;
	private Map<String, String> pmixDataMapIn = null;
	private Map<String, PmixData> pmixDataMap = null;

	private PmixData pmixDataObj = null;
	private List<String> toysItemCodes;
	private Set<BigDecimal> toyPrices;
	
	private Text dtlTextOut = null;
	private StringBuffer dtlStrBuf = null;
	private StringBuffer keyStrBuf = null;
	
	private static int START_TOY_CD = 845;
	private static int END_TOY_CD = 899;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
		toysItemCodes = APTUtil.populateToysItemCodes(START_TOY_CD, END_TOY_CD);
		dtlTextOut = new Text();
		dtlStrBuf = new StringBuffer();
		keyStrBuf = new StringBuffer();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		itemPriceLookup = new HashMap<String, String>();
		pmixDataMapIn = new HashMap<String, String>();
		pmixDataMap = new HashMap<String, PmixData>();
		toyPrices = new HashSet<BigDecimal>();
		
		String valueStr = "";
		String[] mapOutputArray = null;
		String dataKey = key.toString();
		for (Text value : values) {
			valueStr = value.toString();
			mapOutputArray = valueStr.split("\\|");
			if (mapOutputArray[0].equals(APTDriver.STLD_TAG_INDEX)) {
				keyStrBuf.setLength(0);
				keyStrBuf.append(dataKey).append(DaaSConstants.PIPE_DELIMITER) 
						 .append(mapOutputArray[4]).append(DaaSConstants.PIPE_DELIMITER)
						 .append(mapOutputArray[13]).append(DaaSConstants.PIPE_DELIMITER) 
						 .append(mapOutputArray[17]);
				pmixDataMapIn.put(keyStrBuf.toString(), valueStr.substring(2, valueStr.length()));
				
			} else if (mapOutputArray[0].equals(APTDriver.MENU_ITEM_TAG_INDEX)) {
				if (mapOutputArray.length == 3) {
					if (toysItemCodes.contains(mapOutputArray[1])) {
						toyPrices.add(new BigDecimal(mapOutputArray[2]));
					} else	{
						keyStrBuf.setLength(0);
						keyStrBuf.append(dataKey).append(DaaSConstants.PIPE_DELIMITER).append(mapOutputArray[1]);
						itemPriceLookup.put(keyStrBuf.toString(), mapOutputArray[2]);
					}	
				} else {
					System.out.println("Malformed MenuItem Lookup Record : " + valueStr);
				}	
			}
			if (toyPrices.size() > 0) {
				keyStrBuf.setLength(0);
				keyStrBuf.append(dataKey).append(DaaSConstants.PIPE_DELIMITER).append(APTDriver.HAPPY_MEAL_TOY_CD);
				itemPriceLookup.put(keyStrBuf.toString(), String.valueOf(Collections.max(toyPrices)));
			}	
		}
		
		
		String pmixIn = "";
		String[] itemPriceKeyArray = null;
		String itemPriceKey = "";
		String[] pmixInArray = null;
		
		String lookupPrice = "";
		String[] pmixKeyTokens = null;
		
		String itemCode = "";
		String lookupKey = "";

		for (String eachPmixKey : pmixDataMapIn.keySet()) {
			
			pmixIn = pmixDataMapIn.get(eachPmixKey);
			itemPriceKeyArray = eachPmixKey.split("\\|");
			keyStrBuf.setLength(0);
			keyStrBuf.append(itemPriceKeyArray[0]).append(DaaSConstants.PIPE_DELIMITER).append(itemPriceKeyArray[1])
					 .append(DaaSConstants.PIPE_DELIMITER).append(itemPriceKeyArray[2]);
			itemPriceKey = keyStrBuf.toString();
			pmixInArray = pmixIn.split("\\|");
			
			if (pmixInArray[12].equals("999.99")) {
				lookupPrice = itemPriceLookup.containsKey(itemPriceKey) ? itemPriceLookup.get(itemPriceKey) : "0.00";
				pmixKeyTokens = eachPmixKey.split("\\|");
				keyStrBuf.setLength(0);
				keyStrBuf.append(pmixKeyTokens[0]).append(DaaSConstants.PIPE_DELIMITER).append(pmixKeyTokens[1])
						 .append(DaaSConstants.PIPE_DELIMITER).append(pmixKeyTokens[2]).append(DaaSConstants.PIPE_DELIMITER)
						 .append(lookupPrice).append(DaaSConstants.PIPE_DELIMITER).append(pmixKeyTokens[4]);
				eachPmixKey = keyStrBuf.toString();
				
				pmixDataObj = pmixDataMap.containsKey(eachPmixKey) ? pmixDataMap.get(eachPmixKey) : new PmixData();
				
				pmixDataObj.setTerrCd(pmixInArray[0]);
				pmixDataObj.setStoreId(pmixInArray[1]);
				pmixDataObj.setGdwBusinessDate(pmixInArray[2]);
				pmixDataObj.setItemCode(pmixInArray[3]);
				pmixDataObj.setConsPrice(new BigDecimal(lookupPrice));
				
				pmixDataObj.setDlyPmixPrice(new BigDecimal(lookupPrice));
				
				pmixDataObj.setUnitsSold(pmixDataObj.getUnitsSold() + Integer.parseInt(pmixInArray[6]));
				pmixDataObj.setComboQty(pmixDataObj.getComboQty() + Integer.parseInt(pmixInArray[7]));
				pmixDataObj.setTotalQty(pmixDataObj.getTotalQty() + Integer.parseInt(pmixInArray[8]));
			
				pmixDataObj.setPromoQty(pmixDataObj.getPromoQty() + Integer.parseInt(pmixInArray[9]));
				pmixDataObj.setPromoComboQty(pmixDataObj.getPromoComboQty() + Integer.parseInt(pmixInArray[10]));
				pmixDataObj.setPromoTotalQty(pmixDataObj.getPromoTotalQty() + Integer.parseInt(pmixInArray[11]));
				
				pmixDataObj.setStoreType(pmixInArray[13]);
				pmixDataObj.setHaviBusinessDate(pmixInArray[14]);
				pmixDataObj.setHaviTimeKey(pmixInArray[15]);
				pmixDataObj.setOrderKey(pmixInArray[16]);
				pmixDataObj.setItemLevel(pmixInArray[21]);
				pmixDataObj.setGdwMcdGbalLcatIdNu(pmixInArray[22]);
				pmixDataObj.setComboBrkDwnPrice(pmixInArray[23]);
				
			} else {
				
				pmixDataObj = pmixDataMap.containsKey(eachPmixKey) ? pmixDataMap.get(eachPmixKey) : new PmixData();
				
				pmixDataObj.setTerrCd(pmixInArray[0]);
				pmixDataObj.setStoreId(pmixInArray[1]);
				pmixDataObj.setGdwBusinessDate(pmixInArray[2]);
				pmixDataObj.setItemCode(pmixInArray[3]);
				pmixDataObj.setConsPrice(new BigDecimal(pmixInArray[12]));
				
				if (pmixInArray[17].equals("Y")) {
					
					if (pmixInArray[20].equals("Y")) {
						pmixDataObj.setDlyPmixPrice(new BigDecimal(pmixInArray[4]).divide(new BigDecimal(pmixInArray[18]), 2, RoundingMode.HALF_UP));
					} else {
						pmixDataObj.setDlyPmixPrice(new BigDecimal(pmixInArray[12]));
					}
					
					pmixDataObj.setLvl0PmixFlag(pmixInArray[17]);
					pmixDataObj.setAllComboComponents(pmixInArray[19]);
					
					Set<String> comboComponents = new HashSet<String>();
					StringTokenizer st = new StringTokenizer(pmixInArray[19], ",");
					while(st.hasMoreTokens())
						comboComponents.add(st.nextToken());
					
					for (String eachComponent : comboComponents) {
						itemCode = String.valueOf(Integer.parseInt(eachComponent));
						keyStrBuf.setLength(0);
						keyStrBuf.append(itemPriceKeyArray[0]).append(DaaSConstants.PIPE_DELIMITER).append(itemPriceKeyArray[1]) 
						 		 .append(DaaSConstants.PIPE_DELIMITER).append(itemCode);
						lookupKey = keyStrBuf.toString();
						
						lookupPrice = itemPriceLookup.containsKey(lookupKey) ? itemPriceLookup.get(lookupKey) : "0.00";
						
						pmixDataObj.setComboFullPrice(pmixDataObj.getComboFullPrice().add(new BigDecimal(lookupPrice)));
					} 
					pmixDataObj.setComboFullPrice(pmixDataObj.getComboFullPrice().divide(new BigDecimal(pmixInArray[18]), 2, RoundingMode.HALF_UP));
					
					if (pmixDataObj.getComboFullPrice().compareTo(APTDriver.DECIMAL_ZERO) == 0) {
						pmixDataObj.setComboUpDownAmt(APTDriver.DECIMAL_ZERO);
					} else {
						pmixDataObj.setComboUpDownAmt(pmixDataObj.getDlyPmixPrice().subtract(pmixDataObj.getComboFullPrice()));
					}
					
				} else {
					pmixDataObj.setDlyPmixPrice(new BigDecimal(pmixInArray[4]));
				} 
				
				pmixDataObj.setUnitsSold(pmixDataObj.getUnitsSold() + Integer.parseInt(pmixInArray[6]));
				pmixDataObj.setComboQty(pmixDataObj.getComboQty() + Integer.parseInt(pmixInArray[7]));
				pmixDataObj.setTotalQty(pmixDataObj.getTotalQty() + Integer.parseInt(pmixInArray[8]));
			
				pmixDataObj.setPromoQty(pmixDataObj.getPromoQty() + Integer.parseInt(pmixInArray[9]));
				pmixDataObj.setPromoComboQty(pmixDataObj.getPromoComboQty() + Integer.parseInt(pmixInArray[10]));
				pmixDataObj.setPromoTotalQty(pmixDataObj.getPromoTotalQty() + Integer.parseInt(pmixInArray[11]));
				
				pmixDataObj.setStoreType(pmixInArray[13]);
				pmixDataObj.setHaviBusinessDate(pmixInArray[14]);
				pmixDataObj.setHaviTimeKey(pmixInArray[15]);
				pmixDataObj.setOrderKey(pmixInArray[16]);
				pmixDataObj.setItemLevel(pmixInArray[21]);
				pmixDataObj.setGdwMcdGbalLcatIdNu(pmixInArray[22]);
				pmixDataObj.setComboBrkDwnPrice(pmixInArray[23]);
			}
			pmixDataMap.put(eachPmixKey, pmixDataObj);
		}
		
		
		PmixData pmixOutData = null;
		String[] orderInd = null;
		for (String eachPmixKey : pmixDataMap.keySet()) {
			pmixOutData = pmixDataMap.get(eachPmixKey);
			
			orderInd = pmixOutData.getOrderKey().split(",");
			
			// Total Sold
			if (pmixOutData.getTotalQty() > 0) {
				
				dtlStrBuf.setLength(0);
				dtlStrBuf.append(pmixOutData.getTerrCd()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getStoreId()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getGdwMcdGbalLcatIdNu()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getGdwBusinessDate()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(orderInd[1]).append(DaaSConstants.PIPE_DELIMITER)
						 .append(orderInd[0]).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getItemLevel()).append(DaaSConstants.PIPE_DELIMITER)
						 .append("Total Sold").append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getItemCode()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getComboBrkDwnPrice()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getUnitsSold()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getComboQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append("").append(DaaSConstants.PIPE_DELIMITER)
						 .append("");
				
				dtlTextOut.set(dtlStrBuf.toString());
								
				multipleOutputs.write(APTDriver.APTUS_TDA_Detail, NullWritable.get(), dtlTextOut);
			 }
			
			// PROMO
			 if (pmixOutData.getPromoTotalQty() > 0) {
				
				dtlStrBuf.setLength(0);
				dtlStrBuf.append(pmixOutData.getTerrCd()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getStoreId()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getGdwMcdGbalLcatIdNu()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getGdwBusinessDate()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(orderInd[1]).append(DaaSConstants.PIPE_DELIMITER)
						 .append(orderInd[0]).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getItemLevel()).append(DaaSConstants.PIPE_DELIMITER)
						 .append("Promo").append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getItemCode()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getComboBrkDwnPrice()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getPromoQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getPromoComboQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(pmixOutData.getPromoTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append("").append(DaaSConstants.PIPE_DELIMITER)
						 .append("");
				
				dtlTextOut.set(dtlStrBuf.toString());
				
				multipleOutputs.write(APTDriver.APTUS_TDA_Detail, NullWritable.get(), dtlTextOut);
			 }
		}
	}
	
	
	/**
	 * Cleanup method to close multiple outputs
	 */
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		itemPriceLookup.clear();
		pmixDataMapIn.clear();
		pmixDataMap.clear();
		toyPrices.clear();
		multipleOutputs.close();
	}
	
}
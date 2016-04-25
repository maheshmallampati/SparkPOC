package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import com.mcd.gdw.daas.driver.TDADriver;
import com.mcd.gdw.daas.util.PmixData;
import com.mcd.gdw.daas.util.TDAUtil;

/**
 * @author Naveen Kumar B.V
 * Reducer class for TDA.
 */
public class TDAReducer extends Reducer<Text, Text, Text, Text> {

	public static final DecimalFormat decimalFormat = new DecimalFormat("#,##0.00");
	
	private MultipleOutputs<Text, Text> multipleOutputs;
	
	private Map<String, String> itemPriceLookup = null;
	private Map<String, String> pmixDataMapIn = null;
	private Map<String, PmixData> pmixDataMap = null;
	private Map<String, PmixData> finalPmixOutputMap = null;

	private PmixData pmixDataObj = null;
	private List<String> toysItemCodes;
	private Set<BigDecimal> toyPrices;
	
	private static int START_TOY_CD = 845;
	private static int END_TOY_CD = 899;
	
	private StringBuffer keyStrBuf = null;
	
	private String gdwTimeStamp;
	
	private static StringBuffer outputStrBuf = new StringBuffer();
	private static Text outputTxt = new Text();
	
	private String partitionID;
	private String uniqueFileId;
	private int ordDtlRecordCount;
	Set<String> dtlUniqStoreSet = new HashSet<String>();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		partitionID = String.format("%05d", context.getTaskAttemptID().getTaskID().getId());
		uniqueFileId = String.valueOf(System.currentTimeMillis()) + partitionID;
		
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
		toysItemCodes = TDAUtil.populateToysItemCodes(START_TOY_CD, END_TOY_CD);
		keyStrBuf = new StringBuffer();
		
		Date date = new Date();
		SimpleDateFormat gdwDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S"); // Change to constant
		gdwTimeStamp = gdwDateFormat.format(date);
		gdwTimeStamp = gdwTimeStamp.substring(0, gdwTimeStamp.length() - 1);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		itemPriceLookup = new HashMap<String, String>();
		pmixDataMapIn = new HashMap<String, String>();
		pmixDataMap = new HashMap<String, PmixData>();
		finalPmixOutputMap = new HashMap<String, PmixData>();
		toyPrices = new HashSet<BigDecimal>();
		
		String valueStr = "";
		String[] mapOutputArray = null;
		String dataKey = key.toString();
		for (Text value : values) {
			valueStr = value.toString();
			mapOutputArray = valueStr.split(TDADriver.REGEX_PIPE_DELIMITER);
			if (mapOutputArray[0].equals(TDADriver.STLD_TAG_INDEX)) {
				keyStrBuf.setLength(0);
				keyStrBuf.append(dataKey).append(DaaSConstants.PIPE_DELIMITER) 
						 .append(mapOutputArray[4]).append(DaaSConstants.PIPE_DELIMITER)
						 .append(mapOutputArray[13]).append(DaaSConstants.PIPE_DELIMITER) 
						 .append(mapOutputArray[17]);
				
				pmixDataMapIn.put(keyStrBuf.toString(), valueStr.substring(2, valueStr.length()));
				
			} else if (mapOutputArray[0].equals(TDADriver.MENU_ITEM_TAG_INDEX)) {
				if (mapOutputArray.length == 3) {
					if (toysItemCodes.contains(mapOutputArray[1])) {
						toyPrices.add(new BigDecimal(mapOutputArray[2]));
					} else	{
						keyStrBuf.setLength(0);
						keyStrBuf.append(dataKey).append(DaaSConstants.PIPE_DELIMITER).append(mapOutputArray[1]);
						itemPriceLookup.put(keyStrBuf.toString(), mapOutputArray[2]);
					}	
				} else {
					multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), "In Menu Item Look up : " + valueStr);
				}	
			}
			
			if (toyPrices.size() > 0) {
				keyStrBuf.setLength(0);
				keyStrBuf.append(dataKey).append(DaaSConstants.PIPE_DELIMITER).append(TDADriver.HAPPY_MEAL_TOY_CD);
				itemPriceLookup.put(keyStrBuf.toString(), String.valueOf(Collections.max(toyPrices)));
			}	
		}
		
		
		String pmixIn = "";
		String[] itemPriceKeyArray = null;
		String itemPriceKey = "";
		String[] pmixInArray = null;
		String[] pmixKeyTokens = null;
		
		String lookupKey = "";
		String lookupPrice = "";
		
		for (String eachPmixKey : pmixDataMapIn.keySet()) {
						
			pmixIn = pmixDataMapIn.get(eachPmixKey);
			itemPriceKeyArray = eachPmixKey.split(TDADriver.REGEX_PIPE_DELIMITER);
			
			keyStrBuf.setLength(0);
			keyStrBuf.append(itemPriceKeyArray[0]).append(DaaSConstants.PIPE_DELIMITER).append(itemPriceKeyArray[1])
			 		 .append(DaaSConstants.PIPE_DELIMITER).append(itemPriceKeyArray[2]);
			
			itemPriceKey = keyStrBuf.toString();
			pmixInArray = pmixIn.split(TDADriver.REGEX_PIPE_DELIMITER);
			
			if (pmixInArray[12].equals("999.99")) {
				
				lookupPrice = itemPriceLookup.containsKey(itemPriceKey) ? itemPriceLookup.get(itemPriceKey) : TDADriver.DECIMAL_ZERO_STR;
				pmixKeyTokens = eachPmixKey.split(TDADriver.REGEX_PIPE_DELIMITER);
				
				keyStrBuf.setLength(0);
				keyStrBuf.append(pmixKeyTokens[0]).append(DaaSConstants.PIPE_DELIMITER).append(pmixKeyTokens[1])
						 .append(DaaSConstants.PIPE_DELIMITER).append(pmixKeyTokens[2]).append(DaaSConstants.PIPE_DELIMITER)
						 .append(decimalFormat.format(Double.valueOf(lookupPrice))).append(DaaSConstants.PIPE_DELIMITER).append(pmixKeyTokens[4]);
				eachPmixKey = keyStrBuf.toString();
				
				pmixDataObj = pmixDataMap.containsKey(eachPmixKey) ? pmixDataMap.get(eachPmixKey) : new PmixData();
				
				pmixDataObj.setTerrCd(pmixInArray[0]);
				pmixDataObj.setStoreId(pmixInArray[1]);
				pmixDataObj.setGdwBusinessDate(pmixInArray[2]);
				pmixDataObj.setItemCode(pmixInArray[3]);
				pmixDataObj.setConsPrice(new BigDecimal(lookupPrice));
				
				pmixDataObj.setDlyPmixPrice(new BigDecimal(lookupPrice).multiply(new BigDecimal(pmixInArray[18])));
				pmixDataObj.setTotalDlyPmixPrice(new BigDecimal(lookupPrice).multiply(new BigDecimal(pmixInArray[18])));
				
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
				pmixDataObj.setLvl0PmixQty(Integer.parseInt(pmixInArray[18]));
				pmixDataObj.setGdwMcdGbalLcatIdNu(pmixInArray[21]);
				pmixDataObj.setPosEvntTypId(pmixInArray[22]);
				pmixDataObj.setItemLevel(pmixInArray[23]);
				pmixDataObj.setStldItemCode(pmixInArray[24]);
				pmixDataObj.setItemTaxAmt(pmixInArray[25]);
				pmixDataObj.setItemTaxPercent(pmixInArray[26]);
				pmixDataObj.setComboBrkDwnPrice(pmixInArray[27]);
				pmixDataObj.setItemLineSeqNum(Integer.valueOf(pmixInArray[28]));
				pmixDataObj.setMenuItemComboFlag(pmixInArray[29]);
				
			} else {
				
				pmixDataObj = pmixDataMap.containsKey(eachPmixKey) ? pmixDataMap.get(eachPmixKey) : new PmixData();
				
				pmixDataObj.setTerrCd(pmixInArray[0]);
				pmixDataObj.setStoreId(pmixInArray[1]);
				pmixDataObj.setGdwBusinessDate(pmixInArray[2]);
				pmixDataObj.setItemCode(pmixInArray[3]);
				pmixDataObj.setConsPrice(new BigDecimal(pmixInArray[12]));
				
				
				if (pmixInArray[17].equals("Y")) {
					
					if (pmixInArray[20].equals("Y")) {
						if (!pmixInArray[18].equals("0"))
							pmixDataObj.setDlyPmixPrice(new BigDecimal(pmixInArray[4]).divide(new BigDecimal(pmixInArray[18]), 2, RoundingMode.HALF_UP));
						
						pmixDataObj.setTotalDlyPmixPrice(new BigDecimal(pmixInArray[4]));
					} else {
						pmixDataObj.setDlyPmixPrice(new BigDecimal(pmixInArray[12]));
						pmixDataObj.setTotalDlyPmixPrice(new BigDecimal(pmixInArray[4]));
					}
					
					pmixDataObj.setLvl0PmixFlag(pmixInArray[17]);
					pmixDataObj.setAllComboComponents(pmixInArray[19]);
					
					List<String> comboComponents = new ArrayList<String>();
					StringTokenizer st = new StringTokenizer(pmixInArray[19], ";");
					while(st.hasMoreTokens())
						comboComponents.add(st.nextToken());
					
					BigDecimal menuItemPrice = null;
					String[] componentDetails = null;
					String itemCode = "";
					int cmptQty = 1;
					BigDecimal itemPrice = null;
					for (String eachCmptWithQty : comboComponents) {
						componentDetails = eachCmptWithQty.split(",");
						if (componentDetails.length == 2) {
							itemCode = String.valueOf(Integer.parseInt(componentDetails[0]));
							cmptQty = Integer.parseInt(componentDetails[1]);
						}
											
						keyStrBuf.setLength(0);
						keyStrBuf.append(itemPriceKeyArray[0]).append(DaaSConstants.PIPE_DELIMITER).append(itemPriceKeyArray[1]) 
						 		 .append(DaaSConstants.PIPE_DELIMITER).append(itemCode);
						lookupKey = keyStrBuf.toString();
						
						menuItemPrice = itemPriceLookup.containsKey(lookupKey) ? new BigDecimal(itemPriceLookup.get(lookupKey)) : TDADriver.DECIMAL_ZERO;
						itemPrice = menuItemPrice.multiply(new BigDecimal(cmptQty).multiply(new BigDecimal(pmixInArray[18])));
						pmixDataObj.setComboFullPrice(pmixDataObj.getComboFullPrice().add(itemPrice));
						
					} 
					
					if (pmixDataObj.getComboFullPrice().compareTo(TDADriver.DECIMAL_ZERO) == 0) {
						pmixDataObj.setComboUpDownAmt(TDADriver.DECIMAL_ZERO);
					} else {
						pmixDataObj.setComboUpDownAmt(pmixDataObj.getTotalDlyPmixPrice().subtract(pmixDataObj.getComboFullPrice()));
					}
					
					if (!pmixInArray[18].equals("0"))
						pmixDataObj.setComboFullPrice(pmixDataObj.getComboFullPrice().divide(new BigDecimal(pmixInArray[18]), 2, RoundingMode.HALF_UP));
					
				} else {
					
					pmixDataObj.setDlyPmixPrice(new BigDecimal(pmixInArray[4]).multiply(new BigDecimal(pmixInArray[18])));
					pmixDataObj.setTotalDlyPmixPrice(new BigDecimal(pmixInArray[4]).multiply(new BigDecimal(pmixInArray[18])));
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
				pmixDataObj.setLvl0PmixQty(Integer.parseInt(pmixInArray[18]));
				pmixDataObj.setGdwMcdGbalLcatIdNu(pmixInArray[21]);
				pmixDataObj.setPosEvntTypId(pmixInArray[22]);
				pmixDataObj.setItemLevel(pmixInArray[23]);
				pmixDataObj.setStldItemCode(pmixInArray[24]);
				pmixDataObj.setItemTaxAmt(pmixInArray[25]);
				pmixDataObj.setItemTaxPercent(pmixInArray[26]);
				pmixDataObj.setComboBrkDwnPrice(pmixInArray[27]);
				pmixDataObj.setItemLineSeqNum(Integer.valueOf(pmixInArray[28]));
				pmixDataObj.setMenuItemComboFlag(pmixInArray[29]);
			}
			pmixDataMap.put(eachPmixKey, pmixDataObj);
		}
		
		PmixData pmixOutData = null;
		String[] seqNumOrderKey = null;
		for (String eachPmixKey : pmixDataMap.keySet()) {
			pmixOutData = pmixDataMap.get(eachPmixKey);
			seqNumOrderKey = pmixOutData.getOrderKey().split(",");
			
			// Total Sold
			if (pmixOutData.getTotalQty() > 0) {
				ordDtlRecordCount++;
				dtlUniqStoreSet.add(pmixOutData.getGdwMcdGbalLcatIdNu());
				
				outputStrBuf.setLength(0);
				outputStrBuf.append(pmixOutData.getTerrCd()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getGdwBusinessDate()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getGdwMcdGbalLcatIdNu()).append(DaaSConstants.PIPE_DELIMITER)
						.append(seqNumOrderKey[1]).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemLineSeqNum()+TDADriver.PMIX_UNIT_TYPE_SOLD).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getPosEvntTypId()).append(DaaSConstants.PIPE_DELIMITER)
						.append(seqNumOrderKey[0]).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemLevel()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getTerrCd()).append(DaaSConstants.PIPE_DELIMITER)
						.append(TDADriver.PMIX_UNIT_TYPE_SOLD).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemCode()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getStldItemCode()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getComboBrkDwnPrice()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getDlyPmixPrice()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getComboUpDownAmt()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getUnitsSold()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getComboQty()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemTaxPercent()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemTaxAmt()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getMenuItemComboFlag()).append(DaaSConstants.PIPE_DELIMITER)
						.append(uniqueFileId);
				
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.TDAUS_Order_Detail, NullWritable.get(), outputTxt);
			}
			
			
			// PROMO
			if (pmixOutData.getPromoTotalQty() > 0) {
				ordDtlRecordCount++;
				dtlUniqStoreSet.add(pmixOutData.getGdwMcdGbalLcatIdNu());
				
				outputStrBuf.setLength(0);
				outputStrBuf.append(pmixOutData.getTerrCd()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getGdwBusinessDate()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getGdwMcdGbalLcatIdNu()).append(DaaSConstants.PIPE_DELIMITER)
						.append(seqNumOrderKey[1]).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemLineSeqNum()+TDADriver.PMIX_UNIT_TYPE_PROMO).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getPosEvntTypId()).append(DaaSConstants.PIPE_DELIMITER)
						.append(seqNumOrderKey[0]).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemLevel()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getTerrCd()).append(DaaSConstants.PIPE_DELIMITER)
						.append(TDADriver.PMIX_UNIT_TYPE_PROMO).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemCode()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getStldItemCode()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getComboBrkDwnPrice()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getDlyPmixPrice()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getComboUpDownAmt()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getPromoQty()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getPromoComboQty()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getPromoTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemTaxPercent()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getItemTaxAmt()).append(DaaSConstants.PIPE_DELIMITER)
						.append(pmixOutData.getMenuItemComboFlag()).append(DaaSConstants.PIPE_DELIMITER)
						.append(uniqueFileId);
				
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.TDAUS_Order_Detail, NullWritable.get(), outputTxt);
			}
		}
	}
	
	/**
	 * Cleanup method to close multiple outputs
	 */
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		
		multipleOutputs.write(TDADriver.Output_File_Stats, NullWritable.get(), 
				new Text(TDADriver.TDAUS_Order_Detail+"-r-"+partitionID + DaaSConstants.PIPE_DELIMITER + ordDtlRecordCount 
								+ DaaSConstants.PIPE_DELIMITER + dtlUniqStoreSet.size() + 
								DaaSConstants.PIPE_DELIMITER + uniqueFileId));
		
		itemPriceLookup.clear();
		pmixDataMapIn.clear();
		pmixDataMap.clear();
		finalPmixOutputMap.clear();
		multipleOutputs.close();
	}
}

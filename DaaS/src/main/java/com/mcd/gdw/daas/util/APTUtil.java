package com.mcd.gdw.daas.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.mcd.gdw.daas.driver.APTDriver;
import com.mcd.gdw.daas.driver.APTDriver.StldItemType;

/**
 * APTUtil Class
 * This has all the helper methods required to generate order order detail information
 */
public class APTUtil {
	
	public static int JGREG= 15 + 31*(10+12*1582);
	public static double HALFSECOND = 0.5;
	
	/**
	 * Method to check day of week for Event Timestamp
	 * @param eventTimestamp
	 * @return
	 * @throws ParseException
	 */
	public static boolean isWeekend(String businessDateString) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date businessDate = sdf.parse(businessDateString);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(businessDate);
		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
		return (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY) ? true : false;
	}

	/**
	 * Method to return StoreData object based on MapKey
	 * @param hourlyStoreMap
	 * @param mapKey
	 * @return StoreData
	 */
	public static StoreData getStoreDataByMapKey(Map<String, StoreData> hourlyStoreMap, String mapKey) {
		StoreData storeData = null;
		if (hourlyStoreMap.containsKey(mapKey)) {
			storeData = (StoreData) hourlyStoreMap.get(mapKey);
		} else {
			storeData = new StoreData();
		}
		return storeData;
	}
	
	public static PmixData getPmixDataByMapKey(Map<String, PmixData> pmixDataMap, String mapKey) {
		PmixData pmixData = null;
		if (pmixDataMap.containsKey(mapKey)) {
			pmixData = (PmixData) pmixDataMap.get(mapKey);
		} else {
			pmixData = new PmixData();
		}
		return pmixData;
	}
	
	/**
	 * Method for Primary to Secondary Mapping
	 * @param itemCode
	 * @param primaryItemLookup
	 * @return mitmKey
	 */
	public static int getPrimaryItemCode(String itemCode, Map<String, String> primaryItemLookup) {
		
		String primaryItemCode = primaryItemLookup.get(String.format("%04d", Integer.parseInt(itemCode)));
		int mitmKey = (primaryItemCode != null) ? Integer.parseInt(primaryItemCode) : Integer.parseInt(itemCode);
		return mitmKey;
	}
	
	/**
	 * 
	 * @param itemElement
	 * @param orderKind
	 * @param eventType
	 * @param primaryItemLookup
	 * @return
	 */
	public static BigDecimal calculateConsPrice(Element itemElement, String orderKind, String eventType, 
												Map<String, String> primaryItemLookup) {
		
		BigDecimal consPrice = APTDriver.DECIMAL_ZERO;
		
		//int evtTypeQty = eventType.equals("TRX_Sale") ? 1 : -1;
		int promoUnits = Integer.parseInt(itemElement.getAttribute("qtyPromo")); // * evtTypeQty;
		int itemQty = Integer.parseInt(itemElement.getAttribute("qty")); // * evtTypeQty;
		
		//BigDecimal itemTotalPrice = new BigDecimal(itemElement.getAttribute("totalPrice"));
		BigDecimal itemTotalPrice = getBigDecimalValue(itemElement.getAttribute("totalPrice"));
		BigDecimal bpPrice = new BigDecimal(itemElement.getAttribute("BPPrice"));
		BigDecimal bdPrice = new BigDecimal(itemElement.getAttribute("BDPrice"));
		
		if (orderKind.equals("Refund")) {
			consPrice = (itemQty > 0) ? itemTotalPrice.divide(new BigDecimal(itemQty), 2, RoundingMode.HALF_UP) : APTDriver.DECIMAL_ZERO;
		}	
		
		if (orderKind.equals("Sale")) {                                                                                                                        
			   if (itemQty > promoUnits) {
					  consPrice = itemTotalPrice.divide(new BigDecimal(itemQty - promoUnits), 2, RoundingMode.HALF_UP);
			   } else if (itemQty == promoUnits) {
					  consPrice = bpPrice.divide(new BigDecimal(promoUnits), 2, RoundingMode.HALF_UP);
			   }                                                                                                                   
		 }
		 
		 if (orderKind.equals("Generic Discount") || orderKind.equals("Manager Discount") || orderKind.equals("Crew Discount")) {
			   if (itemQty > promoUnits) {
					  consPrice = bdPrice.divide(new BigDecimal(itemQty - promoUnits), 2, RoundingMode.HALF_UP);
			   } else if (itemQty == promoUnits) {
					  consPrice = bpPrice.divide(new BigDecimal(promoUnits), 2, RoundingMode.HALF_UP);
			   } 
		 }
		return consPrice;
	}
	
	/**
	 * Logic to generate PMIX output data
	 * @param itemElement
	 * @param orderKind
	 * @param lgcyLclRfrDefCd
	 * @param pmixStoreData
	 * @param basicPmixMap
	 * @return basicPmixMap
	 */
	public static Map<String, PmixData> generatePmixData(Element itemElement, String orderKind, String orderKey, String lgcyLclRfrDefCd,
														  Map<String, PmixData> pmixDataMap, boolean isParentInCmbLkp, StldItemType itemType,
														  String eventType, Map<String, String> primaryItemLookup) {
		
		int mitmKey = APTUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
		String seqNum = itemElement.getAttribute("id");
		
		//int evtTypeQty = eventType.equals("TRX_Sale") ? 1 : -1;
		int promoUnits = Integer.parseInt(itemElement.getAttribute("qtyPromo")); // * evtTypeQty;
		int itemQty = Integer.parseInt(itemElement.getAttribute("qty")); // * evtTypeQty;
		
		BigDecimal consPrice = calculateConsPrice(itemElement, orderKind, eventType, primaryItemLookup);
		
		String pmixMapKey = lgcyLclRfrDefCd+mitmKey+consPrice+orderKey+seqNum;
		PmixData pmixData = getPmixDataByMapKey(pmixDataMap, pmixMapKey);
		
		pmixData.setMitmKey(mitmKey);
		pmixData.setConsPrice(consPrice);
		pmixData.setOrderKey(seqNum+","+orderKey);
		pmixData.setItemLevel(itemElement.getAttribute("level"));
		pmixData.setComboBrkDwnPrice(consPrice.toString());
		
		// To Set DLY_PMIX_PR_AM
		if (itemType == StldItemType.CMP_IN_CMB_LKP) {
			pmixData.setDlyPmixPrice(new BigDecimal("999.99"));
		} else if (itemType == StldItemType.CMP_NOT_IN_CMB_LKP) {
			pmixData.setDlyPmixPrice(consPrice);
		} else if (itemType == StldItemType.ITEM_AT_LEVEL0) {
			pmixData.setLvl0PmixFlag("Y");
			pmixData.setLvl0PmixQty(Integer.parseInt(itemElement.getAttribute("qty")));
		}
		
		if (isParentInCmbLkp) {
			if (itemQty == promoUnits) {
				pmixData.setPromoComboQty(pmixData.getPromoComboQty() + promoUnits);
				pmixData.setPromoTotalQty(pmixData.getPromoQty() + pmixData.getPromoComboQty());
			} else {
				int soldQty = itemQty - promoUnits;
				pmixData.setPromoComboQty(pmixData.getPromoComboQty() + promoUnits);
				pmixData.setPromoTotalQty(pmixData.getPromoQty() + pmixData.getPromoComboQty());
				
				pmixData.setComboQty(pmixData.getComboQty() + soldQty);
				pmixData.setTotalQty(pmixData.getUnitsSold() + pmixData.getComboQty());
			}
		} else {
			if (itemQty == promoUnits) {
				pmixData.setPromoQty(pmixData.getPromoQty() + promoUnits);
				pmixData.setPromoTotalQty(pmixData.getPromoQty() + pmixData.getPromoComboQty());
			} else {
				int soldQty = itemQty - promoUnits;
				pmixData.setPromoQty(pmixData.getPromoQty() + promoUnits);
				pmixData.setPromoTotalQty(pmixData.getPromoQty() + pmixData.getPromoComboQty());
				
				pmixData.setUnitsSold(pmixData.getUnitsSold() + soldQty);
				pmixData.setTotalQty(pmixData.getUnitsSold() + pmixData.getComboQty());
			}
		}
		
		pmixDataMap.put(pmixMapKey, pmixData);
		
		return pmixDataMap;
	}
	
	/**
	 * Method to generate pmix for disagg
	 * @param lgcyLclRfrDefCd
	 * @param mitmKey
	 * @param pmixStoreData
	 * @param basicPmixMap
	 * @return
	 */
	public static Map<String, PmixData> generatePmixDataForDisagg(Element itemElement, String orderKey, String lgcyLclRfrDefCd, String[] componentDetails,  
																   Map<String, PmixData> pmixDataMap, // Map<String, String> menuItemLookup,
																   String eventType) {
		
		String seqNum = itemElement.getAttribute("id");
		//int evtTypeQty = eventType.equals("TRX_Sale") ? 1 : -1;
		int qty = Integer.parseInt(itemElement.getAttribute("qty")); //* evtTypeQty;
		int promoQty = Integer.parseInt(itemElement.getAttribute("qtyPromo")); //* evtTypeQty;
		
		int mitmKey = Integer.parseInt(componentDetails[1]);
		int lookupQty = Integer.parseInt(componentDetails[3]);
		
		BigDecimal consPrice = new BigDecimal("999.99");
		
		String pmixMapKey = lgcyLclRfrDefCd+mitmKey+consPrice+orderKey+seqNum;
		PmixData pmixData = getPmixDataByMapKey(pmixDataMap, pmixMapKey);
		
		pmixData.setMitmKey(mitmKey);
		pmixData.setConsPrice(consPrice);
		pmixData.setOrderKey(seqNum+","+orderKey);
		pmixData.setItemLevel("1"); // Item level for Disagg'ed Menu Items is by default "1"
		pmixData.setComboBrkDwnPrice("0.00");
		
		pmixData.setDlyPmixPrice(new BigDecimal("999.99")); // Set DLY_PMIX_PRC to 999.99 by default.
		
		if (qty == promoQty) {
			pmixData.setPromoComboQty(pmixData.getPromoComboQty() + (promoQty * lookupQty));
			pmixData.setPromoTotalQty(pmixData.getPromoQty() + pmixData.getPromoComboQty());
		} else {
			int soldQty = qty - promoQty;
	
			pmixData.setPromoComboQty(pmixData.getPromoComboQty() + (promoQty * lookupQty));
			pmixData.setPromoTotalQty(pmixData.getPromoQty() + pmixData.getPromoComboQty());
			 
			pmixData.setComboQty(pmixData.getComboQty() + (soldQty * lookupQty));
			pmixData.setTotalQty(pmixData.getUnitsSold() + pmixData.getComboQty());
		}
	
		pmixDataMap.put(pmixMapKey, pmixData);
		
		return pmixDataMap;
	}
	
	
	/**
	 * This method takes the following data as input and generates order detail output by parsing all the items in an order. 
	 * @param eleOrder
	 * @param pmixDataMap
	 * @param lgcyLclRfrDefCd
	 * @param orderKind
	 * @param orderKey
	 * @param eventType
	 * @param comboItemLookup
	 * @param primaryItemLookup
	 * @return Map<String, PmixData>
	 */
	public static Map<String, PmixData> generateOrderDetail(Element eleOrder, Map<String, PmixData> pmixDataMap, String lgcyLclRfrDefCd, 
															String orderKind, String orderKey, String eventType,
															Map<String, List<String>> comboItemLookup, Map<String, String> primaryItemLookup) {
		
		NodeList itemsList = (eleOrder == null ? null : eleOrder.getElementsByTagName("Item"));
		if (itemsList != null) {

			String level0_code = "";
			String p_level0_code = "";
			String level1_code = "";
			String p_level1_code = "";
			String level2_code = "";
			String p_level2_code = "";
			String level3_code = "";
			String p_level3_code = "";
			
			BigDecimal lvl0ConsPrice = APTDriver.DECIMAL_ZERO;
			
			String lvl0MapKey = "";
			PmixData lvl0PmixData = null;
			int lvl0MitmKey = 0;
			
			boolean isLvl0CodeInCmbcmp = false;
			
		    StringBuffer allComponentItems = null;
			for (int itemIdx = 0; itemIdx < itemsList.getLength(); itemIdx++) {
				
				Element itemElement = (Element) itemsList.item(itemIdx);
				// Parse through items at all levels and generate PMIX Output
	            if (Integer.parseInt(itemElement.getAttribute("qty")) > 0 && itemElement.getAttribute("qtyVoided").equals("")) {
	            	
	            	if (itemElement.getAttribute("level").equals("0")) {
	            		isLvl0CodeInCmbcmp = false;
	            		allComponentItems = new StringBuffer();
	            		level0_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
	            		lvl0MitmKey = APTUtil.getPrimaryItemCode(level0_code, primaryItemLookup);
	            		p_level0_code = String.format("%04d", lvl0MitmKey);
	            		
	            		pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, 
	            														StldItemType.ITEM_AT_LEVEL0, eventType, primaryItemLookup);
	            		
	            		lvl0ConsPrice = APTUtil.calculateConsPrice(itemElement,orderKind, eventType, primaryItemLookup);
	            		lvl0MapKey = lgcyLclRfrDefCd+lvl0MitmKey+lvl0ConsPrice+orderKey+itemElement.getAttribute("id");
	            		lvl0PmixData = APTUtil.getPmixDataByMapKey(pmixDataMap, lvl0MapKey);
	            		
	            		//lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(new BigDecimal(itemElement.getAttribute("totalPrice"))));
	            		lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(getBigDecimalValue(itemElement.getAttribute("totalPrice"))));
	            		pmixDataMap.put(lvl0MapKey, lvl0PmixData);
	            		
	            		if (comboItemLookup.containsKey(p_level0_code)) {
	            			isLvl0CodeInCmbcmp = true;
	            			lvl0PmixData.setLvl0InCmbCmpFlag("Y");
	            		} else {
	            			lvl0PmixData.setLvl0InCmbCmpFlag("N");
	            		}
	            			
	            		// Disagg Logic (Only for Level 0) - Iterate through all the componentItems found in comboItemLookup and generate PMIX if Component Flag is "A"
	            		
	            		if (comboItemLookup.containsKey(p_level0_code)) {
	            			List<String> componentItems = comboItemLookup.get(p_level0_code);
		            		if (componentItems != null) {
		            			for (String eachComponentInfo : componentItems) {
			            			String[] componentDetails = eachComponentInfo.split("\\|");
			            			if (componentDetails[2].equals("A")) {
			            				pmixDataMap = APTUtil.generatePmixDataForDisagg(itemElement, orderKey, lgcyLclRfrDefCd, componentDetails, pmixDataMap, eventType);
			            			} 
			            			
			            			if (!componentDetails[2].equals("B")) {
			            				allComponentItems.append(componentDetails[1]).append(",");
			            			}
			            		}
		            			lvl0PmixData.setAllComboComponents(allComponentItems.toString());
		            			pmixDataMap.put(lvl0MapKey, lvl0PmixData);
		            		}
	            		}
	            	}
	            	
	            	if (itemElement.getAttribute("level").equals("1")) {
	            		level1_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
	            		int mitmKey = APTUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
	            		p_level1_code = String.format("%04d", mitmKey);
	            		
	            		/**
	            		 * If the primary itemCode at previous level exists in comboItemLookup
	            		 * check if the itemCode at current level is a component with a flat != "A"
	            		 * Generate PMIX only if 
	            		 * 		- ItemCode at current level is not a component of the comboItem code OR
	            		 *      - ItemCode is a component and Component flag != "A"
	            		 */
	            		boolean componentExists = false;
            			boolean componentFlagNotA = false;
	            		if (comboItemLookup.containsKey(p_level0_code)) { // If the look up data contains menu item code at level 0
	            			List<String> componentItems = comboItemLookup.get(p_level0_code);
	            		    
	            			for (String eachComponentInfo : componentItems) {
	            				String[] componentDetails = eachComponentInfo.split("\\|");
	            				componentExists = (p_level1_code.equals(componentDetails[1])) ? true : false;
	            				componentFlagNotA = (componentExists && !componentDetails[2].equals("A")) ? true : false;
	            				if (componentExists || componentFlagNotA)
	            					break;
	            			}
	            			
	            			if (!componentExists) {
	            				pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
										StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
	            			}	
	            			
	            			if (componentFlagNotA)
	            				pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
	            																StldItemType.CMP_IN_CMB_LKP, eventType, primaryItemLookup);
	            			
	            		} else { // If the look up data does not contain menu item code at level 1, generate PMIX
	            			pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, 
	            															StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
	            		}
	            		
	            		if (isLvl0CodeInCmbcmp) {
	            			allComponentItems.append(level1_code).append(",");
		            		lvl0PmixData.setAllComboComponents(lvl0PmixData.getAllComboComponents() + allComponentItems.toString());
		            		//lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(new BigDecimal(itemElement.getAttribute("totalPrice"))));
		            		lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(getBigDecimalValue(itemElement.getAttribute("totalPrice"))));
		            		pmixDataMap.put(lvl0MapKey, lvl0PmixData);
	            		}
	            	}
	            	
	            	if (itemElement.getAttribute("level").equals("2")) {
	            		level2_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
	            		int mitmKey = APTUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
	            		p_level2_code = String.format("%04d", mitmKey);
	            		
	            		
	            		boolean componentExists = false;
            			boolean componentFlagNotA = false;
	            		if (comboItemLookup.containsKey(p_level1_code)) { // If the look up data contains menu item code at level 1
	            			List<String> componentItems = comboItemLookup.get(p_level1_code);
	            			
	            			for (String eachComponentInfo : componentItems) {
	            				String[] componentDetails = eachComponentInfo.split("\\|");
	            				componentExists = (p_level2_code.equals(componentDetails[1])) ? true : false;
	            				componentFlagNotA = (componentExists && !componentDetails[2].equals("A")) ? true : false;
	            				if (componentExists || componentFlagNotA)
	            					break;
	            			}
	            			
	            			if (!componentExists) {
	            				pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
										StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
	            			}	
	            			
	            			if (componentFlagNotA) 
	            				pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
	            																StldItemType.CMP_IN_CMB_LKP, eventType, primaryItemLookup);
	            				
	            			
	            		} else { // If the look up data does not contain menu item code at level 2, generate PMIX
	            			pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, 
	            															StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
	            		}
	            		
	            		if (isLvl0CodeInCmbcmp) {
	            			allComponentItems.append(level2_code).append(",");
		            		lvl0PmixData.setAllComboComponents(lvl0PmixData.getAllComboComponents() + allComponentItems.toString());
		            		//lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(new BigDecimal(itemElement.getAttribute("totalPrice"))));
		            		lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(getBigDecimalValue(itemElement.getAttribute("totalPrice"))));
		            		pmixDataMap.put(lvl0MapKey, lvl0PmixData);
	            		}
	            	}
	            	
	            	if (itemElement.getAttribute("level").equals("3")) {
	            		level3_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
	            		int mitmKey = APTUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
	            		p_level3_code = String.format("%04d", mitmKey);
	            		
	            		boolean componentExists = false;
            			boolean componentFlagNotA = false;
	            		if (comboItemLookup.containsKey(p_level2_code)) {
	            			List<String> componentItems = comboItemLookup.get(p_level2_code);
	            			
	            			for (String eachComponentInfo : componentItems) {
	            				String[] componentDetails = eachComponentInfo.split("\\|");
	            				componentExists = (p_level3_code.equals(componentDetails[1])) ? true : false;
	            				componentFlagNotA = (componentExists && !componentDetails[2].equals("A")) ? true : false;
	            				if (componentExists || componentFlagNotA)
	            					break;
	            			}
	            			
	            			if (!componentExists) {
	            				pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
										StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
	            			}
	            			
	            			if (componentFlagNotA)
	            				pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
	            																StldItemType.CMP_IN_CMB_LKP, eventType, primaryItemLookup);
	            				
	            			
	            		} else { // If the look up data does not contain menu item code at level 3, generate PMIX
	            			pmixDataMap = APTUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, 
	            															StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
	            		}
	            		
	            		if (isLvl0CodeInCmbcmp) {
	            			allComponentItems.append(level3_code).append(",");
		            		lvl0PmixData.setAllComboComponents(lvl0PmixData.getAllComboComponents() + allComponentItems.toString());
		            		//lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(new BigDecimal(itemElement.getAttribute("totalPrice"))));
		            		lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(getBigDecimalValue(itemElement.getAttribute("totalPrice"))));
		            		pmixDataMap.put(lvl0MapKey, lvl0PmixData);
	            		}
	            	}
	            }
			}	
		
		}
		return pmixDataMap;
	}
	
	
	
	/**
	 * Method to calculate Julian Number
	 * @param ymd
	 * @return julian Number
	 */
	public static double toJulian(int[] ymd) {
		   int year=ymd[0];
		   int month=ymd[1];
		   int day=ymd[2];    
		   int julianYear = year;
		   if (year < 0) julianYear++;
		   int julianMonth = month;
		   if (month > 2) {
		     julianMonth++;
		   }
		   else {
		     julianYear--;
		     julianMonth += 13;
		   }
		    
		   double julian = (java.lang.Math.floor(365.25 * julianYear)
		        + java.lang.Math.floor(30.6001*julianMonth) + day + 1720995.0);
		   if (day + 31 * (month + 12 * year) >= JGREG) {
		     // change over to Gregorian calendar
		     int ja = (int)(0.01 * julianYear);
		     julian += 2 - ja + (0.25 * ja);
		   }
		   return java.lang.Math.floor(julian);
		 }
	
	 /**
	  * Method to find the difference between two julian Numbers
	  * @param month
	  * @param day
	  * @param year
	  * @return julianNumber
	  */
	 public static long calculateJulianNumber(int month, int day, int year) {
		 double startJulianNumber = toJulian( new int[] {1960, 1, 1 });
		 double currentJulianNumber = toJulian( new int[] {year, month, day});
		 double julianNumber = currentJulianNumber - startJulianNumber;
		 return new Double(julianNumber).longValue();
	 }	
	
	/**
	 * Method to populate list of toy item codes in an array list.
	 * @return toysItemCodes
	 */
	public static List<String> populateToysItemCodes(int startToyCode, int endToyCode) {
		List<String> toysItemCodes = new ArrayList<String>();
		for(int i = startToyCode; i <= endToyCode; i++){
			toysItemCodes.add(String.valueOf(i));
		}
		return toysItemCodes;
	}
	
	/**
	 * Method to get unique item codes comma separated string
	 * @param duplicateComponentString
	 * @return
	 */
	public static String getUniqueComponents(String duplicateComponentString) {
		Set<String> uniqueCmpSet = new HashSet<String>();
		StringTokenizer st = new StringTokenizer(duplicateComponentString, ",");
		while(st.hasMoreTokens())
			uniqueCmpSet.add(st.nextToken());
		
		return StringUtils.join(uniqueCmpSet, ",");
	}
	
	/**
	 * This method takes the following data as input and returns value as BigDecimal.
	 * @param value
	 * @return BigDecimal
	 */
	public static BigDecimal getBigDecimalValue(String value)
	{
		BigDecimal decValue;
		try{	            			
			decValue=new BigDecimal(value);	            			
    		}catch(Exception e)
    		{
    			decValue = APTDriver.DECIMAL_ZERO;
    		}
		return decValue;
	}
	
}
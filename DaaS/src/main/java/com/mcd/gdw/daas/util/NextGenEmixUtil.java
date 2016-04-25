package com.mcd.gdw.daas.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.NextGenEmixDriver;
import com.mcd.gdw.daas.driver.NextGenEmixDriver.StldItemType;

/**
 * @author Naveen Kumar B.V
 *
 * Class for all utility methods - business logic, writing to output, validations, etc.
 */
public class NextGenEmixUtil {
	
	public static int JGREG= 15 + 31*(10+12*1582);
	public static double HALFSECOND = 0.5;
	
	private static StringBuffer outputStrBuf = new StringBuffer();
	private static Text outputTxt = new Text();
	
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
	
	/**
	 * Method to return PmixData object based on MapKey
	 * @param pmixDataMap
	 * @param mapKey
	 * @return
	 */
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
	 *  Method to return item price based on order kind
	 * @param itemElement
	 * @param orderKind
	 * @return
	 */
	public static BigDecimal determineItemPrice(Element itemElement, String orderKind) {
		
		int promoUnits = Integer.parseInt(itemElement.getAttribute("qtyPromo"));
		int itemQty = Integer.parseInt(itemElement.getAttribute("qty"));
		
		if (orderKind.equals("Sale") && (itemQty > promoUnits))
			return new BigDecimal(itemElement.getAttribute("totalPrice"));
		else if(orderKind.equals("Sale") &&  (itemQty == promoUnits))
			return new BigDecimal(itemElement.getAttribute("totalPrice"));
		else if ((orderKind.equals("Generic Discount") || orderKind.equals("Manager Discount") || orderKind.equals("Crew Discount")) 
							&& (itemQty > promoUnits))
			return new BigDecimal(itemElement.getAttribute("BDPrice"));
		else if ((orderKind.equals("Generic Discount") || orderKind.equals("Manager Discount") || orderKind.equals("Crew Discount")) 
							&& (itemQty == promoUnits))
			return new BigDecimal(itemElement.getAttribute("totalPrice"));

		return NextGenEmixDriver.DECIMAL_ZERO;
	}
	
	/**
	 * Method to calculate Cons Price used in PMIX output.
	 * @param itemElement
	 * @param orderKind
	 * @param eventType
	 * @param primaryItemLookup
	 * @return
	 */
	public static BigDecimal calculateConsPrice(Element itemElement, String orderKind, String eventType, 
												Map<String, String> primaryItemLookup) {
		
		BigDecimal consPrice = NextGenEmixDriver.DECIMAL_ZERO;
		
		int evtTypeQty = eventType.equals("TRX_Sale") ? 1 : -1;
		int promoUnits = Integer.parseInt(itemElement.getAttribute("qtyPromo")) * evtTypeQty;
		int itemQty = Integer.parseInt(itemElement.getAttribute("qty")) * evtTypeQty;
		
		BigDecimal itemTotalPrice = new BigDecimal(itemElement.getAttribute("totalPrice"));
		BigDecimal bpPrice = new BigDecimal(itemElement.getAttribute("BPPrice"));
		BigDecimal bdPrice = new BigDecimal(itemElement.getAttribute("BDPrice"));
		
		if (orderKind.equals("Refund")) {
			consPrice = (itemQty > 0) ? itemTotalPrice.divide(new BigDecimal(itemQty), 2, RoundingMode.HALF_UP) : NextGenEmixDriver.DECIMAL_ZERO;
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
		
		int mitmKey = NextGenEmixUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
		String seqNum = itemElement.getAttribute("id");
		
		int evtTypeQty = eventType.equals("TRX_Sale") ? 1 : -1;
		int promoUnits = Integer.parseInt(itemElement.getAttribute("qtyPromo")) * evtTypeQty;
		int itemQty = Integer.parseInt(itemElement.getAttribute("qty")) * evtTypeQty;
		
		BigDecimal consPrice = calculateConsPrice(itemElement, orderKind, eventType, primaryItemLookup);
		
		String pmixMapKey = lgcyLclRfrDefCd+mitmKey+consPrice+orderKey+seqNum;
		PmixData pmixData = getPmixDataByMapKey(pmixDataMap, pmixMapKey);
		
		pmixData.setMitmKey(mitmKey);
		pmixData.setConsPrice(consPrice);
		pmixData.setOrderKey(seqNum+orderKey);
		
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
		
		// To Set DLY_PMIX_PR_AM
		if (itemType == StldItemType.CMP_IN_CMB_LKP) {
			pmixData.setDlyPmixPrice(consPrice);
		} else if (itemType == StldItemType.CMP_NOT_IN_CMB_LKP) {
			pmixData.setDlyPmixPrice(consPrice);
			pmixData.setLvl0PmixQty(pmixData.getTotalQty());
		} else if (itemType == StldItemType.ITEM_AT_LEVEL0) {
			pmixData.setLvl0PmixFlag("Y");
			pmixData.setLvl0PmixQty(pmixData.getTotalQty());
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
																   Map<String, PmixData> pmixDataMap, String eventType) {
		
		String seqNum = itemElement.getAttribute("id");
		
		int evtTypeQty = eventType.equals("TRX_Sale") ? 1 : -1;
		int qty = Integer.parseInt(itemElement.getAttribute("qty")) * evtTypeQty;
		int promoQty = Integer.parseInt(itemElement.getAttribute("qtyPromo")) * evtTypeQty;
		
		int mitmKey = Integer.parseInt(componentDetails[1]);
		int lookupQty = Integer.parseInt(componentDetails[3]);
		
		BigDecimal consPrice = new BigDecimal("999.99");
		
		String pmixMapKey = lgcyLclRfrDefCd+mitmKey+consPrice+orderKey+seqNum;
		PmixData pmixData = getPmixDataByMapKey(pmixDataMap, pmixMapKey);
		
		pmixData.setMitmKey(mitmKey);
		pmixData.setConsPrice(consPrice);
		pmixData.setOrderKey(seqNum+orderKey);
		
		pmixData.setDlyPmixPrice(consPrice); // Set DLY_PMIX_PRC to 999.99 by default.
		
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
    * Method to calculate breakfast stop time.	
    * @param eleEvent
    * @param businessDate
    * @return
    * @throws ParseException
    */
   public static int calculateBreakfastStopTime(Element eleEvent, String businessDate) throws ParseException {
	    String weekendBreakfastStopTime = "";
		String weekdayBreakfastStopTime = "";
		int breakfastStopTime = 0;
		Element trxBaseConfig = null;
		NodeList configNodeList = null;
		Element configElement = null;
		NodeList configChildNodes = null;
		NodeList baseConfigChildNodes = null;
		boolean isbusinessDateWeekend = NextGenEmixUtil.isWeekend(businessDate); 
		
		try {
			baseConfigChildNodes = eleEvent.getChildNodes();		
			for (int idxConfigEvent = 0; idxConfigEvent < baseConfigChildNodes.getLength(); idxConfigEvent++) {
				if (baseConfigChildNodes.item(idxConfigEvent).getNodeType() == Node.ELEMENT_NODE) {
					trxBaseConfig = (Element)baseConfigChildNodes.item(idxConfigEvent);
					configNodeList = trxBaseConfig.getChildNodes();
					
					for (int idxTrxConfig = 0; idxTrxConfig < configNodeList.getLength(); idxTrxConfig++) {
						if (configNodeList.item(idxTrxConfig).getNodeName().equals("Config")) {
							configElement = (Element) configNodeList.item(idxTrxConfig);
							configChildNodes = configElement.getChildNodes();
							
							for (int idxConfig = 0; idxConfig < configChildNodes.getLength(); idxConfig++) {
								if (configChildNodes.item(idxConfig).getNodeName().equals("WeekEndBreakfastStopTime"))
									weekendBreakfastStopTime = configChildNodes.item(idxConfig).getTextContent();
								else if (configChildNodes.item(idxConfig).getNodeName().equals("WeekDayBreakfastStopTime"))
									weekdayBreakfastStopTime = configChildNodes.item(idxConfig).getTextContent();
							}
						    breakfastStopTime = isbusinessDateWeekend ? Integer.parseInt(weekendBreakfastStopTime) : Integer.parseInt(weekdayBreakfastStopTime);
						}
					}
				}
			}
			return breakfastStopTime;
		} catch (Exception e) {
			e.printStackTrace();
			return breakfastStopTime;
		}
	}
	 
	 
	public static StoreData calculateGiftCardAmountAndQty(Element eleOrder, String eventType, StoreData dailySaleStoreData) {
	   int tenderKind = 0;
	   String tenderName = "";
	   NodeList tendersList = (eleOrder == null ? null : eleOrder.getElementsByTagName("Tenders"));
		if (tendersList != null && tendersList.getLength() > 0) {
			for (int tListIdx = 0; tListIdx < tendersList.getLength(); tListIdx++) {
				   NodeList tendersChildNodes = tendersList.item(tListIdx).getChildNodes();
				   for (int tIdx = 0; tIdx < tendersChildNodes.getLength(); tIdx++) {
					    if (tendersChildNodes.item(tIdx).hasChildNodes()) {
						    Element tenderElement = (Element) tendersChildNodes.item(tIdx);
						    NodeList eachTenderNodeList = tenderElement.getChildNodes();
						    
						    if (eventType.equals("TRX_Sale")){
							    for (int i = 0; i < eachTenderNodeList.getLength(); i++) {
							    	
							    	if (eachTenderNodeList.item(i).getNodeName().equals("TenderKind")) {
							    		tenderKind = Integer.parseInt(eachTenderNodeList.item(i).getTextContent());
							    	}
							    	
							    	if (eachTenderNodeList.item(i).getNodeName().equals("TenderName")) {
							    		tenderName = eachTenderNodeList.item(i).getTextContent();
							    	}
							    	
							    	if (tenderKind == 8 && eachTenderNodeList.item(i).getNodeName().equals("TenderAmount")){
							    		dailySaleStoreData.setCouponRdmAmount(dailySaleStoreData.getCouponRdmAmount().
							    						add(new BigDecimal(eachTenderNodeList.item(i).getTextContent())));
							    		//gdwHourlySaleData.setCouponRdmAmount(gdwHourlySaleData.getCouponRdmAmount().
							    						//add(new BigDecimal(eachTenderNodeList.item(i).getTextContent())));
							    	}
							    	
							    	if (tenderKind == 8 && eachTenderNodeList.item(i).getNodeName().equals("TenderQuantity")){
							    		dailySaleStoreData.setCouponRdmQty(dailySaleStoreData.getCouponRdmQty() 
							    						+ Integer.parseInt(eachTenderNodeList.item(i).getTextContent()));
							    		// gdwHourlySaleData.setCouponRdmQty(gdwHourlySaleData.getCouponRdmQty() 
							    						//+ Integer.parseInt(eachTenderNodeList.item(i).getTextContent()));
							    	}
							    	
							    	if (tenderName.equals("Gift Card") && 
							    			eachTenderNodeList.item(i).getNodeName().equals("TenderAmount")) {
							    		dailySaleStoreData.setGiftCertRdmAmount(dailySaleStoreData.getGiftCertRdmAmount().
							    						add(new BigDecimal(eachTenderNodeList.item(i).getTextContent())));
							    		// gdwHourlySaleData.setGiftCertRdmAmount(gdwHourlySaleData.getGiftCertRdmAmount().
							    						// add(new BigDecimal(eachTenderNodeList.item(i).getTextContent())));
							    	}
							    	
							    	if (tenderName.equals("Gift Card") && 
							    				eachTenderNodeList.item(i).getNodeName().equals("TenderQuantity")) {
							    		dailySaleStoreData.setGiftCertRdmQty(dailySaleStoreData.getGiftCertRdmQty() 
							    						+ Integer.parseInt(eachTenderNodeList.item(i).getTextContent()));
							    		// gdwHourlySaleData.setGiftCertRdmQty(gdwHourlySaleData.getGiftCertRdmQty() 
							    					//	+ Integer.parseInt(eachTenderNodeList.item(i).getTextContent()));
							    	}
							    }																			    	
						    }
						}
				   }
				}
			}
			return dailySaleStoreData;
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
				
				BigDecimal lvl0ConsPrice = NextGenEmixDriver.DECIMAL_ZERO;
				
				String lvl0MapKey = "";
				PmixData lvl0PmixData = null;
				int lvl0MitmKey = 0;
				
				boolean isLvl0CodeInCmbcmp = false;
			    StringBuffer allComponentItems = null;
			    BigDecimal itemPrice = NextGenEmixDriver.DECIMAL_ZERO;
			    String lvl0ItemQty = "";
			    String otherLvlItemQty = "";
			    String disaggQty = "";
			    List<String> componentItems = null;
			    Element itemElement = null;
			    
				for (int itemIdx = 0; itemIdx < itemsList.getLength(); itemIdx++) {
					
					itemElement = (Element) itemsList.item(itemIdx);
					allComponentItems = new StringBuffer();
					// Parse through items at all levels and generate PMIX Output
		            if (Integer.parseInt(itemElement.getAttribute("qty")) > 0 && itemElement.getAttribute("qtyVoided").equals("")) {
		            	
		            	if (itemElement.getAttribute("level").equals("0")) {
		            		
		            		isLvl0CodeInCmbcmp = false;
		            		
		            		level0_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
		            		lvl0MitmKey = NextGenEmixUtil.getPrimaryItemCode(level0_code, primaryItemLookup);
		            		p_level0_code = String.format("%04d", lvl0MitmKey);
		            		lvl0ItemQty = itemElement.getAttribute("qty");
		            		
		            		pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, 
		            														StldItemType.ITEM_AT_LEVEL0, eventType, primaryItemLookup);
		            		
		            		lvl0ConsPrice = NextGenEmixUtil.calculateConsPrice(itemElement,orderKind, eventType, primaryItemLookup);
		            		lvl0MapKey = lgcyLclRfrDefCd+lvl0MitmKey+lvl0ConsPrice+orderKey+itemElement.getAttribute("id");
		            		lvl0PmixData = NextGenEmixUtil.getPmixDataByMapKey(pmixDataMap, lvl0MapKey);
		            		
		            		itemPrice = determineItemPrice(itemElement, orderKind);
		            		lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(itemPrice));
		            		pmixDataMap.put(lvl0MapKey, lvl0PmixData);
		            		
		            		if (comboItemLookup.containsKey(p_level0_code)) {
		            			isLvl0CodeInCmbcmp = true;
		            			lvl0PmixData.setLvl0InCmbCmpFlag("Y");
		            		} else {
		            			lvl0PmixData.setLvl0InCmbCmpFlag("N");
		            		}
		            			
		            		// Disagg Logic (Only for Level 0) - Iterate through all the componentItems found in comboItemLookup and generate PMIX if Component Flag is "A"
		            		if (comboItemLookup.containsKey(p_level0_code)) {
		            			componentItems = comboItemLookup.get(p_level0_code);
			            		if (componentItems != null) {
			            			for (String eachComponentInfo : componentItems) {
				            			String[] componentDetails = eachComponentInfo.split("\\|");
				            			if (componentDetails[2].equals("A")) {
				            				pmixDataMap = NextGenEmixUtil.generatePmixDataForDisagg(itemElement, orderKey, lgcyLclRfrDefCd, componentDetails, 
				            																			pmixDataMap, eventType);
				            			} 
				            			
				            			if (!componentDetails[2].equals("B")) {
				            				allComponentItems.append(componentDetails[1]).append(",").append(componentDetails[3]).append(";");
				            			}
				            		}
			            			lvl0PmixData.setAllComboComponents(allComponentItems.toString());
			            			pmixDataMap.put(lvl0MapKey, lvl0PmixData);
			            		}
		            		}
		            	}
		            	
		            	if (itemElement.getAttribute("level").equals("1")) {
		            		level1_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
		            		int mitmKey = NextGenEmixUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
		            		p_level1_code = String.format("%04d", mitmKey);
		            		otherLvlItemQty = itemElement.getAttribute("qty");
		            		
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
		            			componentItems = comboItemLookup.get(p_level0_code);
		            		    
		            			for (String eachComponentInfo : componentItems) {
		            				String[] componentDetails = eachComponentInfo.split("\\|");
		            				componentExists = (p_level1_code.equals(componentDetails[1])) ? true : false;
		            				componentFlagNotA = (componentExists && !componentDetails[2].equals("A")) ? true : false;
		            				if (componentExists || componentFlagNotA)
		            					break;
		            			}
		            			
		            			if (!componentExists) {
		            				pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
											StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
		            			}	
		            			
		            			if (componentFlagNotA)
		            				pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
		            																StldItemType.CMP_IN_CMB_LKP, eventType, primaryItemLookup);
		            			
		            		} else { // If the look up data does not contain menu item code at level 1, generate PMIX
		            			pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, 
		            															StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
		            		}
		            		
		            		if (isLvl0CodeInCmbcmp) {
		            			
		            			boolean codeInCmptStr = checkIfCodeIsInCmptString(lvl0PmixData.getAllComboComponents(), level1_code);
		            			if (!codeInCmptStr) {
		            				disaggQty = lvl0ItemQty.equals(otherLvlItemQty) ?  "1" : otherLvlItemQty;
		            				allComponentItems.append(level1_code).append(",").append(disaggQty).append(";");
				            		lvl0PmixData.setAllComboComponents(lvl0PmixData.getAllComboComponents() + allComponentItems.toString());
		            			}
		            			itemPrice = determineItemPrice(itemElement, orderKind);
			            		lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(itemPrice));
			            		pmixDataMap.put(lvl0MapKey, lvl0PmixData);
		            		}
		            	}
		            	
		            	if (itemElement.getAttribute("level").equals("2")) {
		            		level2_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
		            		int mitmKey = NextGenEmixUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
		            		p_level2_code = String.format("%04d", mitmKey);
		            		otherLvlItemQty = itemElement.getAttribute("qty");
		            		
		            		boolean componentExists = false;
	            			boolean componentFlagNotA = false;
		            		if (comboItemLookup.containsKey(p_level1_code)) { // If the look up data contains menu item code at level 1
		            			componentItems = comboItemLookup.get(p_level1_code);
		            			
		            			for (String eachComponentInfo : componentItems) {
		            				String[] componentDetails = eachComponentInfo.split("\\|");
		            				componentExists = (p_level2_code.equals(componentDetails[1])) ? true : false;
		            				componentFlagNotA = (componentExists && !componentDetails[2].equals("A")) ? true : false;
		            				if (componentExists || componentFlagNotA)
		            					break;
		            			}
		            			
		            			if (!componentExists) {
		            				pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
											StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
		            			}	
		            			
		            			if (componentFlagNotA) 
		            				pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
		            																StldItemType.CMP_IN_CMB_LKP, eventType, primaryItemLookup);
		            				
		            			
		            		} else { // If the look up data does not contain menu item code at level 2, generate PMIX
		            			pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, 
		            															StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
		            		}
		            		
		            		if (isLvl0CodeInCmbcmp) {
		            			boolean codeInCmptStr = checkIfCodeIsInCmptString(lvl0PmixData.getAllComboComponents(), level2_code);
		            			if (!codeInCmptStr) {
		            				disaggQty = lvl0ItemQty.equals(otherLvlItemQty) ?  "1" : otherLvlItemQty;
		            				allComponentItems.append(level2_code).append(",").append(disaggQty).append(";");
				            		lvl0PmixData.setAllComboComponents(lvl0PmixData.getAllComboComponents() + allComponentItems.toString());
		            			}
		            			itemPrice = determineItemPrice(itemElement, orderKind);
			            		lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(itemPrice));
			            		pmixDataMap.put(lvl0MapKey, lvl0PmixData);
		            		}
		            	}
		            	
		            	if (itemElement.getAttribute("level").equals("3")) {
		            		level3_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
		            		int mitmKey = NextGenEmixUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
		            		p_level3_code = String.format("%04d", mitmKey);
		            		otherLvlItemQty = itemElement.getAttribute("qty");
		            		
		            		boolean componentExists = false;
	            			boolean componentFlagNotA = false;
		            		if (comboItemLookup.containsKey(p_level2_code)) {
		            			componentItems = comboItemLookup.get(p_level2_code);
		            			
		            			for (String eachComponentInfo : componentItems) {
		            				String[] componentDetails = eachComponentInfo.split("\\|");
		            				componentExists = (p_level3_code.equals(componentDetails[1])) ? true : false;
		            				componentFlagNotA = (componentExists && !componentDetails[2].equals("A")) ? true : false;
		            				if (componentExists || componentFlagNotA)
		            					break;
		            			}
		            			
		            			if (!componentExists) {
		            				pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
											StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
		            			}
		            			
		            			if (componentFlagNotA)
		            				pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, 
		            																StldItemType.CMP_IN_CMB_LKP, eventType, primaryItemLookup);
		            				
		            			
		            		} else { // If the look up data does not contain menu item code at level 3, generate PMIX
		            			pmixDataMap = NextGenEmixUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, 
		            															StldItemType.CMP_NOT_IN_CMB_LKP, eventType, primaryItemLookup);
		            		}
		            		
		            		if (isLvl0CodeInCmbcmp) {
		            			boolean codeInCmptStr = checkIfCodeIsInCmptString(lvl0PmixData.getAllComboComponents(), level3_code);
		            			if (!codeInCmptStr) {
		            				disaggQty = lvl0ItemQty.equals(otherLvlItemQty) ?  "1" : otherLvlItemQty;
		            				allComponentItems.append(level3_code).append(",").append(disaggQty).append(";");
				            		lvl0PmixData.setAllComboComponents(lvl0PmixData.getAllComboComponents() + allComponentItems.toString());
		            			}
		            			itemPrice = determineItemPrice(itemElement, orderKind);
			            		lvl0PmixData.setDlyPmixPrice(lvl0PmixData.getDlyPmixPrice().add(itemPrice));
			            		pmixDataMap.put(lvl0MapKey, lvl0PmixData);
		            		}
		            	}
		            }
				}	
			
			}
			return pmixDataMap;
		}

		/**
		 * Method to check if an Item exists in component String.
		 * @param componentString
		 * @param itemCode
		 * @return
		 */
		public static boolean checkIfCodeIsInCmptString(String componentString, String itemCode) {
			String[] componentDetails = componentString.split(";");
			for (String eachCmptWithQty : componentDetails) {
				String[] str = eachCmptWithQty.split(",");
				if (itemCode.equals(str[0]))
					return true;
			}
			return false;
		} 
		
	   
	 /**
	  * Method to write Daily Sales data to a file using multiple output
	  * @param multipleOutputs
	  * @param dailySaleStoreData
	  * @param terrCd
	  * @param lgcyLclRfrDefCd
	  * @param gdwBusinessDate
	  * @param gdwTimeStamp
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 public static int[] writeGDWDailySalesToMultipleOutput(MultipleOutputs<Text, Text> multipleOutputs,StoreData dailySaleStoreData, String terrCd,
													String lgcyLclRfrDefCd, String gdwBusinessDate, String gdwTimeStamp) 
													throws IOException, InterruptedException {
		 
		 int recordCount = 0;
		 int uniqStoreCount = 0;
		 int[] recordAndStoreCount = new int[2];
		 
		 // TOTAL
		 if(dailySaleStoreData.getTotalAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
			 recordCount++;
			 uniqStoreCount++;
			 
			 outputStrBuf.setLength(0);
			 outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
							 .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
							 .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
							 .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
							 .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
							 .append(NextGenEmixDriver.TOTAL).append(DaaSConstants.PIPE_DELIMITER)
							 .append(dailySaleStoreData.getTotalAmount()).append(DaaSConstants.PIPE_DELIMITER)
							 .append(dailySaleStoreData.getTotalCount()).append(DaaSConstants.PIPE_DELIMITER)
							 .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
							 .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
							 .append(gdwTimeStamp);
				
			 outputTxt.set(outputStrBuf.toString());
			 multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Sls, NullWritable.get(), outputTxt);
		 }
		
		 // DRIVE THRU		 
		 if(dailySaleStoreData.getDriveThruAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
			 recordCount++;
			 outputStrBuf.setLength(0);
			 outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.DRIVE_THRU).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getDriveThruAmount()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getDriveThruCount()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwTimeStamp);
				
			 outputTxt.set(outputStrBuf.toString());
			 multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Sls, NullWritable.get(), outputTxt);
		 }
			
		// FRONT COUNTER
		 if(dailySaleStoreData.getFrontCounterAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
			 recordCount++;
			 outputStrBuf.setLength(0);
			 outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.FRONT_COUNTER).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getFrontCounterAmount()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getFrontCounterCount()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwTimeStamp);
				
			 outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Sls, NullWritable.get(), outputTxt);
		 }
		 
		 // PRODUCT
		 if(dailySaleStoreData.getProductAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
			 recordCount++;
			 outputStrBuf.setLength(0);				
			 outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.PRODUCT).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getProductAmount()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwTimeStamp);
				
			 outputTxt.set(outputStrBuf.toString());				
			 multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Sls, NullWritable.get(), outputTxt);
		 }	
		
		 // NON PRODUCT		 
		 if(dailySaleStoreData.getNonProductAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
			 recordCount++;
			 outputStrBuf.setLength(0);
			 outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(NextGenEmixDriver.NON_PRODUCT).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(dailySaleStoreData.getNonProductAmount()).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
					   		  .append(gdwTimeStamp);
			 	
			 outputTxt.set(outputStrBuf.toString()); 				
	 		 multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Sls, NullWritable.get(), outputTxt);
		  }
			
		 // COUPONS REDEEMED
		 if(dailySaleStoreData.getCouponRdmAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
			 recordCount++;
			 outputStrBuf.setLength(0);				
			 outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(NextGenEmixDriver.COUPONS_REDEEMED).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(dailySaleStoreData.getCouponRdmAmount()).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(dailySaleStoreData.getCouponRdmQty()).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
				   		  .append(gdwTimeStamp);
					
			 outputTxt.set(outputStrBuf.toString());					
			 multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Sls, NullWritable.get(), outputTxt);
		 }
		
		// GIFT CERTIFICATES REDEEMED
		 if(dailySaleStoreData.getGiftCertRdmAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
			 recordCount++;
			 outputStrBuf.setLength(0);				
			 outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.GIFT_CERT_REDEEMED).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getGiftCertRdmAmount()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getGiftCertRdmQty()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwTimeStamp);
			 
			 outputTxt.set(outputStrBuf.toString());					
			 multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Sls, NullWritable.get(), outputTxt);
		 }
			
		// GIFT CERTIFICATES SOLD
		 if(dailySaleStoreData.getGiftCouponAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
			 recordCount++;
			 outputStrBuf.setLength(0);				
			 outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.GIFT_CERT_SOLD).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getGiftCouponAmount()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.EMPTY_STR).append(DaaSConstants.PIPE_DELIMITER)
						  .append(dailySaleStoreData.getGiftCouponQty()).append(DaaSConstants.PIPE_DELIMITER)
						  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
						  .append(gdwTimeStamp);
			 
			 outputTxt.set(outputStrBuf.toString());					
			 multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Sls, NullWritable.get(), outputTxt);
		 }
		 
		 recordAndStoreCount[0] = recordCount;
		 recordAndStoreCount[1] = uniqStoreCount;
		 return recordAndStoreCount;		 
	 }
	 
	 /**
	  * Method to write HAVI daily sales to multiple output.
	  * @param multipleOutputs
	  * @param dailySaleStoreData
	  * @param lgcyLclRfrDefCd
	  * @param ownerShipType
	  * @param haviTimeKey
	  * @param haviBusinessDate
	  * @param haviTimeStamp
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 public static void writeHAVIDailySalesToMultipleOutput(MultipleOutputs<Text, Text> multipleOutputs, StoreData dailySaleStoreData, String lgcyLclRfrDefCd, String ownerShipType,
			 							long haviTimeKey, String haviBusinessDate, String haviTimeStamp) throws IOException, InterruptedException {
		 
		 
		// Havi Daily Sales Output
		 outputStrBuf.setLength(0);
		 outputStrBuf.append(haviTimeKey).append(DaaSConstants.PIPE_DELIMITER)
	        		  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
	        		  .append(haviBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
	        		  .append(ownerShipType).append(DaaSConstants.PIPE_DELIMITER)
	        		  .append(NextGenEmixDriver.MIX_SRCE_TYP_CD).append(DaaSConstants.PIPE_DELIMITER)                                            
	        		  .append(dailySaleStoreData.getTotalCount()).append(DaaSConstants.PIPE_DELIMITER)                                             
	        		  .append(dailySaleStoreData.getTotalAmount()).append(DaaSConstants.PIPE_DELIMITER)                                                 
	        		  .append(dailySaleStoreData.getDriveThruAmount()).append(DaaSConstants.PIPE_DELIMITER)    
	        		  .append(dailySaleStoreData.getDriveThruCount()).append(DaaSConstants.PIPE_DELIMITER)                                                 
	        		  .append(dailySaleStoreData.getCouponRdmQty()).append(DaaSConstants.PIPE_DELIMITER)
	        		  .append(dailySaleStoreData.getGiftCertRdmQty()).append(DaaSConstants.PIPE_DELIMITER)     
	        		  .append(dailySaleStoreData.getGiftCouponQty()).append(DaaSConstants.PIPE_DELIMITER) 
	        		  .append(dailySaleStoreData.getNonProductAmount()).append(DaaSConstants.PIPE_DELIMITER)
	        		  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)  
	        		  .append(haviTimeStamp).append(DaaSConstants.PIPE_DELIMITER) 
	        		  .append(haviTimeStamp).append(DaaSConstants.PIPE_DELIMITER)
	        		  .append(dailySaleStoreData.getCouponRdmAmount()).append(DaaSConstants.PIPE_DELIMITER)           
	        		  .append(dailySaleStoreData.getGiftCertRdmAmount()).append(DaaSConstants.PIPE_DELIMITER)  
	        		  .append(dailySaleStoreData.getGiftCouponAmount());
				
		 outputTxt.set(outputStrBuf.toString());
		 multipleOutputs.write(NextGenEmixDriver.HAVI_Dly_Sls, NullWritable.get(), outputTxt);
	 }
	 
	 /**
	  * Method to write GDW hourly sales to multiple output
	  * @param multipleOutputs
	  * @param terrCd
	  * @param lgcyLclRfrDefCd
	  * @param gdwBusinessDate
	  * @param gdwHourlyStoreMap
	  * @param gdwTimeStamp
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 public static int[] writeGDWHourlySalesToMultipleOutput(MultipleOutputs<Text, Text> multipleOutputs, String terrCd,String lgcyLclRfrDefCd, String gdwBusinessDate,
												Map<String, StoreData> gdwHourlyStoreMap, String gdwTimeStamp) throws IOException, InterruptedException {
		 
		 int recordCount = 0;
		 Set<String> uniqStoreSet = new HashSet<String>();
		 int[] recordAndStoreCount = new int[2];
		 
		// To Generate Output for GDW Hourly Sales
			SortedSet<String> timeSegments = new TreeSet<String>(gdwHourlyStoreMap.keySet());
			StoreData outStoreData = null;
			for (String eachTimeSeg : timeSegments) {
				outStoreData = gdwHourlyStoreMap.get(eachTimeSeg);
				// TOTAL
				if(outStoreData.getTotalAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
					recordCount++;
					uniqStoreSet.add(lgcyLclRfrDefCd);
					outputStrBuf.setLength(0);					
					outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.TOTAL).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append("Half Hourly").append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(eachTimeSeg).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(outStoreData.getTotalAmount()).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(outStoreData.getTotalCount()).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(gdwTimeStamp);						
					outputTxt.set(outputStrBuf.toString());						
					multipleOutputs.write(NextGenEmixDriver.GLMA_Hrly_Sls, NullWritable.get(), outputTxt);
				}
				
				// DRIVE THRU
				if(outStoreData.getDriveThruAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
					recordCount++;
					outputStrBuf.setLength(0);					
					outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.DRIVE_THRU).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append("Half Hourly").append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(eachTimeSeg).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(outStoreData.getDriveThruAmount()).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(outStoreData.getDriveThruCount()).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(gdwTimeStamp);						
					outputTxt.set(outputStrBuf.toString());						
					multipleOutputs.write(NextGenEmixDriver.GLMA_Hrly_Sls, NullWritable.get(), outputTxt);
				}
				
				// FRONT COUNTER
				if(outStoreData.getFrontCounterAmount().compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) {
					recordCount++;
					outputStrBuf.setLength(0);					
					outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.FRONT_COUNTER).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append("Half Hourly").append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(eachTimeSeg).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(outStoreData.getFrontCounterAmount()).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(outStoreData.getFrontCounterCount()).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
		    				  .append(gdwTimeStamp);
					
					outputTxt.set(outputStrBuf.toString());						
					multipleOutputs.write(NextGenEmixDriver.GLMA_Hrly_Sls, NullWritable.get(), outputTxt);
				}			
			}
			
			recordAndStoreCount[0] = recordCount;
			recordAndStoreCount[1] = uniqStoreSet.size();
			return recordAndStoreCount;
	 }
	 
	 /**
	  * Method to write HAVI hourly sales to multiple output
	  * @param multipleOutputs
	  * @param haviHourlyStoreMap
	  * @param lgcyLclRfrDefCd
	  * @param ownerShipType
	  * @param haviTimeKey
	  * @param haviBusinessDate
	  * @param haviTimeStamp
	  * @throws IOException
	  * @throws InterruptedException
	  */
	 public static void writeHAVIHourlySalesToMultipleOutput(MultipleOutputs<Text, Text> multipleOutputs,Map<String, StoreData> haviHourlyStoreMap, String lgcyLclRfrDefCd, 
			 		String ownerShipType, long haviTimeKey, String haviBusinessDate, String haviTimeStamp) throws IOException, InterruptedException {
		 
		// To generate output for Havi Hourly Sales
			SortedSet<String> hourlySegments = new TreeSet<String>(haviHourlyStoreMap.keySet());
			for (String eachHour : hourlySegments) {
				StoreData hourlyStoreData = haviHourlyStoreMap.get(eachHour);
				outputStrBuf.setLength(0);
				outputStrBuf.append(haviTimeKey).append(DaaSConstants.PIPE_DELIMITER) // TO Simplify
					  .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
					  .append(haviBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
					  .append(ownerShipType).append(DaaSConstants.PIPE_DELIMITER) // TO FIND
					  .append(NextGenEmixDriver.MIX_SRCE_TYP_CD).append(DaaSConstants.PIPE_DELIMITER)
					  .append(eachHour).append(DaaSConstants.PIPE_DELIMITER)
					  .append(hourlyStoreData.getTotalCount()).append(DaaSConstants.PIPE_DELIMITER)                                             
					  .append(hourlyStoreData.getTotalAmount()).append(DaaSConstants.PIPE_DELIMITER)
					  .append(hourlyStoreData.getDriveThruCount()).append(DaaSConstants.PIPE_DELIMITER)
					  .append(hourlyStoreData.getDriveThruAmount()).append(DaaSConstants.PIPE_DELIMITER)
					  .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
					  .append(haviTimeStamp).append(DaaSConstants.PIPE_DELIMITER) 
					  .append(haviTimeStamp).append(DaaSConstants.PIPE_DELIMITER)
					  .append(NextGenEmixDriver.EMPTY_STR);
				
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(NextGenEmixDriver.HAVI_Hrly_Sls, NullWritable.get(), outputTxt);
			}
	 }
	 
	 /**
	  * Method to write GDW pmix output to multiple output
	  * @param multipleOutputs
	  * @param finalPmixOutputMap
	  * @param gdwTimeStamp
	  * @param haviTimeStamp
	  * @throws IOException
	  * @throws InterruptedException
	  */
	public static int[] writeGDWPmixtoMultipleOutput(MultipleOutputs<Text, Text> multipleOutputs, Map<String, PmixData> finalPmixOutputMap, 
												String gdwTimeStamp, String haviTimeStamp) throws IOException, InterruptedException {
		
		PmixData finalPmixOutData = null;
		BigDecimal avgDlyPmixPrice = NextGenEmixDriver.DECIMAL_ZERO;
		BigDecimal avgComboUpDownAmt = NextGenEmixDriver.DECIMAL_ZERO;
		BigDecimal menuItemCount = NextGenEmixDriver.DECIMAL_ZERO;
		
		int recordCount = 0;
		Set<String> uniqStoreSet = new HashSet<String>();
		int[] recordAndStoreCount = new int[2];
		
		for (String eachPmixKey : finalPmixOutputMap.keySet()) {
			finalPmixOutData = finalPmixOutputMap.get(eachPmixKey);
			
			menuItemCount = (finalPmixOutData.getMenuItemCount() == 0) ? new BigDecimal("1.00") : new BigDecimal(finalPmixOutData.getMenuItemCount());
			avgDlyPmixPrice = finalPmixOutData.getTotalDlyPmixPrice().divide(menuItemCount, 2, RoundingMode.HALF_UP);
			avgComboUpDownAmt = finalPmixOutData.getComboUpDownAmt().divide(menuItemCount, 2, RoundingMode.HALF_UP);
			
			if ((avgDlyPmixPrice.compareTo(NextGenEmixDriver.DECIMAL_ZERO) == 0) && 
							(finalPmixOutData.getMenuItemCount() == 0)) {
				avgDlyPmixPrice = finalPmixOutData.getConsPrice();
			}
			
			// Total Sold
			if (finalPmixOutData.getTotalQty() > 0) {
				recordCount++;
				uniqStoreSet.add(finalPmixOutData.getStoreId());
				outputStrBuf.setLength(0);
				outputStrBuf.append(finalPmixOutData.getTerrCd()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getStoreId()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getGdwBusinessDate()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
						 .append("Total Sold").append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.NET_SELLING_PRICE).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getItemCode()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(avgDlyPmixPrice).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getUnitsSold()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getComboQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(avgComboUpDownAmt).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.MIX_SRCE_TYP_CD).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
						 .append(gdwTimeStamp);
						 /*.append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getTotalDlyPmixPrice()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getMenuItemCount()); */
				
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Pmix, NullWritable.get(), outputTxt);
			 }
			
			
			// PROMO
			if (finalPmixOutData.getPromoTotalQty() > 0) {
				recordCount++;
				uniqStoreSet.add(finalPmixOutData.getStoreId());
				outputStrBuf.setLength(0);
				outputStrBuf.append(finalPmixOutData.getTerrCd()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getStoreId()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.STORE_DESCRIPTION).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getGdwBusinessDate()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.US_CURRENCY_CODE).append(DaaSConstants.PIPE_DELIMITER)
						 .append("Promo").append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.NET_SELLING_PRICE).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getItemCode()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(avgDlyPmixPrice).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getPromoQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getPromoComboQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getPromoTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.DECIMAL_ZERO_STR).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.MIX_SRCE_TYP_CD).append(DaaSConstants.PIPE_DELIMITER)
						 .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
						 .append(gdwTimeStamp);
						 /*.append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getTotalDlyPmixPrice()).append(DaaSConstants.PIPE_DELIMITER)
						 .append(finalPmixOutData.getMenuItemCount());*/
				
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(NextGenEmixDriver.GLMA_Dly_Pmix, NullWritable.get(), outputTxt);
			}
			
			/*
			// HAVI PMIX Output	
			outputStrBuf.setLength(0);
			outputStrBuf.append(finalPmixOutData.getHaviTimeKey()).append(DaaSConstants.PIPE_DELIMITER)
					 .append(finalPmixOutData.getStoreId()).append(DaaSConstants.PIPE_DELIMITER)
					 .append(finalPmixOutData.getItemCode()).append(DaaSConstants.PIPE_DELIMITER)
                     .append(NextGenEmixDriver.PROM_KEY).append(DaaSConstants.PIPE_DELIMITER)
                     .append(finalPmixOutData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
                     .append(finalPmixOutData.getHaviBusinessDate()).append(DaaSConstants.PIPE_DELIMITER)
                     .append(finalPmixOutData.getStoreType()).append(DaaSConstants.PIPE_DELIMITER)
                     .append(NextGenEmixDriver.MIX_SRCE_TYP_CD).append(DaaSConstants.PIPE_DELIMITER)
                     .append(finalPmixOutData.getUnitsSold()).append(DaaSConstants.PIPE_DELIMITER)
                     .append(finalPmixOutData.getPromoQty()).append(DaaSConstants.PIPE_DELIMITER)
                     .append(finalPmixOutData.getComboQty()).append(DaaSConstants.PIPE_DELIMITER)
                     .append(NextGenEmixDriver.DECIMAL_ZERO_STR).append(DaaSConstants.PIPE_DELIMITER)
                     .append(NextGenEmixDriver.ORIGINAL_MENU_ITEM).append(DaaSConstants.PIPE_DELIMITER)
                     .append(NextGenEmixDriver.DECIMAL_ZERO_STR).append(DaaSConstants.PIPE_DELIMITER)
                     .append(NextGenEmixDriver.RSTMT_FL).append(DaaSConstants.PIPE_DELIMITER)
                     .append(haviTimeStamp);
			
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.HAVI_Dly_Pmix, NullWritable.get(), outputTxt);
			*/
		}
		
		recordAndStoreCount[0] = recordCount;
		recordAndStoreCount[1] = uniqStoreSet.size();
		return recordAndStoreCount;
	}
	
	
	/**
	 * Method to validate a numeric string.
	 * @param numericStr
	 * @return
	 */
	public static boolean isValidNumericStr(String numericStr) {
		
		return ((numericStr != null) && !(numericStr.equals("")) && numericStr.matches("\\d+"));
		
	}
	
	/**
	 * Method to validate signed numeric string
	 * @param signedNumStr
	 * @return
	 */
	public static boolean isValidSignedNumericStr(String signedNumStr) {		
		return ((signedNumStr != null) && !(signedNumStr.equals("")) && signedNumStr.matches("[0-9-]+"));		
	}
	
	/**
	 * Method to validate decimal string
	 * @param decimalStr
	 * @return
	 */
	public static boolean isValidDecimalStr(String decimalStr) {
		
		DecimalFormat decimalFormat = new DecimalFormat("#,##0.00");
		try {
			if ((decimalStr != null) && !(decimalStr.equals(""))) {
				decimalFormat.format(Double.valueOf(decimalStr));
				return true;
			} else 
				return false;
				
		} catch (NumberFormatException nfe) {
			return false;
		}
	}
	
	/**
	 * Method to validate business date
	 * @param businessDate
	 * @return
	 */
	public static boolean isValidBusinessDate(String businessDate) {
		
		return ((businessDate != null) && (!businessDate.equals("")) && 
					(businessDate.length() == 8) && (businessDate.matches("\\d+")));
	}
	
	/**
	 * Method to check for valid string
	 * @param value
	 * @return
	 */
	public static boolean isValidString(String value) {
		
		return (value != null) && !(value.equals(""));
	}
	
	/**
	 * Method to validate timestamp length
	 * @param timestamp
	 * @return
	 */
	public static boolean isValidTimestamp(String timestamp) {
		
		return ((timestamp != null) &&  (!(timestamp.equals(""))) && 
				(timestamp.matches("\\d+")) && (timestamp.length() >= 12));
	}
	
	/**
	 * Method to validate root element attributes in MenuItem XML.
	 * @param rootElement
	 * @param multipleOutputs
	 * @param fileName
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean validateMenuItemAttrs(Element rootElement, MultipleOutputs<Text, Text> multipleOutputs, String fileName) 
											throws IOException, InterruptedException {
		
		String lgcyLclRfrDefCd = rootElement.getAttribute("gdwLgcyLclRfrDefCd");
		if(!isValidNumericStr(lgcyLclRfrDefCd)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In Menu Item").append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid Store Id : " + lgcyLclRfrDefCd);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		String businessDate = rootElement.getAttribute("gdwBusinessDate");
		if(!isValidBusinessDate(businessDate)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In Menu Item").append(DaaSConstants.PIPE_DELIMITER)
						.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid Business Date : " + businessDate);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		return true;
	}
	
	/**
	 * Method to validate attributes in ProductInfo tag from MenuItem xml.
	 * @param productInfoElement
	 * @param lgcyLclRfrDefCd
	 * @param businessDate
	 * @param multipleOutputs
	 * @param fileName
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean validateProductInfoAttrs(Element productInfoElement, String lgcyLclRfrDefCd, String businessDate, 
												MultipleOutputs<Text, Text> multipleOutputs, String fileName) 
												throws IOException, InterruptedException {
		
		String itemCode = productInfoElement.getAttribute("id");
		if (!isValidNumericStr(itemCode)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In Menu Item").append(DaaSConstants.PIPE_DELIMITER)
						.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid Item Code : " + itemCode);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		String eatinPrice = productInfoElement.getAttribute("eatinPrice");
		if (!isValidDecimalStr(eatinPrice)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In Menu Item").append(DaaSConstants.PIPE_DELIMITER)
						.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid Eat In Price : " + eatinPrice);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		return true;
	}

	/**
	 * Method to validate TLD/root element attributes from STLD.
	 * @param rootElement
	 * @param multipleOutputs
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean validateTldElementAttrs(Element rootElement, MultipleOutputs<Text, Text> multipleOutputs) 
											throws IOException, InterruptedException {
		
		String terrCd = rootElement.getAttribute("gdwTerrCd");
		String lgcyLclRfrDefCd = rootElement.getAttribute("gdwLgcyLclRfrDefCd");
		
		if(!isValidNumericStr(terrCd)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid TerrCode : " + terrCd);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		if(!isValidNumericStr(lgcyLclRfrDefCd)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid StoreId : " + lgcyLclRfrDefCd);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		String businessDate = rootElement.getAttribute("businessDate");
		if(!isValidBusinessDate(businessDate)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid BusinessDate : " + businessDate);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		return true;
	}

	/**
	 * Method to validate Event Element attributes
	 * @param eleEvent
	 * @param lgcyLclRfrDefCd
	 * @param businessDate
	 * @param multipleOutputs
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean validateEventElementAttrs(Element eleEvent, String lgcyLclRfrDefCd, String businessDate, 
									MultipleOutputs<Text, Text> multipleOutputs) throws IOException, InterruptedException {
		
		String eventType = eleEvent.getAttribute("Type");
		if(!isValidString(eventType)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid Event Type : " + eventType);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		String eventTimeStamp = eleEvent.getAttribute("Time");
		if(!isValidTimestamp(eventTimeStamp)) {
			outputStrBuf.setLength(0);
			outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
						.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid EventTimestamp : " + eventTimeStamp);
			outputTxt.set(outputStrBuf.toString());
			multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		return true;
	}
	
	/**
	 * Method to validate TRX element attributes
	 * @param eleTrx
	 * @param lgcyLclRfrDefCd
	 * @param businessDate
	 * @param eventType
	 * @param eventTimeStamp
	 * @param multipleOutputs
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean validateTrxElementAttrs(Element eleTrx, String lgcyLclRfrDefCd, String businessDate, String eventType,
            									String eventTimeStamp, MultipleOutputs<Text, Text> multipleOutputs) 
            									throws IOException, InterruptedException {
       if (eventType.equals("TRX_Sale")) {
            String status = eleTrx.getAttribute("status");
            if (!isValidString(status)) {
                 outputStrBuf.setLength(0);
                 outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
                            .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
                            .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
                            .append(eventTimeStamp).append(DaaSConstants.PIPE_DELIMITER)
                            .append("Invalid Status : " + status);
                 outputTxt.set(outputStrBuf.toString());
                 multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
                 return false;
            }
       }

       String trxPOD = eleTrx.getAttribute("POD");
       if (!isValidString(trxPOD)) {
            outputStrBuf.setLength(0);
            outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
                      .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
                      .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
                      .append(eventTimeStamp).append(DaaSConstants.PIPE_DELIMITER)
                       .append("Invalid TrxPOD : " + trxPOD);
            outputTxt.set(outputStrBuf.toString());
            multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
            return false;
       }
       return true;
   }

	
	/**
	 * 
	 * @param eleOrder
	 * @param lgcyLclRfrDefCd
	 * @param businessDate
	 * @param multipleOutputs
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean validateOrderElementAttrs(Element eleOrder, String lgcyLclRfrDefCd, String businessDate, 
								MultipleOutputs<Text, Text> multipleOutputs) throws IOException, InterruptedException {
		
		String totalAmount = eleOrder.getAttribute("totalAmount");
		if(!isValidDecimalStr(totalAmount)) {
			outputStrBuf.setLength(0);
		    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
					    .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
					    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
					    .append("Invalid Total Amount : " + totalAmount);
		    outputTxt.set(outputStrBuf.toString());
		    multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		String nonProductAmount = eleOrder.getAttribute("nonProductAmount");
		if (!isValidDecimalStr(nonProductAmount)) {
			outputStrBuf.setLength(0);
		    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
					    .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
					    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
					    .append("Invalid NonProductAmount : " + totalAmount);
		    outputTxt.set(outputStrBuf.toString());
		    multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		String orderKind = eleOrder.getAttribute("kind");		
		if (!isValidString(orderKind)) {
			outputStrBuf.setLength(0);
		    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
					    .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
					    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
					    .append("Invalid Order Kind : " + orderKind);
		    outputTxt.set(outputStrBuf.toString());
		    multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		String orderKey = eleOrder.getAttribute("key");
		if (!isValidString(orderKey)) {
			outputStrBuf.setLength(0);
		    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
					    .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
					    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
					    .append("Invalid Order Key : " + orderKey);
		    outputTxt.set(outputStrBuf.toString());
		    multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
			return false;
		}
		
		return true;
	}
	
	/**
	 * Method to validate item tag attributes in STLD
	 * @param eleOrder
	 * @param lgcyLclRfrDefCd
	 * @param businessDate
	 * @param multipleOutputs
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean validateItemElementAttrs(Element eleOrder, String lgcyLclRfrDefCd, String businessDate, 
						MultipleOutputs<Text, Text> multipleOutputs) throws IOException, InterruptedException {
		
		Element itemElement = null;
		NodeList itemsList = (eleOrder == null ? null : eleOrder.getElementsByTagName("Item"));
		if (itemsList != null) {
			for (int itemIdx = 0; itemIdx < itemsList.getLength(); itemIdx++) {
				itemElement = (Element) itemsList.item(itemIdx);
				
				String code = itemElement.getAttribute("code");
				if (!isValidNumericStr(code)) {
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
								.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
								.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
								.append("Invalid Item Code : " + code);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}
				
				String level = itemElement.getAttribute("level");
				if (!isValidNumericStr(level)) {
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
								.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
								.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
								.append("Invalid Item Level : " + level);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}
				
				String id = itemElement.getAttribute("id");
				if (!isValidNumericStr(id)) {
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
								.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
								.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
								.append("Invalid Item Id : " + id);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}
			
				String qty = itemElement.getAttribute("qty");
				if (!isValidSignedNumericStr(qty)) {
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
								.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
								.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
								.append("Invalid Item qty : " + qty);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}	
					
				String qtyPromo = itemElement.getAttribute("qtyPromo");	
				if (!isValidSignedNumericStr(qtyPromo)) {
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
								.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
								.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
								.append("Invalid Item QtyPromo : " + qtyPromo);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}		
				
				String bpPrice = itemElement.getAttribute("BPPrice");		 	
				if (!isValidDecimalStr(bpPrice)) {
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
								.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
								.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
								.append("Invalid BPPrice : " + bpPrice);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}
					
			
				 
				String bdPrice = itemElement.getAttribute("BDPrice");
				if (!isValidDecimalStr(bdPrice)) {
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
								.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
								.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
								.append("Invalid BDPrice : " + bdPrice);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}
					
						
				String totalPrice =  itemElement.getAttribute("totalPrice");
				if (!isValidDecimalStr(totalPrice)){
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
								.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
								.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
								.append("Invalid TotalPrice : " + totalPrice);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(NextGenEmixDriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}
			}
			
		}
		return true;
	}
	
}

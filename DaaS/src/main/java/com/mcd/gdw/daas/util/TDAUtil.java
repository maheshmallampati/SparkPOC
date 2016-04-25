package com.mcd.gdw.daas.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.TDADriver;
import com.mcd.gdw.daas.driver.TDADriver.StldItemType;

/**
 * @author Naveen Kumar B.V
 * 
 * TDAUtil class contains helper methods and other methods with the actual business logic.
 */
public class TDAUtil {
	
	private static StringBuffer outputStrBuf = new StringBuffer();
	private static Text outputTxt = new Text();
	
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
	 * Method to determine itemPrice based on the orderKind.
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

		return TDADriver.DECIMAL_ZERO;
	}
	
	/**
	 * Method to calculate ConsPrice
	 * @param itemElement
	 * @param orderKind
	 * @param eventType
	 * @param primaryItemLookup
	 * @return
	 */
	public static BigDecimal calculateConsPrice(Element itemElement, String orderKind, String eventType, 
												Map<String, String> primaryItemLookup) {
		
		BigDecimal consPrice = TDADriver.DECIMAL_ZERO;
		
		int evtTypeQty = eventType.equals("TRX_Sale") ? 1 : -1;
		int promoUnits = Integer.parseInt(itemElement.getAttribute("qtyPromo")) * evtTypeQty;
		int itemQty = Integer.parseInt(itemElement.getAttribute("qty")) * evtTypeQty;
		
		BigDecimal itemTotalPrice = new BigDecimal(itemElement.getAttribute("totalPrice"));
		BigDecimal bpPrice = new BigDecimal(itemElement.getAttribute("BPPrice"));
		BigDecimal bdPrice = new BigDecimal(itemElement.getAttribute("BDPrice"));
		
		if (orderKind.equals("Refund")) {
			consPrice = (itemQty > 0) ? itemTotalPrice.divide(new BigDecimal(itemQty), 2, RoundingMode.HALF_UP) : TDADriver.DECIMAL_ZERO;
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
														  String eventType, String posEvntTypId, int itemLineSeqNum, 
														  Map<String, String> primaryItemLookup, Map<String, List<String>> comboItemLookup) {
		
		int mitmKey = TDAUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
		String primaryItemCode = String.format("%04d", mitmKey);
		String seqNum = itemElement.getAttribute("id");
		
		int evtTypeQty = eventType.equals("TRX_Sale") ? 1 : -1;
		int promoUnits = Integer.parseInt(itemElement.getAttribute("qtyPromo")) * evtTypeQty;
		int itemQty = Integer.parseInt(itemElement.getAttribute("qty")) * evtTypeQty;
		
		BigDecimal consPrice = calculateConsPrice(itemElement, orderKind, eventType, primaryItemLookup);
		
		String pmixMapKey = lgcyLclRfrDefCd+mitmKey+consPrice+orderKey+seqNum;
		PmixData pmixData = getPmixDataByMapKey(pmixDataMap, pmixMapKey);
		
		pmixData.setMitmKey(mitmKey);
		pmixData.setConsPrice(consPrice);
		pmixData.setOrderKey(seqNum+","+orderKey);
		pmixData.setItemLevel(itemElement.getAttribute("level"));
		pmixData.setStldItemCode(itemElement.getAttribute("code"));

		pmixData.setComboBrkDwnPrice(consPrice.toString());
		pmixData.setItemLineSeqNum(itemLineSeqNum);
		pmixData.setPosEvntTypId(posEvntTypId);
		
		NodeList itemChildNodes = itemElement.getChildNodes();
		int taxChainIdx = 0;
		String itemTaxAmt = "";
		String itemTaxRate = "";
		Element eleTaxChain = null;
		while (eleTaxChain == null && taxChainIdx < itemChildNodes.getLength()) {
			if (itemChildNodes.item(taxChainIdx).getNodeName().equals("TaxChain") && 
					itemChildNodes.item(taxChainIdx).getNodeType() == Node.ELEMENT_NODE) {
				eleTaxChain = (Element) itemChildNodes.item(taxChainIdx);
			}
			taxChainIdx++;
		}
		
		itemTaxAmt = eleTaxChain.getAttribute("amount");
		itemTaxRate = eleTaxChain.getAttribute("rate");
		pmixData.setItemTaxAmt(itemTaxAmt);
		pmixData.setItemTaxPercent(itemTaxRate);
		
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
		
		// Set menuItemComboFlag to 1 if the itemCode exists in CMBCMP else set it to 0
		if (comboItemLookup.containsKey(primaryItemCode)) {
			pmixData.setMenuItemComboFlag("1");
		} else {
			pmixData.setMenuItemComboFlag("0");
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
																   String eventType, String posEvntTypId, int itemLineSeqNum) {
		
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
		pmixData.setOrderKey(seqNum+","+orderKey);
		int itemLevel = Integer.parseInt(itemElement.getAttribute("level")) + 1;
		pmixData.setItemLevel(String.valueOf(itemLevel));
		pmixData.setStldItemCode("");
		pmixData.setItemTaxAmt("0.00");
		pmixData.setItemTaxPercent("0");
		pmixData.setComboBrkDwnPrice("0.00");
		pmixData.setItemLineSeqNum(itemLineSeqNum);
		pmixData.setPosEvntTypId(posEvntTypId);
		
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
																String orderKind, String orderKey, String eventType, String posEvntTypId, 
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
				
				BigDecimal lvl0ConsPrice = TDADriver.DECIMAL_ZERO;
				
				String lvl0MapKey = "";
				PmixData lvl0PmixData = null;
				int lvl0MitmKey = 0;
				
				boolean isLvl0CodeInCmbcmp = false;
			    StringBuffer allComponentItems = null;
			    BigDecimal itemPrice = TDADriver.DECIMAL_ZERO;
			    String lvl0ItemQty = "";
			    String otherLvlItemQty = "";
			    String disaggQty = "";
			    List<String> componentItems = null;
			    Element itemElement = null;
			    
			    int itemLineSeqNum = 0;
			    
				for (int itemIdx = 0; itemIdx < itemsList.getLength(); itemIdx++) {
					
					itemElement = (Element) itemsList.item(itemIdx);
					allComponentItems = new StringBuffer();
					// Parse through items at all levels and generate PMIX Output
		            if (Integer.parseInt(itemElement.getAttribute("qty")) > 0 && itemElement.getAttribute("qtyVoided").equals("")) {
		            	
		            	if (itemElement.getAttribute("level").equals("0")) {
		            		
		            		isLvl0CodeInCmbcmp = false;
		            		
		            		level0_code = String.format("%04d", Integer.parseInt(itemElement.getAttribute("code")));
		            		lvl0MitmKey = TDAUtil.getPrimaryItemCode(level0_code, primaryItemLookup);
		            		p_level0_code = String.format("%04d", lvl0MitmKey);
		            		lvl0ItemQty = itemElement.getAttribute("qty");
		            		
		            		itemLineSeqNum += 1;
		            		pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, StldItemType.ITEM_AT_LEVEL0,
		            												eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
		            		
		            		lvl0ConsPrice = TDAUtil.calculateConsPrice(itemElement,orderKind, eventType, primaryItemLookup);
		            		lvl0MapKey = lgcyLclRfrDefCd+lvl0MitmKey+lvl0ConsPrice+orderKey+itemElement.getAttribute("id");
		            		lvl0PmixData = TDAUtil.getPmixDataByMapKey(pmixDataMap, lvl0MapKey);
		            		
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
				            				itemLineSeqNum += 1;
				            				pmixDataMap = TDAUtil.generatePmixDataForDisagg(itemElement, orderKey, lgcyLclRfrDefCd, componentDetails, 
				            																			pmixDataMap, eventType, posEvntTypId, itemLineSeqNum);
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
		            		int mitmKey = TDAUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
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
		            				itemLineSeqNum += 1;
		            				pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, StldItemType.CMP_NOT_IN_CMB_LKP,
		            							eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
		            			}	
		            			
		            			if (componentFlagNotA) {
		            				itemLineSeqNum += 1;
		            				pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, StldItemType.CMP_IN_CMB_LKP,
											 	eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
		            			}
		            				
		            			
		            		} else { // If the look up data does not contain menu item code at level 1, generate PMIX
		            			itemLineSeqNum += 1;
		            			pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, StldItemType.CMP_NOT_IN_CMB_LKP,
		            													eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
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
		            		int mitmKey = TDAUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
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
		            				itemLineSeqNum += 1;
		            				pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, StldItemType.CMP_NOT_IN_CMB_LKP,
											 	eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
		            			}	
		            			
		            			if (componentFlagNotA) {
		            				itemLineSeqNum += 1;
		            				pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, StldItemType.CMP_IN_CMB_LKP,
											 	eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
		            			}
		            				
		            				
		            			
		            		} else { // If the look up data does not contain menu item code at level 2, generate PMIX
		            			itemLineSeqNum += 1;
		            			pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, StldItemType.CMP_NOT_IN_CMB_LKP,
		            												   eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
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
		            		int mitmKey = TDAUtil.getPrimaryItemCode(itemElement.getAttribute("code"), primaryItemLookup);
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
		            				itemLineSeqNum += 1;
		            				pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, StldItemType.CMP_NOT_IN_CMB_LKP,
											 		eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
		            			}
		            			
		            			if (componentFlagNotA) {
		            				itemLineSeqNum += 1;
		            				pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, true, StldItemType.CMP_IN_CMB_LKP, 
											 		eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
		            			}
		            				
		            				
		            			
		            		} else { // If the look up data does not contain menu item code at level 3, generate PMIX
		            			itemLineSeqNum += 1;
		            			pmixDataMap = TDAUtil.generatePmixData(itemElement, orderKind, orderKey, lgcyLclRfrDefCd, pmixDataMap, false, StldItemType.CMP_NOT_IN_CMB_LKP,
		            													eventType, posEvntTypId, itemLineSeqNum, primaryItemLookup, comboItemLookup);
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
		 * Method to check if an item code is present in component string.
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
		 * Method to check if a given string is valid number
		 * @param numericStr
		 * @return
		 */
		public static boolean isValidNumericStr(String numericStr) {		
			return ((numericStr != null) && !(numericStr.equals("")) && numericStr.matches("\\d+"));		
		}
	
		/**
		 * Method to check if a given string is a valid signed number
		 * @param signedNumStr
		 * @return
		 */
		public static boolean isValidSignedNumericStr(String signedNumStr) {		
			return ((signedNumStr != null) && !(signedNumStr.equals("")) && signedNumStr.matches("[0-9-]+"));		
		}
	
		/**
		 * Method to check if a given string is a valid decimal number
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
		 * Method to check if a given string is valid business date(numbers and length)
		 * @param businessDate
		 * @return
		 */
		public static boolean isValidBusinessDate(String businessDate) {
			
			return ((businessDate != null) && (!businessDate.equals("")) && 
						(businessDate.length() == 8) && (businessDate.matches("\\d+")));
		}
	
		/**
		 * Method to check if a string is valid (empty or null)
		 * @param value
		 * @return
		 */
		public static boolean isValidString(String value) {		
			return (value != null) && !(value.equals(""));
		}
	
		/**
		 * Method to check if a given string is a valid timestamp(number and length)
		 * @param timestamp
		 * @return
		 */
		public static boolean isValidTimestamp(String timestamp) {
			
			return ((timestamp != null) &&  (!(timestamp.equals(""))) && 
					(timestamp.matches("\\d+")) && (timestamp.length() >= 12));
		}
	

		/**
		 * Method to validate attributes under root tag in menu item file 
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
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String businessDate = rootElement.getAttribute("gdwBusinessDate");
			if(!isValidBusinessDate(businessDate)) {
				outputStrBuf.setLength(0);
				outputStrBuf.append("In Menu Item").append(DaaSConstants.PIPE_DELIMITER)
							.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
							.append("Invalid Business Date : " + businessDate);
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			return true;
		}
	
		/**
		 * Method to validate attributes under ProductInfo element in menu item file
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
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
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
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			return true;
		}

		/**
		 * Method to validate attributes under root element in STLD file
		 * @param rootElement
		 * @param multipleOutputs
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static boolean validateTldElementAttrs(Element rootElement, MultipleOutputs<Text, Text> multipleOutputs) 
												throws IOException, InterruptedException {
			
			String terrCd = rootElement.getAttribute("gdwTerrCd");
			if(!isValidNumericStr(terrCd)) {
				outputStrBuf.setLength(0);
				outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
							.append("Invalid TerrCode : " + terrCd);
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String lgcyLclRfrDefCd = rootElement.getAttribute("gdwLgcyLclRfrDefCd");
			if(!isValidNumericStr(lgcyLclRfrDefCd)) {
				outputStrBuf.setLength(0);
				outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
							.append("Invalid lgcyLclRfrDefCd : " + lgcyLclRfrDefCd);
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String gdwMcdGbalLcatIdNu = rootElement.getAttribute("gdwMcdGbalLcatIdNu");
			if(!isValidNumericStr(gdwMcdGbalLcatIdNu)) {
				outputStrBuf.setLength(0);
				outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
							.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
							.append("Invalid gdwMcdGbalLcatIdNu : " + gdwMcdGbalLcatIdNu);
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String businessDate = rootElement.getAttribute("businessDate");
			if(!isValidBusinessDate(businessDate)) {
				outputStrBuf.setLength(0);
				outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
							.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
							.append("Invalid BusinessDate : " + businessDate);
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			return true;
		}
	
		/**
		 * Method to validate attributes under node element in STLD file
		 * @param eleNode
		 * @param multipleOutputs
		 * @return
		 */
		public static boolean validateNodeElementAttrs(Element eleNode, String gdwMcdGbalLcatIdNu, String businessDate,
											MultipleOutputs<Text, Text> multipleOutputs) throws IOException, InterruptedException {
			
			String nodeId = eleNode.getAttribute("id");
			if (!isValidString(nodeId) || nodeId.length() < 7) {
				outputStrBuf.setLength(0);
				outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
				.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
				.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
				.append("Invalid Node Id : " + nodeId);
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			return true;
		}

		/**
		 * Method to validate attributes under Event element in STLD file
		 * @param eleEvent
		 * @param lgcyLclRfrDefCd
		 * @param businessDate
		 * @param multipleOutputs
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static boolean validateEventElementAttrs(Element eleEvent, String gdwMcdGbalLcatIdNu, String businessDate, 
										MultipleOutputs<Text, Text> multipleOutputs) throws IOException, InterruptedException {
			
			String eventType = eleEvent.getAttribute("Type");
			if(!isValidString(eventType)) {
				outputStrBuf.setLength(0);
				outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
							.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
							.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
							.append("Invalid Event Type : " + eventType);
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			return true;
		}

		/**
		 * Method to validate TRX element attributes
		 * @param eleTrx
		 * @param gdwMcdGbalLcatIdNu
		 * @param businessDate
		 * @param eventType
		 * @param eventTimeStamp
		 * @param multipleOutputs
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static boolean validateTrxElementAttrs(Element eleTrx, String gdwMcdGbalLcatIdNu, String businessDate,
													   String eventType, String eventTimeStamp,
													  MultipleOutputs<Text, Text> multipleOutputs) throws IOException, InterruptedException {
			
			if (eventType.equals("TRX_Sale")) {
				String status = eleTrx.getAttribute("status");
				if (!isValidString(status)) {
					outputStrBuf.setLength(0);
					outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
							.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
							.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
							.append(eventTimeStamp).append(DaaSConstants.PIPE_DELIMITER)
							.append("Invalid Status : " + status);
					outputTxt.set(outputStrBuf.toString());
					multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
					return false;
				}
			}
	
			String trxPOD = eleTrx.getAttribute("POD");
			if (!isValidString(trxPOD)) {
				outputStrBuf.setLength(0);
				outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
						.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						.append(eventTimeStamp).append(DaaSConstants.PIPE_DELIMITER)
						.append("Invalid TrxPOD : " + trxPOD);
				outputTxt.set(outputStrBuf.toString());
				multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			return true;
		}
		
		/**
		 * Method to validate attributes under Order element in STLD file
		 * @param eleOrder
		 * @param gdwMcdGbalLcatIdNu
		 * @param businessDate
		 * @param multipleOutputs
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static boolean validateOrderElementAttrs(Element eleOrder, String gdwMcdGbalLcatIdNu, String businessDate, 
									MultipleOutputs<Text, Text> multipleOutputs) throws IOException, InterruptedException {
			
			String orderTimestamp = eleOrder.getAttribute("Timestamp");
			if(!isValidNumericStr(orderTimestamp)) {
				outputStrBuf.setLength(0);
			    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						    .append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
						    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						    .append("Invalid Order Timestamp : " + orderTimestamp);
			    outputTxt.set(outputStrBuf.toString());
			    multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String uniqueId = eleOrder.getAttribute("uniqueId");
			if(!isValidString(uniqueId)) {
				outputStrBuf.setLength(0);
			    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						    .append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
						    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						    .append("Invalid UniqueId : " + uniqueId);
			    outputTxt.set(outputStrBuf.toString());
			    multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			
			String orderKind = eleOrder.getAttribute("kind");		
			if (!isValidString(orderKind)) {
				outputStrBuf.setLength(0);
			    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						    .append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
						    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						    .append("Invalid Order Kind : " + orderKind);
			    outputTxt.set(outputStrBuf.toString());
			    multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String orderKey = eleOrder.getAttribute("key");
			if (!isValidString(orderKey)) {
				outputStrBuf.setLength(0);
			    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						    .append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
						    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						    .append("Invalid Order Key : " + orderKey);
			    outputTxt.set(outputStrBuf.toString());
			    multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String saleType = eleOrder.getAttribute("saleType");
			if (!isValidString(saleType)) {
				outputStrBuf.setLength(0);
			    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						    .append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
						    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						    .append("Invalid Sale Type : " + saleType);
			    outputTxt.set(outputStrBuf.toString());
			    multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String totalAmount = eleOrder.getAttribute("totalAmount");
			if(!isValidDecimalStr(totalAmount)) {
				outputStrBuf.setLength(0);
			    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						    .append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
						    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						    .append("Invalid Total Amount : " + totalAmount);
			    outputTxt.set(outputStrBuf.toString());
			    multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			String orderTax = eleOrder.getAttribute("totalTax");
			if(!isValidDecimalStr(orderTax)) {
				outputStrBuf.setLength(0);
			    outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
						    .append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
						    .append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
						    .append("Invalid Order TotalTax : " + orderTax);
			    outputTxt.set(outputStrBuf.toString());
			    multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
				return false;
			}
			
			return true;
		}
	
		
		/**
		 * Method to validate attributes under Item element in STLD file
		 * @param eleOrder
		 * @param gdwMcdGbalLcatIdNu
		 * @param businessDate
		 * @param multipleOutputs
		 * @return
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public static boolean validateItemElementAttrs(Element eleOrder, String gdwMcdGbalLcatIdNu, String businessDate, 
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
									.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
									.append("Invalid Item Code : " + code);
						outputTxt.set(outputStrBuf.toString());
						multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
						return false;
					}
					
					String level = itemElement.getAttribute("level");
					if (!isValidNumericStr(level)) {
						outputStrBuf.setLength(0);
						outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
									.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
									.append("Invalid Item Level : " + level);
						outputTxt.set(outputStrBuf.toString());
						multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
						return false;
					}
					
					String id = itemElement.getAttribute("id");
					if (!isValidNumericStr(id)) {
						outputStrBuf.setLength(0);
						outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
									.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
									.append("Invalid Item Id : " + id);
						outputTxt.set(outputStrBuf.toString());
						multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
						return false;
					}
				
					String qty = itemElement.getAttribute("qty");
					if (!isValidSignedNumericStr(qty)) {
						outputStrBuf.setLength(0);
						outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
									.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
									.append("Invalid Item qty : " + qty);
						outputTxt.set(outputStrBuf.toString());
						multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
						return false;
					}	
						
					String qtyPromo = itemElement.getAttribute("qtyPromo");	
					if (!isValidSignedNumericStr(qtyPromo)) {
						outputStrBuf.setLength(0);
						outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
									.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
									.append("Invalid Item QtyPromo : " + qtyPromo);
						outputTxt.set(outputStrBuf.toString());
						multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
						return false;
					}		
					
					String bpPrice = itemElement.getAttribute("BPPrice");		 	
					if (!isValidDecimalStr(bpPrice)) {
						outputStrBuf.setLength(0);
						outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
									.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
									.append("Invalid BPPrice : " + bpPrice);
						outputTxt.set(outputStrBuf.toString());
						multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
						return false;
					}
					 
					String bdPrice = itemElement.getAttribute("BDPrice");
					if (!isValidDecimalStr(bdPrice)) {
						outputStrBuf.setLength(0);
						outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
									.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
									.append("Invalid BDPrice : " + bdPrice);
						outputTxt.set(outputStrBuf.toString());
						multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
						return false;
					}
						
							
					String totalPrice =  itemElement.getAttribute("totalPrice");
					if (!isValidDecimalStr(totalPrice)){
						outputStrBuf.setLength(0);
						outputStrBuf.append("In STLD").append(DaaSConstants.PIPE_DELIMITER)
									.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									.append(businessDate).append(DaaSConstants.PIPE_DELIMITER)
									.append("Invalid TotalPrice : " + totalPrice);
						outputTxt.set(outputStrBuf.toString());
						multipleOutputs.write(TDADriver.Invalid_Input_Log, NullWritable.get(), outputTxt);
						return false;
					}
				}
				
			}
			return true;
		}

	/**
	 * Method to add event codes to a hashmap
	 * @param eventCodesMap
	 * @return
	 */
	public static Map<String, String> addEventCodesToMap(Map<String, String> eventCodesMap) {
		eventCodesMap = new HashMap<String, String>();		
		eventCodesMap.put("TRX_Sale", "1");
		eventCodesMap.put("TRX_Refund", "2");
		eventCodesMap.put("TRX_Overring", "3");
		return eventCodesMap;
	}
	
	/**
	 * Method to add trxPod codes to a hashmap
	 * @param trxPodMap
	 * @return
	 */
	public static Map<String, String> addTrxPodCodesToMap(Map<String, String> trxPodMap) {
		trxPodMap = new HashMap<String, String>();
		trxPodMap.put("Front Counter","10001");
		trxPodMap.put("Drive Thru","10002");
		trxPodMap.put("CSO (Customer Self Ordering)","10003");
		return trxPodMap;
		
	}
	
	/**
	 * Method to add orderKind codes to a Hashmap
	 * @param orderKindMap
	 * @return
	 */
	public static Map<String, String> addOrderKindCodesToMap(Map<String, String> orderKindMap) {
		orderKindMap = new HashMap<String, String>();
		orderKindMap.put("Sale", "1");
		orderKindMap.put("Refund", "2");
		orderKindMap.put("Crew Discount", "3");
		orderKindMap.put("Generic Discount", "4");
		orderKindMap.put("Manager Discount", "5");
		return orderKindMap;		
	}

	/**
	 * Method to add saleType codes to a hashmap
	 * @param saleTypeMap
	 * @return
	 */
	public static Map<String, String> addSaleTypeCodesToMap(Map<String, String> saleTypeMap) {
		saleTypeMap = new HashMap<String, String>();
		saleTypeMap.put("EatIn", "20001");
		saleTypeMap.put("TakeOut", "20002");
		saleTypeMap.put("Delivery", "20003");
		return saleTypeMap;		
	}

	/**
	 * Method to determine handHeldordFl based on node id
	 * @param nodeId
	 * @return
	 */
	public static String determineHandheldOrdFl(String nodeId) {
		String nodeSubId = nodeId.substring(3, 7);
		return (Integer.parseInt(nodeSubId) > 15) ? "1" : "0";	
	}
	
	/**
	 * Method to add paymentCodes to a hashmap
	 * @param paymentCodesMap
	 * @return
	 */
	public static Map<String, String> addPaymentCodesToMap(Map<String, String> paymentCodesMap) {
		
		paymentCodesMap = new HashMap<String, String>();
		paymentCodesMap.put("$1 Gift Certif.", "6");
		paymentCodesMap.put("10% off", "6");
		paymentCodesMap.put("Amt Discount", "6");
		paymentCodesMap.put("Cashless", "13");
		paymentCodesMap.put("Coupon A", "5");
		paymentCodesMap.put("Coupon B", "5");
		paymentCodesMap.put("Coupon C", "5");
		paymentCodesMap.put("Employee Meal", "6");
		paymentCodesMap.put("Gift Card", "8");
		paymentCodesMap.put("Manager Meal", "6");
		paymentCodesMap.put("US$", "1");
		
		return paymentCodesMap;
	}
	
	/**
	 * Method to format order timestamp
	 * @param orderTimeStamp
	 * @return
	 */
	public static String formatOrderTimestamp(String orderTimeStamp) {
		String formattedTimestamp = "";
		try {		    
		    SimpleDateFormat inputFormatter  = new SimpleDateFormat("yyyyMMddHHmmssS");
		    SimpleDateFormat outputFormatter  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		    Date date = inputFormatter.parse(orderTimeStamp);
		    formattedTimestamp = outputFormatter.format(date);
		 } catch (ParseException pe) {
		    pe.getMessage();
		}
		return formattedTimestamp;
	}

	/**
	 * Method to format order date
	 * @param dateStr
	 * @return
	 */
	public static String formatOrderDate(String dateStr) {
		String formattedDate = "";
		try {
			SimpleDateFormat inputFormatter  = new SimpleDateFormat("yyyyMMddHHmmssS");
			SimpleDateFormat outputFormatter  = new SimpleDateFormat("yyyy-MM-dd");
			Date date = inputFormatter.parse(dateStr);
			formattedDate =  outputFormatter.format(date);
		} catch (ParseException pe) {
			pe.getMessage();
		}
		
		return formattedDate;
	}
	
	/**
	 * Method to determine payment method
	 * @param tenderDetails
	 * @param paymentCodesMap
	 * @return paymentCode
	 */
	public static String determinePaymentMethod(String tenderDetails, Map<String, String> paymentCodesMap) {
		
		String[] allTenderDetails = null;
		
		if (tenderDetails.equals("") || tenderDetails == null) {
			return "-1";
		} else {
			allTenderDetails = tenderDetails.split(";");
			String[] tenderInfo = null;
			String tenderAmount = null;
			BigDecimal actualAmountPaid = null;
			
			Map<String, String> tenderDetailsMap = new LinkedHashMap<String, String>();
			tenderDetailsMap.put("0:US$", "0.00");
			
			try {
				for (int i = 0; i < allTenderDetails.length; i++) {
					
					tenderInfo = allTenderDetails[i].split(":");
					if (tenderInfo.length == 3) {
						if (tenderInfo[1].equals("US$")) {
							if (tenderInfo[0].equals("0")) {
								tenderDetailsMap.put(tenderInfo[0]+":"+tenderInfo[1], tenderInfo[2]);
							} else if (tenderInfo[0].equals("4")) {
								tenderAmount = tenderDetailsMap.get("0:"+tenderInfo[1]);
								actualAmountPaid = new BigDecimal(tenderAmount).subtract(new BigDecimal(tenderInfo[2]));
								tenderDetailsMap.put("0:"+tenderInfo[1], actualAmountPaid.toString());
							}		
						} else {
							tenderDetailsMap.put(tenderInfo[0]+":"+tenderInfo[1], tenderInfo[2]);
						}
					}
					
				}
				
				String finalTenderName = "";
				BigDecimal finalTenderAmount = new BigDecimal("0.00");
				BigDecimal currentTenderAmount = new BigDecimal("0.00");
				for(String eachKey : tenderDetailsMap.keySet()) {
					currentTenderAmount = new BigDecimal(tenderDetailsMap.get(eachKey));
					if (currentTenderAmount.compareTo(finalTenderAmount) > 0) {
						finalTenderName = eachKey.split(":")[1];
						finalTenderAmount = currentTenderAmount;
					}	
				}
				return paymentCodesMap.get(finalTenderName);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	/**
	 * Method to get transKey and authCode from CashlessData
	 * @param cashlessData
	 * @param maxTenderAmt
	 * @return transKey, authCode
	 */
	public static String[] determineTransKeyAuthCode(String cashlessData, String paymentCode) {
		
		double x = 0.00;
		double y = 0.00;
		double z = 0.00;

		String[] cashlessDataArray = null;
		String[] cashlessDataValues = null;

		String maxAmt = "";
		String eachCashlessDataStr = "";
		String cashlessPOSIndex = "";
		String cashlessPattern = "CASHLESS:";
		String cardType = "", maskedCardNumber = "", expiryDate = "", unmaskedCardNumber = "";
		String transSubKey = "";
		String transKey = "";
		String authCode = "";
		String delim = "[|]";

		Pattern pattern = null;
		try {
			if (paymentCode == "13" || paymentCode == "8") {
				pattern = Pattern.compile(cashlessPattern);
				cashlessDataArray = pattern.split(cashlessData);
				
				// To get the highest amount from all cashless strings.
				for (int i = 1; i < cashlessDataArray.length; i++) {
					eachCashlessDataStr = cashlessDataArray[i];
					cashlessDataValues = eachCashlessDataStr.split(delim);
					cashlessPOSIndex = cashlessDataValues[10];
					x = Double.parseDouble(cashlessPOSIndex);
					z = (x >= y) ? x : y;
					y = z;
					maxAmt = String.format("%.2f", z);
				}

				for (int i = 1; i < cashlessDataArray.length; i++) {
					eachCashlessDataStr = cashlessDataArray[i];
					cashlessDataValues = eachCashlessDataStr.split(delim);
					
					if (maxAmt.equals(cashlessDataValues[10])) {

						cardType = cashlessDataValues[0];
						maskedCardNumber = cashlessDataValues[1];
						expiryDate = cashlessDataValues[2];
						authCode = cashlessDataValues[3];
						unmaskedCardNumber = cashlessDataValues[15];

						cardType = cardType.substring(0, 2);
						expiryDate = expiryDate.substring(0, 2).concat(expiryDate.substring(3, 5));

						transSubKey = cardType + maskedCardNumber + expiryDate;
						transKey = (unmaskedCardNumber.length() > 2) ? unmaskedCardNumber : transSubKey;
						break;
					}
				}
			}
			return new String[] { transKey, authCode };
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Invalid cashless Data string : " + eachCashlessDataStr);
			return new String[] {"", ""};
		}
	}
}
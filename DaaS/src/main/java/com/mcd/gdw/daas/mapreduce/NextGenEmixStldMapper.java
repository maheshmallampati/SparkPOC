package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.NextGenEmixDriver;
import com.mcd.gdw.daas.util.NextGenEmixUtil;
import com.mcd.gdw.daas.util.PmixData;
import com.mcd.gdw.daas.util.StoreData;

/**
 * @author Naveen Kumar B.V
 * 
 * Mapper class to parse STLD xml files and extract the fields required for processing.
 * Output for daily & hourly sales is written to multipleOutputs, whereas PMIX output is written to context
 * for further processing in reducer.
 */
public class NextGenEmixStldMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private BigDecimal totalAmount;
	private BigDecimal nonProductAmount;
	
	private MultipleOutputs<Text, Text> multipleOutputs;
	
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private Document document = null;
	private Element rootElement;
	private NodeList nlEvent;
	private Element eleEvent;
	private String eventType;
	private Element eleTrx;
	private NodeList nlTRX;
	private Element eleOrder;
	private NodeList nlOrder;
	private NodeList nlNode;
	private Element eleNode;
	
	private String[] inputParts = null;
	private String xmlText = "";
	
	private String terrCd = "";
	private String lgcyLclRfrDefCd = "";	
	private String businessDate = "";
	private String gdwBusinessDate = "";
	private String haviBusinessDate = "";
	private String status = "";	
	private String trxPOD = "";	
	private String orderKind = "";
	private String orderKey = "";
	
	String hour = "";
	String minute = "";
	String mapMinute = "";
	String businessHour = "";
	String eventDate = "";
	
	private String gdwTimeStamp;
	private String haviTimeStamp;
	private String eventTimeStamp = "";
	private long haviTimeKey = 0;
	private int breakfastStopTime = 0;
	private String ownerShipType = "";
	private boolean isValidInput = false;
	
	private StoreData gdwHourlySaleData = null;
	private StoreData dailySaleStoreData = null;
	private StoreData haviHourlySaleData = null;
	
	private Map<String, StoreData> gdwHourlyStoreMap = null;
	private Map<String, StoreData> haviHourlyStoreMap = null;
	private Map<String, PmixData> pmixDataMap = null;
		
	private Map<String, List<String>> comboItemLookup =  null;
	private Map<String, String> primaryItemLookup = null;

	private StringBuffer pmixKeyStrBuf = null;
	private StringBuffer pmixValueStrBuf = null;
	private StringBuffer keyStrBuf = null;
	
	private Text pmixKeyTxt = null;
	private Text pmixValueTxt = null;
	
	private int[] recordAndStoreCount = new int[2];
	private int gdwDailyRecordCount;
	private int gdwDailyUniqStoreCount;
	private int gdwHourlyRecordCount;
	private int gdwHourlyUniqStoreCount;
	
	/**
	 * Setup method to create multipleOutputs, initialize required variables and lookup maps.
	 */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		multipleOutputs = new MultipleOutputs<Text, Text>(context);	
        docFactory = DocumentBuilderFactory.newInstance();

        gdwDailyRecordCount = 0;
        gdwDailyUniqStoreCount = 0;
        gdwHourlyRecordCount = 0;
        gdwHourlyUniqStoreCount = 0;
        
        pmixKeyStrBuf = new StringBuffer();
        pmixValueStrBuf = new StringBuffer();
        keyStrBuf = new StringBuffer();
        pmixKeyTxt = new Text();
        pmixValueTxt = new Text();
        
        comboItemLookup =  new HashMap<String, List<String>>();
        primaryItemLookup = new HashMap<String, String>();
        
        URI[] cacheFiles = context.getCacheFiles();
        
        BufferedReader br = null;
        String inputLookUpData = "";
        String[] fileParts = null;
        List<String> comboItems = null;
        String[] inputDataArray = null;
        
        try {
	        for (int fileCounter = 0; fileCounter < cacheFiles.length; fileCounter++) {
	        	
	        	fileParts = cacheFiles[fileCounter].toString().split("#");
	        	br = new BufferedReader(new FileReader("./" + fileParts[1]));
	        	
	        	while ((inputLookUpData = br.readLine()) != null) {
	            	inputDataArray = inputLookUpData.split(NextGenEmixDriver.REGEX_PIPE_DELIMITER);
	            	if (fileCounter == 0) {
	            		// Save CMBCMP lookup data to a hashmap
	            		comboItems = comboItemLookup.containsKey(inputDataArray[0]) ? comboItemLookup.get(inputDataArray[0]) : new ArrayList<String>();
	                	keyStrBuf.setLength(0);
	                	keyStrBuf.append(String.format("%04d", Integer.parseInt(inputDataArray[1]))).append(DaaSConstants.PIPE_DELIMITER).append(inputDataArray[2]) 
	                										 .append(DaaSConstants.PIPE_DELIMITER).append(inputDataArray[4])
	                										 .append(DaaSConstants.PIPE_DELIMITER).append(inputDataArray[6]);
	            		comboItems.add(keyStrBuf.toString());
	            		comboItemLookup.put(inputDataArray[0], comboItems);
	            	} else if (fileCounter == 1) {
	            		// Save CMIM lookup data to a hashmap.
	            		primaryItemLookup.put(inputDataArray[0], String.format("%04d", Integer.parseInt(inputDataArray[1])));
	            	} 
	            }
	        }
        
        	docBuilder = docFactory.newDocumentBuilder();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        	
	}
    
	/**
	 * Map method to parse XML and generate psv output
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
        try{
        	
        	inputParts = value.toString().split(DaaSConstants.TAB_DELIMITER);
    		ownerShipType = inputParts[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS];
    		xmlText = inputParts[DaaSConstants.XML_REC_XML_TEXT_POS];
        	
            document = docBuilder.parse(new InputSource(new StringReader(xmlText)));
            rootElement = (Element) document.getFirstChild();
           
            if(rootElement != null && rootElement.getNodeName().equals("TLD")) {
        	   
        	    dailySaleStoreData = new StoreData();
        	    gdwHourlyStoreMap = new HashMap<String, StoreData>();
        	    haviHourlyStoreMap = new HashMap<String, StoreData>();
        	    pmixDataMap = new HashMap<String, PmixData>();
        	    
        	    // Insert default segments for BKOVR & OVR in haviHourly output
        	    haviHourlyStoreMap.put(NextGenEmixDriver.HAVI_BKOVR_STR, new StoreData());
        	    haviHourlyStoreMap.put(NextGenEmixDriver.HAVI_OVR_STR, new StoreData());
        	    
        		totalAmount = NextGenEmixDriver.DECIMAL_ZERO;
        		
				isValidInput = NextGenEmixUtil.validateTldElementAttrs(rootElement, multipleOutputs);
				
				if (isValidInput) {
					
					terrCd = rootElement.getAttribute("gdwTerrCd");
					lgcyLclRfrDefCd = String.valueOf(Integer.parseInt(rootElement.getAttribute("gdwLgcyLclRfrDefCd")));
					businessDate = rootElement.getAttribute("businessDate");
					gdwBusinessDate = businessDate.substring(0, 4) + NextGenEmixDriver.GDW_DATE_SEPARATOR + businessDate.substring(4, 6) + 
													NextGenEmixDriver.GDW_DATE_SEPARATOR + businessDate.substring(6);
					haviBusinessDate = businessDate.substring(4, 6) + NextGenEmixDriver.HAVI_DATE_SEPARATOR + businessDate.substring(6) + 
													NextGenEmixDriver.HAVI_DATE_SEPARATOR  + businessDate.substring(0,4);
					
					Date date = new Date();
					SimpleDateFormat gdwDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
					SimpleDateFormat haviDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
					gdwTimeStamp = gdwDateFormat.format(date) + "";
					gdwTimeStamp = gdwTimeStamp.substring(0, gdwTimeStamp.length() - 1);
					haviTimeStamp = haviDateFormat.format(date) + "";
					
					haviTimeKey= NextGenEmixUtil.calculateJulianNumber(Integer.parseInt(businessDate.substring(4, 6)), 
											Integer.parseInt(businessDate.substring(6)), Integer.parseInt(businessDate.substring(0, 4)));
					
                    nlNode = rootElement.getChildNodes();
      				if (nlNode != null && nlNode.getLength() > 0) {
      					for (int idxNode = 0; idxNode < nlNode.getLength(); idxNode++) {						
      						if(nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE) {
      							eleNode = (Element) nlNode.item(idxNode);
      							if (eleNode != null && eleNode.getNodeName().equals("Node")) { 
      								nlEvent = eleNode.getChildNodes();
      								if (nlEvent != null && nlEvent.getLength() > 0) {
      									for (int idxEvent = 0; idxEvent < nlEvent.getLength(); idxEvent++) {
      										if (nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE) {
      											eleEvent = (Element) nlEvent.item(idxEvent);
      											if (eleEvent != null && eleEvent.getNodeName().equals("Event")) {
      												
      												isValidInput = NextGenEmixUtil.validateEventElementAttrs(eleEvent, lgcyLclRfrDefCd, 
      														businessDate, multipleOutputs);
      												
      												if (isValidInput) {
      													eventType = eleEvent.getAttribute("Type");
	      												eventTimeStamp = eleEvent.getAttribute("Time");
	      												
	      												
	      												// To get the distinct value of WeekdayBreakfastStopTime or WeekendBreakfastStopTime based on Business Date
	      												if (eventType.equals("TRX_BaseConfig")) {
	      													breakfastStopTime = NextGenEmixUtil.calculateBreakfastStopTime(eleEvent, businessDate);
	      												}
	      												
	      												if (eventType.equals("TRX_Sale") || eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring")) {
	      													eleTrx = null;
	    													nlTRX = eleEvent.getChildNodes();
	    													int idxTRX = 0;
	    													if (nlTRX != null) {
	    														while (eleTrx == null && idxTRX < nlTRX.getLength()) { 
		    														if (nlEvent.item(idxTRX).getNodeType() == Node.ELEMENT_NODE) {
		    															eleTrx = (Element) nlTRX.item(idxTRX);
		    															status = eleTrx.getAttribute("status");
		    															trxPOD = eleTrx.getAttribute("POD");	
		    														}
		    														idxTRX++;
		    													}
	    													}
	    													
	    													if (eleTrx != null) {
	    														
	    														isValidInput = NextGenEmixUtil.validateTrxElementAttrs(eleTrx, lgcyLclRfrDefCd, businessDate, 
	    																					eventType, eventTimeStamp, multipleOutputs);
	    														
	    														if (isValidInput) {
	    															if ((eventType.equals("TRX_Sale") && status.equals("Paid")) || 
		      																(eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring"))) {
		    														eleOrder = null;
		    														nlOrder = eleTrx.getChildNodes();
		    														int idxOrder = 0;
		    														if (nlOrder != null) {
		    															while (eleOrder == null && idxOrder < nlOrder.getLength()) {
			    															if (nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE) {
			    																eleOrder = (Element) nlOrder.item(idxOrder);
			    															}
			    															idxOrder++;
			    														}
		    														}
		    														
		    														// For Gift Items and Coupons
		    														calculateGiftCardAmountAndQty();
		    														
		    														eventDate = eventTimeStamp.substring(0, 8);
	    															hour = eventTimeStamp.substring(8, 10);
	        														minute = eventTimeStamp.substring(10, 12);
	        														
	        														// Method to prepare hourly segments for GDW & HAVI
		    														prepareHourlySegments();
		    														
		    														isValidInput = NextGenEmixUtil.validateOrderElementAttrs(eleOrder, lgcyLclRfrDefCd, 
		    																											businessDate, multipleOutputs);
		    														
		    														if (isValidInput) {
		    															totalAmount = new BigDecimal(eleOrder.getAttribute("totalAmount"));
			    														nonProductAmount = new BigDecimal(eleOrder.getAttribute("nonProductAmount"));
			    														orderKind = eleOrder.getAttribute("kind");
			    														orderKey = eleOrder.getAttribute("key");
			    														
		        														// Method to calculate sales output
			    													   calculateSalesOutput();
			    														
			    													   isValidInput = NextGenEmixUtil.validateItemElementAttrs(eleOrder, lgcyLclRfrDefCd, 
		    														   													businessDate, multipleOutputs);
			    													   if (isValidInput) {
			    														   // Calculate PMIX 
				    													   pmixDataMap = NextGenEmixUtil.generateOrderDetail(eleOrder, pmixDataMap, lgcyLclRfrDefCd, orderKind, 
				    																			orderKey, eventType, comboItemLookup, primaryItemLookup);    
				    														
				    													   // To calculate Gift Certificates Amount and Qty	
				    													   calculateGiftCertificateAmtandQty();
			    													   }
			    													   
		    														}
		    														
		    													  gdwHourlyStoreMap.put(hour + NextGenEmixDriver.TIME_SEPARATOR +
		    																			  mapMinute + NextGenEmixDriver.TIME_SEPARATOR +
		    																			  NextGenEmixDriver.ZERO_MINUTE_OR_SECOND, gdwHourlySaleData);
		    													  haviHourlyStoreMap.put(businessHour, haviHourlySaleData);
		    													}
	    													}	
	    												  }
	      											   }
      												}
      											}	
      										}	
      									}								
      								}
      							} 						
      						} 
      					}					
      				}
      				
      				recordAndStoreCount = NextGenEmixUtil.writeGDWDailySalesToMultipleOutput(multipleOutputs, dailySaleStoreData, terrCd, 
      																			lgcyLclRfrDefCd, gdwBusinessDate, gdwTimeStamp);
      				gdwDailyRecordCount += recordAndStoreCount[0];
      				gdwDailyUniqStoreCount += recordAndStoreCount[1];
      				
      				recordAndStoreCount = NextGenEmixUtil.writeGDWHourlySalesToMultipleOutput(multipleOutputs, terrCd, lgcyLclRfrDefCd, gdwBusinessDate, 
      																			gdwHourlyStoreMap, gdwTimeStamp);
      				gdwHourlyRecordCount += recordAndStoreCount[0];
      				gdwHourlyUniqStoreCount += recordAndStoreCount[1];
      				
      				/*
      				NextGenEmixUtil.writeHAVIDailySalesToMultipleOutput(multipleOutputs, dailySaleStoreData, lgcyLclRfrDefCd, ownerShipType, 
      																	haviTimeKey, haviBusinessDate, haviTimeStamp);
      				
      				NextGenEmixUtil.writeHAVIHourlySalesToMultipleOutput(multipleOutputs, haviHourlyStoreMap, lgcyLclRfrDefCd, ownerShipType, 
      																	haviTimeKey, haviBusinessDate, haviTimeStamp);
      				*/
      				
      			// PMIX Output
      				for (String eachPmixKey : pmixDataMap.keySet()) {
      					PmixData pmixData = pmixDataMap.get(eachPmixKey);
      					if (pmixData.getMitmKey() < 80000000) {
      									
      						pmixKeyStrBuf.setLength(0);
      						pmixValueStrBuf.setLength(0);
      						
      						pmixKeyStrBuf.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER).append(businessDate);
      						pmixValueStrBuf.append(NextGenEmixDriver.STLD_TAG_INDEX).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getMitmKey()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getDlyPmixPrice()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(NextGenEmixDriver.DECIMAL_ZERO).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getUnitsSold()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getComboQty()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getPromoQty()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getPromoComboQty()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getPromoTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(ownerShipType).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(haviBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(haviTimeKey).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getOrderKey()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getLvl0PmixFlag()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getLvl0PmixQty()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getAllComboComponents()).append(DaaSConstants.PIPE_DELIMITER)
      									   .append(pmixData.getLvl0InCmbCmpFlag());
      						
      						pmixKeyTxt.set(pmixKeyStrBuf.toString());
      						pmixValueTxt.set(pmixValueStrBuf.toString());
      						
      						context.write(pmixKeyTxt, pmixValueTxt);
      						
      						/* Testing 
      						multipleOutputs.write("mapperpmixtest", NullWritable.get(), 
      								new Text((new StringBuffer(NextGenEmixDriver.STLD_TAG_INDEX).append(DaaSConstants.PIPE_DELIMITER)
      										.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
      										.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
      										.append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getMitmKey()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getDlyPmixPrice()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(NextGenEmixDriver.DECIMAL_ZERO).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getUnitsSold()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getComboQty()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getPromoQty()).append(DaaSConstants.PIPE_DELIMITER)
      		                                .append(pmixData.getPromoComboQty()).append(DaaSConstants.PIPE_DELIMITER)
      		                                .append(pmixData.getPromoTotalQty()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getConsPrice()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(ownerShipType).append(DaaSConstants.PIPE_DELIMITER)
      										.append(haviBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
      										.append(haviTimeKey).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getOrderKey()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getLvl0PmixFlag()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getLvl0PmixQty()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getAllComboComponents()).append(DaaSConstants.PIPE_DELIMITER)
      										.append(pmixData.getLvl0InCmbCmpFlag()).toString())));
      						*/
      					}
      				}
				}
		 }
			    
 		} catch (SAXException e) {
 			e.printStackTrace();
 		} catch (IOException e) {
 			e.printStackTrace();
 		} catch (InterruptedException e) {
 			e.printStackTrace();
 		} catch (ParseException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
 	}
	
	
	/**
	 * Cleanup method to close multiple outputs
	 */
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {
		
		String partitionID = String.format("%05d", context.getTaskAttemptID().getTaskID().getId());
		
		multipleOutputs.write(NextGenEmixDriver.Output_File_Stats, NullWritable.get(), 
		         new Text(NextGenEmixDriver.GLMA_Dly_Sls+"-m-"+partitionID + DaaSConstants.PIPE_DELIMITER + 
		        		 	gdwDailyRecordCount + DaaSConstants.PIPE_DELIMITER + gdwDailyUniqStoreCount));
		
		multipleOutputs.write(NextGenEmixDriver.Output_File_Stats, NullWritable.get(), 
				new Text(NextGenEmixDriver.GLMA_Hrly_Sls+"-m-"+partitionID + DaaSConstants.PIPE_DELIMITER + 
							gdwHourlyRecordCount + DaaSConstants.PIPE_DELIMITER + gdwHourlyUniqStoreCount));
		
		multipleOutputs.close();	
		pmixKeyTxt.clear();
		pmixValueTxt.clear();
		
		gdwHourlyStoreMap.clear();
		haviHourlyStoreMap.clear();
		pmixDataMap.clear();
		comboItemLookup.clear();
		primaryItemLookup.clear();			
	}
	
	/**
	 * Method to generate hourly segments for Daily time segment sales output based on hour.
	 */
	private void prepareHourlySegments() {
		
		if (!businessDate.equals(eventDate) && Integer.parseInt(hour) >= 6) {
			hour = "23";
			businessHour = NextGenEmixDriver.HAVI_OVR_STR;
		}
		
		if (breakfastStopTime >= 1100) {
			mapMinute = hour.equals(NextGenEmixDriver.TENTH_HOUR_STR) ? NextGenEmixDriver.THIRTY_MINUTE_STR 
																	  : NextGenEmixDriver.ZERO_MINUTE_OR_SECOND;
			gdwHourlySaleData = NextGenEmixUtil.getStoreDataByMapKey(gdwHourlyStoreMap, 
											hour + NextGenEmixDriver.TIME_SEPARATOR + mapMinute + 
											NextGenEmixDriver.TIME_SEPARATOR + NextGenEmixDriver.ZERO_MINUTE_OR_SECOND);
			
			businessHour = businessHour.equals(NextGenEmixDriver.HAVI_OVR_STR)? NextGenEmixDriver.HAVI_OVR_STR 
															: String.format("%02d", Integer.parseInt(hour) + 1);
			haviHourlySaleData = NextGenEmixUtil.getStoreDataByMapKey(haviHourlyStoreMap, businessHour);
		} else {
			if (hour.equals(NextGenEmixDriver.TENTH_HOUR_STR)) {
				if (Integer.parseInt(minute) < 30) {
					mapMinute = NextGenEmixDriver.ZERO_MINUTE_OR_SECOND;
					gdwHourlySaleData = NextGenEmixUtil.getStoreDataByMapKey(gdwHourlyStoreMap, 
											hour + NextGenEmixDriver.TIME_SEPARATOR + mapMinute + 
											NextGenEmixDriver.TIME_SEPARATOR + NextGenEmixDriver.ZERO_MINUTE_OR_SECOND);
					
					businessHour = NextGenEmixDriver.HAVI_BKOVR_STR;
					haviHourlySaleData =  NextGenEmixUtil.getStoreDataByMapKey(haviHourlyStoreMap, businessHour);
					
				} else {
					mapMinute = NextGenEmixDriver.THIRTY_MINUTE_STR;
					gdwHourlySaleData = NextGenEmixUtil.getStoreDataByMapKey(gdwHourlyStoreMap, 
											hour + NextGenEmixDriver.TIME_SEPARATOR + mapMinute + 
											NextGenEmixDriver.TIME_SEPARATOR + NextGenEmixDriver.ZERO_MINUTE_OR_SECOND);
					
					businessHour = String.format("%02d", Integer.parseInt(hour) + 1);
					haviHourlySaleData = NextGenEmixUtil.getStoreDataByMapKey(haviHourlyStoreMap, businessHour);
				}
			} else {
				mapMinute = NextGenEmixDriver.ZERO_MINUTE_OR_SECOND;
				gdwHourlySaleData = NextGenEmixUtil.getStoreDataByMapKey(gdwHourlyStoreMap, 
											hour + NextGenEmixDriver.TIME_SEPARATOR + mapMinute + 
											NextGenEmixDriver.TIME_SEPARATOR + NextGenEmixDriver.ZERO_MINUTE_OR_SECOND);
				
				businessHour = businessHour.equals(NextGenEmixDriver.HAVI_OVR_STR)? NextGenEmixDriver.HAVI_OVR_STR 
																: String.format("%02d", Integer.parseInt(hour) + 1);
				haviHourlySaleData = NextGenEmixUtil.getStoreDataByMapKey(haviHourlyStoreMap, businessHour);
			}
		}
	}
	
	/**
	 * Method to calculate Total, DriveThru, FrontCounter, 
	 * Product and NonProduct amounts and quantities.
	 */
	private void calculateSalesOutput() {
		
		if (eventType.equals("TRX_Sale")) {
			if ((totalAmount.compareTo(NextGenEmixDriver.DECIMAL_ZERO) > 0) ||
					 ((totalAmount.compareTo(NextGenEmixDriver.DECIMAL_ZERO) == 0) && 
							 		(orderKind.equals("Sale") || orderKind.equals("Generic Discount")))) {
					    
					dailySaleStoreData.setTotalCount(dailySaleStoreData.getTotalCount() + 1);
					gdwHourlySaleData.setTotalCount(gdwHourlySaleData.getTotalCount() + 1); 
					haviHourlySaleData.setTotalCount(haviHourlySaleData.getTotalCount() + 1);
					if (trxPOD.equals("Drive Thru")) {
						dailySaleStoreData.setDriveThruCount(dailySaleStoreData.getDriveThruCount() + 1);
						dailySaleStoreData.setDriveThruAmount(dailySaleStoreData.getDriveThruAmount().add(totalAmount));
						
						gdwHourlySaleData.setDriveThruCount(gdwHourlySaleData.getDriveThruCount() + 1);
						gdwHourlySaleData.setDriveThruAmount(gdwHourlySaleData.getDriveThruAmount().add(totalAmount));
						
						haviHourlySaleData.setDriveThruCount(haviHourlySaleData.getDriveThruCount() + 1);
						haviHourlySaleData.setDriveThruAmount(haviHourlySaleData.getDriveThruAmount().add(totalAmount));
					} else {
						dailySaleStoreData.setFrontCounterCount(dailySaleStoreData.getTotalCount() 
																	- dailySaleStoreData.getDriveThruCount());
						gdwHourlySaleData.setFrontCounterCount(gdwHourlySaleData.getTotalCount() 
																	- gdwHourlySaleData.getDriveThruCount());
					}
				}
			
			
			dailySaleStoreData.setTotalAmount(dailySaleStoreData.getTotalAmount().add(totalAmount));
			dailySaleStoreData.setFrontCounterAmount(dailySaleStoreData.getTotalAmount().
														subtract(dailySaleStoreData.getDriveThruAmount()));
			dailySaleStoreData.setNonProductAmount(dailySaleStoreData.getNonProductAmount().add(nonProductAmount));
			dailySaleStoreData.setProductAmount(dailySaleStoreData.getProductAmount().add(totalAmount
																					.subtract(nonProductAmount)));
			
			gdwHourlySaleData.setTotalAmount(gdwHourlySaleData.getTotalAmount().add(totalAmount));
			gdwHourlySaleData.setFrontCounterAmount(gdwHourlySaleData.getTotalAmount().
														subtract(gdwHourlySaleData.getDriveThruAmount()));
			gdwHourlySaleData.setNonProductAmount(gdwHourlySaleData.getNonProductAmount().add(nonProductAmount));
			gdwHourlySaleData.setProductAmount(gdwHourlySaleData.getProductAmount().add(totalAmount.subtract(nonProductAmount)));
			
			haviHourlySaleData.setTotalAmount(haviHourlySaleData.getTotalAmount().add(totalAmount));
		}
		
		if (eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring")) {
			if (trxPOD.equals("Drive Thru")) {
				dailySaleStoreData.setDriveThruAmount(dailySaleStoreData.getDriveThruAmount().subtract(totalAmount));
				gdwHourlySaleData.setDriveThruAmount(gdwHourlySaleData.getDriveThruAmount().subtract(totalAmount));
				haviHourlySaleData.setDriveThruAmount(haviHourlySaleData.getDriveThruAmount().subtract(totalAmount));
				
			} else if(trxPOD.equals("Front Counter")) {
				dailySaleStoreData.setFrontCounterAmount(dailySaleStoreData.getFrontCounterAmount().subtract(totalAmount));
				gdwHourlySaleData.setFrontCounterAmount(gdwHourlySaleData.getFrontCounterAmount().subtract(totalAmount));
			} 
			
			dailySaleStoreData.setTotalAmount(dailySaleStoreData.getTotalAmount().subtract(totalAmount));
			dailySaleStoreData.setNonProductAmount(dailySaleStoreData.getNonProductAmount().
															subtract(nonProductAmount));
			dailySaleStoreData.setProductAmount(dailySaleStoreData.getProductAmount().
															subtract(totalAmount.subtract(nonProductAmount)));
			
			gdwHourlySaleData.setTotalAmount(gdwHourlySaleData.getTotalAmount().subtract(totalAmount));
			gdwHourlySaleData.setNonProductAmount(gdwHourlySaleData.getNonProductAmount().subtract(nonProductAmount));
			gdwHourlySaleData.setProductAmount(gdwHourlySaleData.getProductAmount().subtract(totalAmount.
															subtract(nonProductAmount)));
			
			haviHourlySaleData.setTotalAmount(haviHourlySaleData.getTotalAmount().subtract(totalAmount));
		}
	}
	
	
	/**
	 * Method to calculate Gift Card Amount & Qty using Tender elements.
	 */
	private void calculateGiftCardAmountAndQty() {
		int tenderKind = 0;
		String tenderName = "";
		NodeList tendersList = null;
		NodeList tendersChildNodes = null;
		Element tenderElement = null;
		NodeList eachTenderNodeList = null;
		
		try {
			tendersList = (eleOrder == null ? null : eleOrder.getElementsByTagName("Tenders"));
			if (tendersList != null && tendersList.getLength() > 0) {
				for (int tListIdx = 0; tListIdx < tendersList.getLength(); tListIdx++) {
					tendersChildNodes = tendersList.item(tListIdx).getChildNodes();
					for (int tIdx = 0; tIdx < tendersChildNodes.getLength(); tIdx++) {
						if (tendersChildNodes.item(tIdx).hasChildNodes()) {
							tenderElement = (Element) tendersChildNodes.item(tIdx);
							eachTenderNodeList = tenderElement.getChildNodes();
							
							if (eventType.equals("TRX_Sale")) {
								for (int i = 0; i < eachTenderNodeList.getLength(); i++) {

									if (eachTenderNodeList.item(i).getNodeName().equals("TenderKind")) {
										tenderKind = Integer.parseInt(eachTenderNodeList.item(i).getTextContent());
									}

									if (eachTenderNodeList.item(i).getNodeName().equals("TenderName")) {
										tenderName = eachTenderNodeList.item(i).getTextContent();
									}

									if (tenderKind == 8 && eachTenderNodeList.item(i).getNodeName().equals("TenderAmount")) {
										dailySaleStoreData.setCouponRdmAmount(dailySaleStoreData.getCouponRdmAmount().
												add(new BigDecimal(eachTenderNodeList.item(i).getTextContent())));
										// gdwHourlySaleData.setCouponRdmAmount(gdwHourlySaleData.getCouponRdmAmount().
										// add(new BigDecimal(eachTenderNodeList.item(i).getTextContent())));
									}

									if (tenderKind == 8 && eachTenderNodeList.item(i).getNodeName().equals("TenderQuantity")) {
										dailySaleStoreData.setCouponRdmQty(dailySaleStoreData.getCouponRdmQty()
												+ Integer.parseInt(eachTenderNodeList.item(i).getTextContent()));
										// gdwHourlySaleData.setCouponRdmQty(gdwHourlySaleData.getCouponRdmQty()
										// + Integer.parseInt(eachTenderNodeList.item(i).getTextContent()));
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
										// + Integer.parseInt(eachTenderNodeList.item(i).getTextContent()));
									}
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to calculate Gift Certificate Amt & Qty using Item tag.
	 */
	private void calculateGiftCertificateAmtandQty() {
		
		NodeList itemsList = (eleOrder == null ? null : eleOrder.getElementsByTagName("Item"));
		Element itemElement = null;
		try {
			if (itemsList != null) {
				for (int itemIdx = 0; itemIdx < itemsList.getLength(); itemIdx++) {
					
					// For Gift Certificates Sold
					itemElement = (Element) itemsList.item(itemIdx);

					BigDecimal giftCouponAmount = NextGenEmixDriver.DECIMAL_ZERO;
					if (itemElement.getAttribute("level").equals("0")
							&& itemElement.getAttribute("familyGroup").equals("GIFT_COUPON")) {

						if (eventType.equals("TRX_Sale")) {
							giftCouponAmount = giftCouponAmount.add(new BigDecimal(itemElement.getAttribute("totalPrice")));
							dailySaleStoreData.setGiftCouponQty(dailySaleStoreData.getGiftCouponQty()
									+ Integer.parseInt(itemElement.getAttribute("qty")));
							dailySaleStoreData.setGiftCouponAmount(dailySaleStoreData.getGiftCouponAmount().
									add(giftCouponAmount));

							gdwHourlySaleData.setGiftCouponQty(gdwHourlySaleData.getGiftCouponQty()
									+ Integer.parseInt(itemElement.getAttribute("qty")));

						} else if (eventType.equals("TRX_Refund")) {
							giftCouponAmount = giftCouponAmount.subtract(
									new BigDecimal(itemElement.getAttribute("totalPrice")));
							dailySaleStoreData.setGiftCouponAmount(dailySaleStoreData.getGiftCouponAmount().
									subtract(giftCouponAmount));
						}

						dailySaleStoreData.setTotalAmount(dailySaleStoreData.getTotalAmount().subtract(giftCouponAmount));
						gdwHourlySaleData.setTotalAmount(gdwHourlySaleData.getTotalAmount().subtract(giftCouponAmount));
						if (trxPOD.equals("Drive Thru")) {
							dailySaleStoreData.setDriveThruAmount(dailySaleStoreData.getDriveThruAmount().
									subtract(giftCouponAmount));
							gdwHourlySaleData.setDriveThruAmount(gdwHourlySaleData.getDriveThruAmount().
									subtract(giftCouponAmount));
						}
						dailySaleStoreData.setNonProductAmount(dailySaleStoreData.getNonProductAmount().
								subtract(giftCouponAmount));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
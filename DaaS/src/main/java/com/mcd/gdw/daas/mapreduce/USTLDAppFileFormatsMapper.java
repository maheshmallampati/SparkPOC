package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;

import emix.STLDConstants;

public class USTLDAppFileFormatsMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final BigDecimal DECIMAL_ZERO = new BigDecimal("0.00");
	private final static BigDecimal ONE_HUNDRED = new BigDecimal("100");
	
	private final static String SEPARATOR_CHARACTER 	= "\t";
	private final static String COMMA_DELIMITER  		= ",";
	private final static String REC_MENU 				= "MNU";
	private final static String REC_TRANS 				= "TRN";
	private final static String REC_TRANSITEM 			= "ITM";
	private final static String TRX_SALE     			= "TRX_Sale";
	private final static String TRX_REFUND   			= "TRX_Refund";
	private final static String TRX_OVERRING 			= "TRX_Overring";
	private final static String SALE_TYPE_DRIVE_THRU	= "Drive Thru";
	private final static String SALE_TYPE_FRONT_COUNTER	= "Front Counter";
	private final static String ORDER_KIND_CREW			= "Crew Discount";
	private final static String ORDER_KIND_MANAGER		= "Manager Discount";
	private final static String ORDER_KIND_GENERIC    	= "Generic Discount";

	private String[] parts = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();
	
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;
	
	private Calendar cal = Calendar.getInstance();

	private StringBuffer outputKey 			= new StringBuffer();
	private StringBuffer outputTextValue	= new StringBuffer();
	private StringBuffer tenderInfo		  = new StringBuffer();
	
	private String fileSubType;
	private String terrCd;
	private String lgcyLclRfrDefCd;
	private String storeDate;
	private String posBusnDt;
	private String posAreaTypShrtDs;
	private String posType;
	private String posStatus;
	private String posOrdKey;
	private String posOrdKeyCd;
	private String posTrnStrtTs;
	private String posTrnOrdTs;
	private String posTrnTypCd;
	private String posTrnDscntTypCd;
	private String posPrdDlvrMethDesc;
	private String posPrdDlvrMethCd;
	private String posTrnPymntTyp;
	private String posItmComboId;
	private String posItmId;
	private String posItmLvlNu;
	private String posItmType;
	private String posItmMenuItmId;
	private String posItmMenuItmDesc;
	private String posItmPrmo;
	private String posItmVoidQt;
	private String posSumItmComboId;
	private String posSumItmId;
	private String posSumItmLvlNu;
	private String posSumItmType;
	private String posSumItmMenuItmId;
	private String posSumItmVoidQt;
	private String posOrdTndrId1;
	private String posOrdTndrKind1;
	private String posOrdTndrPaymentName1;
	private String posOrdTndrCardProvider1;
	private String posOrdTndrId2;
	private String posOrdTndrKind2;
	private String posOrdTndrPaymentName2;
	private String posOrdTndrCardProvider2;
	private String dyptIdNu;
	private String dyptDs;
	private String dyOfCalWkDs;
 	private String hrIdNu; 
	
	private int posItmMenuItmIndex = 0;
	private int posTrnItmIndex = 0;
	private int posTrnSeqNu = 0;
	private int posTrnTotQt = 0;
	
	private BigDecimal posTrnTotBDAmt = DECIMAL_ZERO;
	private BigDecimal posTrnTotNetAm = DECIMAL_ZERO;
	private BigDecimal posTrnTotTaxAm = DECIMAL_ZERO;
	
	private String menuHashMapKey;
	private int menuHashMapValue;
	private HashMap<String,String> dayPartMap = new HashMap<String,String>();
	private HashMap<String,String> storeListMap = new HashMap<String,String>();
	private HashMap<String,Integer> processMenuFile = new HashMap<String,Integer>();
	private HashMap<Integer,String> tendersInfoMap = new HashMap<Integer,String>();

	private Element eleRoot;
	private Node nodeText;
	
	private boolean skipItem;
	private boolean skipOrder;

	@Override
	public void setup(Context context) {
		
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;

        try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
			distPaths = context.getCacheFiles();
		    
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		      
		    if (distPaths != null && distPaths.length > 0)  {
		    	  
		    	System.out.println(" number of distcache files : " + distPaths.length);
		    	  
		    	for (int i=0; i<distPaths.length; i++) {
				     
			    	  System.out.println("distpaths:" + distPaths[i].toString());
			    	  
			    	  distPathParts = 	distPaths[i].toString().split("#");
			    	  
			    	  if(distPaths[i].toString().contains("DayPart_ID.psv")) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	  addDaypartKeyValuestoMap(br);
				      	  System.out.println("Loaded Daypart Values Map");
				      	  
				      //sri_run_test.sh
			    	  //} else if (distPaths[i].toString().contains("USTLDStoreList.txt") ) {
				      //TLDreprocess.sh
			    	  } else if (distPaths[i].toString().contains("USTLDStoreList_reprocess.txt") ) {
    	  		      	  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addUSTLDStoreListToMap(br);
				      	  System.out.println("Loaded US TLD Store List Values Map");
				      }
			      }
		      }
        } catch (Exception ex) {
			System.err.println("Error in initializing USTLDAppFileFormatsMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}
	
	private void addDaypartKeyValuestoMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String dayPartKey;
		String terrCd;
		String dayOfWeek;
		String startTime;
		String daypartId;
		String daypartDs;
		
		String timeSegment;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\|", -1);

					terrCd      = String.format("%03d", Integer.parseInt(parts[0]));
					dayOfWeek   = parts[1];
					timeSegment = parts[2];
					startTime   = parts[3];

					daypartId   = parts[5];
					daypartDs   = parts[6];
						
					if (timeSegment.equalsIgnoreCase("QUARTER HOURLY")) {
						dayPartKey = terrCd + SEPARATOR_CHARACTER + dayOfWeek + SEPARATOR_CHARACTER + startTime.substring(0, 2) + startTime.substring(3, 5);
							
						dayPartMap.put(dayPartKey,daypartId+SEPARATOR_CHARACTER+daypartDs);
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(8);
		} finally {
			try {
				if (br != null)
					br.close();
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
private void addUSTLDStoreListToMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String storeListKey;
		String terrCd;
		String lgcyLclRfrDefCd;
		String storeDate;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\|", -1);
					
					terrCd      	= String.format("%03d", Integer.parseInt(parts[0]));
					lgcyLclRfrDefCd	= parts[1];
					storeDate		= parts[2];
					
					storeListKey = terrCd + lgcyLclRfrDefCd + storeDate;
					
					storeListMap.put(storeListKey,storeDate);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(8);
		} finally {
			try {
				if (br != null)
					br.close();
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		boolean includeFile = true;
		String storeListKey;
		
		try {
			processMenuFile.clear();
			
			parts = value.toString().split("\t");

			fileSubType = parts[DaaSConstants.XML_REC_FILE_TYPE_POS];

			terrCd = String.format("%03d", Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS]));
			lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
			
			//storeListKey: 8400004920150930			
			storeDate = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4) + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8);
			
			storeListKey = terrCd + lgcyLclRfrDefCd + storeDate;
			
			if (!storeListMap.containsKey(storeListKey.toString())) {
				includeFile = false;
			}
			
			if (includeFile) {
				//if (lgcyLclRfrDefCd.startsWith("01881") ) {
					
					posItmMenuItmIndex = 0;
					posTrnSeqNu = 0;
					
					posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + "/" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8) + "/" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4);
		
					strReader  = new StringReader(parts[DaaSConstants.XML_REC_XML_TEXT_POS]);
					xmlSource = new InputSource(strReader);
					doc = docBuilder.parse(xmlSource);
					eleRoot = (Element) doc.getFirstChild();
						
					if (fileSubType.equals("STLD") && eleRoot.getNodeName().equals("TLD")) {
							processSTLD(eleRoot.getChildNodes(),context);				
					}
				//}				
			}
		} catch (Exception ex) { 
			System.err.println("Error occured in USTLDReformatMapper.map:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void processSTLD(NodeList nlNode, Context context) {
		
		Element eleNode;
		
		if (nlNode != null && nlNode.getLength() > 0 ) {
			for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
				if (nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {  
					eleNode = (Element)nlNode.item(idxNode);
					if (eleNode.getNodeName().equals("Node")) {
						processEvent(eleNode.getChildNodes(), context);
					}
				}
			}
		}
	}

	private void processEvent(NodeList nlEvent, Context context) {

		Element eleEvent;

		if (nlEvent != null && nlEvent.getLength() > 0 ) {
			for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++) {
				if (nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE) {  
					eleEvent = (Element)nlEvent.item(idxEvent);
					
					posType = eleEvent.getAttribute("Type");
					
					if (eleEvent.getNodeName().equals("Event") &&
						     (posType.equalsIgnoreCase(TRX_SALE)  ||
						      posType.equalsIgnoreCase(TRX_REFUND) ||
						      posType.equalsIgnoreCase(TRX_OVERRING))) {
						processTrx(eleEvent.getChildNodes(), context);
					}
				}
			}
		}
	}

	private void processTrx(NodeList nlTrxSale, Context context) {

		Element eleTrx; 

		if (nlTrxSale != null && nlTrxSale.getLength() > 0 ) {
			for (int idxTrxSale=0; idxTrxSale < nlTrxSale.getLength(); idxTrxSale++ ) {
				if (nlTrxSale.item(idxTrxSale).getNodeType() == Node.ELEMENT_NODE ) {
					eleTrx = (Element)nlTrxSale.item(idxTrxSale);
					
					posAreaTypShrtDs = eleTrx.getAttribute("POD");
					posStatus = eleTrx.getAttribute("status");
					
					posTrnItmIndex = 1;
					posItmComboId = "-1";
					posSumItmComboId = "-1";
					
					if (posType.equals(TRX_SALE) && eleTrx.getAttribute("status").equals("Paid")) {
						posTrnSeqNu+=1;
						processOrder(eleTrx.getChildNodes(), context);
					} else if (posType.equals(TRX_REFUND) ) {
						posTrnSeqNu+=1;
						processOrder(eleTrx.getChildNodes(), context);
					} else if (posType.equals(TRX_OVERRING) ) {
						posTrnSeqNu+=1;
						processOrder(eleTrx.getChildNodes(), context);
					}

				}
			}
		}
	}

	private void processOrder(NodeList nlOrder, Context context) {

		Element eleOrder;
		posTrnTotQt = 0;
		posTrnTotBDAmt = DECIMAL_ZERO;

		try {
			if (nlOrder != null && nlOrder.getLength() > 0 ) {
				for (int idxOrder=0; idxOrder < nlOrder.getLength(); idxOrder++ ) {					
					if (nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {
						eleOrder = (Element)nlOrder.item(idxOrder);
						
						skipOrder = false;
						
						posOrdKey = getValue(eleOrder,"key");
						
						if (posOrdKey != null && !posOrdKey.isEmpty()) {
							skipOrder = false;
						} else {
							skipOrder = true;
						}
						
						if (!skipOrder) {
											
							posOrdKeyCd = posOrdKey.substring(5,7);
							
							posTrnStrtTs = formatAsTs(eleOrder.getAttribute("Timestamp"));

							posTrnOrdTs = formatTimestampAsTime(eleOrder.getAttribute("Timestamp"));
								
							getDaypart();
								
							posTrnTypCd = getValue(eleOrder,"kind");
								
							if (posType.equalsIgnoreCase(TRX_SALE)) {
								if (posTrnTypCd.equals("Sale")) {
									posTrnDscntTypCd = "0";
								
								} else if (posTrnTypCd.equals(ORDER_KIND_CREW)) {
									posTrnDscntTypCd = "1";
								
								} else if (posTrnTypCd.equals(ORDER_KIND_MANAGER)) {
									posTrnDscntTypCd = "2";
								
								} else if (posTrnTypCd.equals(ORDER_KIND_GENERIC)) {
									posTrnDscntTypCd = "3";								
								}
							
							} else if (posType.equalsIgnoreCase(TRX_REFUND)) {
								posTrnDscntTypCd = "-4";
							
							} else if (posType.equalsIgnoreCase(TRX_OVERRING)) {
								posTrnDscntTypCd = "-5";
							}
								
							posPrdDlvrMethDesc = getValue(eleOrder,"saleType");
								
							if (posAreaTypShrtDs.equals(SALE_TYPE_DRIVE_THRU)) {
								posPrdDlvrMethCd = "1";
							
							} else if (posAreaTypShrtDs.equals(SALE_TYPE_FRONT_COUNTER)) {
								
								if (posPrdDlvrMethDesc.equals("EatIn")) {
									posPrdDlvrMethCd = "2";
								} else if (posPrdDlvrMethDesc.equals("TakeOut")) {
									posPrdDlvrMethCd = "3";
								}
							
							} else {
								posPrdDlvrMethCd = "4";
							}
								
							posTrnTotNetAm = new BigDecimal(eleOrder.getAttribute("totalAmount"));
							posTrnTotTaxAm = new BigDecimal(eleOrder.getAttribute("totalTax"));
							
							posTrnTotNetAm = posTrnTotNetAm.setScale(2, RoundingMode.HALF_EVEN);
							posTrnTotTaxAm = posTrnTotTaxAm.setScale(2, RoundingMode.HALF_EVEN);
							
							posTrnTotNetAm = posTrnTotNetAm.multiply(ONE_HUNDRED);
							posTrnTotTaxAm = posTrnTotTaxAm.multiply(ONE_HUNDRED);
															
							processTenders(eleOrder.getChildNodes(),context);							
							
							if (tendersInfoMap !=null && tendersInfoMap.size() > 0 ) {
								if ( tendersInfoMap.containsKey(1)) {
									parts = tendersInfoMap.get(1).split("\t",-1);
									posOrdTndrId1 = parts[0];
									posOrdTndrKind1  = parts[1];
									posOrdTndrPaymentName1 = parts[2];
									posOrdTndrCardProvider1 = parts[3];
								}
								
								if (tendersInfoMap.containsKey(2)) {
									parts = tendersInfoMap.get(2).split("\t",-1);
									posOrdTndrId2 = parts[0];
									posOrdTndrKind2 = parts[1];
									posOrdTndrPaymentName2 = parts[2];
									posOrdTndrCardProvider2 = parts[3];
								}
								
								// 1 Tender
								if (posOrdTndrId2.equals("") || posOrdTndrPaymentName2.equals("% Discount")) {
									if (posOrdTndrId1.equals("0")) {	// Cash Only
										posTrnPymntTyp = "1";
									
									} else if (posOrdTndrId1.equals("10")) {
										
										if (posOrdTndrCardProvider1.equals("Debit")) {	// Debit
											posTrnPymntTyp = "4";
										} else {	// Credit
											posTrnPymntTyp = "2";
										}
									
									} else if (posOrdTndrId1.equals("11")) {	// Gift Card
										posTrnPymntTyp = "8";
									
									} else {	// Manager or Crew Meal - Make it Cash
										posTrnPymntTyp = "1";
									}
								
									// 2 Tenders
								} else {
									if (posOrdTndrId1.equals("0") && posOrdTndrId2.equals("0")) {	// Cash Only
										posTrnPymntTyp = "1";
									
									} else if (posOrdTndrId1.equals("0") || posOrdTndrId2.equals("0") && (posOrdTndrId1.equals("10") || posOrdTndrId2.equals("10"))) {	// Cash and Debit/Credit
										
										if (posOrdTndrCardProvider1.equals("Debit") || posOrdTndrCardProvider2.equals("Debit")) {
											posTrnPymntTyp = "4";
										} else {
											posTrnPymntTyp = "2";
										}
									
									} else if (posOrdTndrId1.equals("0") || posOrdTndrId2.equals("0") && (posOrdTndrId1.equals("11") || posOrdTndrId2.equals("11"))) {	// Gift Card and Cash
										posTrnPymntTyp = "9";
									
									} else if (posOrdTndrId1.equals("10") || posOrdTndrId2.equals("10") && (posOrdTndrId1.equals("11") || posOrdTndrId2.equals("11"))) {
										
										if (posOrdTndrCardProvider1.equals("Debit") || posOrdTndrCardProvider2.equals("Debit")) {	// Gift Card and Debit
											posTrnPymntTyp = "C";
										} else {
											posTrnPymntTyp = "A";	// Gift Card and Credit
										}
										
									} else {
										//Manager or Crew Meal - Make it Cash
										posTrnPymntTyp = "1";										
									}	
								}
							}
							
							processSummaryItem(eleOrder.getChildNodes(),context);
							processItem(eleOrder.getChildNodes(),context);
		
							//Transaction
							outputKey.setLength(0);						
							outputKey.append(REC_TRANS);
							outputKey.append(terrCd);
							outputKey.append(lgcyLclRfrDefCd);
							outputKey.append(posBusnDt);
								
							mapKey.clear();
							mapKey.set(outputKey.toString());
		
							outputTextValue.setLength(0);
							outputTextValue.append(lgcyLclRfrDefCd);
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posBusnDt);
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posTrnDscntTypCd);
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posPrdDlvrMethCd);
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posTrnSeqNu);
							outputTextValue.append(COMMA_DELIMITER);						
							outputTextValue.append(posTrnOrdTs);
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posOrdKeyCd);
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append("1");
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posTrnTotQt);
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posTrnTotTaxAm.toBigInteger());
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posTrnTotNetAm.toBigInteger());
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(posTrnPymntTyp);
							outputTextValue.append(COMMA_DELIMITER);
							posTrnTotBDAmt = posTrnTotBDAmt.multiply(ONE_HUNDRED);
							outputTextValue.append(posTrnTotBDAmt.toBigInteger());
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(dyptIdNu);
							outputTextValue.append(COMMA_DELIMITER);
							outputTextValue.append(dyOfCalWkDs);
								
							mapValue.clear();
							mapValue.set(outputTextValue.toString());
								
							context.write(mapKey, mapValue);	
							context.getCounter("COUNT","POS_TRN").increment(1);
						}
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.processOrder:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void processTenders(NodeList nlOrderItems, Context context) {

		Element eleOrderItems;
		
		try {
			
			tenderInfo.setLength(0);
		    tenderInfo.append("").append(SEPARATOR_CHARACTER); 		//TenderId
            tenderInfo.append("").append(SEPARATOR_CHARACTER); 		//TenderKind
            tenderInfo.append("").append(SEPARATOR_CHARACTER); 		//TenderName
            tenderInfo.append("").append(SEPARATOR_CHARACTER); 		//CardProviderId

            for(int i=1;i<=5;i++){   
		           tendersInfoMap.put(i, tenderInfo.toString());      
			}
		
			if (nlOrderItems != null && nlOrderItems.getLength() > 0 ) {
				for (int idxOrderItems=0; idxOrderItems < nlOrderItems.getLength(); idxOrderItems++ ) {
					if (nlOrderItems.item(idxOrderItems).getNodeType() == Node.ELEMENT_NODE) {
						eleOrderItems = (Element)nlOrderItems.item(idxOrderItems);
			
						if (eleOrderItems.getNodeName().equals("Tenders")) {
							
							 NodeList nlTenders = eleOrderItems.getChildNodes();
							 Element eleTender;
							 NodeList nlTenderSubItems;
							 Node textNode;
							 String textNodeName;
							 Element eleTenderSubItem;
							 
							 int numtenders = 0;
							 
						     if (nlTenders !=null && nlTenders.getLength() > 0) {
						     	for (int idxTenders=0; idxTenders < nlTenders.getLength(); idxTenders++) {
						     		if (nlTenders.item(idxTenders).getNodeType() == Node.ELEMENT_NODE) {
						        	  
						     			numtenders ++;
						        	  
						     			eleTender = (Element)nlTenders.item(idxTenders);
					
						     			nlTenderSubItems = eleTender.getChildNodes();
							            String tenderId  = "";
							            String tenderKind = "@@@@";
							            String tenderName = "@@@@";
							            String tenderCardProvider = "@@@@";
							            String tenderPaymentName ="";
								                   
								    	tenderInfo.setLength(0);
								    	int numberTenders = 0;
						            
								       	if (nlTenderSubItems != null && nlTenderSubItems.getLength() > 0) {
								       		for (int idxTenderSubItems = 0; idxTenderSubItems < nlTenderSubItems.getLength(); idxTenderSubItems++ ) {
								       			if (nlTenderSubItems.item(idxTenderSubItems).getNodeType() == Node.ELEMENT_NODE) {
									            		eleTenderSubItem = (Element)nlTenderSubItems.item(idxTenderSubItems);
						
									            		textNodeName = eleTenderSubItem.getNodeName();
									            		textNode = eleTenderSubItem.getFirstChild();
							                  
									            		if (textNodeName.equalsIgnoreCase("TenderId") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
									            			tenderId = textNode.getNodeValue();
											          	}
										                  
										              	else if (textNodeName.equalsIgnoreCase("TenderKind") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
										                	tenderKind = textNode.getNodeValue();
										              	}
	
										               	else if (textNodeName.equalsIgnoreCase("TenderName") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
										                    tenderName = textNode.getNodeValue();
										               	}
										                  
										             	else if (textNodeName.equalsIgnoreCase("CardProviderID") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE && 
										                		  textNode.getNodeValue() != null && !textNode.getNodeValue().trim().isEmpty()) {
										                    tenderCardProvider = textNode.getNodeValue();
										            	}
									        	}
								       		}

								       		if ( ! tenderName.equals("@@@@") && ( tenderKind.equals("0") || tenderKind.equals("1") || tenderKind.equals("2") || tenderKind.equals("3") || tenderKind.equals("4") || tenderKind.equals("5") || tenderKind.equals("8") ) ) { 
								       			if ( tenderCardProvider.equals("@@@@") ) {
									         		tenderPaymentName = tenderName;
									          	} else {
									          		tenderPaymentName = "Cashless-" + tenderCardProvider;
									           	}   
									       	}
								       	}
									            
									  	tenderInfo.setLength(0);
									            
									   	tenderInfo.append(tenderId).append(SEPARATOR_CHARACTER);			//TenderId
									   	tenderInfo.append(tenderKind).append(SEPARATOR_CHARACTER);			//TenderKind
										tenderInfo.append(tenderPaymentName).append(SEPARATOR_CHARACTER);	//TenderName
										tenderInfo.append(tenderCardProvider).append(SEPARATOR_CHARACTER);	//CardProviderId
									            
									  	tendersInfoMap.put(numtenders, tenderInfo.toString());
									            
									   	numberTenders++;
					     			}
					     		}
					     	}   
						}	
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in USTLDReformatMapper.processOrderItems:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void processSummaryItem(NodeList nlItem, Context context) {

		Element eleSummaryItem;
		@SuppressWarnings("unused")
		int qty;
		String posSumItmQtyText;
		BigDecimal posSumItmQty;
		BigDecimal posSumItmTotalPrice;
		BigDecimal posSumItmBdPrice;
		BigDecimal posSumItmBpPrice;

		try {
			
			if (nlItem != null && nlItem.getLength() > 0 ) {
				for (int idxItem=0; idxItem < nlItem.getLength(); idxItem++ ) {
					if (nlItem.item(idxItem).getNodeType() == Node.ELEMENT_NODE ) {
						eleSummaryItem = (Element)nlItem.item(idxItem);
						if (eleSummaryItem.getNodeName().equals("Item") ) {
							
							skipItem = false;
							
							posSumItmType = getValue(eleSummaryItem,"type");
							posSumItmLvlNu = getValue(eleSummaryItem,"level");
							posSumItmId = getValue(eleSummaryItem,"id");
							posSumItmMenuItmId = getValue(eleSummaryItem,"code");
							
							if (posSumItmMenuItmId.length() > 4 ) {
								skipItem = true;
							}
							
							posSumItmVoidQt = getValue(eleSummaryItem,"qtyVoided");
							
							posSumItmQtyText = getValue(eleSummaryItem,"qty");
							posSumItmQty = new BigDecimal(posSumItmQtyText);
							posSumItmTotalPrice = new BigDecimal(eleSummaryItem.getAttribute("totalPrice"));
							posSumItmBdPrice =  new BigDecimal(eleSummaryItem.getAttribute("BDPrice"));
					        posSumItmBpPrice =  new BigDecimal(eleSummaryItem.getAttribute("BPPrice"));
					        
					        BigDecimal posSumItmMenuItmUntPrcAm = new BigDecimal(0);
					        
					        if (posSumItmBpPrice != null && posSumItmBpPrice.compareTo(DECIMAL_ZERO) > 0 && posSumItmQty.compareTo(DECIMAL_ZERO) > 0) {
					        	posSumItmMenuItmUntPrcAm =  posSumItmBpPrice.divide(posSumItmQty,2,RoundingMode.HALF_UP);
					        } else if (posSumItmBdPrice != null && posSumItmBdPrice.compareTo(DECIMAL_ZERO) > 0 && posSumItmQty.compareTo(DECIMAL_ZERO) > 0) {
					        	posSumItmMenuItmUntPrcAm = posSumItmBdPrice.divide(posSumItmQty,2,RoundingMode.HALF_UP);
					        } else if (posSumItmTotalPrice != null && posSumItmTotalPrice.compareTo(DECIMAL_ZERO) > 0 && posSumItmQty.compareTo(DECIMAL_ZERO) > 0) {
					        	posSumItmMenuItmUntPrcAm = posSumItmTotalPrice.divide(posSumItmQty,2,RoundingMode.HALF_UP);
					        }
							
							try {
								qty = Integer.parseInt(posSumItmVoidQt);
							} catch (Exception ex) {
								posSumItmVoidQt = "0";
							}
							
							if (posSumItmLvlNu.equalsIgnoreCase("0") && posSumItmType.equalsIgnoreCase("VALUE_MEAL")) {
								posSumItmComboId = posSumItmId;
							}
							
							if ( posSumItmLvlNu.equals("1") && posSumItmMenuItmUntPrcAm.compareTo(DECIMAL_ZERO) == 0 && posSumItmComboId.equalsIgnoreCase(posSumItmId) ) {
								skipItem = true;
							}
							
							if (posSumItmType.equals("COMMENT")) {
								skipItem = true;
							}
							
							if (posSumItmVoidQt.equals("0")) {
							
								if (!skipItem) {
								
									if ((posType.equalsIgnoreCase(TRX_SALE) && posStatus.equals("Paid")) || (posType.equalsIgnoreCase(TRX_REFUND) || posType.equalsIgnoreCase(TRX_OVERRING)) ) {																			
										if (posType.equalsIgnoreCase(TRX_SALE)) {
											
											if (posTrnTypCd.equals("Sale")) {
												posTrnTotQt++;
												posTrnTotBDAmt = DECIMAL_ZERO;
												
											} else if (posTrnTypCd.equals(ORDER_KIND_MANAGER) || posTrnTypCd.equals(ORDER_KIND_CREW) || posTrnTypCd.equals(ORDER_KIND_GENERIC)) {

												if (posSumItmBdPrice.compareTo(DECIMAL_ZERO) > 0 ) {
													posTrnTotQt++;
													posTrnTotBDAmt = posTrnTotBDAmt.add(posSumItmBdPrice.subtract(posSumItmTotalPrice));
													
												}
											}
										}
			
										if (posType.equalsIgnoreCase(TRX_OVERRING) ) {
											posTrnTotQt--;
										}
									
										if (posType.equalsIgnoreCase(TRX_REFUND) || posType.equalsIgnoreCase(TRX_OVERRING)) {
											posTrnTotBDAmt = DECIMAL_ZERO;
										}
									}
								}
								
								processSummaryItem(eleSummaryItem.getChildNodes(),context);
							}
						}
					}
				}
			}			

		} catch (Exception ex) {
			System.err.println("Error occured in USTLDReformatMapper.processSummaryItem:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	@SuppressWarnings("null")
	private void processItem(NodeList nlItem, Context context) {
		
		Element eleItem;
		int qty;
		String posItmQtyText;
		String posItmPrmoQtyText;
		BigDecimal posItmQty;
		BigDecimal posItmPrmoQty;
		BigDecimal posItmTotalPrice;
		BigDecimal posItmBdPrice;
		BigDecimal posItmBpPrice;
		boolean isMenuAdded = false;
		
		try {
			if (nlItem != null && nlItem.getLength() > 0 ) {
				for (int idxItem=0; idxItem < nlItem.getLength(); idxItem++ ) {
					if (nlItem.item(idxItem).getNodeType() == Node.ELEMENT_NODE ) {
						eleItem = (Element)nlItem.item(idxItem);
						if (eleItem.getNodeName().equals("Item") ) {
							
							isMenuAdded = false;
							skipItem = false;
							
							posItmType = getValue(eleItem,"type");
							posItmLvlNu = getValue(eleItem,"level");
							posItmId = getValue(eleItem,"id");
							posItmMenuItmId = getValue(eleItem,"code");
							
							if (posItmMenuItmId.length() > 4 ) {
								skipItem = true;
							}
							
							posItmMenuItmDesc = getValue(eleItem, "description");
							
							if (posItmMenuItmDesc != null && !posItmMenuItmDesc.isEmpty()) {
								if (posItmMenuItmDesc.length() > 7 ) {
									posItmMenuItmDesc = posItmMenuItmDesc.substring(0,7);
								}
							} else {
								posItmMenuItmDesc = "Unkwn";
							}
							
							posItmVoidQt = getValue(eleItem,"qtyVoided");
							
							posItmQtyText = getValue(eleItem,"qty");
							posItmQty = new BigDecimal(posItmQtyText);
							posItmPrmoQtyText = getValue(eleItem,"qtyPromo");
							posItmPrmoQty = new BigDecimal(posItmPrmoQtyText);
							posItmTotalPrice = new BigDecimal(eleItem.getAttribute("totalPrice"));
							posItmBdPrice =  new BigDecimal(eleItem.getAttribute("BDPrice"));
					        posItmBpPrice =  new BigDecimal(eleItem.getAttribute("BPPrice"));
					        
					        BigDecimal posSldItmQty = new BigDecimal(0);						        
					        
					        if (posItmPrmoQty.compareTo(DECIMAL_ZERO) > 0) {
					        	posItmPrmo = "true";
					        	posSldItmQty = posItmQty.subtract(posItmPrmoQty);
							} else {
								posItmPrmo = "false";
								posSldItmQty = posItmQty;
							}
					        
					        BigDecimal posItmMenuItmUntPrcAm = new BigDecimal(0);
					        
					        if (posItmBpPrice != null && posItmBpPrice.compareTo(DECIMAL_ZERO) > 0 && posItmQty.compareTo(DECIMAL_ZERO) > 0) {
					        	posItmMenuItmUntPrcAm =  posItmBpPrice.divide(posItmQty,2,RoundingMode.HALF_UP);
					        } else if (posItmBdPrice != null && posItmBdPrice.compareTo(DECIMAL_ZERO) > 0 && posItmQty.compareTo(DECIMAL_ZERO) > 0) {
					        	posItmMenuItmUntPrcAm = posItmBdPrice.divide(posItmQty,2,RoundingMode.HALF_UP);
					        } else if (posItmTotalPrice != null && posItmTotalPrice.compareTo(DECIMAL_ZERO) > 0 && posItmQty.compareTo(DECIMAL_ZERO) > 0) {
					        	posItmMenuItmUntPrcAm = posItmTotalPrice.divide(posItmQty,2,RoundingMode.HALF_UP);
					        }
							
							try {
								qty = Integer.parseInt(posItmVoidQt);
							} catch (Exception ex) {
								posItmVoidQt = "0";
							}
							
							if (posItmLvlNu.equalsIgnoreCase("0") && posItmType.equalsIgnoreCase("VALUE_MEAL")) {
								posItmComboId = posItmId;
							}
							
							if (posItmLvlNu.equals("1") && 
								posItmMenuItmUntPrcAm.compareTo(DECIMAL_ZERO) == 0 && 
								posItmComboId.equalsIgnoreCase(posItmId)) {
									skipItem = true;
							}
							
							if (posItmType.equals("COMMENT")) {
								skipItem = true;
							}
							
							if (posItmVoidQt.equals("0")) {
								
								if (!skipItem) {
							
									menuHashMapKey = "MNU" + terrCd + lgcyLclRfrDefCd + posBusnDt + posItmMenuItmId + posItmMenuItmUntPrcAm;
									menuHashMapValue = posItmMenuItmIndex;								
									
									if (!processMenuFile.containsKey(menuHashMapKey)) {
										processMenuFile.put(menuHashMapKey, menuHashMapValue);
										
										isMenuAdded = true;
									
										//Menu
										outputKey.setLength(0);						
										outputKey.append(REC_MENU);
										outputKey.append(terrCd);
										outputKey.append(lgcyLclRfrDefCd);
										outputKey.append(posBusnDt);
										outputKey.append(posItmMenuItmId);
										outputKey.append(posItmMenuItmUntPrcAm.toString());
										
										mapKey.clear();
										mapKey.set(outputKey.toString());								
		
										outputTextValue.setLength(0);
										outputTextValue.append(lgcyLclRfrDefCd);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posBusnDt);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmMenuItmDesc);
										outputTextValue.append(COMMA_DELIMITER);
										posItmMenuItmUntPrcAm = posItmMenuItmUntPrcAm.multiply(ONE_HUNDRED);
										outputTextValue.append(posItmMenuItmUntPrcAm.toBigInteger());
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmMenuItmId);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(menuHashMapValue);
									
										mapValue.clear();
										mapValue.set(outputTextValue.toString());
										
										context.write(mapKey, mapValue);
										context.getCounter("COUNT","POS_MNU_ITM").increment(1);
									}
								
									//Transaction Item
									if (posSldItmQty.compareTo(DECIMAL_ZERO) != 0) {
										outputKey.setLength(0);						
										outputKey.append(REC_TRANSITEM);
										outputKey.append(terrCd);
										outputKey.append(lgcyLclRfrDefCd);
										outputKey.append(posBusnDt);
										
										mapKey.clear();
										mapKey.set(outputKey.toString());
			
										outputTextValue.setLength(0);
										outputTextValue.append(lgcyLclRfrDefCd);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posBusnDt);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnSeqNu);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnOrdTs);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posOrdKeyCd);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append("1");
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnTotQt);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnTotNetAm.toBigInteger());
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posPrdDlvrMethCd);
										outputTextValue.append(COMMA_DELIMITER);
										
										if (isMenuAdded) {
											outputTextValue.append(menuHashMapValue);
											//posItmMenuItmIndex+=1;
										} else {
											Integer menuHashMapValue=(Integer)processMenuFile.get(menuHashMapKey);
											outputTextValue.append(menuHashMapValue);
										}
										
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posSldItmQty.toString());
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmMenuItmId);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(DECIMAL_ZERO.toString());
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append("false");
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmMenuItmId);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmMenuItmDesc);
										outputTextValue.append(COMMA_DELIMITER);					
										outputTextValue.append(posTrnPymntTyp);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(dyptIdNu);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(dyOfCalWkDs);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnDscntTypCd);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnItmIndex);
										posTrnItmIndex+=1;
									
										mapValue.clear();
										mapValue.set(outputTextValue.toString());
										
										context.write(mapKey, mapValue);
										context.getCounter("COUNT","POS_TRN_ITM").increment(1);
									}
									
									if (posItmPrmoQty.compareTo(DECIMAL_ZERO) != 0) {
										//Transaction Item
										outputKey.setLength(0);						
										outputKey.append(REC_TRANSITEM);
										outputKey.append(terrCd);
										outputKey.append(lgcyLclRfrDefCd);
										outputKey.append(posBusnDt);
										
										mapKey.clear();
										mapKey.set(outputKey.toString());
			
										outputTextValue.setLength(0);
										outputTextValue.append(lgcyLclRfrDefCd);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posBusnDt);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnSeqNu);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnOrdTs);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posOrdKeyCd);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append("1");
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnTotQt);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnTotNetAm.toBigInteger());
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posPrdDlvrMethCd);
										outputTextValue.append(COMMA_DELIMITER);
										
										if (isMenuAdded) {
											outputTextValue.append(menuHashMapValue);
											//posItmMenuItmIndex+=1;
										} else {
											Integer menuHashMapValue=(Integer)processMenuFile.get(menuHashMapKey);
											outputTextValue.append(menuHashMapValue);
										}
										
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmPrmoQty.toString());
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmMenuItmId);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmPrmoQty.toString());
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append("true");
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmMenuItmId);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posItmMenuItmDesc);
										outputTextValue.append(COMMA_DELIMITER);					
										outputTextValue.append(posTrnPymntTyp);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(dyptIdNu);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(dyOfCalWkDs);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnDscntTypCd);
										outputTextValue.append(COMMA_DELIMITER);
										outputTextValue.append(posTrnItmIndex);
										posTrnItmIndex+=1;
									
										mapValue.clear();
										mapValue.set(outputTextValue.toString());
										
										context.write(mapKey, mapValue);
										context.getCounter("COUNT","POS_TRN_ITM").increment(1);
									}
									
									if (isMenuAdded) {
										posItmMenuItmIndex+=1;
									}									
								}
								
								processItem(eleItem.getChildNodes(),context);
							}
						}
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in USTLDReformatMapper.processItem:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void getDaypart() {
		
		String dayPartKey;
		String hour;
		String minute;
		int minuteInt;
		
		dyptIdNu = "";
		dyptDs = "";
		dyOfCalWkDs = "";
		
		cal.set(Integer.parseInt(posTrnStrtTs.substring(0, 4)), Integer.parseInt(posTrnStrtTs.substring(5, 7))-1, Integer.parseInt(posTrnStrtTs.substring(8, 10)));
		hour = posTrnStrtTs.substring(11, 13);
		hrIdNu = String.valueOf(hour);
		minuteInt = Integer.parseInt(posTrnStrtTs.substring(14, 16));
		
		if ( minuteInt < 15 ) {
			minute = "00";
		} else if ( minuteInt < 30 ) {
			minute = "15";
		} else if ( minuteInt < 45 ) {
			minute = "30";
		} else {
			minute = "45";
		}
		
		switch ( cal.get(Calendar.DAY_OF_WEEK) ) {
			case 1: 
				dyOfCalWkDs = "7";	//Sunday
			    break;
			
			case 2: 
				dyOfCalWkDs = "1";	//Monday
			    break;
			
			case 3: 
				dyOfCalWkDs = "2";	//Tuesday
			    break;
			
			case 4: 
				dyOfCalWkDs = "3";	//Wednesday
			    break;
			
			case 5: 
				dyOfCalWkDs = "4";	//Thursday
			    break;
			
			case 6: 
				dyOfCalWkDs = "5";	//Friday
			    break;
			
			case 7: 
				dyOfCalWkDs = "6";	//Saturday
			    break;
			
			default: 
				dyOfCalWkDs = "**";
			    break;
		}

		dayPartKey = terrCd + SEPARATOR_CHARACTER + String.valueOf(cal.get(Calendar.DAY_OF_WEEK)) + SEPARATOR_CHARACTER + hour + minute;
		
		if ( dayPartMap.containsKey(dayPartKey) ) {
			parts = dayPartMap.get(dayPartKey).split("\t",-1);
			dyptIdNu = parts[0];
			dyptDs = parts[1];
		}
	}
	
	private String formatAsTs(String in) {
		
		String retTs = "";
		
		if ( in.length() >= 14 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8) + " " + in.substring(8, 10) + ":" + in.substring(10, 12) + ":" + in.substring(12, 14);
		}

		return(retTs);
	}
	
	private String formatTimestampAsTime(String in) {

		String retTime = "";
		
		if ( in.length() >= 14 ) {
			retTime = in.substring(8, 10) + ":" + in.substring(10, 12) + ":" + in.substring(12, 14);
		}

		return(retTime);
	}

	private String getValue(Element ele
                           ,String attribute) {

		String retValue = "";

		try {
			retValue = ele.getAttribute(attribute);

		} catch (Exception ex) {
			
		}

		return(retValue.trim());
	}
}

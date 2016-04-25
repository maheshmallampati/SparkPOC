package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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
import com.mcd.gdw.daas.driver.TDADriver;
import com.mcd.gdw.daas.util.PmixData;
import com.mcd.gdw.daas.util.TDAUtil;

/**
 * @author Naveen Kumar B.V
 *
 * Mapper class to parse STLD xml files and extract the fields required for processing.
 * Output for order header is written to multipleOutputs, and detail output is written to context
 * for further processing in reducer.
 */
public class TDAStldMapper extends Mapper<LongWritable, Text, Text, Text> {	
	
	private MultipleOutputs<Text, Text> multipleOutputs;
	
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private Document document = null;
	private Element rootElement;
	private NodeList nlEvent;
	private Element eleEvent;
	private Element eleTrx;
	private NodeList nlTRX;
	private Element eleOrder;
	private NodeList nlOrder;
	private NodeList nlNode;
	private Element eleNode;
	private NodeList offersList;
	private Element offerElement;
	private NodeList customerList;
	private Element customerElement;
	
	private String[] inputParts = null;
	private String xmlText = "";
	
	private String terrCd = "";
	private String gdwMcdGbalLcatIdNu = "";
	private String orderKey = "";
	private String businessDate = "";
	private String eventType = "";
	private String eventTimeStamp = "";
	private String posEvntTypId = "";
	private String curnIsoNnu = "";
	private String orderTimeStamp = "";
	private BigDecimal totalAmount;
	private String nodeId = "";
	private String posOrdDt = "";
	private String posUnqKey = "";
	private String handHeldOrdFl = "";
	private String orderSide = "";
	private String offersPromotionId = "";
	private String offersOverride = "";
	private String offersApplied = "";
	private String offersofferId = "";
	private String customerId = "";
	private String terrPymtMethCd = "";
	private String saleType = "";
	private String terrPrdDlvrMethCd = "";
	private String trxPOD = "";	
	private String terrPosAreaCd = "";
	private String orderKind = "";
	private String posTrnKindTypCd = "";	
	private String orderTax = "";
	private String tenderKind = "";
	private String tenderName = "";
	private String tenderAmount = "";
	private String tenderDetails = "";
	private String cashlessData = "";
	private String[] transKeyAndAuthCode = null;
	
	private String lgcyLclRfrDefCd = "";	
	private String gdwBusinessDate = "";
	private String haviBusinessDate = "";
	private long haviTimeKey = 0;
	private String status = "";	
	private String gdwTimeStamp;
	private String ownerShipType = "";
	
	private boolean isValidInput = false;
	private PmixData pmixData = null;
	
	private Map<String, PmixData> pmixDataMap = null;		
	private Map<String, List<String>> comboItemLookup =  null;
	private Map<String, String> primaryItemLookup = null;
	private Map<String,String> eventCodesMap = null;
	private Map<String, String> trxPodMap = null;
	private Map<String, String> orderKindMap = null;	
	private Map<String, String> saleTypeMap = null;
	private Map<String, String> paymentCodesMap = null;
	
	private StringBuffer pmixKeyStrBuf = null;
	private StringBuffer pmixValueStrBuf = null;
	private StringBuffer keyStrBuf = null;
	
	private Text pmixKeyTxt = null;
	private Text pmixValueTxt = null;
	
	
	private static StringBuffer outputStrBuf = new StringBuffer();
	private static Text outputTxt = new Text();
	
	private String uniqueFileId;
	private int ordHdrRecordCount;
	private int ordHdrUniqStoreCount;
	
	private String partitionID;
	
	/**
	 * Setup method to create multipleOutputs
	 */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		partitionID = String.format("%05d", context.getTaskAttemptID().getTaskID().getId());
		uniqueFileId = String.valueOf(System.currentTimeMillis()) + partitionID;
		
		
		eventCodesMap = TDAUtil.addEventCodesToMap(eventCodesMap);
		trxPodMap = TDAUtil.addTrxPodCodesToMap(trxPodMap);
		orderKindMap = TDAUtil.addOrderKindCodesToMap(orderKindMap);
		saleTypeMap = TDAUtil.addSaleTypeCodesToMap(saleTypeMap);
		paymentCodesMap = TDAUtil.addPaymentCodesToMap(paymentCodesMap);
		
		multipleOutputs = new MultipleOutputs<Text, Text>(context);	
        docFactory = DocumentBuilderFactory.newInstance();

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
        
        for (int fileCounter = 0; fileCounter < cacheFiles.length; fileCounter++) {
        	
        	fileParts = cacheFiles[fileCounter].toString().split("#");
        	br = new BufferedReader(new FileReader("./" + fileParts[1]));
        	
        	while ((inputLookUpData = br.readLine()) != null) {
            	inputDataArray = inputLookUpData.split(TDADriver.REGEX_PIPE_DELIMITER);
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
        try {
        	docBuilder = docFactory.newDocumentBuilder();
        } catch (ParserConfigurationException pce) {
        	pce.printStackTrace();
        }
        	
	}
    
	/**
	 * Map method to parse XML and generate psv output
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		inputParts = value.toString().split(DaaSConstants.TAB_DELIMITER);
		ownerShipType = inputParts[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS];
		xmlText = inputParts[DaaSConstants.XML_REC_XML_TEXT_POS];
		
        try {

            document = docBuilder.parse(new InputSource(new StringReader(xmlText)));
            rootElement = (Element) document.getFirstChild();               
            if(rootElement != null && rootElement.getNodeName().equals("TLD")) {        	   
         	       	               	  
         	    pmixDataMap = new HashMap<String, PmixData>(); 
         		totalAmount = TDADriver.DECIMAL_ZERO;
				isValidInput = TDAUtil.validateTldElementAttrs(rootElement, multipleOutputs);
					
				if (isValidInput) {						
					terrCd = rootElement.getAttribute("gdwTerrCd");
					curnIsoNnu = terrCd.equals("840") ? terrCd : "";
					
					businessDate = rootElement.getAttribute("businessDate");
					gdwMcdGbalLcatIdNu = rootElement.getAttribute("gdwMcdGbalLcatIdNu");					
					
					lgcyLclRfrDefCd = String.valueOf(Integer.parseInt(rootElement.getAttribute("gdwLgcyLclRfrDefCd")));						
					gdwBusinessDate = businessDate.substring(0, 4) + TDADriver.GDW_DATE_SEPARATOR + businessDate.substring(4, 6) + 
													TDADriver.GDW_DATE_SEPARATOR + businessDate.substring(6);
					haviBusinessDate = businessDate.substring(4, 6) + TDADriver.HAVI_DATE_SEPARATOR + businessDate.substring(6) + 
							TDADriver.HAVI_DATE_SEPARATOR  + businessDate.substring(0, 4);												
				
					Date date = new Date();
					SimpleDateFormat gdwDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
					gdwTimeStamp = gdwDateFormat.format(date) + "";
					gdwTimeStamp = gdwTimeStamp.substring(0, gdwTimeStamp.length() - 1);
					
                    nlNode = rootElement.getChildNodes();
      				if (nlNode != null && nlNode.getLength() > 0) {
      					for (int idxNode = 0; idxNode < nlNode.getLength(); idxNode++) {						
      						if(nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE) {
      							eleNode = (Element) nlNode.item(idxNode);
      							if (eleNode != null && eleNode.getNodeName().equals("Node")) {
      								
      								isValidInput = TDAUtil.validateNodeElementAttrs(eleNode, gdwMcdGbalLcatIdNu, businessDate, multipleOutputs);
      								
      								if (isValidInput) {
      									
      									nodeId = eleNode.getAttribute("id");      								
          								handHeldOrdFl = TDAUtil.determineHandheldOrdFl(nodeId); 
          								nlEvent = eleNode.getChildNodes();
          								
          								if (nlEvent != null && nlEvent.getLength() > 0) {
          									for (int idxEvent = 0; idxEvent < nlEvent.getLength(); idxEvent++) {
          										if (nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE) {
          											eleEvent = (Element) nlEvent.item(idxEvent);
          											if (eleEvent != null && eleEvent.getNodeName().equals("Event")) {
          												
          												isValidInput = TDAUtil.validateEventElementAttrs(eleEvent, gdwMcdGbalLcatIdNu, 
          																	businessDate, multipleOutputs);	
          												
          												if (isValidInput) {
          													eventType = eleEvent.getAttribute("Type");
          													eventTimeStamp = eleEvent.getAttribute("Time");
          													posEvntTypId = eventCodesMap.get(eventType) == null ? 
          																   TDADriver.UNKNOWN_EVT_CODE : eventCodesMap.get(eventType);
          													
    	      												if (eventType.equals("TRX_Sale") || eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring")) {
    	      													eleTrx = null;
    	    													nlTRX = eleEvent.getChildNodes();
    	    													int idxTRX = 0;
    	    													while (eleTrx == null && idxTRX < nlTRX.getLength()) { 
    	    														if (nlEvent.item(idxTRX).getNodeType() == Node.ELEMENT_NODE) {
    	    															eleTrx = (Element) nlTRX.item(idxTRX);
    	    															status = eleTrx.getAttribute("status");
    	    															trxPOD = eleTrx.getAttribute("POD");
    	    															terrPosAreaCd = trxPodMap.get(trxPOD) == null ? 
    	    																			TDADriver.UNKNOWN_POD_CODE : trxPodMap.get(trxPOD);
    	    														}
    	    														idxTRX++;
    	    													}
    	      													
    	    													isValidInput = TDAUtil.validateTrxElementAttrs(eleTrx, gdwMcdGbalLcatIdNu, businessDate,
    	    																							 eventType, eventTimeStamp, multipleOutputs);
    	    													
    	    													if (isValidInput) {
    	    														if ((eleTrx != null) && (eventType.equals("TRX_Sale") && status.equals("Paid")) || 
    	      																(eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring"))) {
    	    														eleOrder = null;
    	    														nlOrder = eleTrx.getChildNodes();
    	    														int idxOrder = 0;
    	    														while (eleOrder == null && idxOrder < nlOrder.getLength()) {
    	    															if (nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE) {
    	    																eleOrder = (Element) nlOrder.item(idxOrder);
    	    															}
    	    															idxOrder++;
    	    														}
    	    														 														    														
    	    														isValidInput = TDAUtil.validateOrderElementAttrs(eleOrder, gdwMcdGbalLcatIdNu, 
    	    																											businessDate, multipleOutputs);		    														
    	    														if (isValidInput) {
    	    															
    	    															tenderDetails = "";
    	    															cashlessData = "";
    	    															
    	    															orderTimeStamp = eleOrder.getAttribute("Timestamp");	
        																orderTimeStamp = String.format("%-17s", orderTimeStamp).replace(' ', '0');
        																posOrdDt = TDAUtil.formatOrderDate(orderTimeStamp);
        																orderTimeStamp = TDAUtil.formatOrderTimestamp(orderTimeStamp); 
    	    															 	    														
    	    															orderKey = eleOrder.getAttribute("key").trim();
    	    															posUnqKey = eleOrder.getAttribute("uniqueId").trim();
    	    															orderKind = eleOrder.getAttribute("kind").trim();
    	    															
    	    															posTrnKindTypCd = orderKindMap.get(orderKind) == null ? 
    	    																			TDADriver.UNKNOWN_ORD_KIND_CODE : orderKindMap.get(orderKind);	
    	    															
    	    															orderSide = eleOrder.getAttribute("side");
    	    															saleType = eleOrder.getAttribute("saleType");
    	    															terrPrdDlvrMethCd = saleTypeMap.get(saleType) == null ? 
    	    																			TDADriver.UNKNOWN_SALE_TYPE_CODE : saleTypeMap.get(saleType);
    	    															
    	    															totalAmount = new BigDecimal(eleOrder.getAttribute("totalAmount"));
    	    															orderTax = eleOrder.getAttribute("totalTax");			    																										
    	    															
    	    															offersList = (eleOrder == null ? null : eleOrder.getElementsByTagName("Offers")); 
    	    															
    		    														if (offersList != null) {
    		    															for (int offerIdx = 0; offerIdx < offersList.getLength(); offerIdx++) {
    		    																 offerElement = (Element) offersList.item(offerIdx);			    																   
    		    																 offersofferId = offerElement.getAttribute("offerId");
    		    																 offersOverride = offerElement.getAttribute("override").equalsIgnoreCase("true") ? "1" : "0";
    		    																 offersApplied = offerElement.getAttribute("applied").equalsIgnoreCase("true") ? "1" : "0";
    		    																 offersPromotionId = offerElement.getAttribute("promotionId");
    		    															}
    		    														}
    		    														   
    	    														   customerList = (eleOrder == null ? null : eleOrder.getElementsByTagName("Customer")); 
    	    														   if (customerList != null) { 
    	    															   for (int custIdx = 0; custIdx < customerList.getLength(); custIdx++) {
    	    																   customerElement = (Element) customerList.item(custIdx);
    	    																   customerId = customerElement.getAttribute("id");
    	    															   }
    	    														   }
    	    														   
    	    														   // To get all tender details for an order.
    	    														   getTenderDetails();
    	    														   
    	    														   String paymentCode = TDAUtil.determinePaymentMethod(tenderDetails, paymentCodesMap);
    	    														   terrPymtMethCd = (paymentCode == null) ? TDADriver.UNKNOWN_PMT_CODE : paymentCode; 
    	    														   
    	    														   transKeyAndAuthCode = TDAUtil.determineTransKeyAuthCode(cashlessData, paymentCode);
    	    														   
    	    														   writeOrderHeaderToMultipleOutput();	
    	    														   
    	    														   isValidInput = TDAUtil.validateItemElementAttrs(eleOrder, gdwMcdGbalLcatIdNu, 
        														   														businessDate, multipleOutputs);
    	    														   if (isValidInput) {
    		    														   // Calculate Order Detail 
    			    													   pmixDataMap = TDAUtil.generateOrderDetail(eleOrder, pmixDataMap, lgcyLclRfrDefCd, orderKind, 
    			    																			orderKey, eventType, posEvntTypId, comboItemLookup, primaryItemLookup); 				    													
    			    													   
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
      					}					
      				}
				}
					
				ordHdrUniqStoreCount++;
				
				// PMIX Output
				/**
				 * The output data for order detail from this mapper is written to context.
				 * Key: StoreId|Date
				 * Value: S|<Pipe Separated Order Detail Data>
				 * 
				 * StoreId|Date is used as the join key in the reducer to join output from TDAMenuItemMapper.
				 */
				for (String eachPmixKey : pmixDataMap.keySet()) {

					pmixData = pmixDataMap.get(eachPmixKey);
					if (pmixData.getMitmKey() < 80000000) {
									
						pmixKeyStrBuf.setLength(0);
						pmixValueStrBuf.setLength(0);
						
						pmixKeyStrBuf.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER).append(businessDate);
						pmixValueStrBuf.append(TDADriver.STLD_TAG_INDEX).append(DaaSConstants.PIPE_DELIMITER)
									   .append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
									   .append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER)
									   .append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getMitmKey()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getDlyPmixPrice()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(TDADriver.DECIMAL_ZERO).append(DaaSConstants.PIPE_DELIMITER)
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
									   .append(pmixData.getLvl0InCmbCmpFlag()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getPosEvntTypId()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getItemLevel()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getStldItemCode()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getItemTaxAmt()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getItemTaxPercent()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getComboBrkDwnPrice()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getItemLineSeqNum()).append(DaaSConstants.PIPE_DELIMITER)
									   .append(pmixData.getMenuItemComboFlag());
						
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
        } catch (SAXException e) {
 			e.printStackTrace();
 		} catch (IOException e) {
 			e.printStackTrace();
 		} catch (InterruptedException e) {
 			e.printStackTrace();
 		} catch (Exception e) {
 			e.printStackTrace();
 		}
	}
	
	
	/**
	 * Method to get the tender details
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void getTenderDetails() throws IOException, InterruptedException {

		NodeList tendersList = (eleOrder == null ? null : eleOrder.getElementsByTagName("Tenders"));
		NodeList tendersChildNodes = null;
		Element tenderElement = null;
		
		NodeList tenderKindNl = null;
		NodeList tenderNameNl = null;
		NodeList tenderAmountNl = null;
		NodeList cashlessDataN1 = null;
		
		if (tendersList != null && tendersList.getLength() > 0) {
			for (int tListIdx = 0; tListIdx < tendersList.getLength(); tListIdx++) {
				tendersChildNodes = tendersList.item(tListIdx).getChildNodes();
				for (int tIdx = 0; tIdx < tendersChildNodes.getLength(); tIdx++) {
					
					if (tendersChildNodes.item(tIdx).hasChildNodes()) {
						tenderElement = (Element) tendersChildNodes.item(tIdx);
						
						if (eventType.equals("TRX_Sale")) {
							
							tenderKindNl = tenderElement.getElementsByTagName("TenderKind");
							tenderKind = (tenderKindNl != null) ? tenderKindNl.item(0).getTextContent() : "";
							
							tenderNameNl = tenderElement.getElementsByTagName("TenderName");
							tenderName = (tenderNameNl != null) ? tenderNameNl.item(0).getTextContent() : "";
							
							tenderAmountNl = tenderElement.getElementsByTagName("TenderAmount");
							tenderAmount = (tenderAmountNl != null) ? tenderAmountNl.item(0).getTextContent() : "";
							
							tenderDetails = tenderDetails + tenderKind + TDADriver.COLON_SEPARATOR;
							tenderDetails = tenderDetails + tenderName + TDADriver.COLON_SEPARATOR;
							tenderDetails = tenderDetails + tenderAmount + TDADriver.SEMI_COLON_SEPARATOR;
							
							// Note: For orders using multiple payment methods, cashless data information is repeated in all the tenders.
							// This will consider cashless data information from the last tender section.
							if (tenderName.equals("Cashless") || tenderName.equals("Gift Card")) {
								cashlessDataN1 = tenderElement.getElementsByTagName("CashlessData");
								cashlessData = (cashlessDataN1 != null) ? cashlessDataN1.item(0).getTextContent() : "";									
							}
							
						}	
					}
				}
			}
		}
	}
	
	
	/**
	 * Method to write Order header to multiple outputs 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void writeOrderHeaderToMultipleOutput() throws IOException, InterruptedException {
		
		ordHdrRecordCount++;
		
		outputStrBuf.setLength(0);
		outputStrBuf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER)
					.append(gdwMcdGbalLcatIdNu).append(DaaSConstants.PIPE_DELIMITER)
					.append(orderKey).append(DaaSConstants.PIPE_DELIMITER)
					.append(gdwBusinessDate).append(DaaSConstants.PIPE_DELIMITER)				
					.append(posEvntTypId).append(DaaSConstants.PIPE_DELIMITER)
					.append(curnIsoNnu).append(DaaSConstants.PIPE_DELIMITER)
					.append(posOrdDt).append(DaaSConstants.PIPE_DELIMITER)
					.append(nodeId).append(DaaSConstants.PIPE_DELIMITER)
					.append(orderTimeStamp).append(DaaSConstants.PIPE_DELIMITER)						
					.append(posUnqKey).append(DaaSConstants.PIPE_DELIMITER)	
					.append(handHeldOrdFl).append(DaaSConstants.PIPE_DELIMITER)
					.append(orderSide).append(DaaSConstants.PIPE_DELIMITER)	
					.append(offersPromotionId).append(DaaSConstants.PIPE_DELIMITER)			
					.append(offersOverride).append(DaaSConstants.PIPE_DELIMITER)
					.append(offersApplied).append(DaaSConstants.PIPE_DELIMITER)
					.append(offersofferId).append(DaaSConstants.PIPE_DELIMITER)
					.append(customerId).append(DaaSConstants.PIPE_DELIMITER)
					.append(terrPymtMethCd).append(DaaSConstants.PIPE_DELIMITER)
					.append(transKeyAndAuthCode[0]).append(DaaSConstants.PIPE_DELIMITER)
					.append(transKeyAndAuthCode[1]).append(DaaSConstants.PIPE_DELIMITER)
					.append(terrPrdDlvrMethCd).append(DaaSConstants.PIPE_DELIMITER)					
					.append(terrPosAreaCd).append(DaaSConstants.PIPE_DELIMITER)
					.append(posTrnKindTypCd).append(DaaSConstants.PIPE_DELIMITER)
					.append(totalAmount).append(DaaSConstants.PIPE_DELIMITER)
					.append(orderTax).append(DaaSConstants.PIPE_DELIMITER)
					.append(uniqueFileId);
		
		outputTxt.set(outputStrBuf.toString());
		multipleOutputs.write(TDADriver.TDAUS_Order_Header, NullWritable.get(), outputTxt);
		
	}

	/**
	 * Cleanup method to close multiple outputs
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		multipleOutputs.write(TDADriver.Output_File_Stats, NullWritable.get(), 
		         new Text(TDADriver.TDAUS_Order_Header+"-m-"+partitionID + DaaSConstants.PIPE_DELIMITER + 
		        		 ordHdrRecordCount + DaaSConstants.PIPE_DELIMITER + ordHdrUniqStoreCount + 
		        		 DaaSConstants.PIPE_DELIMITER + uniqueFileId));
		
		multipleOutputs.close();	
		pmixKeyTxt.clear();
		pmixValueTxt.clear();		
		pmixDataMap.clear();
		comboItemLookup.clear();
		primaryItemLookup.clear();		
	}
}
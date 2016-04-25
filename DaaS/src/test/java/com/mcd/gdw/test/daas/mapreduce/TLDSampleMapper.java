package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.Calendar;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.test.daas.driver.GenerateSampleReport;
import com.mcd.gdw.test.daas.driver.GenerateTLDSample;

public class TLDSampleMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final String EATIN_TYPE      = "EATIN";
	private static final String TAKEOUT_TYPE    = "TAKEOUT";
	
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;

	private FileSplit fileSplit = null;
	private String fileName = "";

	private String[] parts = null;

	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private Calendar calDt = Calendar.getInstance();

	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputTextPrefixValue = new StringBuffer();
	private StringBuffer outputTextValue = new StringBuffer();
	
	private StringBuffer componentsValue = new StringBuffer();
	
	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private Element eleTrx; 
	private Element eleOrder;
	
	private Element eleSOSNode;
	private Element eleServiceTime;
	
	private Element eleProduct;
	private Element eleProductNode;
	private Element elePriceTagNodes;
	private Element elePricingNodes;
	private Element elePricing;
	private Element elePricingItem;
	private Element eleComponent;
	private Element eleComponentNode;
	
	private Node nodeText;

	private HashMap<String,String> orderKeys = new HashMap<String,String>();
	private HashMap<String,String> dayPartMap = new HashMap<String,String>();
	
	private String fileSubType;
	private String eventType;
	private String status;
	
	private String terrCd;
	private String posBusnDt;
	private String lgcyLclRfrDefCd;
	private String posOrdKey;
	private String posAreaDs;
	private String posTrnStrtTs;
	private String daypartId;
	private String posTrnTypCd;
	private String posPrdDlvrMethCd;
	private String posTotNetTrnAm;
	private String posTotNprdNetTrnAm;
	private String posTotTaxAm;
	private String posTotNprdTaxAm;

	private int posTrnItmSeqNu;
	private String posItmLvlNu;
	private String sldMenuItmId;
	private String posPrdTypCd;
	private String posItmActnCd;
	private String posItmTotQt;
	private String posItmGrllQt;
	private String posItmPrmoQt;
	private String posItmNetUntPrcB4PrmoAm;
	private String posItmTaxB4PrmoAm;
	private String posItmNetUntPrcB4DiscAm;
	private String posItmTaxB4DiscAm;
	private String posItmActUntPrcAm;
	private String posItmActTaxAm;
	private String posItmVoidQt;
	
	private String productionNodeId;
	private String untilPay;
	private String untilServe;
	private String totalTime;
	private String holdTime;
	
	private String statusCode;
	private String productClass;
	private String productCode;
	private String componentProuctCode;
	private String componentDefaultQuantity;
	private String priceCode;
	private String eatInPrice;
	private String takeOutPrice;

	private Element[] eleIteme = new Element[3];
	private StringBuffer[] outputItemValue = new StringBuffer[3];
	private StringBuffer itmList = new StringBuffer();
	private int[] idxItem = new int[3];
	private boolean[] isValueMeal = new boolean[3];
	
	@Override
	public void setup(Context context) {
	      
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;

	    fileSplit = (FileSplit) context.getInputSplit();
	    fileName = fileSplit.getPath().getName();

        try {
        	for ( int idx=0; idx < outputItemValue.length; idx++ ) {
        		outputItemValue[idx] = new StringBuffer();
        	}
        	
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();

			if ( fileName.contains("STLD") ) {
				distPaths = context.getCacheFiles();
				
			    if (distPaths == null){
			    	System.err.println("distpath is null");
			    	System.exit(8);
			    }
			      
			    if ( distPaths != null && distPaths.length > 0 )  {
			    	  
			    	System.out.println(" number of distcache files : " + distPaths.length);
			    	  
			    	for ( int i=0; i<distPaths.length; i++ ) {
					     
				    	  System.out.println("distpaths:" + distPaths[i].toString());
				    	  
				    	  distPathParts = 	distPaths[i].toString().split("#");
				    	  
				    	  if( distPaths[i].toString().contains(GenerateTLDSample.DIST_CACHE_DAYPART) ) {
				    		  		      	  
				    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
					      	  addDaypartKeyValuestoMap(br);
					      	  System.out.println("Loaded Daypart Values Map");
					      }
				      }
			      }
				
			}
			
        } catch (Exception ex) {
			ex.printStackTrace(System.err);
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
						
					if ( timeSegment.equalsIgnoreCase("QUARTER HOURLY") ) {
						dayPartKey = terrCd + GenerateSampleReport.SEPARATOR_CHARACTER + dayOfWeek + GenerateSampleReport.SEPARATOR_CHARACTER + startTime.substring(0, 2) + startTime.substring(3, 5);
							
						dayPartMap.put(dayPartKey,daypartId+GenerateSampleReport.SEPARATOR_CHARACTER+daypartDs);
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
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		try {
			
			parts = value.toString().split("\t");

			fileSubType = parts[DaaSConstants.XML_REC_FILE_TYPE_POS];

			terrCd = String.format("%03d", Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS]));
			posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8); 
			lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
			
			//strReader  = new StringReader(parts[DaaSConstants.XML_REC_XML_TEXT_POS]);
			//xmlSource = new InputSource(strReader);
			//doc = docBuilder.parse(xmlSource);

			try {
				strReader  = new StringReader(parts[DaaSConstants.XML_REC_XML_TEXT_POS]);
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			} catch (Exception ex1) {
				strReader  = new StringReader(parts[DaaSConstants.XML_REC_XML_TEXT_POS].replaceAll("&#x1F" , "_"));
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			}
			
			outputKey.setLength(0);						
			outputKey.append(terrCd);
			outputKey.append(posBusnDt);
			outputKey.append(lgcyLclRfrDefCd);

			mapKey.clear();
			mapKey.set(outputKey.toString());

			eleRoot = (Element) doc.getFirstChild();

			context.getCounter("DaaS","1) STLD Count").increment(0);
			context.getCounter("DaaS","2) DetailedSOS Count").increment(0);
			context.getCounter("DaaS","3) Product-Db Count").increment(0);

			if ( fileSubType.equals("STLD") && eleRoot.getNodeName().equals("TLD") ) {
				orderKeys.clear();

				context.getCounter("DaaS","1) STLD Count").increment(1);

				processSTLD(eleRoot.getChildNodes(),context);
				
			} else if ( fileSubType.equals("DetailedSOS") && eleRoot.getNodeName().equals("DetailedSOS") ) {
				context.getCounter("DaaS","2) DetailedSOS Count").increment(1);
				
				processDetailedSos(eleRoot.getChildNodes(),context);
			
			} else if ( fileSubType.equals("Product-Db") && eleRoot.getNodeName().equals("ProductDb") ) {
				context.getCounter("DaaS","3) Product-Db Count").increment(1);
				
				processProductDb(eleRoot.getChildNodes(),context);
			}
			
		} catch(Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	
	private void processSTLD(NodeList nlNode
			                ,Context context) {
		
		if (nlNode != null && nlNode.getLength() > 0 ) {
			for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
				if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {  
					eleNode = (Element)nlNode.item(idxNode);
					if ( eleNode.getNodeName().equals("Node") ) {
						processEvent(eleNode.getChildNodes(),context);
					}
				}
			}
		}
		
	}

	private void processEvent(NodeList nlEvent
            ,Context context) {

		if (nlEvent != null && nlEvent.getLength() > 0 ) {
			for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++ ) {
				if ( nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE ) {  
					eleEvent = (Element)nlEvent.item(idxEvent);
					
					if ( eleEvent.getNodeName().equals("Event") ) {
						eventType = getValue(eleEvent,"Type");
						
 						if ( eventType.equals("TRX_Sale") || eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring") || eventType.equals("TRX_Waste") ) {
 							processTrx(eleEvent.getChildNodes(),context);
 						}
					}
				}
			}
		}

	}

	private void processTrx(NodeList nlTrxSale
                           ,Context context) {

		if (nlTrxSale != null && nlTrxSale.getLength() > 0 ) {
			for (int idxTrxSale=0; idxTrxSale < nlTrxSale.getLength(); idxTrxSale++ ) {
				if ( nlTrxSale.item(idxTrxSale).getNodeType() == Node.ELEMENT_NODE ) {
					eleTrx = (Element)nlTrxSale.item(idxTrxSale);
					status = getValue(eleTrx,"status");
					posAreaDs = getValue(eleTrx,"POD");
		
					if ( (eventType.equals("TRX_Sale") && status.equals("Paid")) || !eventType.equals("TRX_Sale") ) {
						processOrder(eleTrx.getChildNodes(),context);
					}
				}
			}
		}

	}

	private void processOrder(NodeList nlOrder
                             ,Context context) {

		if (nlOrder != null && nlOrder.getLength() > 0 ) {
			for (int idxOrder=0; idxOrder < nlOrder.getLength(); idxOrder++ ) {
				if ( nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {
					eleOrder = (Element)nlOrder.item(idxOrder);
	
					posOrdKey = getValue(eleOrder,"key");
					posTrnStrtTs = formatAsTs(getValue(eleOrder,"Timestamp"));
					
					if ( !orderKeys.containsKey(eventType + posOrdKey) && posTrnStrtTs.length() > 0 ) {
						orderKeys.put(eventType + posOrdKey, "");
						
						daypartId = getDaypartIdFromTs(terrCd,posTrnStrtTs);
						posTrnTypCd = getValue(eleOrder,"kind");
						posPrdDlvrMethCd = getValue(eleOrder,"saleType");
						posTotNetTrnAm = getValue(eleOrder,"totalAmount");
						posTotNprdNetTrnAm = getValue(eleOrder,"nonProductAmount");
						posTotTaxAm = getValue(eleOrder,"totalTax");
						posTotNprdTaxAm = getValue(eleOrder,"nonProductTax");

						outputTextPrefixValue.setLength(0);
						outputTextPrefixValue.append(GenerateTLDSample.REC_TRN);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(eventType.substring(4, 5));
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posOrdKey);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posAreaDs);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posTrnStrtTs);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(daypartId);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posTrnTypCd);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posPrdDlvrMethCd);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posTotNetTrnAm);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posTotNprdNetTrnAm);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posTotTaxAm);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
						outputTextPrefixValue.append(posTotNprdTaxAm);
						outputTextPrefixValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);

						posTrnItmSeqNu = 0;

						context.getCounter("DaaS","4) Order Count").increment(1);
						
						//processItem(eleOrder.getChildNodes(),"",0,context);
						processItem(eleOrder.getChildNodes(),"",0,0,context);
						
					}

				}
			}
		}

	}
	
	private void processItem(NodeList nlItem
			                ,String parentSldMnuItm
			                ,int parentSeqNum
			                ,int level
			                ,Context context) {

		try {
			if (nlItem != null && nlItem.getLength() > 0 ) {
				for (idxItem[level]=0; idxItem[level] < nlItem.getLength(); idxItem[level]++ ) {
					if ( nlItem.item(idxItem[level]).getNodeType() == Node.ELEMENT_NODE ) {
						eleIteme[level] = (Element)nlItem.item(idxItem[level]);
						if ( eleIteme[level].getNodeName().equals("Item") ) {
							posItmVoidQt = String.valueOf(getIntValue(getValue(eleIteme[level],"qtyVoided")));

							if ( posItmVoidQt.equals("0") ) {

								posItmLvlNu = getValue(eleIteme[level],"level");
								sldMenuItmId = getValue(eleIteme[level],"code");
		
								posPrdTypCd = getValue(eleIteme[level],"type");
								posItmActnCd = getValue(eleIteme[level],"action");
								posItmTotQt = String.valueOf(getIntValue(getValue(eleIteme[level],"qty")));
								posItmGrllQt = String.valueOf(getIntValue(getValue(eleIteme[level],"grillQty")));
								posItmPrmoQt = String.valueOf(getIntValue(getValue(eleIteme[level],"qtyPromo")));
								posItmNetUntPrcB4PrmoAm = getValue(eleIteme[level],"BPPrice");
								posItmTaxB4PrmoAm = getValue(eleIteme[level],"BPTax");
								posItmNetUntPrcB4DiscAm = getValue(eleIteme[level],"BDPrice");
								posItmTaxB4DiscAm = getValue(eleIteme[level],"BDTax");
								posItmActUntPrcAm = getValue(eleIteme[level],"totalPrice");
								posItmActTaxAm = getValue(eleIteme[level],"totalTax");

								if ( level == 0 ) {
									itmList.setLength(0);
									
									if ( posPrdTypCd.equals("VALUE_MEAL") ) {
										isValueMeal[level] = true;
									} else {
										isValueMeal[level] = false;
									}
										
								} else {
									isValueMeal[level] = false;
									
									itmList.append(sldMenuItmId);
									itmList.append("|");
								}
			
								outputItemValue[level].setLength(0);
								outputItemValue[level].append(outputTextPrefixValue);
			
								posTrnItmSeqNu+=10;
								outputItemValue[level].append(String.valueOf(posTrnItmSeqNu));          
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
			
								outputItemValue[level].append(posItmLvlNu);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(sldMenuItmId);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(parentSldMnuItm); 
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(parentSeqNum); 
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posPrdTypCd);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER); 
								outputItemValue[level].append(posItmActnCd);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posItmTotQt);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posItmGrllQt);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER); 
								outputItemValue[level].append(posItmPrmoQt);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posItmNetUntPrcB4PrmoAm);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posItmTaxB4PrmoAm);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posItmNetUntPrcB4DiscAm);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posItmTaxB4DiscAm);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posItmActUntPrcAm);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputItemValue[level].append(posItmActTaxAm);
								outputItemValue[level].append(GenerateTLDSample.SEPARATOR_CHARACTER);

								if ( (level+1) < eleIteme.length ) {
									processItem(eleIteme[level].getChildNodes(),sldMenuItmId,posTrnItmSeqNu,level+1,context);
								}
								
								if ( isValueMeal[level] ) {
									outputItemValue[level].append(itmList);
								}
								
								mapValue.clear();
								mapValue.set(outputItemValue[level].toString());
								context.write(mapKey, mapValue);	

								context.getCounter("DaaS","5) Item Row Count").increment(1);
							}
						}
					}
				}
			}
	
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	/*
	private String processItem(NodeList nlItem
                              ,String parentSldMnuItm
                              ,int parentSeqNum
                              ,Context context) {

		Element eleItem;
		String outLine;
		String retList = "";

		try {
			if (nlItem != null && nlItem.getLength() > 0 ) {
				for (int idxItem=0; idxItem < nlItem.getLength(); idxItem++ ) {
					if ( nlItem.item(idxItem).getNodeType() == Node.ELEMENT_NODE ) {
						eleItem = (Element)nlItem.item(idxItem);
						if ( eleItem.getNodeName().equals("Item") ) {

							posItmLvlNu = getValue(eleItem,"level");
							sldMenuItmId = getValue(eleItem,"code");
							
							retList += sldMenuItmId + "|";
							
							posPrdTypCd = getValue(eleItem,"type");
							posItmActnCd = getValue(eleItem,"action");
							posItmTotQt = String.valueOf(getIntValue(getValue(eleItem,"qty")));
							posItmGrllQt = String.valueOf(getIntValue(getValue(eleItem,"grillQty")));
							posItmPrmoQt = String.valueOf(getIntValue(getValue(eleItem,"qtyPromo")));
							posItmNetUntPrcB4PrmoAm = getValue(eleItem,"BPPrice");
							posItmTaxB4PrmoAm = getValue(eleItem,"BPTax");
							posItmNetUntPrcB4DiscAm = getValue(eleItem,"BDPrice");
							posItmTaxB4DiscAm = getValue(eleItem,"BDTax");
							posItmActUntPrcAm = getValue(eleItem,"totalPrice");
							posItmActTaxAm = getValue(eleItem,"totalTax");
							posItmVoidQt = String.valueOf(getIntValue(getValue(eleItem,"qtyVoided")));

							if ( posItmVoidQt.equals("0") ) {
								
								outputTextValue.setLength(0);
								outputTextValue.append(outputTextPrefixValue);
								
								posTrnItmSeqNu+=10;
								outputTextValue.append(String.valueOf(posTrnItmSeqNu));          
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								
								outputTextValue.append(posItmLvlNu);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(sldMenuItmId);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(parentSldMnuItm); 
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posPrdTypCd);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER); 
								outputTextValue.append(posItmActnCd);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posItmTotQt);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posItmGrllQt);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER); 
								outputTextValue.append(posItmPrmoQt);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posItmNetUntPrcB4PrmoAm);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posItmTaxB4PrmoAm);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posItmNetUntPrcB4DiscAm);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posItmTaxB4DiscAm);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posItmActUntPrcAm);
								outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
								outputTextValue.append(posItmActTaxAm);

								outLine = outputTextValue.toString();
								
								retList = processItem(eleItem.getChildNodes(),sldMenuItmId,posTrnItmSeqNu,context);
								
								mapValue.clear();
								mapValue.set(outLine);
								context.write(mapKey, mapValue);	

								context.getCounter("DaaS","5) Item Row Count").increment(1);
							}
							
						}
					}
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
		return(retList);
		
	}
	*/
	
	private void processDetailedSos(NodeList nlSOSNodes
                                   ,Context context) {

		if (nlSOSNodes != null && nlSOSNodes.getLength() > 0 ) {
			for (int idxSOSNodes=0; idxSOSNodes < nlSOSNodes.getLength(); idxSOSNodes++ ) {
				if ( nlSOSNodes.item(idxSOSNodes).getNodeType() == Node.ELEMENT_NODE ) {  
					eleSOSNode = (Element)nlSOSNodes.item(idxSOSNodes);
					if ( eleSOSNode.getNodeName().equals("StoreTotals") ) {
						productionNodeId = getValue(eleSOSNode,"productionNodeId");
						
						if ( productionNodeId.equals("DT") || productionNodeId.equals("FC") ) {
							processServiceTime(eleSOSNode.getChildNodes(),context);
							
							
						}
					}
				}
			}
		}

	}
	
	private void processServiceTime(NodeList nlServiceTime
                                   ,Context context) {

		try {
			if (nlServiceTime != null && nlServiceTime.getLength() > 0 ) {
				for (int idxServiceTime=0; idxServiceTime < nlServiceTime.getLength(); idxServiceTime++ ) {
					if ( nlServiceTime.item(idxServiceTime).getNodeType() == Node.ELEMENT_NODE ) {  
						eleServiceTime = (Element)nlServiceTime.item(idxServiceTime);
						
						if ( eleServiceTime.getNodeName().equals("ServiceTime") ) {
							posOrdKey = getValue(eleServiceTime,"orderKey");
							
							untilPay = getValue(eleServiceTime,"untilPay");
							untilServe = getValue(eleServiceTime,"untilServe");
							
							try {
								totalTime = String.valueOf(Math.round(Double.parseDouble(untilPay) / 1000));
							} catch (Exception ex) {
								totalTime = "0";
							}

							try {
								holdTime = String.valueOf(Math.round((Double.parseDouble(untilServe) - Double.parseDouble(untilPay)) / 1000));
							} catch (Exception ex) {
								holdTime = "0";
							}
							
							outputTextValue.setLength(0);
							outputTextValue.append(GenerateTLDSample.REC_SOS);
							outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdKey);
							outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
							outputTextValue.append(totalTime);          
							outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
							outputTextValue.append(holdTime);
							
							mapValue.clear();
							mapValue.set(outputTextValue.toString());
							context.write(mapKey, mapValue);	
	
							context.getCounter("DaaS","6) Timings Row Count").increment(1);
						}
					}
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}
	
	private void processProductDb(NodeList nlProduct
                                 ,Context context) {

		if (nlProduct != null && nlProduct.getLength() > 0 ) {
			for (int idxProduct=0; idxProduct < nlProduct.getLength(); idxProduct++ ) {
				if ( nlProduct.item(idxProduct).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProduct = (Element)nlProduct.item(idxProduct);
					if ( eleProduct.getNodeName().equals("Product") ) {
						
						statusCode = getValue(eleProduct,"statusCode");
						productClass = getValue(eleProduct,"productClass");
						
						if ( statusCode.equalsIgnoreCase("ACTIVE") && (productClass.equalsIgnoreCase("VALUE_MEAL") || productClass.equalsIgnoreCase("PRODUCT")) ) {

							processProduct(eleProduct.getChildNodes(),productClass.equalsIgnoreCase("VALUE_MEAL"),context);
							
						}
					}
				}
			}
		}

	}

	private void processProduct(NodeList nlProductNodes
			                   ,boolean isValueMeal
                               ,Context context) {


		try {
			componentsValue.setLength(0);
			eatInPrice = "";
			takeOutPrice = "";
	
			if (nlProductNodes != null && nlProductNodes.getLength() > 0 ) {
				for (int idxProductNodes=0; idxProductNodes < nlProductNodes.getLength(); idxProductNodes++ ) {
					if ( nlProductNodes.item(idxProductNodes).getNodeType() == Node.ELEMENT_NODE ) {  
						eleProductNode = (Element)nlProductNodes.item(idxProductNodes);
						
						if ( eleProductNode.getNodeName().equals("ProductCode") ) {
							nodeText = eleProductNode.getFirstChild();
							
							if ( nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
								productCode = nodeText.getNodeValue();
							}
							
						} else if ( eleProductNode.getNodeName().equals("PriceList") ) {
							processPriceList(eleProductNode.getChildNodes());
							
						} else if (  isValueMeal && eleProductNode.getNodeName().equals("Composition") ) {
							processCompents(eleProductNode.getChildNodes());
						}
					}
				}
			}
	
			if ( productCode.length() > 0 && ((eatInPrice.length() > 0 && takeOutPrice.length() > 0) || componentsValue.length() > 0) ) {
				outputTextValue.setLength(0);
				outputTextValue.append(GenerateTLDSample.REC_PRD);
				outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
				outputTextValue.append(productCode);
				outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
				outputTextValue.append(eatInPrice);
				outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
				outputTextValue.append(takeOutPrice);
				outputTextValue.append(GenerateTLDSample.SEPARATOR_CHARACTER);
				outputTextValue.append(componentsValue);          
				
				mapValue.clear();
				mapValue.set(outputTextValue.toString());
				context.write(mapKey, mapValue);	
	
				context.getCounter("DaaS","7) Value Meal Products Row Count").increment(1);
				
			}
		
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void processPriceList(NodeList nlPriceTagNodes) {

		if (nlPriceTagNodes != null && nlPriceTagNodes.getLength() > 0 ) {
			for (int idxPriceTagNodes=0; idxPriceTagNodes < nlPriceTagNodes.getLength(); idxPriceTagNodes++ ) {
				if ( nlPriceTagNodes.item(idxPriceTagNodes).getNodeType () == Node.ELEMENT_NODE ) {  
					elePriceTagNodes = (Element)nlPriceTagNodes.item(idxPriceTagNodes);

					if ( elePriceTagNodes.getNodeName().equals("PriceTag") ) {
						processPricingNodes(elePriceTagNodes.getChildNodes());
					}
				}
			}
		}

	}

	private void processPricingNodes(NodeList nlPricingNodes) {

		if (nlPricingNodes != null && nlPricingNodes.getLength() > 0 ) {
			for (int idxPricingNodes=0; idxPricingNodes < nlPricingNodes.getLength(); idxPricingNodes++ ) {
				if ( nlPricingNodes.item(idxPricingNodes).getNodeType () == Node.ELEMENT_NODE ) {  
					elePricingNodes = (Element)nlPricingNodes.item(idxPricingNodes);

					if ( elePricingNodes.getNodeName().equals("Pricing") ) {
						priceCode = getValue(elePricingNodes,"priceCode");
					
						if ( priceCode.equalsIgnoreCase(EATIN_TYPE) || priceCode.equalsIgnoreCase(TAKEOUT_TYPE) ) {
							processPricingItems(elePricingNodes.getChildNodes());
						}
					}
				}
			}
		}

	}

	private void processPricingItems(NodeList nlPricingItems) {

		if (nlPricingItems != null && nlPricingItems.getLength() > 0 ) {
			for (int idxPricingItems=0; idxPricingItems < nlPricingItems.getLength(); idxPricingItems++ ) {
				if ( nlPricingItems.item(idxPricingItems).getNodeType () == Node.ELEMENT_NODE ) {  
					elePricingItem = (Element)nlPricingItems.item(idxPricingItems);

					if ( elePricingItem.getNodeName().equals("Price") ) {
						nodeText = elePricingItem.getFirstChild();
						
						if ( nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							if ( priceCode.equalsIgnoreCase(EATIN_TYPE) ) {
								eatInPrice = nodeText.getNodeValue();
							} else if ( priceCode.equalsIgnoreCase(TAKEOUT_TYPE) ) {
								takeOutPrice = nodeText.getNodeValue();
							}
						}
					}
				}
			}
		}

	}

	private void processCompents(NodeList nlComponents) {

		if (nlComponents != null && nlComponents.getLength() > 0 ) {
			for (int idxComponents=0; idxComponents < nlComponents.getLength(); idxComponents++ ) {
				if ( nlComponents.item(idxComponents).getNodeType () == Node.ELEMENT_NODE ) {  
					eleComponent = (Element)nlComponents.item(idxComponents);

					if ( eleComponent.getNodeName().equals("Component") ) {
						processCompentNodes(eleComponent.getChildNodes());
					}
				}
			}
		}

	}
	
	private void processCompentNodes(NodeList nlComponentNodes) {

		componentProuctCode = "";
		componentDefaultQuantity = "";
		
		if (nlComponentNodes != null && nlComponentNodes.getLength() > 0 ) {
			for (int idxComponentNodes=0; idxComponentNodes < nlComponentNodes.getLength(); idxComponentNodes++ ) {
				if ( nlComponentNodes.item(idxComponentNodes).getNodeType () == Node.ELEMENT_NODE ) {  
					eleComponentNode = (Element)nlComponentNodes.item(idxComponentNodes);

					if ( eleComponentNode.getNodeName().equals("ProductCode") ) {
						nodeText = eleComponentNode.getFirstChild();
						
						if ( nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							componentProuctCode = nodeText.getNodeValue();
						}
						
					} else if ( eleComponentNode.getNodeName().equals("DefaultQuantity") ) {
						nodeText = eleComponentNode.getFirstChild();
						
						if ( nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							componentDefaultQuantity = nodeText.getNodeValue();
						}
						
					}
				}
			}
		}
		
		if ( componentProuctCode.length() > 0 && componentDefaultQuantity.length() > 0 ) {
			if ( componentsValue.length() > 0 ) {
				componentsValue.append(GenerateTLDSample.COMPONENTS_SPEARATOR_CHARACTER);
			}
			componentsValue.append(componentProuctCode);
			componentsValue.append(GenerateTLDSample.COMPONENT_SPEARATOR_CHARACTER);
			componentsValue.append(componentDefaultQuantity);
		}

	}
	
	private String getDaypartIdFromTs(String terrCd
			                         ,String ts) {
	
		String retId = "";
		String daypartKey = "";
		int minute;
		String minuteText = "";
		
		calDt.set(Integer.parseInt(ts.substring(0, 4)), Integer.parseInt(ts.substring(5, 7))-1, Integer.parseInt(ts.substring(8, 10)));
		
		minute = Integer.parseInt(ts.substring(14, 16));
		
		if ( minute >= 00 && minute <= 14 ) {
			minuteText = "00";
		} else if ( minute >= 15 && minute <= 29 ) {
			minuteText = "15";
		} else if ( minute >= 30 && minute <= 44 ) {
			minuteText = "30";
		} else if ( minute >= 45 && minute <= 59 ) {
			minuteText = "45";
		}
		
		daypartKey = terrCd + GenerateSampleReport.SEPARATOR_CHARACTER + calDt.get(Calendar.DAY_OF_WEEK) + GenerateSampleReport.SEPARATOR_CHARACTER + ts.substring(11, 13) + minuteText; 
		
		if ( dayPartMap.containsKey(daypartKey) ) {
			retId = dayPartMap.get(daypartKey).substring(0, 1);
		} else { 
			retId = "-1:";
		}
		
		return(retId);
		
	}
	
	private String formatAsTs(String in) {
		
		String retTs = "";
		
		if ( in.length() >= 14 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8) + " " + in.substring(8, 10) + ":" + in.substring(10, 12) + ":" + in.substring(12, 14);
		}

		return(retTs);
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
	
	private int getIntValue(String valueIn) {

		int retValue = 0;

		try {
			retValue = Integer.parseInt(valueIn);
		} catch (Exception ex) {
			retValue = 0;
		}

		return(retValue);
	}
}

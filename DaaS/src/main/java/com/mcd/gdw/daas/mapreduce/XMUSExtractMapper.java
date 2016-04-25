package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.GenerateXMUSExtract;

public class XMUSExtractMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static final String SEPARATOR_CHARACTER = "\t";
	
	private FileSplit fileSplit = null;
	private String fileName = "";

	private String[] parts = null;
	private Text mapValue = new Text();

	private Calendar calDt = Calendar.getInstance();
	private Calendar begCalDt = Calendar.getInstance();
	private Calendar endCalDt = Calendar.getInstance();
	
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;
	
	private MultipleOutputs<NullWritable, Text> mos;
	
	private StringBuffer orderValue = new StringBuffer();
	private StringBuffer orderDetailPrefixValue = new StringBuffer();
	private StringBuffer orderDetailValue = new StringBuffer();
	private StringBuffer timingsValue = new StringBuffer();
	private StringBuffer menuValue = new StringBuffer();
	
	private String terrCd;
	private String posBusnDt;
	private String lgcyLclRfrDefCd;
	private String posAreaTypShrtDs;
	private String posOrdKey;
	private String posTrnStrtTs;
	private String posTrnTypCd;
	private String posPrdDlvrMethCd;
	private String posTotNetTrnAm;
	private String posTotNprdNetTrnAm;
	private String posTrnItmSeqNu;
	private String posItmLvlNu;
	private String sldMenuItmId;
	private String posPrdTypCd;
	private String posItmActnCd;
	private String posItmTotQt;
	private String posItmGrllQt;
	private String posItmGrllModCd;
	private String posItmPrmoQt;
	private String posItmActUntPrcAm;
	private String productionNodeId;
	private String checkPoint;
	private String segmentId;
	private String tc;
	private String itemsCount;
	private String cars;
	private String untilTotal;
	private String untilStore;
	private String untilRecall;
	private String untilCloseDrawer;
	private String untilPay;
	private String untilServe;
	private String totalTime;
	private String totalAmount;
	private String creationDate;
	private String id;
	private String longName;
	private String familyGroup;
	private String grillGroup;
	private String eatinPrice;
	private String showOnMain;
	private String showOnMFY;
	private String doNotDecompVM;
	private String showOnSummary;
	private String grillable;
	private String dontPrintGrill;
	private int lineNum;
	
	private HashMap<String,String> includeListMap = new HashMap<String,String>();
	private boolean includeListHist = false;

	private HashMap<String,String> orders = new HashMap<String,String>();

	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private	Element eleTrxSale; 
	private	Element eleOrder;
	private Element eleStoreTotals;
	private Element eleServiceTime;
	private Element eleProductInfo;
	private Element eleProduction;							
	
	private int dateDiff;
	private Calendar wkdt = Calendar.getInstance();
	private SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");


	@Override
	public void setup(Context context) {
	      
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;

        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();

        //System.out.println("File Name = " + fileName);
        
		mos = new MultipleOutputs<NullWritable, Text>(context);

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
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
			    	  
			    	  if( distPaths[i].toString().contains(GenerateXMUSExtract.LOCATION_INCLUDE_LIST) || distPaths[i].toString().contains(GenerateXMUSExtract.LOCATION_HIST_INCLUDE_LIST) ) {
			    		  		      	  
			    		  if ( distPaths[i].toString().contains(GenerateXMUSExtract.LOCATION_HIST_INCLUDE_LIST) ) {
				    		  includeListHist = true; 
			    		  }
			    		  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addIncludeListToMap(br);
				      	  System.out.println("Loaded Include List Values Map");
    	  		      	  
				      }
			      }
		      }
			
		} catch (Exception ex) {
			System.err.println("Error in initializing XMUSExtractMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
		
	}
	
	private void addIncludeListToMap(BufferedReader br) {
		
		String line = null;
		String[] lineParts;
		
		//String terrCd;
		//String lgcyLclRfrDefCd;
		String includeListKey;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					lineParts = line.split("\t", -1);

					//terrCd          = parts[0];
					//lgcyLclRfrDefCd = parts[1];

					includeListKey = lineParts[0] + SEPARATOR_CHARACTER + lineParts[1]; 
					
					if ( !includeListMap.containsKey(includeListKey) ) {
						if ( includeListHist ) {
							includeListMap.put(includeListKey,lineParts[2] + SEPARATOR_CHARACTER + lineParts[3]);
						} else {
							includeListMap.put(includeListKey,"1");
						}
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

		boolean includeFile = false;
		String includeListKey;
	    String[] fromToDates;
		
		try {
			if ( fileName.toUpperCase().contains("STLD") || fileName.toUpperCase().contains("DETAILEDSOS") || fileName.toUpperCase().contains("MENUITEM") ) {
				parts = value.toString().split("\t");
				
				if ( parts.length >= 8 ) {
					includeFile = true;
				}
			}
			
			if ( includeFile ) {
					
				includeListKey = parts[DaaSConstants.XML_REC_TERR_CD_POS] + SEPARATOR_CHARACTER + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
				
				if ( !includeListMap.containsKey(includeListKey) ) {
					includeFile = false;
				}
				
				if ( includeFile && includeListHist ) {
					fromToDates = includeListMap.get(includeListKey).split(SEPARATOR_CHARACTER);
					calDt.set(Integer.parseInt(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4)), Integer.parseInt(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6))-1, Integer.parseInt(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8)));
					begCalDt.set(Integer.parseInt(fromToDates[0].substring(0, 4)), Integer.parseInt(fromToDates[0].substring(4, 6))-1, Integer.parseInt(fromToDates[0].substring(6, 8)));
					endCalDt.set(Integer.parseInt(fromToDates[1].substring(0, 4)), Integer.parseInt(fromToDates[1].substring(4, 6))-1, Integer.parseInt(fromToDates[1].substring(6, 8)));
					
					if ( (calDt.after(begCalDt) || calDt.equals(begCalDt)) && (calDt.before(endCalDt) || calDt.equals(endCalDt)) ) {
						includeFile = true;
					} else {
						includeFile = false;
					}
				}
			}
			
			if ( includeFile ) {
				terrCd = parts[DaaSConstants.XML_REC_TERR_CD_POS];
				posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8);
				lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
				
				if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
			    	getOrderDataStld(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
					context.getCounter("DaaS","1) XML File Counts: STLD").increment(1);

				} else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("DETAILEDSOS") ) {
					getTimingDataDetailedSos(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
					context.getCounter("DaaS","1) XML File Counts: DetailedSOS").increment(1);

				} else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("MENUITEM") ) {
					getMenuDataMenuItem(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
					context.getCounter("DaaS","1) XML File Counts: MenuItem").increment(1);

				} else {
					context.getCounter("DaaS","1) XML File Counts: Error Type: " + parts[DaaSConstants.XML_REC_FILE_TYPE_POS]).increment(1);
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in XMUSExtractMapper.Map:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
	
	private void getOrderDataStld(String xmlText
			                     ,Context context) {
		
		try {
			try {
				strReader  = new StringReader(xmlText);
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			} catch (Exception ex1) {
				strReader  = new StringReader(xmlText.replaceAll("&#x1F" , "_"));
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			}

			orders.clear();
			
			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("TLD") ) {
				processNode(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in XMUSExtractMapper.getOrderData:");
			ex.printStackTrace(System.err);
			System.exit(8); 
		}
		
	}

	private void processNode(NodeList nlNode
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
					if ( eleEvent.getNodeName().equals("Event") &&
					     eleEvent.getAttribute("Type").equals("TRX_Sale") ) {
						processTrxSale(eleEvent.getChildNodes(),context);
					}
				}
			}
		}
		
	}
	
	private void processTrxSale(NodeList nlTrxSale
			                   ,Context context) {
		
		if (nlTrxSale != null && nlTrxSale.getLength() > 0 ) {
			for (int idxTrxSale=0; idxTrxSale < nlTrxSale.getLength(); idxTrxSale++ ) {
				if ( nlTrxSale.item(idxTrxSale).getNodeType() == Node.ELEMENT_NODE ) {
					eleTrxSale = (Element)nlTrxSale.item(idxTrxSale);

					if ( eleTrxSale.getAttribute("status").equals("Paid") ) {
						posAreaTypShrtDs = eleTrxSale.getAttribute("POD");
						
						processOrder(eleTrxSale.getChildNodes(),context);
					}
				}
			}
		}

	}

	private void processOrder(NodeList nlOrder
			                 ,Context context) {
		
		try {
			if (nlOrder != null && nlOrder.getLength() > 0 ) {
				for (int idxOrder=0; idxOrder < nlOrder.getLength(); idxOrder++ ) {
					if ( nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {
						eleOrder = (Element)nlOrder.item(idxOrder);
						
						posOrdKey = getValue(eleOrder,"key");
						posTrnStrtTs = 	eleOrder.getAttribute("Timestamp");
						
						if ( !orders.containsKey(posOrdKey) && eleOrder.getAttribute("Timestamp").length() >= 14 ) {
							orders.put(posOrdKey, "1");
							
							posTrnStrtTs = 	posTrnStrtTs.substring(0,4) + "-" + 
									        posTrnStrtTs.substring(4,6) + "-" +
									        posTrnStrtTs.substring(6,8) + " " +
									        posTrnStrtTs.substring(8,10) + ":" +
									        posTrnStrtTs.substring(10,12) + ":" +
									        posTrnStrtTs.substring(12,14);
						
							posTrnTypCd = getValue(eleOrder,"kind");
							posPrdDlvrMethCd = getValue(eleOrder,"saleType");
							posTotNetTrnAm = getValue(eleOrder,"totalAmount");
							posTotNprdNetTrnAm = getValue(eleOrder,"nonProductAmount");

							orderValue.setLength(0);
							orderValue.append(lgcyLclRfrDefCd);
							orderValue.append(SEPARATOR_CHARACTER);
							orderValue.append(posBusnDt + " 00:00:00");
							orderValue.append(SEPARATOR_CHARACTER);
							orderValue.append(posOrdKey);
							orderValue.append(SEPARATOR_CHARACTER);
							orderValue.append(posTrnStrtTs);
							orderValue.append(SEPARATOR_CHARACTER);
							orderValue.append(posAreaTypShrtDs);
							orderValue.append(SEPARATOR_CHARACTER);
							orderValue.append(posPrdDlvrMethCd);
							orderValue.append(SEPARATOR_CHARACTER);
							orderValue.append(posTrnTypCd);
							orderValue.append(SEPARATOR_CHARACTER);
							orderValue.append(posTotNetTrnAm);
							orderValue.append(SEPARATOR_CHARACTER);
							orderValue.append(posTotNprdNetTrnAm);
						
							context.getCounter("DaaS","2) STLD File Details: Order Count").increment(1);

							wkdt.set(Integer.parseInt(posBusnDt.substring(0, 4)), Integer.parseInt(posBusnDt.substring(5,7))-1, Integer.parseInt(posBusnDt.substring(8,10)));
							
							switch (wkdt.get(Calendar.DAY_OF_WEEK)) {
								case Calendar.SUNDAY:    dateDiff = 4;
								   	                     break;
								   	                     
								case Calendar.MONDAY:    dateDiff = 3;
			   	                                         break;
							
								case Calendar.TUESDAY:   dateDiff = 2;
                                                         break;
                           							
								case Calendar.WEDNESDAY: dateDiff = 1;
                                                         break;
                                							
   								case Calendar.THURSDAY:  dateDiff = 0;
                                                         break;
                             							
								case Calendar.FRIDAY:    dateDiff = 6;
                                                         break;
                              							
 								case Calendar.SATURDAY:  dateDiff = 5;
                                                         break;
                                
                                default:                 dateDiff = 0;
							}
							
							wkdt.add(Calendar.DATE, dateDiff);
							
							orderDetailPrefixValue.setLength(0);
							orderDetailPrefixValue.append(dtFmt.format(wkdt.getTime()));
							orderDetailPrefixValue.append(SEPARATOR_CHARACTER);
							orderDetailPrefixValue.append(posBusnDt + " 00:00:00");
							orderDetailPrefixValue.append(SEPARATOR_CHARACTER);
							orderDetailPrefixValue.append(lgcyLclRfrDefCd);
							orderDetailPrefixValue.append(SEPARATOR_CHARACTER);
							orderDetailPrefixValue.append(posOrdKey);
							orderDetailPrefixValue.append(SEPARATOR_CHARACTER);
							orderDetailPrefixValue.append(posTrnTypCd);
							orderDetailPrefixValue.append(SEPARATOR_CHARACTER);
							orderDetailPrefixValue.append(posPrdDlvrMethCd);
							orderDetailPrefixValue.append(SEPARATOR_CHARACTER);
							orderDetailPrefixValue.append(posAreaTypShrtDs);
							orderDetailPrefixValue.append(SEPARATOR_CHARACTER);
							orderDetailPrefixValue.append(posTrnStrtTs);
							orderDetailPrefixValue.append(SEPARATOR_CHARACTER);
							orderDetailPrefixValue.append(posTotNetTrnAm);

						
							processItem(eleOrder.getChildNodes(),context);
						
							mapValue.clear();
							mapValue.set(orderValue.toString());
							mos.write(NullWritable.get(), mapValue, GenerateXMUSExtract.ORDER_FILE_PREFIX);	
						}
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in XMUSExtractMapper.processOrder:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void processItem(NodeList nlItem
			                ,Context context) {
		
		Element eleItem;
		@SuppressWarnings("unused")
		int qty;
		
		try {
			if (nlItem != null && nlItem.getLength() > 0 ) {
				for (int idxItem=0; idxItem < nlItem.getLength(); idxItem++ ) {
					if ( nlItem.item(idxItem).getNodeType() == Node.ELEMENT_NODE ) {
						eleItem = (Element)nlItem.item(idxItem);
						if ( eleItem.getNodeName().equals("Item") ) {

							posTrnItmSeqNu = getValue(eleItem,"id");
							posItmLvlNu = getValue(eleItem,"level");
							sldMenuItmId = getValue(eleItem,"code");
							posPrdTypCd = getValue(eleItem,"type");
							posItmActnCd = getValue(eleItem,"action");
							posItmTotQt = getValue(eleItem,"qty");
							posItmGrllQt = getValue(eleItem,"grillQty");
							posItmGrllModCd = getValue(eleItem,"grillModifer");
							posItmPrmoQt = getValue(eleItem,"qtyPromo");
							posItmActUntPrcAm = getValue(eleItem,"totalPrice");

							try {
								qty = Integer.parseInt(posItmTotQt);
							} catch (Exception ex) {
								posItmTotQt = "0";
							}

							try {
								qty = Integer.parseInt(posItmGrllQt);
							} catch (Exception ex) {
								posItmGrllQt = "0";
							}

							try {
								qty = Integer.parseInt(posItmGrllModCd);
							} catch (Exception ex) {
								posItmGrllModCd = "0";
							}

							try {
								qty = Integer.parseInt(posItmPrmoQt);
							} catch (Exception ex) {
								posItmPrmoQt = "0";
							}

							orderDetailValue.setLength(0);
							orderDetailValue.append(orderDetailPrefixValue);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posItmLvlNu);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posPrdTypCd);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posItmActnCd);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(sldMenuItmId);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posTrnItmSeqNu);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posItmTotQt);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posItmPrmoQt);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posItmGrllQt);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posItmGrllModCd);
							orderDetailValue.append(SEPARATOR_CHARACTER);
							orderDetailValue.append(posItmActUntPrcAm);
							
							mapValue.clear();
							mapValue.set(orderDetailValue.toString());
							mos.write(NullWritable.get(), mapValue, GenerateXMUSExtract.ORDER_DETAIL_FILE_PREFIX);
							
							context.getCounter("DaaS","2) STLD File Details: Order Detail Count").increment(1);

							processItem(eleItem.getChildNodes(),context);
							
						}
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in XMUSExtractMapper.processItem:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	private void getTimingDataDetailedSos(String xmlText
			                             ,Context context) {

		try {
			try {
				strReader  = new StringReader(xmlText);
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			} catch (Exception ex1) {
				strReader  = new StringReader(xmlText.replaceAll("&#x1F" , "_"));
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			}

			lineNum = 0;
			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("DetailedSOS") ) {
				processStoreTotals(eleRoot.getChildNodes(),context);

			}
		} catch (Exception ex) {
			System.err.println("Error occured in XMUSExtractMapper.getTimingDataDetailedSos:");
			ex.printStackTrace(System.err);
			System.exit(8); 
		}
		
	}

	private void processStoreTotals(NodeList nlStoreTotals
			                       ,Context context) {

		try {
			if (nlStoreTotals != null && nlStoreTotals.getLength() > 0 ) {
				for (int idxStoreTotals=0; idxStoreTotals < nlStoreTotals.getLength(); idxStoreTotals++ ) {
					if ( nlStoreTotals.item(idxStoreTotals).getNodeType() == Node.ELEMENT_NODE ) {  
						eleStoreTotals = (Element)nlStoreTotals.item(idxStoreTotals);
						if ( eleStoreTotals.getNodeName().equals("StoreTotals") ) {

							lineNum++;
							productionNodeId = getValue(eleStoreTotals,"productionNodeId");
							checkPoint = getValue(eleStoreTotals,"checkPoint");
							
							processServiceTime(eleStoreTotals.getChildNodes());

							timingsValue.setLength(0);
							timingsValue.append(lineNum);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(terrCd);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(lgcyLclRfrDefCd);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(posBusnDt);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(posOrdKey);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(segmentId);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(productionNodeId);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(tc);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(itemsCount);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(cars);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(untilTotal);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(untilStore);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(untilRecall);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(untilCloseDrawer);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(untilPay);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(untilServe);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(totalTime);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(totalAmount);
							timingsValue.append(SEPARATOR_CHARACTER);
							timingsValue.append(checkPoint);
							
							mapValue.clear();
							mapValue.set(timingsValue.toString());
							mos.write(NullWritable.get(), mapValue, GenerateXMUSExtract.TIMINGS_FILE_PREFIX);
							
							context.getCounter("DaaS","2) DetailedSOS File Details: Timings Count").increment(1);

						}
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in XMUSExtractMapper.processStoreTotals:");
			ex.printStackTrace(System.err);
			System.exit(8); 
		}
		
	}

	private void processServiceTime(NodeList nlServiceTime) {
		
		if (nlServiceTime != null && nlServiceTime.getLength() > 0 ) {
			for (int idxServiceTime=0; idxServiceTime < nlServiceTime.getLength(); idxServiceTime++ ) {
				if ( nlServiceTime.item(idxServiceTime).getNodeType() == Node.ELEMENT_NODE ) {  
					eleServiceTime = (Element)nlServiceTime.item(idxServiceTime);
					if ( eleServiceTime.getNodeName().equals("ServiceTime") ) {
						posOrdKey = getValue(eleServiceTime,"orderKey");
						segmentId = getValue(eleServiceTime,"segmentId");
						tc = getValue(eleServiceTime,"tc","0");
						itemsCount = getValue(eleServiceTime,"itemsCount","0");
						cars = getValue(eleServiceTime,"cars","0");
						untilTotal = getValue(eleServiceTime,"untilTotal","0");
						untilStore = getValue(eleServiceTime,"untilStore","0");
						untilRecall = getValue(eleServiceTime,"untilRecall","0");
						untilCloseDrawer = getValue(eleServiceTime,"untilCloseDrawer","0");
						untilPay = getValue(eleServiceTime,"untilPay","0");
						untilRecall = getValue(eleServiceTime,"untilRecall","0");
						untilServe = getValue(eleServiceTime,"untilServe","0");
						totalTime = getValue(eleServiceTime,"totalTime","0");
						totalAmount = getValue(eleServiceTime,"totalAmount","0");
					}
				}
			}
		}
		
	}

	private void getMenuDataMenuItem(String xmlText
			                        ,Context context) {

		try {
			try {
				strReader  = new StringReader(xmlText);
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			} catch (Exception ex1) {
				strReader  = new StringReader(xmlText.replaceAll("&#x1F" , "_"));
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			}

			eleRoot = (Element) doc.getFirstChild();
			
			if ( eleRoot.getNodeName().equals("MenuItem") ) {
				creationDate = eleRoot.getAttribute("creationDate");
				creationDate = creationDate.substring(0, 4) + "-" + 
						       creationDate.substring(4, 6) + "-" +
				               creationDate.substring(6, 8);
						       
				processProductInfo(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in XMUSExtractMapper.getTimingDataDetailedSos:");
			ex.printStackTrace(System.err);
			System.exit(8); 
		}
		
	}	

	private void processProductInfo(NodeList nlProductInfo
			                       ,Context context) {

		try {
			if (nlProductInfo != null && nlProductInfo.getLength() > 0 ) {
				for (int idxProductInfo=0; idxProductInfo < nlProductInfo.getLength(); idxProductInfo++ ) {
					if ( nlProductInfo.item(idxProductInfo).getNodeType() == Node.ELEMENT_NODE ) {  
						eleProductInfo = (Element)nlProductInfo.item(idxProductInfo);
						if ( eleProductInfo.getNodeName().equals("ProductInfo") ) {

							id = getValue(eleProductInfo,"id");
							longName = getValue(eleProductInfo,"longName");
							familyGroup = getValue(eleProductInfo,"familyGroup");
							grillGroup = getValue(eleProductInfo,"grillGroup");
							eatinPrice = getValue(eleProductInfo,"eatinPrice");
							
							processProduction(eleProductInfo.getChildNodes());

							menuValue.setLength(0);
							menuValue.append(lgcyLclRfrDefCd);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(terrCd);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(posBusnDt + " 00:00:00");
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(creationDate + " 00:00:00");
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(id);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(longName);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(familyGroup);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(grillGroup);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(eatinPrice);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(showOnMain);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(showOnMFY);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(doNotDecompVM);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(showOnSummary);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(grillable);
							menuValue.append(SEPARATOR_CHARACTER);
							menuValue.append(dontPrintGrill);
							
							mapValue.clear();
							mapValue.set(menuValue.toString());
							mos.write(NullWritable.get(), mapValue, GenerateXMUSExtract.MENU_FILE_PREFIX);

							context.getCounter("DaaS","2) MenuItem File Details: Menu Item Count").increment(1);
						}
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in XMUSExtractMapper.processStoreTotals:");
			ex.printStackTrace(System.err);
			System.exit(8); 
		}
		
	}

	private void processProduction(NodeList nlProduction) {
		
		if (nlProduction != null && nlProduction.getLength() > 0 ) {
			for (int idxProduction=0; idxProduction < nlProduction.getLength(); idxProduction++ ) {
				if ( nlProduction.item(idxProduction).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProduction = (Element)nlProduction.item(idxProduction);
					if ( eleProduction.getNodeName().equals("Production") ) {
						showOnMain = getValue(eleProduction,"showOnMain");
						showOnMFY = getValue(eleProduction,"showOnMFY");
						doNotDecompVM = getValue(eleProduction,"doNotDecompVM");
						showOnSummary = getValue(eleProduction,"showOnSummary");
						grillable = getValue(eleProduction,"grillable");
						dontPrintGrill = getValue(eleProduction,"dontPrintGrill");
					}
				}
			}
		}
		
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
	
	private String getValue(Element ele
                           ,String attribute
                           ,String substute) {

		String retValue = "";

		try {
			retValue = ele.getAttribute(attribute);

		} catch (Exception ex) {
		}

		if ( retValue.trim().length() == 0 ) {
			retValue = substute;
		}
		
		return(retValue.trim());
	}
}

package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URI;
//import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.Path;
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

public class TLDFormatMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private class TenderEntry {
		
		public BigDecimal tenderAmount;
		public String cardNu;
		public String cardXpirDt;
		public String cardAthzCd;
		public String cardAthzAm;		
	}
	
	private final static String REC_TLD_ITM = "ITM";
	private final static String REC_MENU    = "MNU";
	private final static String SEPARATOR_CHARACTER = "\t";
	private final static BigDecimal NEGATIVE_ONE = new BigDecimal("-1.00");
	private final static BigDecimal ZERO = new BigDecimal("0.00");
	
	private static String[] parts = null;
	private static String[] parts2 = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;

	private static FileSplit fileSplit = null;
	private static String fileName = "";

	boolean includeFile = false;
	
	private Calendar cal = Calendar.getInstance();
	private Calendar cal2 = Calendar.getInstance();
//	private SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd");

	private HashMap<String,String> dayPartMap = new HashMap<String,String>();
//	private HashMap<String,String> countryMap = new HashMap<String,String>();
	private HashMap<String,TenderEntry> tenderMap = new HashMap<String,TenderEntry>();

	private static StringBuffer outputKey = new StringBuffer();
	private static StringBuffer outputTextValue = new StringBuffer();
	
	private String infoName;
	private String infoValue;

	private BigDecimal maxTenderValue;
	
	private String terrCd;
	private String lgcyLclRfrDefCd;
	private String posBusnDt;
	private String posDvceId;
	private String posOrdKeyId;
	private int posTrnItmSeqNu;
	private String posOrdDt;
	private String posOrdTm;
	private String posAreaTypShrtDs;
	private String posTrnTypCd;
	private String posPrdDlvrMethCd;
	private String posMfySideCd;
	private String posTotNetTrnAm;
	private String posTotNprdNetTrnAm;
	private String posTotTaxAm;
	private String posTotNprdTaxAm;
	private String posTotItmQt;
	private String pymtMethDs;
	private String mobileOrdFl;
	private String custId;
	private String paidByMobileApplFl;
	private String dyptIdNu;
	private String posItmLvlNu;
	private String sldMenuItmId;
	private String posPrdTypCd;
	private String posItmActnCd;
	private String posItmTotQt;
	private String posItmVoidQt;
	private String posItmGrllQt;
	private String posItmGrllModCd;
	private String posItmPrmoQt;
	private String posItmNetUntPrcB4PrmoAm;
	private String posItmTaxB4PrmoAm;
	private String posItmNetUntPrcB4DiscAm;
	private String posItmTaxB4DiscAm;
	private String posItmActUntPrcAm;
	private String posItmActTaxAm;
	private String cardNu;
	private String cardXpirDt;
	private String cardAthzCd;
	private String cardAthzAm;
	
	private String sldMenuItmDs; 
	
	@Override
	public void setup(Context context) {
	      
		//Path[] distPaths;
		URI[] distPaths;
	    //Path distpath = null;
	    BufferedReader br = null;

        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
		    //distPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		    distPaths = context.getCacheFiles();
		    
		    
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		      
		    if ( distPaths != null && distPaths.length > 0 )  {
		    	  
		    	System.out.println(" number of distcache files : " + distPaths.length);
		    	  
		    	for ( int i=0; i<distPaths.length; i++ ) {
			    	  
			    	  //distpath = new Path(distPaths[i].getPath());
				     
			    	  System.out.println("distpaths:" + distPaths[i].toString());
			    	  
			    	  if( distPaths[i].toString().contains("DayPart_ID.psv") ) {
				    	  String[] distPathParts = 	distPaths[i].toString().split("#");
				    	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	  addDaypartKeyValuestoMap(br);
				      	  System.out.println("Loaded Daypart Values Map");
				      /*
			    	  } else if ( distpath.toUri().toString().contains("country.txt")  ) {

				    	  br  = new BufferedReader(new FileReader(distPaths[i].toString())); 
				    	  addCountryKeyValuestoMap(br);
				      	  System.out.println("Loaded Country Map");
				    	*/
				      }
			      }
		      }
			
		} catch (Exception ex) {
			System.err.println("Error in initializing TLDFormatMapper:");
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
		//String daypartDs;
		
		String timeSegment;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\|", -1);

					terrCd = parts[0];
					dayOfWeek   = parts[1];
					timeSegment = parts[2];
					startTime   = parts[3];

					daypartId   = parts[5];
					//daypartDs   = parts[6];
						
					if ( timeSegment.equalsIgnoreCase("QUARTER HOURLY") ) {
						dayPartKey = terrCd + SEPARATOR_CHARACTER + dayOfWeek + SEPARATOR_CHARACTER + startTime.substring(0, 2) + startTime.substring(3, 5);
							
						dayPartMap.put(dayPartKey,daypartId);
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
	
	/*
	private void addCountryKeyValuestoMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String ctryIsoNu;
		String ctryIso3AbbrCd;
		String ctryIso2AbbrCd;
		String ctryNa;
		String lgcyCtryRfrNu;
		String lgcyLcatRfrDefNa;
		String ctryShrtNa;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\t", -1);

					if ( parts.length >= 7 ) {
						ctryIsoNu        = parts[0];
						ctryIso3AbbrCd   = parts[1];
						ctryIso2AbbrCd   = parts[2];
						ctryNa           = parts[3];
						lgcyCtryRfrNu    = parts[4];
						lgcyLcatRfrDefNa = parts[5];
						ctryShrtNa       = parts[6];
						
						countryMap.put(ctryIsoNu,ctryIso3AbbrCd + SEPARATOR_CHARACTER + ctryIso2AbbrCd + SEPARATOR_CHARACTER + ctryNa + SEPARATOR_CHARACTER + lgcyCtryRfrNu + SEPARATOR_CHARACTER + lgcyLcatRfrDefNa + SEPARATOR_CHARACTER + ctryShrtNa);
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
    */
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		includeFile = false;
		
		try {
			if ( fileName.toUpperCase().contains("STLD") || fileName.toUpperCase().contains("MENUITEM") ) {
				parts = value.toString().split("\t");
				
				if ( parts.length >= 8 ) {
					includeFile = true;
				}
			}
			
			if ( includeFile ) {
				try {
					terrCd = String.format("%03d", Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS]));
				} catch (Exception ex) {
					terrCd = "000";
				}

				/*
				if ( countryMap.containsKey(terrCd) ) {
					parts2 = countryMap.get(terrCd).split("\\|",-1);
					ctryNa = parts2[5];
				} else {
					ctryNa = "UNKNOWN";
				}
				*/

				posBusnDt = formatDateAsIsoDate(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
				lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];

				if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
			    	getOrderDataStld(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);

				} else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("MENUITEM") ) {
					getMenuData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.Map:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void getOrderDataStld(String xmlText
                                 ,Context context) {


		Element eleRoot;

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

			if ( eleRoot.getNodeName().equals("TLD") ) {
				processNode(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.getOrderData:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void processNode(NodeList nlNode
                            ,Context context) {

		Element eleNode;

		if (nlNode != null && nlNode.getLength() > 0 ) {
			for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
				if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {  
					eleNode = (Element)nlNode.item(idxNode);
					if ( eleNode.getNodeName().equals("Node") ) {
						posDvceId = eleNode.getAttribute("id");
		
						processEvent(eleNode.getChildNodes(),context);
					}
				}
			}
		}

	}

	private void processEvent(NodeList nlEvent
                             ,Context context) {

		Element eleEvent;

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

		Element eleTrxSale; 

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

		Element eleOrder;

		try {
			if (nlOrder != null && nlOrder.getLength() > 0 ) {
				for (int idxOrder=0; idxOrder < nlOrder.getLength(); idxOrder++ ) {
					if ( nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {
						eleOrder = (Element)nlOrder.item(idxOrder);
						
						posOrdKeyId = getValue(eleOrder,"key");
						posOrdDt = formatDateAsIsoDate(eleOrder.getAttribute("Timestamp"));
						
						posTrnItmSeqNu = 0;
						
						if ( posOrdDt.length() > 0 ) {
							posOrdTm = formatTimestampAsTime(eleOrder.getAttribute("Timestamp"));
							
							getDaypart();
			
							posTrnTypCd = getValue(eleOrder,"kind");
							posMfySideCd = getValue(eleOrder,"side");
							posPrdDlvrMethCd = getValue(eleOrder,"saleType");
							posTotNetTrnAm = getValue(eleOrder,"totalAmount");
							posTotNprdNetTrnAm = getValue(eleOrder,"nonProductAmount");
							posTotTaxAm = getValue(eleOrder,"totalTax");
							posTotNprdTaxAm = getValue(eleOrder,"nonProductTax");

							outputKey.setLength(0);
							outputKey.append(REC_TLD_ITM);
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(lgcyLclRfrDefCd);
							
							mapKey.clear();
							mapKey.set(outputKey.toString());

							processOrderItems(eleOrder.getChildNodes(),context);

							processItem(eleOrder.getChildNodes(),context);
						}
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.processOrder:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void processOrderItems(NodeList nlOrderItems
                                  ,Context context) {

		Element eleOrderItems;

		posTotItmQt = "";
		mobileOrdFl = "0";
		custId = "";
		paidByMobileApplFl = "0";
		pymtMethDs = "";
		cardNu = "";
		cardXpirDt = "";
		cardAthzCd = "";
		cardAthzAm = "";

		try {
			if (nlOrderItems != null && nlOrderItems.getLength() > 0 ) {
				for (int idxOrderItems=0; idxOrderItems < nlOrderItems.getLength(); idxOrderItems++ ) {
					if ( nlOrderItems.item(idxOrderItems).getNodeType() == Node.ELEMENT_NODE ) {
						eleOrderItems = (Element)nlOrderItems.item(idxOrderItems);
						
						posTotItmQt = getValue(eleOrderItems,"itemsCount");
		
						if ( eleOrderItems.getNodeName().equals("CustomInfo") ) {
					        processCustomInfo(eleOrderItems.getChildNodes());
						}

						if ( eleOrderItems.getNodeName().equals("Tenders") ) {
							processTenders(eleOrderItems.getChildNodes());
						}

					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.processOrderItems:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void processCustomInfo(NodeList nlCustomInfo) {

		Element eleCustomInfo;

		mobileOrdFl = "1";

		try {
			if (nlCustomInfo != null && nlCustomInfo.getLength() > 0 ) {
				for (int idxCustomInfo=0; idxCustomInfo < nlCustomInfo.getLength(); idxCustomInfo++ ) {
					if ( nlCustomInfo.item(idxCustomInfo).getNodeType() == Node.ELEMENT_NODE ) {
						eleCustomInfo = (Element)nlCustomInfo.item(idxCustomInfo);
						
						if ( eleCustomInfo.getNodeName().equals("Info") ) {
							infoName = getValue(eleCustomInfo,"name");
							infoValue = getValue(eleCustomInfo,"value");
							
							if ( infoName.equals("customerId") ) {
								custId = infoValue;
							} else if ( infoName.equals("isPaidMobileOrder") ) {
								if ( infoValue.equalsIgnoreCase("TRUE") || infoValue.equalsIgnoreCase("0") ) {
									paidByMobileApplFl = "1";
								} 
							}
						}
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.processCustomInfo:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	private void processTenders(NodeList nlTenders) {

		Element eleTenders;

		maxTenderValue = ZERO;

		tenderMap.clear();

		try {
			if (nlTenders != null && nlTenders.getLength() > 0 ) {
				for (int idxTenders=0; idxTenders < nlTenders.getLength(); idxTenders++ ) {
					if ( nlTenders.item(idxTenders).getNodeType() == Node.ELEMENT_NODE ) {
						eleTenders = (Element)nlTenders.item(idxTenders);
						
						if ( eleTenders.getNodeName().equals("Tender") ) {
							processTender(eleTenders.getChildNodes());
						}
					}
				}
			}

			for (Map.Entry<String, TenderEntry> entry : tenderMap.entrySet()) {
				if ( entry.getValue().tenderAmount.compareTo(maxTenderValue) > 0 ) {
					pymtMethDs = entry.getKey();
					cardNu = entry.getValue().cardNu;
					cardXpirDt = entry.getValue().cardXpirDt;
					cardAthzCd = entry.getValue().cardAthzCd;
					cardAthzAm = entry.getValue().cardAthzAm;
					maxTenderValue = entry.getValue().tenderAmount;
				}
			}

			
		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.processTenders:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void processTender(NodeList nlTenderItem) {
		
		Element eleTenderItem;
		Node nodeText;
		
		String tenderKind = "";
		String tenderName = "";
		String tenderCardNu = "";
		String tenderCardXpirDt = "";
		String tenderCardAthzCd = "";
		String tenderCardAthzAm = "";
		String tenderCardProviderId = "";
		BigDecimal tenderValue = new BigDecimal("0.00");
		
		try {
			if (nlTenderItem != null && nlTenderItem.getLength() > 0 ) {
				for (int idxTenderItem=0; idxTenderItem < nlTenderItem.getLength(); idxTenderItem++ ) {
					if ( nlTenderItem.item(idxTenderItem).getNodeType() == Node.ELEMENT_NODE ) {

						eleTenderItem = (Element)nlTenderItem.item(idxTenderItem);
						nodeText = eleTenderItem.getFirstChild();
						
						if ( eleTenderItem.getNodeName().equals("TenderKind") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							tenderKind = nodeText.getNodeValue();
							
						} else if ( eleTenderItem.getNodeName().equals("TenderName") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							tenderName = nodeText.getNodeValue();
							
						} else if ( eleTenderItem.getNodeName().equals("CardProviderID") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							tenderCardProviderId = nodeText.getNodeValue().trim();
							
						} else if ( eleTenderItem.getNodeName().equals("TenderAmount") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							try {
								tenderValue = new BigDecimal(nodeText.getNodeValue());
								
							} catch (Exception ex1 ) {
							}
							
						} else if ( eleTenderItem.getNodeName().equals("CashlessData") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							parts2 = nodeText.getNodeValue().split("\\|");
							
							try {
								tenderCardNu = parts2[1];
								tenderCardXpirDt = parts2[2].substring(3, 5) + parts2[2].substring(0,2);
								tenderCardAthzCd = parts2[3];
								tenderCardAthzAm = parts2[10];
							} catch (Exception ex) {
								
							}
						}
					}
					
					
				}
				
				if ( tenderCardProviderId.length() > 0 ) {
					tenderName = tenderName + "-" + tenderCardProviderId; 
				} else {
					tenderCardNu = "";
					tenderCardXpirDt = "";
					tenderCardAthzCd = "";
					tenderCardAthzAm = "";
				}
				
				if ( tenderKind.equals("4") ) {
					tenderValue = tenderValue.multiply(NEGATIVE_ONE);
				}
				
				if ( tenderMap.containsKey(tenderName) ) {
					tenderValue = tenderMap.get(tenderName).tenderAmount.add(tenderValue); 
				}
				
				TenderEntry entryItem = new TenderEntry();
				entryItem.tenderAmount = tenderValue;
				entryItem.cardNu = tenderCardNu;
				entryItem.cardXpirDt = tenderCardXpirDt;
				entryItem.cardAthzCd = tenderCardAthzCd;
				entryItem.cardAthzAm = tenderCardAthzAm;
				
				tenderMap.put(tenderName, entryItem);
				
			}	
			
		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.processTender:");
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

							posTrnItmSeqNu++; // = getValue(eleItem,"id");
							sldMenuItmId = getValue(eleItem,"code");
							posItmLvlNu = getValue(eleItem,"level");
							sldMenuItmId = getValue(eleItem,"code");
							posPrdTypCd = getValue(eleItem,"type");
							posItmActnCd = getValue(eleItem,"action");
							posItmTotQt = getValue(eleItem,"qty");
							posItmGrllQt = getValue(eleItem,"grillQty");
							posItmGrllModCd = getValue(eleItem,"grillModifer");
							posItmPrmoQt = getValue(eleItem,"qtyPromo");
							posItmNetUntPrcB4PrmoAm = getValue(eleItem,"BPPrice");
							posItmTaxB4PrmoAm = getValue(eleItem,"BPTax");
							posItmNetUntPrcB4DiscAm = getValue(eleItem,"BDPrice");
							posItmTaxB4DiscAm = getValue(eleItem,"BDTax");
							posItmActUntPrcAm = getValue(eleItem,"totalPrice");
							posItmActTaxAm = getValue(eleItem,"totalTax");
							posItmVoidQt = getValue(eleItem,"qtyVoided");

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
								qty = Integer.parseInt(posItmPrmoQt);
							} catch (Exception ex) {
								posItmPrmoQt = "0";
							}
							
							outputTextValue.setLength(0);
							outputTextValue.append(posDvceId);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdKeyId);
							outputTextValue.append(SEPARATOR_CHARACTER);
						    outputTextValue.append(posTrnItmSeqNu);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdDt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdTm);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posAreaTypShrtDs); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTrnTypCd);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posPrdDlvrMethCd);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posMfySideCd); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTotNetTrnAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTotNprdNetTrnAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTotTaxAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTotNprdTaxAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTotItmQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(pymtMethDs); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(cardNu); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(cardXpirDt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(cardAthzCd); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(cardAthzAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(mobileOrdFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(custId);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(paidByMobileApplFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(dyptIdNu);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmLvlNu); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(sldMenuItmId); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posPrdTypCd); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmActnCd); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmTotQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmVoidQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmGrllQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmGrllModCd); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmPrmoQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmNetUntPrcB4PrmoAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmTaxB4PrmoAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmNetUntPrcB4DiscAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmTaxB4DiscAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmActUntPrcAm); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmActTaxAm);
							
							mapValue.clear();
							mapValue.set(outputTextValue.toString());

							context.write(mapKey, mapValue);

							processItem(eleItem.getChildNodes(),context);
			
						}
					}
				}
			}

		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.processItem:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void getMenuData(String xmlText
                            ,Context context) {

		Element eleRoot;
			
		try {
		
			try {
				strReader  = new StringReader(xmlText);
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			} catch (Exception ex1) {
				strReader  = new StringReader(xmlText.replaceAll("&" , "&amp;"));
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			}

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("MenuItem") ) {
				processProductInfo(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.getMenuData:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void processProductInfo(NodeList nlProductInfo
                                   ,Context context) {

		Element eleProductInfo;

		try {
			if (nlProductInfo != null && nlProductInfo.getLength() > 0 ) {
				for (int idxProductInfo=0; idxProductInfo < nlProductInfo.getLength(); idxProductInfo++ ) {
					if ( nlProductInfo.item(idxProductInfo).getNodeType() == Node.ELEMENT_NODE ) {
						eleProductInfo = (Element)nlProductInfo.item(idxProductInfo);
						if ( eleProductInfo.getNodeName().equals("ProductInfo") ) {

							sldMenuItmId = getValue(eleProductInfo,"id");
							sldMenuItmDs = getValue(eleProductInfo,"longName");

							outputKey.setLength(0);
							outputKey.append(REC_MENU);
							outputKey.append(terrCd);
							outputKey.append(sldMenuItmId);
							outputKey.append(SEPARATOR_CHARACTER);
							outputKey.append(sldMenuItmDs.replaceAll("&#38;", "&"));

							mapKey.clear();
							mapKey.set(outputKey.toString());

							mapValue.clear();
							mapValue.set("1"
									    );
							context.write(mapKey, mapValue);

						}
					}
				}
			}

		} catch (Exception ex) {
			System.err.println("Error occured in TLDFormatMapper.processProductInfo:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void getDaypart() {
		
		String dayPartKey;
		String hour;
		String minute;
		int minuteInt;
//		String[] parts;
		
		dyptIdNu = "";
//		dyptDs = "";
//		dyOfCalWkDs = "";
		
		cal.set(Integer.parseInt(posOrdDt.substring(0, 4)), Integer.parseInt(posOrdDt.substring(5, 7))-1, Integer.parseInt(posOrdDt.substring(8, 10)));
		
		hour = posOrdTm.substring(0, 2);
//		hrIdNu = String.valueOf(hour);
		minuteInt = Integer.parseInt(posOrdTm.substring(3, 5));
		
		if ( minuteInt < 15 ) {
			minute = "00";
		} else if ( minuteInt < 30 ) {
			minute = "15";
		} else if ( minuteInt < 45 ) {
			minute = "30";
		} else {
			minute = "45";
		}

		/*
		switch ( cal.get(Calendar.DAY_OF_WEEK) ) {
			case 1: 
				dyOfCalWkDs = "Sunday";
			    break;
			
			case 2: 
				dyOfCalWkDs = "Monday";
			    break;
			
			case 3: 
				dyOfCalWkDs = "Tuesday";
			    break;
			
			case 4: 
				dyOfCalWkDs = "Wednesday";
			    break;
			
			case 5: 
				dyOfCalWkDs = "Thursday";
			    break;
			
			case 6: 
				dyOfCalWkDs = "Friday";
			    break;
			
			case 7: 
				dyOfCalWkDs = "Saturday";
			    break;
			
			default: 
				dyOfCalWkDs = "**";
			    break;
		}
        */

		cal2.set(Integer.parseInt(posBusnDt.substring(0, 4)), Integer.parseInt(posBusnDt.substring(5, 7))-1, Integer.parseInt(posBusnDt.substring(8, 10)));
		cal2.add(Calendar.DAY_OF_MONTH, ((cal2.get(Calendar.DAY_OF_WEEK)-1)*-1));

//		weekDs = dtFmt.format(cal2.getTime()) + " (wk " + String.format("%02d", cal2.get(Calendar.WEEK_OF_YEAR)) + ")";
		
		dayPartKey = String.valueOf(terrCd) + SEPARATOR_CHARACTER + String.valueOf(cal.get(Calendar.DAY_OF_WEEK)) + SEPARATOR_CHARACTER + hour + minute;
		
		if ( dayPartMap.containsKey(dayPartKey) ) {
//			parts = dayPartMap.get(dayPartKey).split("\\|",-1);
//			dyptIdNu = parts[0];
//			dyptDs = parts[1];
			dyptIdNu = dayPartMap.get(dayPartKey);
		}
		
	}
	
	private String formatDateAsIsoDate(String in) {
		
		String retIsoDate = "";
		
		if ( in.length() >= 8 ) {
			retIsoDate = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8);
		}

		return(retIsoDate);
		
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

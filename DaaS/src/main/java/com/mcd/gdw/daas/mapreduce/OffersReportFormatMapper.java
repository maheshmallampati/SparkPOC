package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.Calendar;
import java.util.HashMap;

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

public class OffersReportFormatMapper extends Mapper<LongWritable, Text, Text, Text> {

	public class IncludeListKey {
	
		private String terrCd;
		private String lgcyLclRfrDefCd;
		
		public IncludeListKey(String terrCd
				             ,String lgcyLclRfrDefCd) {
			
			this.terrCd = terrCd;
			this.lgcyLclRfrDefCd = lgcyLclRfrDefCd;
			
		}
		
		public String getTerrCd() {
			
			return(this.terrCd);
			
		}
		
		public String getLgcyLclRfrDefCd() {
			
			return(this.lgcyLclRfrDefCd);
			
		}
		
		public String toString() {
			
			return(this.terrCd + "_" + this.lgcyLclRfrDefCd);
			
		}
	}
	
	public class IncludeListDates {
		
		private Calendar fromDt = Calendar.getInstance();
		private Calendar toDt = Calendar.getInstance();
		
		public IncludeListDates(String fromDt
				               ,String toDt) {
			
			String[] parts;
			
			parts = (fromDt+"-1-1").split("-");
			this.fromDt.set(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));

			parts = (toDt+"-1-1").split("-");
			this.toDt.set(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
			
		}
		
		public boolean isIsoDateBetween(String date) {
			
			String[] parts;

			parts = (date+"-1-1").split("-");
			
			return(isDateBetween(parts[0] + parts[1] + parts[2]));
			
		}

		public boolean isDateBetween(String date) {
			
			Calendar dt = Calendar.getInstance();
			boolean isBetween = false;

			dt.set(Integer.parseInt(date.substring(0, 4)), Integer.parseInt(date.substring(4, 6)), Integer.parseInt(date.substring(6, 8)));

			if ( (dt.equals(fromDt) || dt.after(fromDt)) && ( dt.equals(toDt) || dt.before(toDt)) ) {
				isBetween = true;
			}
			return(isBetween);
			
		}
	}
	
	private final static String SEPARATOR_CHARACTER = "\t";
	private final static String REC_POSTRN          = "TRN";
	private final static String REC_POSTRNOFFR      = "OFR";
	private final static String REC_POSTRNITM       = "ITM";
	private final static String REC_POSTRNITMOFFR   = "IOF";
	
	private String[] parts = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;

	private FileSplit fileSplit = null;
	private String fileName = "";
	
	private Calendar cal = Calendar.getInstance();

	private String posBusnDt;
    private String mcdGbalLcatIdNu;
    private String terrCd;
	private String posOrdKey;
	private String posRestId;
	private String posDvceId;
	private String posAreaTypShrtDs;
	private String posTrnStrtTs;
	private String posTrnTypCd;
	private String posMfySideCd;
	private String posPrdDlvrMethCd;
	private String posTotNetTrnAm;
	private String posTotNprdNetTrnAm;
	private String posTotTaxAm;
	private String posTotNprdTaxAm;
	private String posTotItmQt;
	private String posPaidForOrdTs;
	private String posTotKeyPrssTs;
	private String posOrdStrInSysTs;
	private String posOrdUniqId;
	private String posOrdStrtDt;
	private String posOrdStrtTm;
	private String posOrdEndDt;
	private String posOrdEndTm;
	private String offrCustId;
	private String ordOffrApplFl;
	private String dyptIdNu;
	private String dyptDs;
	private String dyOfCalWkDs;
 	private String hrIdNu; 

	private String untlTotKeyPrssScQt;
	private String untlStrInSysScQt;
	private String untlOrdRcllScQt;
	private String untlDrwrClseScQt;
	private String untlPaidScQt;
	private String untlSrvScQt;
	private String totOrdTmScQt;
	private String abovPsntTmTrgtFl;
	private String abovTotTmTrgtFl;
	private String abovTotMfyTrgtTmTmFl;
	private String abovTotFrntCterTrgtTmFl;
	private String abovTotDrvTrgtTmFl;
	private String abov50ScFl;
	private String bel25ScFl;
	private String heldTmScQt;
	private String ordHeldFl;

	private String posTrnItmSeqNu;
	private String posItmLvlNu;
	private String sldMenuItmId;
	private String posPrdTypCd;
	private String posItmActnCd;
	private String posItmTotQt;
	private String posItmGrllQt;
	private String posItmGrllModCd;
	private String posItmPrmoQt;
	private String posChgAftTotCd;
	private String posItmNetUntPrcB4PrmoAm;
	private String posItmTaxB4PrmoAm;
	private String posItmNetUntPrcB4DiscAm;
	private String posItmTaxB4DiscAm;
	private String posItmActUntPrcAm;
	private String posItmActTaxAm;
	private String posItmCatCd;
	private String posItmFmlyGrpCd;
	private String posItmVoidQt;
	private String posItmTaxRateAm;
	private String posItmTaxBasAm;
	private String itmOffrAppdFl;
	
	private boolean itmOfferAppdFoundFl;
	
	private HashMap<String,String> dayPartMap = new HashMap<String,String>();
	private HashMap<String,IncludeListDates> includeListMap = new HashMap<String,IncludeListDates>();
	private HashMap<String,Integer> promoIdMap = new HashMap<String,Integer>();

	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputTextValue = new StringBuffer();
	
	@Override
	public void setup(Context context) {
	      
		URI[] distPaths;
	    //Path distpath = null;
	    BufferedReader br = null;
	    String[] distPathParts;

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
			    	  
			    	  //distpath = distPaths[i];
				     
			    	  System.out.println("distpaths:" + distPaths[i].toString());
			    	  //System.out.println("distpaths URI:" + distPaths[i].toUri());
			    	  
			    	  distPathParts = 	distPaths[i].toString().split("#");
			    	  
			    	  if( distPaths[i].toString().contains("DayPart_ID.psv") ) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	  addDaypartKeyValuestoMap(br);
				      	  System.out.println("Loaded Daypart Values Map");
				      	  
				      } else if ( distPaths[i].toString().contains("offers_include_list.txt") ) {
    	  		      	  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addIncludeListToMap(br);
				      	  System.out.println("Loaded Include List Values Map");
				      }
			      }
		      }
			
		} catch (Exception ex) {
			System.err.println("Error in initializing OffersReportFormatMapper:");
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
	
	private void addIncludeListToMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String terrCd;
		String lgcyLclRfrDefCd;
		String fromDt;
		String toDt;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\t", -1);

					terrCd          = String.format("%03d", Integer.parseInt(parts[0]));
					lgcyLclRfrDefCd = parts[1];
					fromDt          = parts[2];
					toDt            = parts[3];

					includeListMap.put(new IncludeListKey(terrCd,lgcyLclRfrDefCd).toString(),new IncludeListDates(fromDt,toDt));
					
					//System.out.println(terrCd + " " + lgcyLclRfrDefCd + " " + fromDt + " " + toDt );
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
		IncludeListKey includeListKey;
		
		try {
			if ( fileName.toUpperCase().contains("STLD") || fileName.toUpperCase().contains("DETAILEDSOS") ) {
				parts = value.toString().split("\t");
				
				if ( parts.length >= 8 ) {
					includeFile = true;
				}
			}
			
			if ( includeFile ) {
					
				includeListKey = new IncludeListKey(parts[DaaSConstants.XML_REC_TERR_CD_POS],parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]);
				
				if ( includeListMap.containsKey(includeListKey.toString()) ) {
					if ( !includeListMap.get(includeListKey.toString()).isDateBetween(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]) ) {
						includeFile = false;
					}
				} else {
					includeFile = false;
				}
			}
			
			if ( includeFile ) {
				terrCd = String.format("%03d", Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS]));
				posBusnDt = formatDateAsTsDtOnly(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
				mcdGbalLcatIdNu = parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS];

				if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
			    	getOrderDataStld(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);

				} else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("DETAILEDSOS") ) {
					getOrderDataDetailedSos(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.Map:");
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
				posRestId = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");

				processNode(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.getOrderData:");
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
						
						posOrdKey = getValue(eleOrder,"key");
						
						posTrnStrtTs = formatAsTs(eleOrder.getAttribute("Timestamp"));

						getDaypart();
						
						itmOfferAppdFoundFl = false;
						
						posTrnTypCd = getValue(eleOrder,"kind");
						posMfySideCd = getValue(eleOrder,"side");
						posPrdDlvrMethCd = getValue(eleOrder,"saleType");
						posTotNetTrnAm = getValue(eleOrder,"totalAmount");
						posTotNprdNetTrnAm = getValue(eleOrder,"nonProductAmount");
						posTotTaxAm = getValue(eleOrder,"totalTax");
						posTotNprdTaxAm = getValue(eleOrder,"nonProductTax");
						posOrdUniqId = getValue(eleOrder,"uniqueId");
						posOrdStrtDt = formatDateAsTs(eleOrder.getAttribute("startSaleDate"));
						posOrdStrtTm = formatTimeAsTs(eleOrder.getAttribute("startSaleTime"),posOrdStrtDt);
						posOrdEndDt = formatDateAsTs(eleOrder.getAttribute("endSaleDate"));
						posOrdEndTm = formatTimeAsTs(eleOrder.getAttribute("endSaleTime"),posOrdEndDt);

						processOrderItems(eleOrder.getChildNodes(),context);

						context.getCounter("COUNT","POS_TRN_ITM_UNIQUE").increment(1);
						processItem(eleOrder.getChildNodes(),context);

						outputKey.setLength(0);						
						outputKey.append(REC_POSTRN);
						outputKey.append(terrCd);
						outputKey.append(posBusnDt);
						outputKey.append(posOrdKey);
						outputKey.append(SEPARATOR_CHARACTER);
						outputKey.append(mcdGbalLcatIdNu);

						outputTextValue.setLength(0);
						outputTextValue.append("1");
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posRestId);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posDvceId);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posAreaTypShrtDs); 
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posTrnStrtTs);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posTrnTypCd);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posMfySideCd);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posPrdDlvrMethCd);
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
						outputTextValue.append(posPaidForOrdTs);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posTotKeyPrssTs);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posOrdStrInSysTs);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posOrdUniqId);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posOrdStrtDt);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posOrdStrtTm);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posOrdEndDt);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(posOrdEndTm);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(offrCustId);
						outputTextValue.append(SEPARATOR_CHARACTER);

						if ( itmOfferAppdFoundFl ) {
							ordOffrApplFl = "1";
						}
						
						outputTextValue.append(ordOffrApplFl);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(dyptIdNu);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(dyptDs);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(dyOfCalWkDs);
						outputTextValue.append(SEPARATOR_CHARACTER);
						outputTextValue.append(hrIdNu); 
						
				    	mapKey.clear();
						mapKey.set(outputKey.toString());
						mapValue.clear();
						mapValue.set(outputTextValue.toString());
						context.write(mapKey, mapValue);	
						context.getCounter("COUNT","POS_TRN").increment(1);

					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.processOrder:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}
	
	private void processOrderItems(NodeList nlOrderItems
			                      ,Context context) {

		Element eleOrderItems;
		
		posTotItmQt = "";
		posPaidForOrdTs = "";
		posTotKeyPrssTs = "";
		posOrdStrInSysTs = "";
		offrCustId = "";
		ordOffrApplFl = "0";
		
		String offrOverride;
		String offrApplied;
		String tmpOffrCustId = "";
		String promoId = "";

		promoIdMap.clear();
		
		try {
			if (nlOrderItems != null && nlOrderItems.getLength() > 0 ) {
				for (int idxOrderItems=0; idxOrderItems < nlOrderItems.getLength(); idxOrderItems++ ) {
					if ( nlOrderItems.item(idxOrderItems).getNodeType() == Node.ELEMENT_NODE ) {
						eleOrderItems = (Element)nlOrderItems.item(idxOrderItems);
						
						if ( eleOrderItems.getNodeName().equals("Customer") ) {
							offrCustId = getValue(eleOrderItems,"id");
						}
					}
				}
			}

			
			if (nlOrderItems != null && nlOrderItems.getLength() > 0 ) {
				for (int idxOrderItems=0; idxOrderItems < nlOrderItems.getLength(); idxOrderItems++ ) {
					if ( nlOrderItems.item(idxOrderItems).getNodeType() == Node.ELEMENT_NODE ) {
						eleOrderItems = (Element)nlOrderItems.item(idxOrderItems);
						
						if ( eleOrderItems.getNodeName().equals("POSTimings") ) {
							posTotItmQt = getValue(eleOrderItems,"itemsCount");
							posPaidForOrdTs = formatAsTs(eleOrderItems.getAttribute("untilPay"));
							posTotKeyPrssTs = formatAsTs(eleOrderItems.getAttribute("untilTotal"));
							posOrdStrInSysTs = formatAsTs(eleOrderItems.getAttribute("untilStore"));
						}
						if ( eleOrderItems.getNodeName().equals("Offers") ) {
							offrOverride = "0";
							offrApplied = "0";

							if ( !ordOffrApplFl.equals("1") ) {
								tmpOffrCustId = getValue(eleOrderItems,"customerId");
								if ( offrCustId.length() == 0 ) {
									offrCustId = tmpOffrCustId;
								}
								ordOffrApplFl = "1";
							}

							if ( getValue(eleOrderItems,"override").equalsIgnoreCase("TRUE") ) {
								offrOverride = "1";
							}
							
							if ( getValue(eleOrderItems,"applied").equalsIgnoreCase("TRUE") ) {
								offrApplied = "1";
							}
							
							promoId = getValue(eleOrderItems,"promotionId");
							if ( promoId.length() > 0 ) {
								if ( promoIdMap.containsKey(promoId) ) {
									promoIdMap.put(promoId, promoIdMap.get(promoId) + 1);
								} else {
									promoIdMap.put(promoId, 1);
								}
							}

							outputKey.setLength(0);						
							outputKey.append(REC_POSTRNOFFR);
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(posOrdKey);
							outputKey.append(SEPARATOR_CHARACTER);
							outputKey.append(mcdGbalLcatIdNu);

							mapKey.clear();
							mapKey.set(outputKey.toString());
							
							outputTextValue.setLength(0);
							outputTextValue.append(getValue(eleOrderItems,"tagId")); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							
							if ( offrCustId.length() > 0 ) {
								outputTextValue.append(offrCustId);
							} else {
								outputTextValue.append(getValue(eleOrderItems,"customerId"));
							}
							
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(getValue(eleOrderItems,"offerId")); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(offrOverride);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(offrApplied);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(getValue(eleOrderItems,"discountType")); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(getValue(eleOrderItems,"discountAmount")); 

							mapValue.clear();
							mapValue.set(outputTextValue.toString());
							context.write(mapKey, mapValue);	
							context.getCounter("COUNT","POS_TRN_OFFR").increment(1);
							
						}

					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.processOrderItems:");
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
							posChgAftTotCd = getValue(eleItem,"chgAfterTotal");
							posItmNetUntPrcB4PrmoAm = getValue(eleItem,"BPPrice");
							posItmTaxB4PrmoAm = getValue(eleItem,"BPTax");
							posItmNetUntPrcB4DiscAm = getValue(eleItem,"BDPrice");
							posItmTaxB4DiscAm = getValue(eleItem,"BDTax");
							posItmActUntPrcAm = getValue(eleItem,"totalPrice");
							posItmActTaxAm = getValue(eleItem,"totalTax");
							posItmCatCd = getValue(eleItem,"category");
							posItmFmlyGrpCd = getValue(eleItem,"familyGroup");
							posItmVoidQt = getValue(eleItem,"qtyVoided");
							posItmTaxRateAm = getValue(eleItem,"unitPrice");
							posItmTaxBasAm = getValue(eleItem,"unitTax");
							itmOffrAppdFl = "0";

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

							processItemOffers(eleItem.getChildNodes(),context);

							outputKey.setLength(0);						
							outputKey.append(REC_POSTRNITM);
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(posOrdKey);
							outputKey.append(SEPARATOR_CHARACTER);
							outputKey.append(mcdGbalLcatIdNu);
							
							mapKey.clear();
							mapKey.set(outputKey.toString());

							outputTextValue.setLength(0);
							outputTextValue.append("1");
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTrnItmSeqNu);
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
							outputTextValue.append(posItmGrllQt);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posItmGrllModCd);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posItmPrmoQt);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posChgAftTotCd);
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
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posItmCatCd);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posItmFmlyGrpCd);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posItmVoidQt);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posItmTaxRateAm);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posItmTaxBasAm);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posDvceId);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posAreaTypShrtDs);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posTrnStrtTs);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdUniqId);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTrnTypCd);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posMfySideCd);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(posPrdDlvrMethCd);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posTotNetTrnAm);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdStrtDt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdStrtTm);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdEndDt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posOrdEndTm);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(offrCustId);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(ordOffrApplFl);
							outputTextValue.append(SEPARATOR_CHARACTER); 
							outputTextValue.append(dyptIdNu);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(dyptDs);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(dyOfCalWkDs);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(hrIdNu);
 							outputTextValue.append(SEPARATOR_CHARACTER);  
							outputTextValue.append(itmOffrAppdFl);
							
							mapValue.clear();
							mapValue.set(outputTextValue.toString());
							context.write(mapKey, mapValue);
							context.getCounter("COUNT","POS_TRN_ITM").increment(1);

							processItem(eleItem.getChildNodes(),context);
							
						}
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.processItem:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	private void processItemOffers(NodeList nlItemOffers,
			                       Context context) {

		Element eleItemOffers;
		String promoId = "";
		boolean isItemOfferFl = false;
		boolean isPromoAppledNode = false;

		try {
			if (nlItemOffers != null && nlItemOffers.getLength() > 0 ) {
				for (int idxItemOffers=0; idxItemOffers < nlItemOffers.getLength(); idxItemOffers++ ) {
					if ( nlItemOffers.item(idxItemOffers).getNodeType() == Node.ELEMENT_NODE ) {
						eleItemOffers = (Element)nlItemOffers.item(idxItemOffers);
						
						isItemOfferFl = false;
						isPromoAppledNode = false; 
						
						if ( eleItemOffers.getNodeName().equals("PromotionApplied") ) {
							promoId = getValue(eleItemOffers,"promotionId");
							if ( promoId.length() > 0 && promoIdMap.containsKey(promoId) ) {
								isItemOfferFl = true;
								isPromoAppledNode = true;
							}
						} else if ( eleItemOffers.getNodeName().equals("Offers") ) {
							isItemOfferFl = true;
						}
						
						if ( isItemOfferFl ) { 

							itmOffrAppdFl = "1";
							itmOfferAppdFoundFl = true;

							outputKey.setLength(0);						
							outputKey.append(REC_POSTRNITMOFFR);
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(posOrdKey);
							outputKey.append(SEPARATOR_CHARACTER);
							outputKey.append(mcdGbalLcatIdNu);
							
							mapKey.clear();
							mapKey.set(outputKey.toString());

							outputTextValue.setLength(0);
							outputTextValue.append(posTrnItmSeqNu); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(posItmLvlNu); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(getValue(eleItemOffers,"offerId")); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(getValue(eleItemOffers,"discountType")); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(getValue(eleItemOffers,"discountAmount")); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							
							if ( isPromoAppledNode ) {
								outputTextValue.append(getValue(eleItemOffers,"originalPrice"));
							} else {
								outputTextValue.append(getValue(eleItemOffers,"beforeOfferPrice"));
							}
							
							mapValue.clear();
							mapValue.set(outputTextValue.toString());
							context.write(mapKey, mapValue);
							context.getCounter("COUNT","POS_TRN_ITM_OFFR").increment(1);
						}
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.processItemOffers:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void getOrderDataDetailedSos(String xmlText
                                        ,Context context) {


		Element eleRoot;

		try {
			strReader  = new StringReader(xmlText);
			xmlSource = new InputSource(strReader);
			doc = docBuilder.parse(xmlSource);

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("DetailedSOS") ) {
				posRestId = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");

				processStoreTotals(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.getOrderDataDetailedSos:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}
	
	private void processStoreTotals(NodeList nlStoreTotals
                                   ,Context context) {

		Element eleStoreTotals;

		if (nlStoreTotals != null && nlStoreTotals.getLength() > 0 ) {
			for (int idxStoreTotals=0; idxStoreTotals < nlStoreTotals.getLength(); idxStoreTotals++ ) {
				if ( nlStoreTotals.item(idxStoreTotals).getNodeType() == Node.ELEMENT_NODE ) {  
					eleStoreTotals = (Element)nlStoreTotals.item(idxStoreTotals);
					if ( eleStoreTotals.getNodeName().equals("StoreTotals") && (eleStoreTotals.getAttribute("productionNodeId").equalsIgnoreCase("DT") || eleStoreTotals.getAttribute("productionNodeId").equalsIgnoreCase("FC")) ) {

						processServiceTime(eleStoreTotals.getChildNodes(),context);
					}
				}
			}
		}

	}
	
	private void processServiceTime(NodeList nlServiceTime
                                   ,Context context) {

		Element eleServiceTime;

		untlTotKeyPrssScQt = "";
		untlStrInSysScQt = "";
		untlOrdRcllScQt = "";
		untlDrwrClseScQt = "";
		untlPaidScQt = "";
		untlSrvScQt = "";
		totOrdTmScQt = "";
		abovPsntTmTrgtFl = "";
		abovTotTmTrgtFl = "";
		abovTotMfyTrgtTmTmFl = "";
		abovTotFrntCterTrgtTmFl = "";
		abovTotDrvTrgtTmFl = "";
		abov50ScFl = "";
		bel25ScFl = "";
		heldTmScQt = "";
		ordHeldFl = "";

		try {
			
			if (nlServiceTime != null && nlServiceTime.getLength() > 0 ) {
				for (int idxServiceTime=0; idxServiceTime < nlServiceTime.getLength(); idxServiceTime++ ) {
					if ( nlServiceTime.item(idxServiceTime).getNodeType() == Node.ELEMENT_NODE ) {  
						eleServiceTime = (Element)nlServiceTime.item(idxServiceTime);
						if ( eleServiceTime.getNodeName().equals("ServiceTime") ) {
							posOrdKey = eleServiceTime.getAttribute("orderKey");

							untlTotKeyPrssScQt = getScQt(eleServiceTime.getAttribute("untilTotal"));
							untlStrInSysScQt = getScQt(eleServiceTime.getAttribute("untilStore"));
							untlOrdRcllScQt = getScQt(eleServiceTime.getAttribute("untilRecall"));
							untlDrwrClseScQt = getScQt(eleServiceTime.getAttribute("untilCloseDrawer"));
							untlPaidScQt = getScQt(eleServiceTime.getAttribute("untilPay"));
							untlSrvScQt = getScQt(eleServiceTime.getAttribute("untilServe"));
							totOrdTmScQt = getScQt(eleServiceTime.getAttribute("totalTime"));
							
							abovPsntTmTrgtFl = getValue(eleServiceTime,"tcOverPresentationPreset");
							abovTotTmTrgtFl = getValue(eleServiceTime,"tcOverTotalPreset");
							abovTotMfyTrgtTmTmFl = getValue(eleServiceTime,"tcOverTotalMFY");
							abovTotFrntCterTrgtTmFl = getValue(eleServiceTime,"tcOverTotalFC");
							abovTotDrvTrgtTmFl = getValue(eleServiceTime,"tcOverTotalDT");
							
							processProductionTime(eleServiceTime.getChildNodes(),context);

							outputKey.setLength(0);						
							outputKey.append(REC_POSTRN);
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(posOrdKey);
							outputKey.append(SEPARATOR_CHARACTER);
							outputKey.append(mcdGbalLcatIdNu);
							
					    	mapKey.clear();
							mapKey.set(outputKey.toString());

							outputTextValue.setLength(0);
							outputTextValue.append("2");
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlTotKeyPrssScQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlStrInSysScQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlOrdRcllScQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlDrwrClseScQt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlPaidScQt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlSrvScQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(totOrdTmScQt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovPsntTmTrgtFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovTotTmTrgtFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovTotMfyTrgtTmTmFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovTotFrntCterTrgtTmFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovTotDrvTrgtTmFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abov50ScFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(bel25ScFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(heldTmScQt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(ordHeldFl); 
							
							mapValue.clear();
							mapValue.set(outputTextValue.toString());
							context.write(mapKey, mapValue);	
							context.getCounter("COUNT","POS_TRN_DetailedSOS").increment(1);

							outputKey.setLength(0);						
							outputKey.append(REC_POSTRNITM);
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(posOrdKey);
							outputKey.append(SEPARATOR_CHARACTER);
							outputKey.append(mcdGbalLcatIdNu);
							
					    	mapKey.clear();
							mapKey.set(outputKey.toString());
							context.write(mapKey, mapValue);	
							context.getCounter("COUNT","POS_TRN_ITM_DetailedSOS").increment(1);

						}
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.processServiceTime:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void processProductionTime(NodeList nlProductionTime
                                      ,Context context) {

		Element eleProductionTime;

		if (nlProductionTime != null && nlProductionTime.getLength() > 0 ) {
			for (int idxProductionTime=0; idxProductionTime < nlProductionTime.getLength(); idxProductionTime++ ) {
				if ( nlProductionTime.item(idxProductionTime).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProductionTime = (Element)nlProductionTime.item(idxProductionTime);
					if ( eleProductionTime.getNodeName().equals("ProductionTime") ) {
						abov50ScFl = getValue(eleProductionTime,"tcOver50");
						bel25ScFl = getValue(eleProductionTime,"tcUnder25");
						heldTmScQt = getScQt(eleProductionTime.getAttribute("heldTime"));
						ordHeldFl = getValue(eleProductionTime,"tcHeld");
					}
				}
			}
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
	
	private String formatDateAsTsDtOnly(String in) {

		String retTs = "";
		
		if ( in.length() >= 8 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8);
		}

		return(retTs);
		
	}
	
	private String formatDateAsTs(String in) {

		String retTs = "";
		
		if ( in.length() >= 8 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8) + " 00:00:00";
		}

		return(retTs);
		
	}
	
	private String formatTimeAsTs(String in
			                     ,String inDate) {

		String retTs = "";
		
		if ( in.length() >= 6 ) {
			retTs = inDate.substring(0, 10) + " " + in.substring(0, 2) + ":" + in.substring(3, 4) + ":" + in.substring(4, 6);
		}

		return(retTs);
		
	}
	
	private String getScQt(String time) {
		
		String retScQt = "";
		
		Double tmpScQt;
		
		try {
			tmpScQt = Double.parseDouble(time) / 1000.0;
			retScQt = String.valueOf(tmpScQt);
		} catch (Exception ex) {
		}
		
		return(retScQt);
		
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

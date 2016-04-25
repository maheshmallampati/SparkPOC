package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;

public class NpSalesSummaryMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final BigDecimal DECIMAL_ZERO = new BigDecimal("0.00");
	
	private static String[] parts = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;

	private Element eleRoot;
	private NodeList nlNode;
	private Element eleNode;
	private NodeList nlEvent;
	private Element eleEvent;
	private String eventType;
	private Element eleTrx;
	private Element eleOrder;
	private NodeList nlTRX;
	private NodeList nlOrder;

	private String terrCd = "";
	private String lgcyLclRfrDefCd = "";
	private String businessDate = "";
	private String status = "";
	private String kind = "";

	private BigDecimal tranTotalAmount = DECIMAL_ZERO;
	private BigDecimal totalAmount = DECIMAL_ZERO;
	private int tranCount = 0;

	private static FileSplit fileSplit = null;
	private static String fileName = "";

	private String owshFltr = "*";
	private boolean keepValue = false;

	@Override
	public void setup(Context context) {

        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();

		owshFltr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER);

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
		} catch (Exception ex) {
			System.err.println("Error in initializing NpSalesSummaryMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
		
	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			if ( fileName.equals("DailySales.psv") ) {
		    	
				parts = value.toString().split("\\|");

				if ( parts.length >= 3 ) {
			    	mapKey.clear();
					mapKey.set(parts[0] + "|" + parts[1] + "|" + parts[2]);
					mapValue.clear();
					mapValue.set(value.toString() + "|0.00|0|0| ");
					context.write(mapKey, mapValue);	
	    			context.getCounter("DaaS Counters", "Daily Sales LOCATION COUNT").increment(1);
				} else {
					return;
				}
				parts = null;
			} else {
		    	parts = value.toString().split("\\t");

		    	if ( parts.length >= 8 ) {
		    		
		    		if ( owshFltr.equals("*") ) {
		    			keepValue = true;
	    				context.getCounter("DaaS Counters", "LOCATION SKIPPED OWERSHIP FILTER").increment(0);
	    				context.getCounter("DaaS Counters", "LOCATION KEPT OWERSHIP FILTER").increment(1);
		    		} else {
		    			if ( owshFltr.equalsIgnoreCase(parts[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS]) ) {
		    				keepValue = true;
		    				context.getCounter("DaaS Counters", "LOCATION SKIPPED OWERSHIP FILTER").increment(0);
		    				context.getCounter("DaaS Counters", "LOCATION KEPT OWERSHIP FILTER").increment(1);
		    			} else {
		    				keepValue = false;
		    				context.getCounter("DaaS Counters", "LOCATION SKIPPED OWERSHIP FILTER").increment(1);
		    			}
		    		}
		    		
		    		if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
	    				context.getCounter("DaaS Counters", "LOCATION COUNT").increment(1);
		    		} else {
		    			context.getCounter("DaaS Counters", "LOCATION COUNT").increment(0);
		    		}
		    		
		    		String xmlText = parts[DaaSConstants.XML_REC_XML_TEXT_POS];
		    		xmlText  = xmlText.replace("&", " ");//[Fatal Error] :1:6425447: Character reference "&#
		    		if ( keepValue ) {
			    		if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
			    			getSalesSummary(xmlText,context);
			    		} else if ( parts[0].equalsIgnoreCase("STORE-DB") ) {
			    			getTaxBasis(xmlText,context);
			    		} 
		    		} else {
		    			return;
		    		}
		    	} else {
		    		return;
		    	}
		    	parts = null;
			}
	    } catch (Exception ex) {
	    	System.err.println("Error in map NpSalesSummaryMapper:");
	    	ex.printStackTrace(System.err);
	    }
	}

	private void getSalesSummary(String xmlText
			                    ,Context context) {

		tranTotalAmount = DECIMAL_ZERO;
		tranCount = 0;
		StringReader strReader = null;
		try {
			strReader  = new StringReader(xmlText);
			xmlSource = new InputSource(strReader);
			doc = docBuilder.parse(xmlSource);

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("TLD") ) {
				terrCd = eleRoot.getAttribute("gdwTerrCd");
				lgcyLclRfrDefCd = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");
				businessDate = eleRoot.getAttribute("gdwBusinessDate");

				nlNode = eleRoot.getChildNodes();
				if (nlNode != null && nlNode.getLength() > 0 ) {
					for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
						if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {  
							eleNode = (Element)nlNode.item(idxNode);
							if ( eleNode.getNodeName().equals("Node") ) {

								nlEvent = eleNode.getChildNodes();
								if (nlEvent != null && nlEvent.getLength() > 0 ) {
									for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++ ) {
										if ( nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE ) {  
											eleEvent = (Element)nlEvent.item(idxEvent);
											
											if ( eleEvent.getNodeName().equals("Event") ) {
												eventType = eleEvent.getAttribute("Type");

												if ( eventType.equals("TRX_Sale") || eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring") ) {
													eleTrx = null;
													nlTRX = eleEvent.getChildNodes();
													int idxTRX = 0;
													
													while (eleTrx == null && idxTRX < nlTRX.getLength() ) { 
														if ( nlEvent.item(idxTRX).getNodeType() == Node.ELEMENT_NODE ) {
															eleTrx = (Element)nlTRX.item(idxTRX);
															status = eleTrx.getAttribute("status") + "";
														}
														idxTRX++;
													}
													
													if ( (eventType.equals("TRX_Sale") && status.equals("Paid")) || (eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring")) ) {
														eleOrder = null;
														nlOrder = eleTrx.getChildNodes();
														int idxOrder =0;
														
														while ( eleOrder == null && idxOrder < nlOrder.getLength() ) {
															if ( nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {
																eleOrder = (Element)nlOrder.item(idxOrder);
															}
															idxOrder++;
														}
														
														totalAmount = new BigDecimal(eleOrder.getAttribute("totalAmount"));
														String nonProductAmount = eleOrder.getAttribute("nonProductAmount");
														if(StringUtils.isBlank(nonProductAmount))
															nonProductAmount = "0";
														totalAmount = totalAmount.subtract(new BigDecimal(nonProductAmount));
														kind = eleOrder.getAttribute("kind");
													
														if ( eventType.equals("TRX_Sale") ) {
															
															if ( kind.contains("Manager") || kind.contains("Crew") ) {
																if ( totalAmount.compareTo(DECIMAL_ZERO) != 0 ) {
																	tranCount++;
																}
															} else {
																tranCount++;
															}
															
															tranTotalAmount = tranTotalAmount.add(totalAmount);
														}

														if ( eventType.equals("TRX_Overring") ) {
															tranCount--;
														}
													
														if ( eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring") ) {
															tranTotalAmount = tranTotalAmount.subtract(totalAmount);
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
		        
				mapKey.clear();
				mapKey.set(businessDate + "|" + terrCd + "|" + lgcyLclRfrDefCd );
				mapValue.clear();
				mapValue.set(businessDate + "|" + terrCd + "|" + lgcyLclRfrDefCd + "|0.00|0.00|0|" + tranTotalAmount.toString() + "|" + tranCount + "|1| ");
				context.write(mapKey, mapValue);	
			}
			
		} catch (Exception ex) {
			System.err.println("Error in NpSalesSummary.getSalesSummary:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}finally{
			
				doc = null;
				xmlSource = null;
				
				if(strReader != null){
					strReader.close();
					strReader = null;
					
				}
		}
	}

	private void getTaxBasis(String xmlText
                            ,Context context) {

		StringReader strReader = null;
		try {
			
			strReader = new StringReader(xmlText);
			xmlSource = new InputSource(strReader);
			doc = docBuilder.parse(xmlSource);

			NodeList nlStoreDb;
			Element eleStoreDb;
			NodeList nlStoreProfile;
			Element eleStoreProfile;
			NodeList nlTaxDefinition;
			Element eleTaxDefinition;
			NodeList nlMenuPriceBasis;
			Element eleMenuPriceBasis;
			
			String menuBasis="";
			String menuBasisCode="";
			
			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("Document") ) {
				terrCd = eleRoot.getAttribute("gdwTerrCd");
				lgcyLclRfrDefCd = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");
				businessDate = eleRoot.getAttribute("gdwBusinessDate");
				
				nlStoreDb = eleRoot.getChildNodes();
				
				for ( int idx=0; idx < nlStoreDb.getLength() && menuBasis.length() == 0; idx++ ) {
					if ( nlStoreDb.item(idx).getNodeType() == Node.ELEMENT_NODE ) {
						eleStoreDb=(Element)nlStoreDb.item(idx);
						
						if ( eleStoreDb.getNodeName().equals("StoreDB") ) {
							nlStoreProfile=eleStoreDb.getChildNodes();

							for ( int idx2=0; idx2 < nlStoreDb.getLength() && menuBasis.length() == 0; idx2++ ) {
								if ( nlStoreProfile.item(idx2).getNodeType() == Node.ELEMENT_NODE ) {
									eleStoreProfile=(Element)nlStoreProfile.item(idx2);
									
									if ( eleStoreProfile.getNodeName().equals("StoreProfile") ) {
										nlTaxDefinition=eleStoreProfile.getChildNodes();

										for ( int idx3=0; idx3 < nlTaxDefinition.getLength() && menuBasis.length() == 0; idx3++ ) {
											if ( nlTaxDefinition.item(idx3).getNodeType() == Node.ELEMENT_NODE ) {
												eleTaxDefinition=(Element)nlTaxDefinition.item(idx3);
												
												if (eleTaxDefinition.getNodeName().equals("TaxDefinition") ) {
													nlMenuPriceBasis=eleTaxDefinition.getChildNodes();

													for ( int idx4=0; idx4 < nlMenuPriceBasis.getLength() && menuBasis.length() == 0; idx4++ ) {
														if ( nlMenuPriceBasis.item(idx4).getNodeType() == Node.ELEMENT_NODE ) {
															eleMenuPriceBasis=(Element)nlMenuPriceBasis.item(idx4);
															
															if ( eleMenuPriceBasis.getNodeName().equals("MenuPriceBasis") ) {
																menuBasis=eleMenuPriceBasis.getTextContent();
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
				        
			if ( menuBasis.equalsIgnoreCase("GROSS") ) {
				menuBasisCode="G";
			} else {
				menuBasisCode="N";
			}
			
			mapKey.clear();
			mapKey.set(businessDate + "|" + terrCd + "|" + lgcyLclRfrDefCd );
			mapValue.clear();
			mapValue.set(businessDate + "|" + terrCd + "|" + lgcyLclRfrDefCd + "|0.00|0.00|0|0.00|0|0|"+menuBasisCode);
			context.write(mapKey, mapValue);	
			
		} catch (Exception ex) {
			System.err.println("Error in NpSalesSummary.getSummary:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		finally{
			
			doc = null;
			xmlSource = null;
			
			if(strReader != null){
				strReader.close();
				strReader = null;
				
			}
	}
	}
}

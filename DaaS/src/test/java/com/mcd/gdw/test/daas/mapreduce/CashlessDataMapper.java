package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.SimpleEncryptAndDecrypt;
import com.mcd.gdw.test.daas.driver.CashlessData;

public class CashlessDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;
	
	private String[] parts = null;
	private String[] cashlessParts;
	
	//private String terrCd;
	private String lgcyLclRfrDefCd;
	private String posBusnDt;
	private String orderTimestamp;
	private String orderDate;
	private String orderTime;
	private String kind;
	private BigDecimal posTotNetTrnAm;
	private String cashlessData;
	

	private boolean skip;
	
	private BigDecimal totCashAm;
	private BigDecimal totCashlessAm;
	private int trnCashQty;
	private int trnCashlessQty;
	
	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private Element eleTrxSale;
	private Element eleOrder;
	private Element eleTenders;
	private Element eleTender;
	private Element eleTenderItem;
	
	
	
	private Node nodeText;
	
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	//@mc41946
	SimpleEncryptAndDecrypt encryptString=null;
	private StringBuffer customerId=new StringBuffer();
	private String storeId;

	
	@Override
	public void setup(Context context) {
		
		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			encryptString=new SimpleEncryptAndDecrypt();
			
		} catch (Exception ex) {
			System.err.println("Error in initializing CashlessDataMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			parts = value.toString().split("\t");
			
			if ( parts.length >= 8 ) {
				//terrCd = parts[DaaSConstants.XML_REC_TERR_CD_POS];
				lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
				posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS];

				totCashAm = CashlessData.DEC_ZERO;
				totCashlessAm = CashlessData.DEC_ZERO;
				trnCashQty = 0;
				trnCashlessQty = 0;
				
				getData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);

				context.getCounter("DaaS","File Count").increment(1);
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in CashlessDataMapper.Map:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void getData(String xmlText
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

			if ( eleRoot.getNodeName().equals("TLD") ) {
				 storeId   = eleRoot.getAttribute("storeId");
	                /*if(!storeId.equalsIgnoreCase("28636"))
	    	             return;*/
				
				processNode(eleRoot.getChildNodes(),context);
				
				keyOut.clear();
				keyOut.set("A\t" + posBusnDt);
				valueOut.clear();
				valueOut.set(lgcyLclRfrDefCd + "\t" + totCashAm.toString() + "\t" + trnCashQty + "\t" +  totCashlessAm.toString() + "\t" + trnCashlessQty);
				context.write(keyOut, valueOut);

			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in CashlessDataMapper.getData:");
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

						orderTimestamp = getValue(eleOrder,"Timestamp");
						
						if ( orderTimestamp.length() >= 14 ) {
							orderDate = orderTimestamp.substring(0, 8);
							orderTime = orderTimestamp.substring(8, 14);
						} else {
							orderDate = "********";
							orderTime = "******";
						}
						
						posTotNetTrnAm = new BigDecimal(getValue(eleOrder,"totalAmount"));
						kind = getValue(eleOrder,"kind");
						
						skip = false;
						
						if ( kind.contains("Manager") || kind.contains("Crew") ) {
							if ( posTotNetTrnAm.equals(CashlessData.DEC_ZERO) ) {
								skip = true;
							}
						}
						
						if ( !skip ) {
							if ( processOrderItems(eleOrder.getChildNodes(),context) ) {
								totCashlessAm = totCashlessAm.add(posTotNetTrnAm);
								trnCashlessQty++;
								
							} else {
								totCashAm = totCashAm.add(posTotNetTrnAm);
								trnCashQty++;
							}
								
							context.getCounter("DaaS","Order Count").increment(1);
						}
						
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in CashlessDataMapper.processOrder:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private boolean processOrderItems(NodeList nlOrderItems
			                         ,Context context) throws Exception {

		boolean isCashless = false;
		
		if (nlOrderItems != null && nlOrderItems.getLength() > 0 ) {
			for (int idxOrderItems=0; idxOrderItems < nlOrderItems.getLength(); idxOrderItems++ ) {
				if ( nlOrderItems.item(idxOrderItems).getNodeType() == Node.ELEMENT_NODE ) {
					eleTenders = (Element)nlOrderItems.item(idxOrderItems);
					
					if ( eleTenders.getNodeName().equals("Tenders") ) {
						isCashless = processTenders(eleTenders.getChildNodes(),context);
					}
					
				}
			}
		}
		
		return(isCashless);
		
	}

	private boolean processTenders(NodeList nlTender
			                      ,Context context) throws Exception {

		boolean isCashless = false;

		if (nlTender != null && nlTender.getLength() > 0 ) {
			for (int idxTender=0; idxTender < nlTender.getLength(); idxTender++ ) {
				if ( nlTender.item(idxTender).getNodeType() == Node.ELEMENT_NODE ) {
					eleTender = (Element)nlTender.item(idxTender);
					
					isCashless = processTender(eleTender.getChildNodes(),context);
				}
			}
		}

		return(isCashless);
		
	}

	private boolean processTender(NodeList nlTenderItem
			                     ,Context context) throws Exception {

		boolean isCashless = false;
		
		if (nlTenderItem != null && nlTenderItem.getLength() > 0 ) {
			for (int idxTenderItem=0; idxTenderItem < nlTenderItem.getLength() && !isCashless; idxTenderItem++ ) {
				if ( nlTenderItem.item(idxTenderItem).getNodeType() == Node.ELEMENT_NODE ) {
					eleTenderItem = (Element)nlTenderItem.item(idxTenderItem);
					
					if ( eleTenderItem.getNodeName().equals("CashlessData") ) {
						nodeText = eleTenderItem.getFirstChild();
						
						if ( nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							cashlessData = nodeText.getNodeValue();
							
							cashlessParts = cashlessData.split("\\|");
		
							if ( cashlessParts[0].startsWith("CASHLESS") ) {
								isCashless = true;
						/*}
							//if ( cashlessParts[0].endsWith("Visa") && cashlessParts[1].endsWith("0301") && cashlessParts[2].equals("03/17") ) {
							if ( cashlessParts[0].endsWith("Visa") ) {*/
								/*customerId.setLength(0);
								customerId.append(cashlessParts[0].substring(cashlessParts[0].length()-4));
								customerId.append(cashlessParts[1].substring(cashlessParts[1].length()-4));
								customerId.append(cashlessParts[2].replace("/", ""));
								customerId.append(storeId);*/
								keyOut.clear();
								keyOut.set("B");
								valueOut.clear();
								//valueOut.set(orderDate + "\t" + orderTime + "\t" + lgcyLclRfrDefCd + "\t" + posTotNetTrnAm.toString()+"\t"+encryptString.encryptAsHexString(customerId.toString())+"\t"+customerId.toString());
								valueOut.set(orderDate + "\t" + orderTime + "\t" + lgcyLclRfrDefCd + "\t" + posTotNetTrnAm.toString());
								context.write(keyOut, valueOut);
							}
						}

					}
				}
			}
		}
		
		return(isCashless);
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

package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
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

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.SimpleEncryptAndDecrypt;

import com.mcd.gdw.test.daas.driver.GenerateUnauthorizedOpen;

public class UnauthorizedDataMapper extends Mapper<LongWritable, Text, Text, Text> {

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
	private Element eleDrawerClose;
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
	private MultipleOutputs<Text, Text> mos;
	
	@Override
	public void setup(Context context) {
		
		try {
			mos = new MultipleOutputs<Text, Text>(context);
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			encryptString=new SimpleEncryptAndDecrypt();
			
		} catch (Exception ex) {
			System.err.println("Error in initializing CashlessDataMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}
	StringBuffer mosKey = new StringBuffer();
	StringBuffer mosValue = new StringBuffer();
	Text outputkey = new Text();
	Text outputvalue = new Text();
	private String terrCd;
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			parts = value.toString().split("\t");
			
			if ( parts.length >= 8 ) {
				//terrCd = parts[DaaSConstants.XML_REC_TERR_CD_POS];
				lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
				posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS];
				terrCd = parts[DaaSConstants.XML_REC_TERR_CD_POS];

				
				trnCashQty = 0;
				trnCashlessQty = 0;
				
				getData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
				
				context.getCounter("StoreCount", "File Count").increment(1);

				// Writing out data after calling all methods.
				//System.out.println(managerIdList.size());
				
				mosKey.setLength(0);
				mosKey.append(GenerateUnauthorizedOpen.fileNameUnauthorizedOpen);
				

				mosValue.setLength(0);
				mosValue.append(storeId);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(posBusnDt);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(terrCd);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(unauthCount);
				

				outputkey.clear();
				outputvalue.clear();

				outputkey.set(HDFSUtil.replaceMultiOutSpecialChars(mosKey.toString()));
				outputvalue.set(mosValue.toString());
				// mos.write( outputkey.toString(),NullWritable.get(),outputvalue);
                context.write(outputkey,outputvalue);
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
				System.out.println("Inside exception");
				return;	
			}

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("TLD") ) {
				 storeId   = eleRoot.getAttribute("storeId");
				 System.out.println("storeId"+storeId);
	                /*if(!storeId.equalsIgnoreCase("28636"))
	    	             return;*/
				
				processNode(eleRoot.getChildNodes(),context);
				
				

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
					     eleEvent.getAttribute("Type").equalsIgnoreCase("Ev_DrawerClose") ) {
						//System.out.println("------->"+eleEvent.getAttribute("Type"));
						processEvDrawerClose(eleEvent.getChildNodes(),context);
					}
				}
			}
		}
		
	}
	int unauthCount=0;
	private void processEvDrawerClose(NodeList nlTrxSale
			                   ,Context context) {
 		
		if (nlTrxSale != null && nlTrxSale.getLength() > 0 ) {
			for (int idxTrxSale=0; idxTrxSale < nlTrxSale.getLength(); idxTrxSale++ ) {
				if ( nlTrxSale.item(idxTrxSale).getNodeType() == Node.ELEMENT_NODE ) {
					eleDrawerClose = (Element)nlTrxSale.item(idxTrxSale);

					if ( eleDrawerClose.getAttribute("UnauthorizedOpen").equalsIgnoreCase("true") ) {
						unauthCount=unauthCount+1;
						//processOrder(eleDrawerClose.getChildNodes(),context);
					}
				}
			}
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

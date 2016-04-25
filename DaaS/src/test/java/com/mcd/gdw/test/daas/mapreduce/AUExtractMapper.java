package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

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

public class AUExtractMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private final static String SEPARATOR_CHARACTER = "\t";
	
	private final static String TRX_SALE     = "TRX_Sale";
	private final static String TRX_REFUND   = "TRX_Refund";
	private final static String TRX_OVERRING = "TRX_Waste";

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private String[] parts;
	private StringReader strReader;

	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private Element eleTRX;
	private Element eleOrder;
	private Element eleOrderItem;
	private Element eleInfoItem;

	private Node nodeText;

	private String trxType;
	private String terrCd;
	private String lgcyLclRfrDefCd;
	private String calDt;
	private String pod;
	private String podType;
	private String kind;
	private String key;
	private String timestamp;
	private String totalAmount;
	private String custId;
	private String hour;
	private String minute;
	private String dayPart;
	private String year;
	private String month;
	
	private int hourNum;
	private int minuteNum;
	
	private StringBuffer valueOut = new StringBuffer();
	
	private Text value = new Text();
	
	@Override
	public void setup(Context context) {

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();

		} catch (Exception ex) {
			System.err.println("Error in initializing AUExtractMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
		
	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		parts = value.toString().split("\\t");

		if ( parts.length >= 8 ) {
			getData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
		}
		
	}
	
	private void getData(String xmlText
                    ,Context context) {
	
		try {
			strReader  = new StringReader(xmlText);
			xmlSource = new InputSource(strReader);
			doc = docBuilder.parse(xmlSource);
			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("TLD") ) {
				terrCd = eleRoot.getAttribute("gdwTerrCd");
				lgcyLclRfrDefCd = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");
				calDt = eleRoot.getAttribute("gdwBusinessDate");
				calDt = calDt.substring(0, 4) + "-" + calDt.substring(4, 6) + "-" + calDt.substring(6, 8);

				processNodes(eleRoot.getChildNodes(),context);
				
			}
			
		} catch ( Exception ex ) {
			eleRoot = (Element) doc.getFirstChild();
			System.err.println("Error in AUExtractMapper.getData:");
			ex.printStackTrace(System.err);
			System.exit(8);

		} finally {
			doc = null;
			xmlSource = null;
				
			if(strReader != null){
				strReader.close();
				strReader = null;
				
			}
		}
	}

	
	private void processNodes(NodeList nlNode, Context context) throws Exception {

		if (nlNode != null && nlNode.getLength() > 0 ) {
			for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
				if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {  
					eleNode = (Element)nlNode.item(idxNode);
					if ( eleNode.getNodeName().equals("Node") ) {
						processEvents(eleNode.getChildNodes(),context);
					}
				}
			}
		}

	}
	
	private void processEvents(NodeList nlEvent,Context context) throws Exception {

		if (nlEvent != null && nlEvent.getLength() > 0 ) {
			for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++ ) {
				if ( nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE ) {  
					eleEvent = (Element)nlEvent.item(idxEvent);
					
					trxType = eleEvent.getAttribute("Type");
					
					if ( trxType.equals(TRX_SALE) || 
					     trxType.equals(TRX_REFUND) || 
					     trxType.equals(TRX_OVERRING) ) {
					
						processTRX(eleEvent.getChildNodes(),context);
					}
				}
			}
		}

	}

	private void processTRX(NodeList nlTRX,Context context) throws Exception {

		if (nlTRX != null && nlTRX.getLength() > 0 ) {
			for (int idxTRX=0; idxTRX < nlTRX.getLength(); idxTRX++ ) {
				if ( nlTRX.item(idxTRX).getNodeType() == Node.ELEMENT_NODE ) {  
					eleTRX = (Element)nlTRX.item(idxTRX);
					
					pod = eleTRX.getAttribute("POD");

					if ( trxType.equals(TRX_SALE) && eleTRX.getAttribute("status").equals("Paid") ) {
						processOrder(eleTRX.getChildNodes(),context);
					} else if ( trxType.equals(TRX_REFUND) ) {
						processOrder(eleTRX.getChildNodes(),context);
					} else if ( trxType.equals(TRX_OVERRING) ) {
						processOrder(eleTRX.getChildNodes(),context);
					}

				}
			}
		}
		
	}

	
	private void processOrder(NodeList nlOrder,Context context) throws Exception {

		if (nlOrder != null && nlOrder.getLength() > 0 ) {
			for (int idxOrder=0; idxOrder < nlOrder.getLength(); idxOrder++ ) {
				if ( nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {  
					eleOrder = (Element)nlOrder.item(idxOrder);
					
					if ( eleOrder.getNodeName().equals("Order") ) {
						timestamp = eleOrder.getAttribute("Timestamp");
						
						if ( timestamp.length() >= 11 ) {
							hour = timestamp.substring(8,10);
							minute = timestamp.substring(10,12);
							hourNum = Integer.parseInt(hour);
							minuteNum = Integer.parseInt(minute);
							
							year = calDt.substring(0,4);
							month = calDt.substring(5,7);
						
							if ( hourNum == 23 || (hourNum >= 0 && hourNum <= 4) ) {
								dayPart = "Late Night";
							} else if ( (hourNum >= 5 && hourNum <= 9) || ( hourNum == 10 && minuteNum < 30) ) {
								dayPart = "Breakfast";
							} else if ( (hourNum == 10 && minuteNum >= 30) || ( hourNum >= 11 && hourNum <= 13) ) {
								dayPart = "Lunch";
							} else if ( (hourNum >= 14 && hourNum <= 16) ) {
								dayPart = "Snack";
							} else if ( (hourNum >= 17 && hourNum <= 19) ) {
								dayPart = "Dinner";
							} else if ( (hourNum >= 20 && hourNum <= 22) ) {
								dayPart = "Evening";
							} else {
								dayPart = "ERROR";
							}
						
							timestamp = timestamp.substring(0,4) + "-" + timestamp.substring(4,6) + "-" + timestamp.substring(6,8) + " " + timestamp.substring(8,10) + ":" + timestamp.substring(10,12) + ":" + timestamp.substring(12,14);   
							key = eleOrder.getAttribute("key");
							kind = eleOrder.getAttribute("kind");
							totalAmount = eleOrder.getAttribute("totalAmount");
							custId = "";
						
							processOrderItems(eleOrder.getChildNodes());

							valueOut.setLength(0);
							valueOut.append(terrCd);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(lgcyLclRfrDefCd);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(calDt);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(year);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(month);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(timestamp);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(hour);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(minute);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(dayPart);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(trxType);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(pod);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(kind);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(key);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(totalAmount);
							valueOut.append(SEPARATOR_CHARACTER);
							valueOut.append(custId);
						
							value.clear();
							value.set(valueOut.toString());
							context.write(NullWritable.get(), value);
						}
					}
				}
			}
		}
	}

	private void processOrderItems(NodeList nlOrderItem) throws Exception {

		if (nlOrderItem != null && nlOrderItem.getLength() > 0 ) {
			for (int idxOrderItem=0; idxOrderItem < nlOrderItem.getLength(); idxOrderItem++ ) {
				if ( nlOrderItem.item(idxOrderItem).getNodeType() == Node.ELEMENT_NODE ) {  
					eleOrderItem = (Element)nlOrderItem.item(idxOrderItem);
					
					if ( eleOrderItem.getNodeName().equals("CustomInfo") ) {
						processCustomInfo(eleOrderItem.getChildNodes());
					}
				}
			}
		}
	}

	private void processCustomInfo(NodeList nlCustomInfo) throws Exception {

		if (nlCustomInfo != null && nlCustomInfo.getLength() > 0 ) {
			for (int idxCustomInfo=0; idxCustomInfo < nlCustomInfo.getLength(); idxCustomInfo++ ) {
				if ( nlCustomInfo.item(idxCustomInfo).getNodeType() == Node.ELEMENT_NODE ) {  
					eleInfoItem = (Element)nlCustomInfo.item(idxCustomInfo);
					
					if ( eleInfoItem.getNodeName().equals("Info") ) {
						if ( eleInfoItem.getAttribute("name").equals("customerId") ) {
							custId = eleInfoItem.getAttribute("value");
						}
					}
				}
			}
		}
	}

}

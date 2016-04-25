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

public class AUExtractMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private final static String SEPARATOR_CHARACTER = "\t";
	
	private final static BigDecimal DEC_ZERO   = new BigDecimal("0.00");
	private final static BigDecimal DEC_NEG_ONE = new BigDecimal("-1.00");
	
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

	private String trxType;
	private String terrCd;
	private String lgcyLclRfrDefCd;
	private String calDt;
	private String kind;
	private String custId;
	private BigDecimal totalAmount;
	private int transCount;
	
	private BigDecimal nonMobileTotalAmount;
	private int nonMobileTransCount;
	private BigDecimal mobileTotalAmount;
	private int mobileTransCount;
	
	private StringBuffer valueOut = new StringBuffer();
	
	private Text keyText = new Text();
	private Text valueText = new Text();
	
	@Override
	public void setup(Context context) {

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();

		} catch (Exception ex) {
			System.err.println("Error in initializing AUExtractMapper2:");
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

				nonMobileTotalAmount = DEC_ZERO;
				nonMobileTransCount = 0;
				mobileTotalAmount = DEC_ZERO;
				mobileTransCount = 0;

				processNodes(eleRoot.getChildNodes(),context);
				
				keyText.clear();
				keyText.set(calDt);
				
				valueOut.setLength(0);
				valueOut.append(calDt);
				valueOut.append(SEPARATOR_CHARACTER);
				valueOut.append(terrCd);
				valueOut.append(SEPARATOR_CHARACTER);
				valueOut.append(lgcyLclRfrDefCd);
				valueOut.append(SEPARATOR_CHARACTER);
				valueOut.append(nonMobileTotalAmount.toString());
				valueOut.append(SEPARATOR_CHARACTER);
				valueOut.append(nonMobileTransCount);
				valueOut.append(SEPARATOR_CHARACTER);
				valueOut.append(mobileTotalAmount.toString());
				valueOut.append(SEPARATOR_CHARACTER);
				valueOut.append(mobileTransCount);
				
				valueText.clear();
				valueText.set(valueOut.toString());
				context.write(keyText, valueText);
				
			}
			
		} catch ( Exception ex ) {
			eleRoot = (Element) doc.getFirstChild();
			System.err.println("Error in AUExtractMapper2.getData:");
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

						kind = eleOrder.getAttribute("kind");
						totalAmount = new BigDecimal(eleOrder.getAttribute("totalAmount"));
						custId = "";
						
						processOrderItems(eleOrder.getChildNodes());
						
						if ( trxType.equals(TRX_SALE) ) {
							if ( kind.contains("Manager") || kind.contains("Crew") ) {
								if ( totalAmount.compareTo(DEC_ZERO) != 0 ) {
									transCount=1;
								} else {
									transCount=0;
								}
							} else {
								transCount=1;
							}
									
						} else if ( trxType.equals(TRX_REFUND) ) {
							totalAmount = totalAmount.multiply(DEC_NEG_ONE);
							transCount=0;
							
						} else if ( trxType.equals(TRX_OVERRING) ) {
							totalAmount = totalAmount.multiply(DEC_NEG_ONE);
							transCount=-1;
							
						}
						
						if ( custId.length() > 0 ) {
							mobileTotalAmount = mobileTotalAmount.add(totalAmount);
							mobileTransCount += transCount;
						} else {
							nonMobileTotalAmount = nonMobileTotalAmount.add(totalAmount);
							nonMobileTransCount += transCount;
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

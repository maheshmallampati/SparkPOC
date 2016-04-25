package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
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

public class CostsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String inFileName;
	private String[] parts;
	
	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputValue = new StringBuffer();
	private StringBuffer outputValuePrefix = new StringBuffer();
	
	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;
	
	private HashMap<String,String> orderKeyMap = new HashMap<String,String>();
	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private Element eleTrxSale; 
	private Element eleOrder;
	private String posBusnDt;
    private String mcdGbalLcatIdNu;
	private String posOrdKey;
	private String posTrnStrtTs;
	private String posTotNetTrnAm;
	private int seqNum;

	private StringBuffer childItems = new StringBuffer();
	
	@Override
	public void setup(Context context) {
		
		try {
			inFileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		parts = value.toString().split("\t");
		
		if ( inFileName.toUpperCase().contains("COST") ) {
			getCosts(context);
		} else {
			mcdGbalLcatIdNu = parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS];
			posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8);
				
			if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
				getTldData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
			}
		}
	}	
	
	private void getCosts(Context context) {
		
		try {
			outputKey.setLength(0);
			outputKey.append(parts[2]);
			outputKey.append(parts[3]);
			//outputKey.append("201203");
			outputKey.append(parts[1]);
			
			outputValue.setLength(0);
			outputValue.append("C");
			outputValue.append(parts[4]);
			outputValue.append("|");
			//outputValue.append(parts[5]);
			//outputValue.append("\t");
			outputValue.append(parts[6]);
			outputValue.append("\t");
			outputValue.append(parts[7]);
			outputValue.append("\t");
			outputValue.append(parts[8]);
			outputValue.append("\t");
			outputValue.append(parts[9]);

			mapKey.clear();
			mapKey.set(outputKey.toString());

			mapValue.clear();
			mapValue.set(outputValue.toString());

			context.write(mapKey, mapValue);
			
			context.getCounter("DaaS","Costs").increment(1);
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void getTldData(String xmlText
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

			orderKeyMap.clear();

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("TLD") ) {
				processNode(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
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

						posOrdKey = getValue(eleOrder,"key");
			
						if ( !orderKeyMap.containsKey(posOrdKey) ) {
							orderKeyMap.put(posOrdKey, "");
	
							posTrnStrtTs = formatAsTs(getValue(eleOrder,"Timestamp"));
				
							posTotNetTrnAm = getValue(eleOrder,"totalAmount");

							outputKey.setLength(0);	
							outputKey.append(posBusnDt.substring(0, 4));
							outputKey.append(posBusnDt.substring(5, 7));
							outputKey.append(mcdGbalLcatIdNu);

							outputValuePrefix.setLength(0);
							outputValuePrefix.append("T");
							outputValuePrefix.append(posBusnDt);
							outputValuePrefix.append("\t");
							outputValuePrefix.append(posOrdKey);
							outputValuePrefix.append("\t");
							outputValuePrefix.append(posTrnStrtTs);
							outputValuePrefix.append("\t");
							outputValuePrefix.append(posTotNetTrnAm);

							mapKey.clear();
							mapKey.set(outputKey.toString());
				
							context.getCounter("DaaS","TLD TRANS").increment(1);

							seqNum = 0;
							
							procesItem(eleOrder.getChildNodes(),context);
						}
					}	
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void procesItem(NodeList nlItem
                           ,Context context) {

		Element eleItem;
		int qty;
		String type;

		try {
			if (nlItem != null && nlItem.getLength() > 0 ) {
				for (int idxItem=0; idxItem < nlItem.getLength(); idxItem++ ) {
					if ( nlItem.item(idxItem).getNodeType() == Node.ELEMENT_NODE ) {
						eleItem = (Element)nlItem.item(idxItem);

						if ( eleItem.getNodeName().equals("Item") ) {
							try {
								qty = Integer.parseInt(getValue(eleItem,"qty"));
							} catch (Exception ex1) {
								qty = 0;
							}
		
							type = getValue(eleItem,"type");
				
							if ( qty > 0 && (type.equalsIgnoreCase("VALUE_MEAL") || type.equalsIgnoreCase("PRODUCT")) ) {
					
								if ( getValue(eleItem,"level").equals("0") ) {
									childItems.setLength(0);
								} else {
									childItems.append(getValue(eleItem,"code"));
									childItems.append("~");
								}
								
								seqNum++;
								
								procesItem(eleItem.getChildNodes(),context);
								
								outputValue.setLength(0);
								outputValue.append(outputValuePrefix);
								outputValue.append("\t");
								outputValue.append(seqNum);
								outputValue.append("\t");
								outputValue.append(getValue(eleItem,"code"));
								outputValue.append("\t");
								outputValue.append(getValue(eleItem,"description"));
								outputValue.append("\t");
								outputValue.append(getValue(eleItem,"level"));
								outputValue.append("\t");
								outputValue.append(getValue(eleItem,"id"));
								outputValue.append("\t");
								outputValue.append(qty);
								outputValue.append("\t");
								outputValue.append(getValue(eleItem,"totalPrice"));
								outputValue.append("\t");
								
								if ( getValue(eleItem,"level").equals("0") ) {
									outputValue.append(childItems);
								}
					
								mapValue.clear();
								mapValue.set(outputValue.toString());

								context.write(mapKey, mapValue);	
								context.getCounter("DaaS","TLD ITEM").increment(1);
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

	private String formatAsTs(String in) {

		String retTs = "";

		if ( in.length() >= 14 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8) + " " + in.substring(8, 10) + ":" + in.substring(10, 12) + ":" + in.substring(12, 14);
		} else {
			retTs = "0000-00-00 00:00:00";
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
	
}

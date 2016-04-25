package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.Path;
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

public class TLDVoiceMapper  extends Mapper<LongWritable, Text, Text, Text> {

	private String[] parts = null;
	private String[] parts2 = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;
	
	private HashMap<String,String> orderKeyMap = new HashMap<String,String>();
	
	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputValue = new StringBuffer();
	
	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private Element eleTrxSale; 
	private Element eleOrder;
	private String posBusnDt;
    private String lgcyLclRfrDefCd;
    private String terrCd;
	private String posOrdKey;
	private String posTrnStrtTs;
	private String posRegId;
	private String recepitNum;
	private String posTotGrossTrnAm;
	private String[] ordParts;
	
	private String splitFileName;
	
	private String[] dateParts1;
	private String[] dateParts2;
	private int hour;
	
	private String paidNode;
	
	@Override
	public void setup(Context context) {

		Path path;
		
		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();

	    	path = ((FileSplit) context.getInputSplit()).getPath();
	    	splitFileName = path.getName();

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			if ( splitFileName.toUpperCase().contains("STLD") ) {
				parts = value.toString().split("\t");
				
				terrCd = String.format("%03d", Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS]));
				lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
				posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8);
				
				if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
			    	getTldData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
				}
			} else {
				parts = value.toString().split("\\|");

				if (parts.length >=2 ) {
					if ( parts[0].endsWith("2015-09-01") ) {
						outputKey.setLength(0);
						outputKey.append(parts[0]);
						
						parts2 = parts[1].split("\t");
						
						outputValue.setLength(0);
						outputValue.append("VOICE");
						outputValue.append("\t");
						outputValue.append(parts2[0]);
						outputValue.append("\t");
						outputValue.append(parts2[1]);
						outputValue.append("\t");
						outputValue.append(parts2[2]);
						outputValue.append("\t");
						outputValue.append(parts2[3]);
						outputValue.append("\t");
						outputValue.append(parts2[4]);
						outputValue.append("\t");
						outputValue.append(parts2[5]);

						mapKey.clear();
						mapKey.set(outputKey.toString());

						mapValue.clear();
						mapValue.set(outputValue.toString());

						context.write(mapKey, mapValue);	
						context.getCounter("DaaS","VOICE").increment(1);
					} else {
						context.getCounter("DaaS","VOICE SKIPPED").increment(1);
					}
				}

				//if ( parts.length >= 35 ) {
				//	getVoiceData(context);
				//}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	/*
	private void getVoiceDataOld(Context context) {
		
		try {
			dateParts1 = parts[34].split(" ");
			dateParts2 = dateParts1[0].split("/");
			
			posTrnStrtTs = dateParts2[2] + "-" + String.format("%02d", Integer.parseInt(dateParts2[0])) + "-" + String.format("%02d", Integer.parseInt(dateParts2[1]));
			
			dateParts2 = dateParts1[1].split(":");
			
			hour = Integer.parseInt(dateParts2[0]);
			if ( dateParts1[2].equalsIgnoreCase("PM") && hour < 12 ) {
				hour += 12;
			}
			
			posTrnStrtTs += " " + String.format("%02d", hour) + ":" + String.format("%02d", Integer.parseInt(dateParts2[1])) + ":" + String.format("%02d", Integer.parseInt(dateParts2[2]));
			
			String amt;
			try {
				String[] tmpParts = ("0" + parts[1].trim() + ".00").split("\\.");
				String dollars = String.valueOf(Integer.parseInt(tmpParts[0]));
				String cents = String.format("%02d",(Integer.parseInt(tmpParts[1])));
				
				amt = dollars + "." + cents;
			} catch (Exception ex) {
				amt = "0.00";
			}
				
			String regNum;
			String orderNum;
			
			try {
				regNum = String.valueOf(Integer.parseInt(parts[30]));
				
			} catch (Exception ex ) {
				regNum = "0";
			}
			
			try {
				orderNum = String.valueOf(Integer.parseInt(parts[32]));
				
				if ( orderNum.length() > 2 ) {
					orderNum = String.valueOf(Integer.parseInt(orderNum.substring(orderNum.length()-2)));
				}
			} catch (Exception ex) {
				orderNum = "0";
			}
			
			outputKey.setLength(0);						
			outputKey.append("840");
			outputKey.append("\t");
			outputKey.append(parts[2]);
			outputKey.append("\t");
			outputKey.append(posTrnStrtTs.substring(0, 10));
			
			outputValue.setLength(0);
			outputValue.append("VOICE");
			outputValue.append("\t");
			outputValue.append(posTrnStrtTs);
			outputValue.append("\t");
			//outputValue.append(String.valueOf(Integer.parseInt(parts[30])));
			outputValue.append(regNum);
			outputValue.append("\t");
			//outputValue.append(String.valueOf(Integer.parseInt(parts[32])));
			outputValue.append(orderNum);
			outputValue.append("\t");
			outputValue.append(amt);
			outputValue.append("\t");
			outputValue.append(parts[0]);
			outputValue.append("\t");
			outputValue.append(parts[3]);

			if ( posTrnStrtTs.substring(0, 10).equals("2015-09-01") ) {
				mapKey.clear();
				mapKey.set(outputKey.toString());

				mapValue.clear();
				mapValue.set(outputValue.toString());

				context.write(mapKey, mapValue);	
				context.getCounter("DaaS","VOICE").increment(1);
			} else {
				context.getCounter("DaaS","VOICE SKIPPED").increment(1);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	*/
	
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
						paidNode = String.valueOf(Integer.parseInt(getValue(eleNode,"id").substring(3)));
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

							ordParts = posOrdKey.split(":");
							
							posRegId = String.valueOf(Integer.parseInt(ordParts[0].substring(3)));
							posTrnStrtTs = formatAsTs(eleOrder.getAttribute("Timestamp")).substring(0, 16) + ":00";
							
							try {
								recepitNum = String.valueOf(Integer.parseInt(getValue(eleOrder,"receiptNumber")));
							} catch (Exception ex1) {
								recepitNum = "0";
							}
							posTotGrossTrnAm = (new BigDecimal(getValue(eleOrder,"totalAmount")).add(new BigDecimal(getValue(eleOrder,"totalTax")))).toString() ;

							outputKey.setLength(0);						
							outputKey.append(terrCd);
							outputKey.append("\t");
							outputKey.append(lgcyLclRfrDefCd);
							outputKey.append("\t");
							outputKey.append(posTrnStrtTs.substring(0, 10));

							outputValue.setLength(0);
							outputValue.append("TLD");
							outputValue.append("\t");
							outputValue.append(posTrnStrtTs);
							outputValue.append("\t");
							//outputValue.append(posRegId);
							outputValue.append(paidNode);
							outputValue.append("\t");
							outputValue.append(recepitNum);
							outputValue.append("\t");
							outputValue.append(posBusnDt);
							outputValue.append("\t");
							outputValue.append(posOrdKey);
							outputValue.append("\t");
							outputValue.append(posTotGrossTrnAm);

							mapKey.clear();
							mapKey.set(outputKey.toString());
				
							mapValue.clear();
							mapValue.set(outputValue.toString());

							//if ( lgcyLclRfrDefCd.equals("00006") ) {
								context.write(mapKey, mapValue);	
								context.getCounter("DaaS","TLD").increment(1);
							//}
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


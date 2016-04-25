package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URI;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.test.daas.driver.TLDVoice;

public class TLDVoicePostMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String[] parts = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;
	
	private HashMap<String,String> orderKeyMap = new HashMap<String,String>();
	
	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputValuePrefix = new StringBuffer();
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
	private String recepitNum;
	private String posTotGrossTrnAm;
	private String[] ordParts;
	private String findKey;
	
	private HashMap<String,String> includeMap = new HashMap<String,String>();
	private HashMap<String,String> transIncludeMap = new HashMap<String,String>();

	@Override
	public void setup(Context context) {

		URI[] distPaths;
	    String[] distPathParts;
	    BufferedReader br = null; 

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();

			distPaths = context.getCacheFiles();
		    
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		      
		    if ( distPaths != null && distPaths.length > 0 )  {
		    	  
		    	for ( int i=0; i<distPaths.length; i++ ) {
				     
		    		System.out.println("distpaths:" + distPaths[i].toString());
			    	  
		    		distPathParts = distPaths[i].toString().split("#");
			    	  
		    		if ( distPaths[i].toString().contains(TLDVoice.CACHE_FILE_NAME) ) {
			    		
		    			includeMap.clear();
		    			transIncludeMap.clear();
		    			br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	addIncludeKeyValuestoMap(br);
				      	System.out.println("Loaded Include Values Map");
			    	}
		    	}
		    }

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void addIncludeKeyValuestoMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String terrCd;
		String lcat;
		String busnDt;
		String posKey;
		String voiceKey;
		
		String keyText;
		String valueText;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\t");

					terrCd      = parts[0];
					lcat        = parts[1];
					busnDt      = parts[2];
					posKey      = parts[3];
					
					voiceKey    = parts[4];
					
					keyText = terrCd + "\t" + lcat + "\t" + busnDt;
					valueText = posKey;

					if ( !includeMap.containsKey(keyText) ) {
						includeMap.put(keyText,valueText);
					}
					
					keyText = terrCd + "\t" + lcat + "\t" + busnDt + "\t" + posKey; 
					valueText = voiceKey;
					transIncludeMap.put(keyText,valueText);
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

		try {
			parts = value.toString().split("\t");
				
			terrCd = parts[DaaSConstants.XML_REC_TERR_CD_POS];
			lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
			posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8);
				
			if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
				
				if ( includeMap.containsKey(terrCd + "\t" + lgcyLclRfrDefCd + "\t" + posBusnDt) ) {
					getTldData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
				}
			}
			
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

						findKey = terrCd + "\t" + lgcyLclRfrDefCd + "\t" + posBusnDt + "\t" + posOrdKey;
						
						if ( !orderKeyMap.containsKey(posOrdKey) && transIncludeMap.containsKey(findKey) ) {
							orderKeyMap.put(posOrdKey, "");

							ordParts = posOrdKey.split(":");
				
							posTrnStrtTs = formatAsTs(getValue(eleOrder,"Timestamp"));
							
							posTotGrossTrnAm = (new BigDecimal(getValue(eleOrder,"totalAmount")).add(new BigDecimal(getValue(eleOrder,"totalTax")))).toString() ;

							outputKey.setLength(0);						
							outputKey.append(terrCd);
							outputKey.append("\t");
							outputKey.append(lgcyLclRfrDefCd);
							outputKey.append("\t");
							outputKey.append(posBusnDt);
							outputKey.append("\t");
							outputKey.append(posOrdKey);

							outputValuePrefix.setLength(0);
							outputValuePrefix.append(posTrnStrtTs);
							outputValuePrefix.append("\t");
							outputValuePrefix.append(posTotGrossTrnAm);
							outputValuePrefix.append("\t");
							outputValuePrefix.append(transIncludeMap.get(findKey));

							mapKey.clear();
							mapKey.set(outputKey.toString());
							
							context.getCounter("DaaS","TLD TRANS").increment(1);

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
								
								outputValue.setLength(0);
								outputValue.append(outputValuePrefix);
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
								outputValue.append(getValue(eleItem,"grillQty"));
								outputValue.append("\t");
								outputValue.append(getValue(eleItem,"qtyPromo"));
								
								mapValue.clear();
								mapValue.set(outputValue.toString());

								context.write(mapKey, mapValue);	
								context.getCounter("DaaS","TLD ITEM").increment(1);
								
								procesItem(eleItem.getChildNodes(),context);
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

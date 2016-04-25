package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

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
import com.mcd.gdw.test.daas.driver.GenerateSampleTLD;

public class SampleTLDMapper extends Mapper<LongWritable, Text, Text, Text> {
	
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
	private StringBuffer outputValue = new StringBuffer();
	
	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private Element eleTrxSale; 
	private Element eleOrder;
	private Element eleProductInfo;

	private String posBusnDt;
    private String lgcyLclRfrDefCd;
    private String terrCd;
	private String posOrdKey;
	private String posTrnStrtTs;
	private String posTrnTypCd;
	private String posPrdDlvrMethCd;
	private String posTotNetTrnAm;
	private String posTotTaxAm;

	private String posTrnItmSeqNu;
	private String posItmLvlNu;
	private String sldMenuItmId;
	private String posItmType;
	//private String posItmAction;
	private String posItmTotQt;
	private String posItmGrllQt;
	private String posItmGrllModCd;
	private String posItmPrmoQt;
	private String posItmNetUntPrcB4PrmoAm;
	private String posItmTaxB4PrmoAm;
	private String posItmNetUntPrcB4DiscAm;
	private String posItmTaxB4DiscAm;
	private String posItmActUntPrcAm;
	private String posItmActTaxAm;
	private String posItmVoidQt;

	private String menuItemId;
	private String menuItemPrice;
	
	@SuppressWarnings("unused")
	private int qty;
	
	@Override
	public void setup(Context context) {

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			parts = value.toString().split("\t");

			if ( parts.length >= 8 ) {
				
				terrCd = String.format("%03d", Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS]));
				lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
				posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8);
				
				if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
			    	getTldData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);

				} else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("MENUITEM") ) {
					getMenuData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
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
	
						if ( !orderKeyMap.containsKey(posOrdKey) ) {
							orderKeyMap.put(posOrdKey, "");

							posTrnStrtTs = formatAsTs(eleOrder.getAttribute("Timestamp"));
							
							posTrnTypCd = getValue(eleOrder,"kind");
							posPrdDlvrMethCd = getValue(eleOrder,"saleType");
							posTotNetTrnAm = getValue(eleOrder,"totalAmount");
							posTotTaxAm = getValue(eleOrder,"totalTax");

							processItem(eleOrder.getChildNodes(),context);

							outputKey.setLength(0);						
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(lgcyLclRfrDefCd);

							outputValue.setLength(0);
							outputValue.append(GenerateSampleTLD.REC_TYPE_HDR);
							outputValue.append(posOrdKey);
							outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
							outputValue.append(posTrnStrtTs);
							outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
							outputValue.append(posTrnTypCd);
							outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
							outputValue.append(posPrdDlvrMethCd);
							outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
							outputValue.append(posTotNetTrnAm);
							outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
							outputValue.append(posTotTaxAm);
			
							mapKey.clear();
							mapKey.set(outputKey.toString());
							
							mapValue.clear();
							mapValue.set(outputValue.toString());

							context.write(mapKey, mapValue);	
							context.getCounter("DaaS","Header").increment(1);
						}
						

					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void processItem(NodeList nlItem
                            ,Context context) {

		Element eleItem;

		try {
			if (nlItem != null && nlItem.getLength() > 0 ) {
				for (int idxItem=0; idxItem < nlItem.getLength(); idxItem++ ) {
					if ( nlItem.item(idxItem).getNodeType() == Node.ELEMENT_NODE ) {
						eleItem = (Element)nlItem.item(idxItem);
						if ( eleItem.getNodeName().equals("Item") ) {

							posItmType = getValue(eleItem,"type");
							//posItmAction = getValue(eleItem,"action");

							if ( !posItmType.equalsIgnoreCase("COMMENT") ) {
								posTrnItmSeqNu = getValue(eleItem,"id");
								posItmLvlNu = getValue(eleItem,"level");
								sldMenuItmId = getValue(eleItem,"code");
								posItmTotQt = getValue(eleItem,"qty");
								posItmGrllQt = getValue(eleItem,"grillQty");
								posItmGrllModCd = getValue(eleItem,"grillModifer");
								posItmPrmoQt = getValue(eleItem,"qtyPromo");
								posItmVoidQt = getValue(eleItem,"qtyVoided");
								posItmNetUntPrcB4PrmoAm = getValue(eleItem,"BPPrice");
								posItmTaxB4PrmoAm = getValue(eleItem,"BPTax");
								posItmNetUntPrcB4DiscAm = getValue(eleItem,"BDPrice");
								posItmTaxB4DiscAm = getValue(eleItem,"BDTax");
								posItmActUntPrcAm = getValue(eleItem,"totalPrice");
								posItmActTaxAm = getValue(eleItem,"totalTax");

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

								outputKey.setLength(0);						
								outputKey.append(terrCd);
								outputKey.append(posBusnDt);
								outputKey.append(lgcyLclRfrDefCd);
								
								outputValue.setLength(0);
								outputValue.append(GenerateSampleTLD.REC_TYPE_DTL);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posOrdKey);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posTrnItmSeqNu);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmLvlNu);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(sldMenuItmId);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmTotQt);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmGrllQt);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmGrllModCd);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmPrmoQt);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmNetUntPrcB4PrmoAm);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmTaxB4PrmoAm);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmNetUntPrcB4DiscAm);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmTaxB4DiscAm);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmActUntPrcAm);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmActTaxAm);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posItmVoidQt);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
								outputValue.append(posTotNetTrnAm);
								outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);

								mapKey.clear();
								mapKey.set(outputKey.toString());

								mapValue.clear();
								mapValue.set(outputValue.toString());

								context.write(mapKey, mapValue);
								context.getCounter("DaaS","Detail").increment(1);

								processItem(eleItem.getChildNodes(),context);
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

	private void getMenuData(String xmlText
			               ,Context context) {

		try {
			strReader  = new StringReader(xmlText);
			xmlSource = new InputSource(strReader);
			doc = docBuilder.parse(xmlSource);

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("MenuItem") ) {
				processProductInfo(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void processProductInfo(NodeList nlProductInfo
                                   ,Context context) {

		try {
			if (nlProductInfo != null && nlProductInfo.getLength() > 0 ) {
				for (int idxProductInfo=0; idxProductInfo < nlProductInfo.getLength(); idxProductInfo++ ) {
					if ( nlProductInfo.item(idxProductInfo).getNodeType() == Node.ELEMENT_NODE ) {  
						eleProductInfo = (Element)nlProductInfo.item(idxProductInfo);
						if ( eleProductInfo.getNodeName().equals("ProductInfo") ) {
							menuItemId = getValue(eleProductInfo,"id");
							menuItemPrice = getValue(eleProductInfo,"eatinPrice");

							outputKey.setLength(0);						
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(lgcyLclRfrDefCd);
							
							outputValue.setLength(0);
							outputValue.append(GenerateSampleTLD.REC_TYPE_MENU);
							outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
							outputValue.append(menuItemId);
							outputValue.append(GenerateSampleTLD.SEPARATOR_CHARACTER);
							outputValue.append(menuItemPrice);

							mapKey.clear();
							mapKey.set(outputKey.toString());

							mapValue.clear();
							mapValue.set(outputValue.toString());

							context.write(mapKey, mapValue);
							context.getCounter("DaaS","Menu").increment(1);
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

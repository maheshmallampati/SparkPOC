package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;

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

public class USTLDReformatMapper extends Mapper<LongWritable, Text, Text, Text> {

	private final static String SEPARATOR_CHARACTER            = "\t";
	private final static String COMPONENT_SPEARATOR_CHARACTER  = ":";
	private final static String COMPONENTS_SPEARATOR_CHARACTER = "~";
	private final static String REC_TRN                        = "TRN";
	private final static String REC_PRD                        = "PRD";

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;

	private String[] parts = null;

	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputTextValue = new StringBuffer();
	
	private String fileSubType;
	private String terrCd;
	private String posBusnDt;
	private String lgcyLclRfrDefCd;
	private String posOrdKey;
	private String posAreaTypShrtDs;
	private String posTrnStrtTs;
	private String posTrnTypCd;
	private String posPrdDlvrMethCd;
	private String posTotNetTrnAm;
	private String posTotNprdNetTrnAm;
	private String posTotTaxAm;
	private String posTotNprdTaxAm;

	private int posTrnItmSeqNu;
	private String posItmLvlNu;
	private String sldMenuItmId;
	private String posPrdTypCd;
	private String posItmActnCd;
	private String posItmTotQt;
	private String posItmGrllQt;
	private String posItmPrmoQt;
	private String posItmNetUntPrcB4PrmoAm;
	private String posItmTaxB4PrmoAm;
	private String posItmNetUntPrcB4DiscAm;
	private String posItmTaxB4DiscAm;
	private String posItmActUntPrcAm;
	private String posItmActTaxAm;
	private String posItmVoidQt;
	
	private Element eleRoot;
	private Node nodeText;
	
	private String prdProductCode;
	private String prdComponentProductCode;
	private String prdComponentDefaultQuantity;
	private StringBuffer componentList = new StringBuffer();

	@Override
	public void setup(Context context) {

        try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();

        } catch (Exception ex) {
			System.err.println("Error in initializing USTLDReformatMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
        
	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			
			parts = value.toString().split("\t");

			fileSubType = parts[DaaSConstants.XML_REC_FILE_TYPE_POS];

			terrCd = String.format("%03d", Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS]));
			lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
			
			//if ( lgcyLclRfrDefCd.startsWith("05626") ) {
				posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0, 4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4, 6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6, 8);

				strReader  = new StringReader(parts[DaaSConstants.XML_REC_XML_TEXT_POS]);
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
				eleRoot = (Element) doc.getFirstChild();
			
				if ( fileSubType.equals("STLD") && eleRoot.getNodeName().equals("TLD") ) {
					processSTLD(eleRoot.getChildNodes(),context);
				
				} else if ( fileSubType.equals("Product-Db") && eleRoot.getNodeName().equals("ProductDb") ) {
					processProuctDb(eleRoot.getChildNodes(),context);
				
				}
			//}
			
		} catch (Exception ex) { 
			System.err.println("Error occured in USTLDReformatMapper.map:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}
	
	private void processSTLD(NodeList nlNode
			                ,Context context) {
		
		Element eleNode;
		
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
		
						posTrnTypCd = getValue(eleOrder,"kind");
						posPrdDlvrMethCd = getValue(eleOrder,"saleType");
						posTotNetTrnAm = getValue(eleOrder,"totalAmount");
						posTotNprdNetTrnAm = getValue(eleOrder,"nonProductAmount");
						posTotTaxAm = getValue(eleOrder,"totalTax");
						posTotNprdTaxAm = getValue(eleOrder,"nonProductTax");

						posTrnItmSeqNu = 0;
						
						processItem(eleOrder.getChildNodes(),"",context);

					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in OffersReportFormatMapper.processOrder:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private void processItem(NodeList nlItem
			                ,String parentSldMnuItm
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

							posItmLvlNu = getValue(eleItem,"level");
							sldMenuItmId = getValue(eleItem,"code");
							posPrdTypCd = getValue(eleItem,"type");
							posItmActnCd = getValue(eleItem,"action");
							posItmTotQt = getValue(eleItem,"qty");
							posItmGrllQt = getValue(eleItem,"grillQty");
							posItmPrmoQt = getValue(eleItem,"qtyPromo");
							posItmNetUntPrcB4PrmoAm = getValue(eleItem,"BPPrice");
							posItmTaxB4PrmoAm = getValue(eleItem,"BPTax");
							posItmNetUntPrcB4DiscAm = getValue(eleItem,"BDPrice");
							posItmTaxB4DiscAm = getValue(eleItem,"BDTax");
							posItmActUntPrcAm = getValue(eleItem,"totalPrice");
							posItmActTaxAm = getValue(eleItem,"totalTax");
							posItmVoidQt = getValue(eleItem,"qtyVoided");

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

							try {
								qty = Integer.parseInt(posItmVoidQt);
							} catch (Exception ex) {
								posItmVoidQt = "0";
							}

							if ( posItmVoidQt.equals("0") ) {
								
								outputKey.setLength(0);						
								outputKey.append(terrCd);
								outputKey.append(posBusnDt);
								outputKey.append(lgcyLclRfrDefCd);

								outputTextValue.setLength(0);
								outputTextValue.append(REC_TRN);                                 // 00
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posOrdKey);                               // 01
								outputTextValue.append(SEPARATOR_CHARACTER);
								//outputTextValue.append(String.format("%3d",posTrnItmSeqNu));     // 02
								posTrnItmSeqNu+=1;
								outputTextValue.append(String.valueOf(posTrnItmSeqNu));          // 02
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posAreaTypShrtDs);                        // 03
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posTrnStrtTs);                            // 04
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posTrnTypCd);                             // 05
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posPrdDlvrMethCd);                        // 06 
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posTotNetTrnAm);                          // 07
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posTotNprdNetTrnAm);                      // 08
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posTotTaxAm);                             // 09 
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posTotNprdTaxAm);                         // 10
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmLvlNu);                             // 11
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(sldMenuItmId);                            // 12
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(parentSldMnuItm);                         // 13 
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posPrdTypCd);                             // 14
								outputTextValue.append(SEPARATOR_CHARACTER); 
								outputTextValue.append(posItmActnCd);                            // 15
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmTotQt);                             // 16
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmGrllQt);                            // 17
								outputTextValue.append(SEPARATOR_CHARACTER); 
								outputTextValue.append(posItmPrmoQt);                            // 18
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmNetUntPrcB4PrmoAm);                 // 19
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmTaxB4PrmoAm);                       // 20
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmNetUntPrcB4DiscAm);                 // 21
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmTaxB4DiscAm);                       // 22
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmActUntPrcAm);                       // 23
								outputTextValue.append(SEPARATOR_CHARACTER);
								outputTextValue.append(posItmActTaxAm);                          // 24
				
								mapKey.clear();
								mapKey.set(outputKey.toString());
								mapValue.clear();
								mapValue.set(outputTextValue.toString());
								context.write(mapKey, mapValue);	
								context.getCounter("DaaS","ITEM").increment(1);

								processItem(eleItem.getChildNodes(),sldMenuItmId,context);
							}
							
						}
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in USTLDReformatMapper.processItem:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	
	private void processProuctDb(NodeList nlProduct
                                ,Context context) {

		Element eleProduct;
		
		if (nlProduct != null && nlProduct.getLength() > 0 ) {
			for (int idxProduct=0; idxProduct < nlProduct.getLength(); idxProduct++ ) {
				if ( nlProduct.item(idxProduct).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProduct = (Element)nlProduct.item(idxProduct);
					
					if ( eleProduct.getNodeName().equals("Product") ) {
						if ( getValue(eleProduct,"productClass").equalsIgnoreCase("VALUE_MEAL") && getValue(eleProduct,"statusCode").equalsIgnoreCase("ACTIVE") ) {
							processProductItems(eleProduct.getChildNodes(),context);
						}
					}
				}
			}
		}
	}

	private void processProductItems(NodeList nlProductItems
                                    ,Context context) {
		
		Element eleProductItem;
		
		prdProductCode = "";
		
		if (nlProductItems != null && nlProductItems.getLength() > 0 ) {
			for (int idxProductItems=0; idxProductItems < nlProductItems.getLength(); idxProductItems++ ) {
				if ( nlProductItems.item(idxProductItems).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProductItem = (Element)nlProductItems.item(idxProductItems);
					
					if ( eleProductItem.getNodeName().equals("ProductCode") ) {
						nodeText = eleProductItem.getFirstChild();
						
						if ( nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							prdProductCode = nodeText.getNodeValue();
						}
						
					} else if ( eleProductItem.getNodeName().equals("Composition") && prdProductCode.length() > 0 ) {
						processProductComposition(eleProductItem.getChildNodes(),context);
					}
				}
			}
		}
	}

	private void processProductComposition(NodeList nlComposition
                                          ,Context context) {

		Element eleComponent;

		componentList.setLength(0);

		try {
			if (nlComposition != null && nlComposition.getLength() > 0 ) {
				for (int idxComponent=0; idxComponent < nlComposition.getLength(); idxComponent++ ) {
					if ( nlComposition.item(idxComponent).getNodeType() == Node.ELEMENT_NODE ) {  
						eleComponent = (Element)nlComposition.item(idxComponent);
						
						if ( eleComponent.getNodeName().equals("Component") ) {
							processProductComponent(eleComponent.getChildNodes(),context);
						}
					}
				}
				
				if ( componentList.length() > 0 ) {
					outputKey.setLength(0);						
					outputKey.append(terrCd);
					outputKey.append(posBusnDt);
					outputKey.append(lgcyLclRfrDefCd);

					outputTextValue.setLength(0);
					outputTextValue.append(REC_PRD);
					outputTextValue.append(SEPARATOR_CHARACTER);
					outputTextValue.append(prdProductCode); 
					outputTextValue.append(SEPARATOR_CHARACTER);
					outputTextValue.append(componentList.toString());

					mapKey.clear();
					mapKey.set(outputKey.toString());
					mapValue.clear();
					mapValue.set(outputTextValue.toString());
					context.write(mapKey, mapValue);	
					context.getCounter("DaaS","PRODUCT").increment(1);
				}

			}
		} catch (Exception ex) {
			System.err.println("Error occured in USTLDReformatMapper.processProductComposition:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	private void processProductComponent(NodeList nlComponents
			                            ,Context context) {
		
		Element eleComponentItems;
		
		prdComponentProductCode = "";
		prdComponentDefaultQuantity = "";
		
		if (nlComponents != null && nlComponents.getLength() > 0 ) {
			for (int idxComponentItems=0; idxComponentItems < nlComponents.getLength(); idxComponentItems++ ) {
				if ( nlComponents.item(idxComponentItems).getNodeType() == Node.ELEMENT_NODE ) {  
					eleComponentItems = (Element)nlComponents.item(idxComponentItems);
					
					if ( eleComponentItems.getNodeName().equals("ProductCode") ) {
						nodeText = eleComponentItems.getFirstChild();
						
						if ( nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							prdComponentProductCode = nodeText.getNodeValue();
						}
						
					} else if ( eleComponentItems.getNodeName().equals("DefaultQuantity") ) {
						nodeText = eleComponentItems.getFirstChild();
						
						if ( nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
							prdComponentDefaultQuantity = nodeText.getNodeValue();
						}
						
					}
				}
			}
			
			if ( prdComponentProductCode.length() > 0 && prdComponentDefaultQuantity.length() > 0 ) {
				if ( componentList.length() > 0 ) {
					componentList.append(COMPONENTS_SPEARATOR_CHARACTER);
				}
				componentList.append(prdComponentProductCode);
				componentList.append(COMPONENT_SPEARATOR_CHARACTER);
				componentList.append(prdComponentDefaultQuantity);
			}
			
		}
		
	}
	
	private String formatAsTs(String in) {
		
		String retTs = "";
		
		if ( in.length() >= 14 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8) + " " + in.substring(8, 10) + ":" + in.substring(10, 12) + ":" + in.substring(12, 14);
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

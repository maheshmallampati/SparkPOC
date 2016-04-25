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
import com.mcd.gdw.test.daas.driver.GenerateGDWFileFormat;

public class GDWFileFormatPreMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private String[] parts;
	private StringReader strReader;

	private Element eleRoot;
	private Element eleProduct;
	private Element eleProductItem;
	private Element eleComponent;
	private Element eleComponentItem;
	
	private Node nodeText;

	private String terrCd;
	private String calDt;
	private String productClass;
	private String productCode;
	private String componentProductCode;
	private String componentDefaultQty;
	
	private Text textKey = new Text();
	private Text textValue = new Text();
	
	private StringBuffer components = new StringBuffer();
	
	@Override
	public void setup(Context context) {

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
		} catch (Exception ex) {
			System.err.println("Error in initializing GDWFileFormatPreMapper:");
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

			if ( eleRoot.getNodeName().equals("ProductDb") ) {
				terrCd = eleRoot.getAttribute("gdwTerrCd");
				calDt = eleRoot.getAttribute("gdwBusinessDate");
				calDt = calDt.substring(0, 4) + "-" + calDt.substring(4, 6) + "-" + calDt.substring(6, 8);
				
				processProducts(eleRoot.getChildNodes(),context);
			}
			
		} catch ( Exception ex ) {
			eleRoot = (Element) doc.getFirstChild();
			System.err.println("Error in GDWFileFormatPreMapper.getData:");
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
	
	private void processProducts(NodeList nlProduct
			                    ,Context context) throws Exception {
		
		if (nlProduct != null && nlProduct.getLength() > 0 ) {
			for (int idxProduct=0; idxProduct < nlProduct.getLength(); idxProduct++ ) {
				if ( nlProduct.item(idxProduct).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProduct = (Element)nlProduct.item(idxProduct);
					
					if ( eleProduct.getNodeName().equals("Product") ) {
						productClass = eleProduct.getAttribute("productClass");
						
						if ( productClass.equals("VALUE_MEAL") ) {
							
							components.setLength(0);
							
							processProductItem(eleProduct.getChildNodes());
							
							if ( productCode.length() > 0 && components.length() > 0 ) {
								textKey.clear();
								textKey.set(calDt + GenerateGDWFileFormat.SEPARATOR_CHARACTER + terrCd + GenerateGDWFileFormat.SEPARATOR_CHARACTER + productCode);

								textValue.clear();
								textValue.set(components.toString());
								context.write(textKey, textValue);
							}
						}
					}
				}
			}
		}
		
	}
	
	private void processProductItem(NodeList nlProductItem) throws Exception {

		productCode = "";
		
		if (nlProductItem != null && nlProductItem.getLength() > 0 ) {
			for (int idxProductItem=0; idxProductItem < nlProductItem.getLength(); idxProductItem++ ) {
				if ( nlProductItem.item(idxProductItem).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProductItem = (Element)nlProductItem.item(idxProductItem);

					nodeText = eleProductItem.getFirstChild();
					
					if ( eleProductItem.getNodeName().equals("ProductCode") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
						productCode = nodeText.getNodeValue();
						
					} else if ( eleProductItem.getNodeName().equals("Composition") ) {
						processComponent(eleProductItem.getChildNodes());
					}
				}
			}
		}
	
	}

	private void processComponent(NodeList nlComponent) throws Exception {
		
		if (nlComponent != null && nlComponent.getLength() > 0 ) {
			for (int idxComponent=0; idxComponent < nlComponent.getLength(); idxComponent++ ) {
				if ( nlComponent.item(idxComponent).getNodeType() == Node.ELEMENT_NODE ) {  
					eleComponent = (Element)nlComponent.item(idxComponent);
					if ( eleComponent.getNodeName().equals("Component") ) {
						processComponentItem(eleComponent.getChildNodes());
					}
				}
			}
		}
		
	}

	private void processComponentItem(NodeList nlComponentItem) throws Exception {
		
		componentProductCode = "";
		componentDefaultQty = "";
		
		if (nlComponentItem != null && nlComponentItem.getLength() > 0 ) {
			for (int idxComponentItem=0; idxComponentItem < nlComponentItem.getLength(); idxComponentItem++ ) {
				if ( nlComponentItem.item(idxComponentItem).getNodeType() == Node.ELEMENT_NODE ) {  
					eleComponentItem = (Element)nlComponentItem.item(idxComponentItem);
					
					nodeText = eleComponentItem.getFirstChild();
					
					if ( eleComponentItem.getNodeName().equals("ProductCode") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
						componentProductCode = nodeText.getNodeValue();
						
					} else if ( eleComponentItem.getNodeName().equals("DefaultQuantity") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
						componentDefaultQty = nodeText.getNodeValue();
						
					}
				}
			}
		}
		
		if ( componentProductCode.length() > 0 && componentDefaultQty.length() > 0 ) {
			if ( components.length() > 0 ) {
				components.append(GenerateGDWFileFormat.COMPONENT_SEPARATOR_CHARACTER);
			}
			components.append(componentProductCode);
			components.append(GenerateGDWFileFormat.COMPONENT_ITEM_SEPARATOR_CHARACTER);
			components.append(componentDefaultQty);
		}
		
	}
}

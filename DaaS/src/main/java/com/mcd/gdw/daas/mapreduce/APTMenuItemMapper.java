package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.APTDriver;

/**
 * 
 * Mapper class to parse MenuItem XML files
 * 
 */
public class APTMenuItemMapper extends Mapper<LongWritable, Text, Text, Text>{

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private Document document = null;
	private Element rootElement;
	private NodeList productInfoNodeList;
	private Element productInfoElement;
	
	private String[] inputParts = null;
	private String xmlText = "";
	
	private String lgcyLclRfrDefCd = "";
	private String businessDate = "";
	
	private Text menuItemKeyTxt = null;
	private Text menuItemValueTxt = null;
	private StringBuffer menuItemKeyStrBuf = null;
	private StringBuffer menuItemValueStrBuf = null;
	
	/**
	 * Setup method
	 */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {	
        docFactory = DocumentBuilderFactory.newInstance();
        menuItemKeyTxt = new Text();
        menuItemValueTxt = new Text();
        menuItemKeyStrBuf = new StringBuffer();
        menuItemValueStrBuf = new StringBuffer();
        
        try {
        	docBuilder = docFactory.newDocumentBuilder();
        } catch (ParserConfigurationException pce) {
        	pce.printStackTrace();
        }	
	}
	
	/**
	 * Map method to parse XML and generate psv output
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		inputParts = value.toString().split(DaaSConstants.TAB_DELIMITER);
		xmlText = inputParts[DaaSConstants.XML_REC_XML_TEXT_POS];
		
		try {
			
			document = docBuilder.parse(new InputSource(new StringReader(xmlText)));
			rootElement = (Element) document.getFirstChild();

			if(rootElement.getNodeName().equals("MenuItem")) {
				businessDate = rootElement.getAttribute("gdwBusinessDate");
				lgcyLclRfrDefCd = String.valueOf(Integer.parseInt(rootElement.getAttribute("gdwLgcyLclRfrDefCd")));
				productInfoNodeList = rootElement.getChildNodes();
				
				if (productInfoNodeList != null && productInfoNodeList.getLength() > 0) {
					for (int idxNode = 0; idxNode < productInfoNodeList.getLength(); idxNode++) {
						if(productInfoNodeList.item(idxNode).getNodeType() == Node.ELEMENT_NODE) {
							productInfoElement = (Element) productInfoNodeList.item(idxNode);
							menuItemKeyStrBuf.setLength(0);
							menuItemValueStrBuf.setLength(0);
							
							menuItemKeyStrBuf.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER).append(businessDate);
							menuItemValueStrBuf.append(APTDriver.MENU_ITEM_TAG_INDEX).append(DaaSConstants.PIPE_DELIMITER)
												  .append(productInfoElement.getAttribute("id")).append(DaaSConstants.PIPE_DELIMITER)
												  .append(productInfoElement.getAttribute("eatinPrice"));
							
							menuItemKeyTxt.set(menuItemKeyStrBuf.toString());
							menuItemValueTxt.set(menuItemValueStrBuf.toString());
							
							context.write(menuItemKeyTxt, menuItemValueTxt);
						}
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Clean up method
	 */
	@Override
	public void cleanup(Context context) throws IOException ,InterruptedException {
		menuItemKeyTxt.clear();
		menuItemValueTxt.clear();
	}
}
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
import com.mcd.gdw.daas.driver.NextGenEmixDriver;

public class EmixMenuItemMapper extends Mapper<LongWritable, Text, Text, Text>{

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private Document document = null;
	private Element rootElement;
	private NodeList productInfoNodeList;
	private Element productInfoElement;
	
	private String xmlText = "";
	private String lgcyLclRfrDefCd = "";
	private String businessDate = "";
	
	/**
	 * Setup method
	 */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {	
        docFactory = DocumentBuilderFactory.newInstance();
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
		
		xmlText = value.toString().substring(value.toString().indexOf("<"));
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
							context.write(new Text(lgcyLclRfrDefCd + DaaSConstants.PIPE_DELIMITER + businessDate + DaaSConstants.PIPE_DELIMITER + productInfoElement.getAttribute("id")), 
										  new Text(new StringBuffer(NextGenEmixDriver.MENU_ITEM_TAG_INDEX).append(DaaSConstants.PIPE_DELIMITER)
												  .append(productInfoElement.getAttribute("eatinPrice")).toString()));
						}
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
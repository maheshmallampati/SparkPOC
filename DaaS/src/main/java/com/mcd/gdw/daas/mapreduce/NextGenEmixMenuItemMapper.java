package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.NextGenEmixDriver;
import com.mcd.gdw.daas.util.NextGenEmixUtil;

/**
 * @author Naveen Kumar B.V
 *
 * Mapper class to parse MenuItem XML files and extract the fields required for processing.
 * 
 */
public class NextGenEmixMenuItemMapper extends Mapper<LongWritable, Text, Text, Text>{

	private MultipleOutputs<Text, Text> multipleOutputs;
	
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
	
	private boolean isValidInput = false;
	
	/**
	 * Setup method to create all the required objects for processing.
	 */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {	
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
		
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
	 * Map method to parse a MenuItem xml file. The output data from this mapper is written to context.
	 * Key: StoreId|Date
	 * Value: M|ItemCode|eatinPrice
	 * 
	 * StoreId|Date is used as the join key in the reducer to join pmix output from NetxtGenEmixStldMapper.
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		inputParts = value.toString().split(DaaSConstants.TAB_DELIMITER);
		xmlText = inputParts[DaaSConstants.XML_REC_XML_TEXT_POS];
		
		try {
			document = docBuilder.parse(new InputSource(new StringReader(xmlText)));
			rootElement = (Element) document.getFirstChild();

			if(rootElement != null && rootElement.getNodeName().equals("MenuItem")) {
				
				isValidInput = NextGenEmixUtil.validateMenuItemAttrs(rootElement, multipleOutputs, "");
				
				if (isValidInput) {
					
					businessDate = rootElement.getAttribute("gdwBusinessDate");
					lgcyLclRfrDefCd = String.valueOf(Integer.parseInt(rootElement.getAttribute("gdwLgcyLclRfrDefCd")));
					productInfoNodeList = rootElement.getChildNodes();
					
					if (productInfoNodeList != null && productInfoNodeList.getLength() > 0) {
						for (int idxNode = 0; idxNode < productInfoNodeList.getLength(); idxNode++) {
							if(productInfoNodeList.item(idxNode).getNodeType() == Node.ELEMENT_NODE) {
								
								productInfoElement = (Element) productInfoNodeList.item(idxNode);
								isValidInput = NextGenEmixUtil.validateProductInfoAttrs(productInfoElement, lgcyLclRfrDefCd, 
																businessDate, multipleOutputs, "");
								
								if (isValidInput) {
									menuItemKeyStrBuf.setLength(0);
									menuItemValueStrBuf.setLength(0);
									
									menuItemKeyStrBuf.append(lgcyLclRfrDefCd).append(DaaSConstants.PIPE_DELIMITER).append(businessDate);
									menuItemValueStrBuf.append(NextGenEmixDriver.MENU_ITEM_TAG_INDEX).append(DaaSConstants.PIPE_DELIMITER)
														  .append(productInfoElement.getAttribute("id")).append(DaaSConstants.PIPE_DELIMITER)
														  .append(productInfoElement.getAttribute("eatinPrice"));
									
									menuItemKeyTxt.set(menuItemKeyStrBuf.toString());
									menuItemValueTxt.set(menuItemValueStrBuf.toString());
									
									context.write(menuItemKeyTxt, menuItemValueTxt);
									
									/* Testing 
									multipleOutputs.write("menuitems", NullWritable.get(), menuItemKeyTxt + "=>" + menuItemValueTxt); 
									*/
								}
							}
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
		multipleOutputs.close();
	}
}
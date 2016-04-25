package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;

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

public class MenuItemExtractMapper extends Mapper<LongWritable,Text,NullWritable,Text>{

	Text outValue = new Text();
	StringBuffer outValbf = new StringBuffer();
	
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		

	    DocumentBuilderFactory docFactory;
	    DocumentBuilder docBuilder;
	    InputSource xmlSource;
	    Document xmlDoc;
	    Element eleTLD;

	    try {
	      String[] recArr = value.toString().split("\t");    
	     
	      docFactory = DocumentBuilderFactory.newInstance();
	      docBuilder = docFactory.newDocumentBuilder();
	      xmlSource = new InputSource(new StringReader(recArr[recArr.length -1]));
	      xmlDoc = docBuilder.parse(xmlSource);

	      eleTLD = (Element)xmlDoc.getFirstChild();

	      if ( eleTLD.getNodeName().equals("MenuItem") ) {
	        npParseXml(eleTLD, context);
	      }
	    } catch (Exception ex) {
	    	
	      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
	    }
		
	}

	
	  private void npParseXml(Element eleMenuItem, Context context) throws IOException, InterruptedException {

		    String gdwLgcyLclRfrDefCd;
		    String gdwBusinessDate;
		    String gdwTerrCd;
		    NodeList nlNode;
		    Element eleNode;
		    String storeId;

		    try {
		      gdwLgcyLclRfrDefCd = eleMenuItem.getAttribute("gdwLgcyLclRfrDefCd");
		      gdwBusinessDate = eleMenuItem.getAttribute("gdwBusinessDate");
		      gdwTerrCd = eleMenuItem.getAttribute("gdwTerrCd");
		      storeId   = eleMenuItem.getAttribute("storeId");
//		      if(!storeId.equalsIgnoreCase("865") || !gdwBusinessDate.equalsIgnoreCase("20120803"))
//		    	  return;

		      nlNode = eleMenuItem.getChildNodes();

		      if (nlNode != null && nlNode.getLength() > 0 ) {
		        for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
		          if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE  && (((Element)nlNode).getNodeName().equalsIgnoreCase("ProductInfo") ) ) {
		            eleNode = (Element)nlNode.item(idxNode);

		            
		            
		            
		            outValbf.setLength(0);
		            outValbf.append(eleNode.getAttribute("id")).append(DaaSConstants.PIPE_DELIMITER);
		            outValbf.append(eleNode.getAttribute("shortName")).append(DaaSConstants.PIPE_DELIMITER);
		            outValbf.append(eleNode.getAttribute("longName")).append(DaaSConstants.PIPE_DELIMITER);
		            outValbf.append(eleNode.getAttribute("familyGroup")).append(DaaSConstants.PIPE_DELIMITER);
		            outValbf.append(eleNode.getAttribute("grillGroup")).append(DaaSConstants.PIPE_DELIMITER);
		            outValbf.append(eleNode.getAttribute("class")).append(DaaSConstants.PIPE_DELIMITER);
		            
		            
		            outValue.clear();
		            outValue.set(outValbf.toString());
		            context.write(NullWritable.get(), outValue);
		          }
		        }
		      }
		    } catch (Exception ex) {
		      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
		    }
		  }
	
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	
	

}

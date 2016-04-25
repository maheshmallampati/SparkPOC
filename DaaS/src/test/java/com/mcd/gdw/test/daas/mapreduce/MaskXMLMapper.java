package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

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
import com.mcd.gdw.test.daas.driver.MaskXML;

public class MaskXMLMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static String[] parts = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private DocumentBuilderFactory docFactory;
	private DocumentBuilder docBuilder;
	private Document xmlDoc;
	private StringReader strReader;
	private InputSource xmlSource = null;
	private TransformerFactory transformerFactory;
	private Transformer transformer;
	private DOMSource source;

    private StringWriter xmlStringWriter;
    private StreamResult xmlStringResults;
	
	private Element eleRoot;
    private Element eleNode;
	private Element eleEvent;
	private Element eleTRX; 
	private Element eleOrder;
	private Element eleOrderItem; 
    private Element eleProductInfo;
    private Element eleStoreDb;
    private Element eleStoreDbItem;
    private Element eleStoreProfileItem;
    private Element eleDetailField;
	private Node nodeText;
	
	private String eventType;
	private String newStoreId;
	private String newStoreName;
	
	private String foundPartId;
	
	private HashMap<String,String> lcatHashMap = new HashMap<String,String>();
	private HashMap<String,String> partHashMap = new HashMap<String,String>();
	
	private String configCtryDecode;
	private int pos;
	private String newCtryIsoAbbr2;
	private Path path;
	private String fileName;
	
	
	@Override
	public void setup(Context context) {

		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;
	    String configCacheName;
	    String configPartName;
	    
		try {
		    path = ((FileSplit)context.getInputSplit()).getPath();
			fileName = path.getName();
			
			configCacheName = context.getConfiguration().get(MaskXML.CONFIG_CACHE_NAME); 
			configPartName = context.getConfiguration().get(MaskXML.CONFIG_PART_NAME); 
			configCtryDecode = context.getConfiguration().get(MaskXML.CONFIG_CTRY_DECODE);
			
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
		
			distPaths = context.getCacheFiles();
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		      
		    if ( distPaths != null && distPaths.length > 0 )  {
		    	  
		    	System.out.println(" number of distcache files : " + distPaths.length);
		    	  
		    	for ( int i=0; i<distPaths.length; i++ ) {
				     
			    	  System.out.println("distpaths:" + distPaths[i].toString());
			    	  
			    	  distPathParts = 	distPaths[i].toString().split("#");
			    	  
			    	  if( distPaths[i].toString().contains(configCacheName) ) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	  addLcatHashValuestoMap(br);
				      	  System.out.println("Loaded Mask Values Map");
				      	  
				      } else if ( distPaths[i].toString().contains(configPartName) ) {

				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addPartHashValuestoMap(br);
				      	  System.out.println("Loaded Part Values Map");
				      }
		    	}
		    }
			    	  
		} catch (Exception ex) {
			System.err.println("Error in initializing MaskXMLMapper:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void addLcatHashValuestoMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String terrCd;
		String lcat;
		String hashValue;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\t", -1);

					terrCd = parts[0];
					lcat = parts[1];
					hashValue = parts[2];
							
					lcatHashMap.put(terrCd + "|" + lcat,hashValue);
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
				ex.printStackTrace(System.err);
			}
		}

	}

	private void addPartHashValuestoMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String terrCd;
		String lcat;
		String calDt;
		String partNum;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\t", -1);

					terrCd = parts[0];
					lcat = parts[1];
					calDt = parts[2];
					partNum = parts[3];
							
					partHashMap.put(terrCd + "|" + lcat + "|" + calDt,partNum);
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
				ex.printStackTrace(System.err);
			}
		}

	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {

			context.getCounter("DaaS Counters", "Missing LCAT Hash").increment(0);
			context.getCounter("DaaS Counters", "Skipping non-part file").increment(0);

			parts = value.toString().split("\\t");
		
			if ( parts.length >= (DaaSConstants.XML_REC_XML_TEXT_POS+1) ) {
				context.getCounter("DaaS Counters", parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + " file count").increment(1);
				
				try {
					strReader  = new StringReader(parts[DaaSConstants.XML_REC_XML_TEXT_POS]);
					xmlSource = new InputSource(strReader);
					xmlDoc = docBuilder.parse(xmlSource);
				} catch (Exception ex1) {
					strReader  = new StringReader(parts[DaaSConstants.XML_REC_XML_TEXT_POS].replaceAll("&#x1F" , "_"));
					xmlSource = new InputSource(strReader);
					xmlDoc = docBuilder.parse(xmlSource);
				}

			    eleRoot= (Element)xmlDoc.getFirstChild();
			    
			    pos = configCtryDecode.indexOf(String.format("%03d",Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS])));
			    newCtryIsoAbbr2 = configCtryDecode.substring(pos+3, pos+5);
			    
			    if ( lcatHashMap.containsKey(parts[DaaSConstants.XML_REC_TERR_CD_POS] + "|" + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]) ) {
				    newStoreId = lcatHashMap.get(parts[DaaSConstants.XML_REC_TERR_CD_POS] + "|" + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]);
				    newStoreName = newStoreId + " location"; 
				    
				    eleRoot.removeAttribute("gdwFileName");
				    eleRoot.removeAttribute("gdwBusinessDate");
				    eleRoot.removeAttribute("gdwFileId");
				    eleRoot.removeAttribute("gdwMcdGbalLcatIdNu");
				    eleRoot.removeAttribute("gdwTerrCd");
				    eleRoot.removeAttribute("gdwLgcyLclRfrDefCd");

				    if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equals("STLD") ) {
				    	eleRoot.setAttribute("storeId", newStoreId);
				    	maskTLD(eleRoot.getChildNodes());

				    } else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equals("DetailedSOS") ) {
				    	
				    } else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equals("MenuItem") ) {
				    	eleRoot.setAttribute("storeId", newStoreId);
				    	eleRoot.setAttribute("storeName", newStoreName);
				    	maskMenuItem(eleRoot.getChildNodes());

				    } else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equals("SecurityData") ) {
				    	eleRoot.setAttribute("storeId", newStoreId);

				    } else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equals("Store-Db") ) {
				    	maskStoreDb(eleRoot.getChildNodes());
				    	
				    } else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equals("Product-Db") ) {

				    }
				    
				    if ( partHashMap.containsKey(parts[DaaSConstants.XML_REC_TERR_CD_POS] + "|" + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS] + "|" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]) ) {
				    	foundPartId = partHashMap.get(parts[DaaSConstants.XML_REC_TERR_CD_POS] + "|" + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS] + "|" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
				    } else {
				    	foundPartId = "0";
				    }
				    
					transformerFactory = TransformerFactory.newInstance();
					transformer = transformerFactory.newTransformer();
					source = new DOMSource(xmlDoc);

					xmlStringWriter = new StringWriter();
				    xmlStringResults = new StreamResult(xmlStringWriter);
			 
					transformer.transform(source, xmlStringResults);
					xmlStringWriter.flush();
					
					mapKey.clear();
					mapKey.set(newCtryIsoAbbr2 + "\t" + newStoreId + "\t" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
					
					mapValue.clear();
					mapValue.set(xmlStringWriter.toString()); //.substring(0,200));
					context.write(mapKey, mapValue);
			    } else {
			    	context.getCounter("DaaS Counters", "Missing LCAT Hash").increment(1);
			    }
			
			} else {
				context.getCounter("DaaS Counters", "Skipping non-part file").increment(1);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			
			try {
				System.err.println("TERRITORY CODE = " + parts[DaaSConstants.XML_REC_TERR_CD_POS]);
				System.err.println("LOCATION CODE  = " + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]);
				System.err.println("BUSINESS DATE  = " + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
				System.err.println("FILE TYPE      = " + parts[DaaSConstants.XML_REC_FILE_TYPE_POS]);
				System.err.println("FILE NAME      = " + fileName);
			} catch (Exception ex1) {
			}
			
			System.exit(8);
			
		}
	}

	private void maskTLD(NodeList nlNode) {
	    
	    if (nlNode != null && nlNode.getLength() > 0 ) {
			for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
				if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {  
					eleNode = (Element)nlNode.item(idxNode);
					if ( eleNode.getNodeName().equals("Node") ) {
						maskTLD_Event(eleNode.getChildNodes());
					}
				}
			}
	    }
	    
	}
	
	private void maskTLD_Event(NodeList nlEvent) {

	    if (nlEvent != null && nlEvent.getLength() > 0 ) {
			for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++ ) {
				if ( nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE ) {  
					eleEvent = (Element)nlEvent.item(idxEvent);
					
					if ( eleEvent.getNodeName().equals("Event") ) {
						eventType = eleEvent.getAttribute("Type");
						
						if ( eventType.equals("TRX_Sale") || eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring") || eventType.equals("TRX_Waste") ) {
							maskTLD_TRX(eleEvent.getChildNodes());
						}
					}
				}
			}
	    }
	}
	
	private void maskTLD_TRX(NodeList nlTRX) {
		
		if (nlTRX != null && nlTRX.getLength() > 0 ) {
			for (int idxTRX=0; idxTRX < nlTRX.getLength(); idxTRX++ ) {
				if ( nlTRX.item(idxTRX).getNodeType() == Node.ELEMENT_NODE ) {  
					eleTRX = (Element)nlTRX.item(idxTRX);
					
					maskTLD_Order(eleTRX.getChildNodes());
				}
			}
		}
	}
	
	private void maskTLD_Order(NodeList nlOrder) {
		
		if (nlOrder != null && nlOrder.getLength() > 0 ) {
			for (int idxOrder=0; idxOrder < nlOrder.getLength(); idxOrder++ ) {
				if ( nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {  
					eleOrder = (Element)nlOrder.item(idxOrder);
					
					maskTLD_OrderItems(eleOrder.getChildNodes());
				}
			}
		}
	}

	private void maskTLD_OrderItems(NodeList nlOrderItems) {
		
		if (nlOrderItems != null && nlOrderItems.getLength() > 0 ) {
			for (int idxOrderItems=0; idxOrderItems < nlOrderItems.getLength(); idxOrderItems++ ) {
				if ( nlOrderItems.item(idxOrderItems).getNodeType() == Node.ELEMENT_NODE ) {  
					eleOrderItem = (Element)nlOrderItems.item(idxOrderItems);
					
					maskTLD_Item(eleOrderItem);
				}
			}
		}
		
	}

	private void maskTLD_Item(Element eleItem) {
		
		NodeList nlItemSubItems; 
		Element eleOrderSubItem;
		
		if ( eleItem.getNodeName().equals("Item") ) {
			if ( eleItem.getAttribute("description") != null ) {
				eleItem.setAttribute("description", eleItem.getAttribute("code") + " description");
			}

			nlItemSubItems = eleItem.getChildNodes();
			
			if (nlItemSubItems != null && nlItemSubItems.getLength() > 0 ) {
				for (int idxOrderSubItems=0; idxOrderSubItems < nlItemSubItems.getLength(); idxOrderSubItems++ ) {
					if ( nlItemSubItems.item(idxOrderSubItems).getNodeType() == Node.ELEMENT_NODE ) {  
						eleOrderSubItem = (Element)nlItemSubItems.item(idxOrderSubItems);

						maskTLD_Item(eleOrderSubItem);
					}
				}
			}
		}
	}
	
	private void maskMenuItem(NodeList nlProductInfo) {
	    
	    if (nlProductInfo != null && nlProductInfo.getLength() > 0 ) {
			for (int idxProductNode=0; idxProductNode < nlProductInfo.getLength(); idxProductNode++ ) {
				if ( nlProductInfo.item(idxProductNode).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProductInfo = (Element)nlProductInfo.item(idxProductNode);
					if ( eleProductInfo.getNodeName().equals("ProductInfo") ) {
						eleProductInfo.setAttribute("shortName", eleProductInfo.getAttribute("id") + " short name");
						eleProductInfo.setAttribute("longName", eleProductInfo.getAttribute("id") + " long name");
					}
				}
			}
	    }
	    
	}

	
	private void maskStoreDb(NodeList nlStoreDb) {
	    
	    if (nlStoreDb != null && nlStoreDb.getLength() > 0 ) {
			for (int idxStoreDb=0; idxStoreDb < nlStoreDb.getLength(); idxStoreDb++ ) {
				if ( nlStoreDb.item(idxStoreDb).getNodeType() == Node.ELEMENT_NODE ) {  
					eleStoreDb = (Element)nlStoreDb.item(idxStoreDb);
					
					if ( eleStoreDb.getNodeName().equals("StoreDB") ) {
						maskStoreDb_Items(eleStoreDb.getChildNodes());
					}
				}
			}
	    }
	    
	}

	private void maskStoreDb_Items(NodeList nlStoreDbItem) {

		if (nlStoreDbItem != null && nlStoreDbItem.getLength() > 0 ) {
			for (int idxStoreDbItem=0; idxStoreDbItem < nlStoreDbItem.getLength(); idxStoreDbItem++ ) {
				if ( nlStoreDbItem.item(idxStoreDbItem).getNodeType() == Node.ELEMENT_NODE ) {  
					eleStoreDbItem = (Element)nlStoreDbItem.item(idxStoreDbItem);
					
					if ( eleStoreDbItem.getNodeName().equals("StoreProfile") ) {
						maskStoreDb_StoreProfile(eleStoreDbItem.getChildNodes());
						
					} else if ( eleStoreDbItem.getNodeName().equals("TaxTable") ) {
						
					}
				}
			}
	    }
		
	}

	private void maskStoreDb_StoreProfile(NodeList nlStoreProfile) {

		if (nlStoreProfile != null && nlStoreProfile.getLength() > 0 ) {
			for (int idxStoreProfileItem=0; idxStoreProfileItem < nlStoreProfile.getLength(); idxStoreProfileItem++ ) {
				if ( nlStoreProfile.item(idxStoreProfileItem).getNodeType() == Node.ELEMENT_NODE ) {  
					eleStoreProfileItem = (Element)nlStoreProfile.item(idxStoreProfileItem);
					
					if ( eleStoreProfileItem.getNodeName().equals("StoreDetails") ) {
						maskStoreDb_DetailsFields(eleStoreProfileItem.getNodeName(),eleStoreProfileItem.getChildNodes());
						
					} else if ( eleStoreProfileItem.getNodeName().equals("TaxDefinition") ) {
						maskStoreDb_DetailsFields(eleStoreProfileItem.getNodeName(),eleStoreProfileItem.getChildNodes());

					}
				}
			}
	    }
	}

	private void maskStoreDb_DetailsFields(String type, NodeList nlStoreProfileFields) {

		if (nlStoreProfileFields != null && nlStoreProfileFields.getLength() > 0 ) {
			for (int idxStoreProfileFields=0; idxStoreProfileFields < nlStoreProfileFields.getLength(); idxStoreProfileFields++ ) {
				if ( nlStoreProfileFields.item(idxStoreProfileFields).getNodeType() == Node.ELEMENT_NODE ) {  
					eleDetailField = (Element)nlStoreProfileFields.item(idxStoreProfileFields);
	
					if ( type.equals("StoreDetails") ) {
						if ( !eleDetailField.getNodeName().equals("StoreType") ) {
							nodeText = eleDetailField.getFirstChild();

							if ( nodeText != null ) {
								nodeText.setNodeValue("XXX");
							}
						}
					} else if ( type.equals("TaxDefinition")  ) {
						if ( eleDetailField.getNodeName().equals("LegalName") ||
							 eleDetailField.getNodeName().equals("DefaultReceiptHeader") ||
							 eleDetailField.getNodeName().equals("DefaultReceiptFooter") ) {
							nodeText = eleDetailField.getFirstChild();
							
							if ( nodeText != null ) {
								nodeText.setNodeValue("YYY");
							}
						}
					}
				}
			}
		}
	}
}

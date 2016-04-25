package com.mcd.gdw.daas.driver;

import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.util.SimpleEncryptAndDecrypt;

public class Utility {

	private static HashMap<String, String> configMap = new HashMap<String, String>();

	public static void main(String[] args) throws Exception {

		Utility instance = new Utility();
		
		if ( args.length == 1 && args[0].equalsIgnoreCase("-H") ) {
			System.out.println("Usage Utility -E encryptText | -D decryptText");
			System.out.println("or    Utility -C configXmlFile -A attribute");
			System.exit(0);
		}
		
		if ( args.length == 2 && ( args[0].equalsIgnoreCase("-D") || args[0].equalsIgnoreCase("-E") ) ) {
			instance.encryptDecrypt(args[0],args[1]);
		} else { 
			if ( args.length == 4 && (( args[0].equalsIgnoreCase("-C") && args[2].equalsIgnoreCase("-A") ) ||
					                  ( args[0].equalsIgnoreCase("-A") && args[2].equalsIgnoreCase("-C") ) ) ) {
				
				String configFile = "";
				String attribute = "";

				if ( args[0].equalsIgnoreCase("-C") ) {
					configFile = args[1];
					attribute = args[3];
				} else {
					configFile = args[3];
					attribute = args[1];
				}
				
				instance.getAttribute(configFile,attribute);
				
			} else {
				System.err.println("Missing 1 or more arguments:");
				System.err.println("Usage Utility -E encryptText | -D decryptText");
				System.out.println("or    Utility -C configXmlFile -A attribute");
				System.exit(8);
			}
		}
	}

	private void encryptDecrypt(String type
			                   ,String value) throws Exception {

		SimpleEncryptAndDecrypt sec = new SimpleEncryptAndDecrypt();
		
		if (type.equalsIgnoreCase("-E") ) {
			System.out.println(type + "=" + sec.encryptAsHexString(value) );
		}

		if (type.equalsIgnoreCase("-D") ) {
			System.out.println(type + "=" + sec.decryptFromHexString(value) );
		}
	}
	
	private void getAttribute(String configXmlFile
			                 ,String attribute) throws Exception {
		
		parseConfig(configXmlFile);
		
		System.out.println(configMap.get(attribute));
		
	}
	
	private void parseConfig(String configXmlFile) throws Exception {
		
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		InputSource xmlSource = new InputSource(configXmlFile);
		Document doc = docBuilder.parse(xmlSource);
		Element eleRoot = (Element)doc.getFirstChild();
		NodeList parmsNodeList = eleRoot.getChildNodes();

		if ( parmsNodeList != null && parmsNodeList.getLength() > 0 ) {
			for ( int idx=0; idx < parmsNodeList.getLength(); idx++ ) {
				
				if ( parmsNodeList.item(idx).getNodeType() == Node.ELEMENT_NODE ) {
					Element parmSection = (Element)parmsNodeList.item(idx);
					
					String nodeName = parmSection.getNodeName();
					boolean isFileSection = false;
					
					if ( nodeName.equals("File") ) {
						isFileSection = true;
					}
					
					if ( isFileSection ) {
						nodeName = nodeName + "(" + parmSection.getAttribute("fileType") + ")";
					}
					
					NamedNodeMap attributesList = parmSection.getAttributes();
					
				    for (int j = 0; j < attributesList.getLength(); j++) {
				    	if ( !(isFileSection && attributesList.item(j).getNodeName().equals("fileType")) ) {
					        //System.out.println(nodeName + "." + attributesList.item(j).getNodeName() + " = " + attributesList.item(j).getNodeValue());
					        configMap.put(nodeName + "." + attributesList.item(j).getNodeName(), attributesList.item(j).getNodeValue());
				    	}
				    }
				}
			}
		}

	}
}

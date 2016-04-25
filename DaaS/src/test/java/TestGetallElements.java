import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;


import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class TestGetallElements {

	public static void main(String[] args) throws SAXException, IOException,
    ParserConfigurationException, TransformerException {

	DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
	        .newInstance();
	DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
	Document document = docBuilder.parse(new File(args[0]));
	
	NodeList nodeList = document.getElementsByTagName("*");
	TreeSet<String> orderedSet = new TreeSet<String>();
	
	for (int i = 0; i < nodeList.getLength(); i++) {
	    Node node = nodeList.item(i);
	    if (node.getNodeType() == Node.ELEMENT_NODE) {
	        // do something with the current element
//	        System.out.println(node.getNodeName());
	    	orderedSet.add(node.getNodeName());
	    }
	}
	
	File outfile = new File(args[1]);
	BufferedWriter output = new BufferedWriter(new FileWriter(outfile));
	Iterator<String> it = orderedSet.iterator();
	while(it.hasNext()){
		output.write(it.next());
		output.write("\n");
	}
	output.close();
	}
}

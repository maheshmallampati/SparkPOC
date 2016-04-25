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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;

public class GoldToZipMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final String SEPARATOR_CHARACTER = "|";
	
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
	
	private HashMap<String,String> lcatHashMap = new HashMap<String,String>();

    private StringWriter xmlStringWriter;
    private StreamResult xmlStringResults;
	
	private Element eleRoot;
	
	@Override
	public void setup(Context context) {

		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;
	    
		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
			distPaths = context.getCacheFiles();
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		     
     	    System.out.println("distpaths:" + distPaths[0].toString());
	    	  
	    	distPathParts = distPaths[0].toString().split("#");
	    	  
	    	br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
	    	addLcatHashValuestoMap(br);
	    	System.out.println("Loaded Location Selection List Map");
			    	  
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	private void addLcatHashValuestoMap(BufferedReader br) {
	
		String line = null;
		String[] parts;
	
		String terrCd;
		String lcat;
	
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\t", -1);

					terrCd = parts[0];
					lcat = parts[1];
						
					lcatHashMap.put(terrCd + SEPARATOR_CHARACTER + lcat,"");
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
				ex.printStackTrace();
			}
		}

	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {

			context.getCounter("DaaS Counters", "Skipping non-selected XML file").increment(0);
			context.getCounter("DaaS Counters", "Skipping non-part file").increment(0);

			parts = value.toString().split("\\t");
		
			if ( parts.length >= (DaaSConstants.XML_REC_XML_TEXT_POS+1) ) {
			
				strReader  = new StringReader(parts[DaaSConstants.XML_REC_XML_TEXT_POS]);
				xmlSource = new InputSource(strReader);

				xmlDoc = docBuilder.parse(xmlSource);
			    eleRoot= (Element)xmlDoc.getFirstChild();
			    
			    if (  lcatHashMap.containsKey(parts[DaaSConstants.XML_REC_TERR_CD_POS] + SEPARATOR_CHARACTER + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]) ) {
					context.getCounter("DaaS Counters", "Selected " + parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + " file count").increment(1);
					    
				    eleRoot.removeAttribute("gdwFileName");
				    eleRoot.removeAttribute("gdwBusinessDate");
				    eleRoot.removeAttribute("gdwFileId");
				    eleRoot.removeAttribute("gdwMcdGbalLcatIdNu");
				    eleRoot.removeAttribute("gdwTerrCd");
				    eleRoot.removeAttribute("gdwLgcyLclRfrDefCd");
					    
					transformerFactory = TransformerFactory.newInstance();
					transformer = transformerFactory.newTransformer();
					source = new DOMSource(xmlDoc);

					xmlStringWriter = new StringWriter();
					xmlStringResults = new StreamResult(xmlStringWriter);
				 
					transformer.transform(source, xmlStringResults);
					xmlStringWriter.flush();
						
					mapKey.clear();
					mapKey.set(parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS] + "\t" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
						
					mapValue.clear();
					mapValue.set(xmlStringWriter.toString()); 
					context.write(mapKey, mapValue);
					
			    } else {
			    	context.getCounter("DaaS Counters", "Skipping non-selected XML file").increment(1);
			    }
			
			} else {
				context.getCounter("DaaS Counters", "Skipping non-part file").increment(1);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
			
		}
	}

}

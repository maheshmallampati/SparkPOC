
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.validation.Validator;

import org.apache.hadoop.io.compress.SnappyCodec;
import org.w3c.dom.Document;
import org.w3c.dom.ls.LSResourceResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class TestXmlValidation {

	public static void main(String[] args){
		try{
		String fileName = "C:/Users/mc32445/Desktop/DaaS/"+args[0];
		
		byte[] filebytes = read(new File(fileName));
		
		FileReader fr = new FileReader(new File(fileName));
		
		BufferedReader br = new BufferedReader(fr);
		
		StringBuilder sb = new StringBuilder();
		String line;
		while( (line = br.readLine()) != null){
			sb.append(line);
		}
		System.out.println(" calling xml validation ");
		
		System.out.println(" Dom validation " + new TestXmlValidation().validateXMLUsingDom(new StringBuffer(new String(filebytes))));
		
//		System.out.println(" Sax validation " + new TestXmlValidation().validateXMLUsingSax(fixXML(filebytes)));
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public static byte[] read(File file) throws IOException {



	    byte []buffer = new byte[(int) file.length()];
	    InputStream ios = null;
	    try {
	        ios = new FileInputStream(file);
	        if ( ios.read(buffer) == -1 ) {
	            throw new IOException("EOF reached while trying to read the whole file");
	        }        
	    } finally { 
	        try {
	             if ( ios != null ) 
	                  ios.close();
	        } catch ( IOException e) {
	        }
	    }

	    return buffer;
	}
	
	private static StringBuffer fixXML(byte[] flbytes){
		StringBuffer xmlBuffer = new StringBuffer(flbytes.length);
		System.out.println(" fix received " +xmlBuffer.toString() );
		try{

			
			String valuebytesstr = new String(flbytes,"UTF-8");
		
			xmlBuffer.append(valuebytesstr);
			
			int outStart = 0;
			int outSize = xmlBuffer.length();
	
			boolean foundStartEndTag = false;
	
			while (foundStartEndTag == false && outStart < outSize) {
				if (xmlBuffer.charAt(outStart) == '<') {
					foundStartEndTag = true;
				} else {
					outStart++;
				}
			}
	
			if (outStart > 0) {
				xmlBuffer = xmlBuffer.delete(0, outStart);
			}
	
			outStart = 0;
			outSize = xmlBuffer.length();
			int outSizeOrig = outSize;
	
			foundStartEndTag = false;
	
			while (foundStartEndTag == false && outSize > 0) {
				if (xmlBuffer.charAt(outSize - 1) == '>') {
					foundStartEndTag = true;
				} else {
					outSize--;
				}
			}
	
			if (outSize != outSizeOrig) {
				xmlBuffer = xmlBuffer.delete(outSize, outSizeOrig);
			}
			if (outStart > 0) {
				outSize -= (outStart - 1);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		System.out.println(" fix returning " + xmlBuffer.toString());
		return xmlBuffer;
		
	}
	
	public boolean validateXMLUsingDom(StringBuffer filebuf) {
		
//		System.out.println( " Dom received " + filebuf.toString());
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		Document document =null;
		InputSource xmlSource = null;
		try{
			
			factory.setValidating(true);
//			factory.setNamespaceAware(true);
			
		
		
	
			builder = factory.newDocumentBuilder();
		
	
			xmlSource = new InputSource(new StringReader(filebuf.toString().trim()));
			document = builder.parse(xmlSource);
		}catch(Exception ex){
			ex.printStackTrace();
			System.out.println(" DOM validation returning false ");
			return false;
		}finally{
			document = null;
			builder = null;
			factory = null;
			
					
		}
		
		System.out.println(" DOM validation returning true ");
		return true;
	}
	public boolean validateXMLUsingSax(StringBuffer filebuf) {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser parser  = null;
		XMLReader reader = null;
		InputSource xmlSource = null;
		try {
			
			
			
			factory.setValidating(true);
			factory.setNamespaceAware(true);

			parser = factory.newSAXParser();
		

			reader = parser.getXMLReader();
			xmlSource = new InputSource(new StringReader(filebuf.toString().trim()));
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		} finally {
			reader = null;
			parser = null;
			factory = null;
		}

		return true;
	}
}

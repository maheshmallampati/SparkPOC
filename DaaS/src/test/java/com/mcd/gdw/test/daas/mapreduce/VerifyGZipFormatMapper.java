package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.mcd.gdw.daas.DaaSConstants;

public class VerifyGZipFormatMapper extends Mapper<LongWritable, Text, Text, Text> {

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	@SuppressWarnings("unused")
	private Document doc = null;
	private StringReader strReader = null;

	private FileSplit fileSplit = null;
	private String fileName = "";

	private String[] parts = null;
	
	private StringBuffer outKey = new StringBuffer();
	private StringBuffer outValue = new StringBuffer();
	
	private Text mapKey = new Text();
	private Text mapValue = new Text();


	private String fileSubType;
	private String terrCd;
	private String posBusnDt;
	private String lgcyLclRfrDefCd;
	private String mcdGbalLcatIdNu;

	@Override
	public void setup(Context context) {
	      
        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();

      
        
	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			
			
			parts = value.toString().split("\t");
			
		} catch(Exception ex) { 
			try{
				
			}catch(Exception ex1){
				
			}
		}
		
	}
}

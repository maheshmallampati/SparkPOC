package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.GenerateExtractICRawXMLFormat;

public class ExtractICRawXMLFormatMapper  extends Mapper<LongWritable, Text, Text, Text> {

	private String[] parts;
	private String fileTypes;
	private boolean isHistoryRequest; 
	private HashMap<String,String> selectedValueListMap = new HashMap<String,String>();
	private StringBuffer textBuffer = new StringBuffer();
	private String fileType;
	private String posBusnDt;
	private String terrCd;
	private String lgcyLclRfrDefCd;
	
	private StringBuffer lookupKey = new StringBuffer();

	private int tabPos1;
	private int tabPos2;
	private int tabPos3;
	private int tabPos4;
	private int tabPos5;
	private int tabPos6;

	private Text keyOut = new Text();
	private Text valueOut = new Text();
	
	private StringBuffer valueOutText = new StringBuffer();
	
	@Override
	public void setup(Context context) {
	      
		URI[] distPaths;
	    BufferedReader br = null;
	    String distFileName;

	    try {
    	
	    	fileTypes = context.getConfiguration().get(GenerateExtractICRawXMLFormat.CONFIG_SETTING_FILE_TYPES);
	    	isHistoryRequest = context.getConfiguration().get(GenerateExtractICRawXMLFormat.CONFIG_SETTING_IS_HISTORY_REQUEST).equalsIgnoreCase("TRUE");
	    	
	    	distPaths = context.getCacheFiles();
	    	
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		      
		    if ( distPaths != null && distPaths.length > 0 )  {
		    	  
		    	System.out.println(" number of distcache files : " + distPaths.length);
		    	  
		    	for ( int i=0; i<distPaths.length; i++ ) {
			    	  System.out.println("distpaths:" + distPaths[i].toString());
			    	  
			    	  parts = distPaths[i].toString().split("#");
			    	  distFileName = parts[1];
			    	  
			    	  if( distPaths[i].toString().contains(GenerateExtractICRawXMLFormat.DIST_CACHE_SELECTION_LIST) ) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distFileName)); 

			    		  addSelectedListValuestoMap(br);
			    		  
			    		  System.out.println("Selected List Map");
				      }
			      }
		      }
			
		} catch (Exception ex) {
			System.err.println(ex.toString());
			System.exit(8);
		}
	}
	

	private void addSelectedListValuestoMap(BufferedReader br) {
	
		String line = null;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER, -1);

					lookupKey.setLength(0);
					lookupKey.append(parts[0]);
					lookupKey.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
					lookupKey.append(parts[1]);
					
					if ( isHistoryRequest ) {
						lookupKey.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
						lookupKey.append(parts[2]);
					}
					
					selectedValueListMap.put(lookupKey.toString(), "");
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
		
		tabPos1 = getNextTabPos(value,0);
		tabPos2 = getNextTabPos(value,tabPos1+1);
		tabPos3 = getNextTabPos(value,tabPos2+1);
		tabPos4 = getNextTabPos(value,tabPos3+1);
		tabPos5 = getNextTabPos(value,tabPos4+1);
		tabPos6 = getNextTabPos(value,tabPos5+1);

		if ( tabPos6 > 0 ) {
			
			fileType = getTextSubString(value,0,tabPos1);
			posBusnDt = getTextSubString(value,tabPos1+1,tabPos2);
			terrCd = getTextSubString(value,tabPos4+1,tabPos5);
			lgcyLclRfrDefCd = getTextSubString(value,tabPos5+1,tabPos6);

			context.getCounter("DaaS Map","Read XML File: " + fileType).increment(1);
			context.getCounter("DaaS Map","Write XML File: " + fileType).increment(0);
			context.getCounter("DaaS Map","Invalid XML File: " + fileType).increment(0);
			context.getCounter("DaaS Map","Skip XML File: " + fileType).increment(0);
			context.getCounter("DaaS Map","Invalid Record").increment(0);
			
			lookupKey.setLength(0);
			lookupKey.append(terrCd);
			lookupKey.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
			lookupKey.append(lgcyLclRfrDefCd);

			if ( isHistoryRequest ) {
				lookupKey.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
				lookupKey.append(posBusnDt);
			}
			
			if ( fileTypes.contains(fileType) && selectedValueListMap.containsKey(lookupKey.toString()) ) {
				parts = value.toString().split(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
				
				if ( parts.length >= 8 ) {
					
					keyOut.clear();
					keyOut.set(terrCd + GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER + lgcyLclRfrDefCd + GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER + posBusnDt);

					valueOutText.setLength(0);
					valueOutText.append(fileType);
					valueOutText.append(GenerateExtractICRawXMLFormat.SEPARATOR_CHARACTER);
					valueOutText.append(parts[DaaSConstants.XML_REC_XML_TEXT_POS]);
					
					valueOut.clear();
					valueOut.set(valueOutText.toString());

					context.getCounter("DaaS Map","Write XML File: " + fileType).increment(1);
					
					context.write(keyOut, valueOut);
				} else {
					context.getCounter("DaaS Map","Invalid XML File: " + fileType).increment(1);
				}
			} else {
				context.getCounter("DaaS Map","Skip XML File: " + fileType).increment(1);
			}
		} else {
			context.getCounter("DaaS Map","Invalid Record").increment(1);
		}
	}
	
	private int getNextTabPos(Text value
			                 ,int strtPos) {

		int retPos = strtPos;
		int charValue;
		
		try {
			
			charValue = value.charAt(retPos);
			
			while ( charValue > -1 && charValue != '\t' ) {
				retPos++;
				charValue = value.charAt(retPos);
			}
			
		} catch (Exception ex) {
			retPos = -1;
		}

		return(retPos);
	}
	
	private String getTextSubString(Text value
			                       ,int strtPos
			                       ,int endPos) {
		
		textBuffer.setLength(0);
				
		for ( int pos=strtPos; pos < endPos; pos++ ) {
			textBuffer.append((char)value.charAt(pos));
		}
		
		return(textBuffer.toString());
		
	}

}

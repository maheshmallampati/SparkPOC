package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.driver.ExtractRawXML;

public class ExtractRawXMLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private String[] parts;
	private String fileTypes;
	private String ctryToISO;
	private String timestamp;
	private HashMap<String,String> selectedValueListMap = new HashMap<String,String>();
	private StringBuffer textBuffer = new StringBuffer();
	private String fileType;
	private String posBusnDt;
	private String terrCd;
	private String terrCdLookup;
	private int terrCdPos;
	private String lgcyLclRfrDefCd;
	private String fileName;

	private int tabPos1;
	private int tabPos2;
	private int tabPos3;
	private int tabPos4;
	private int tabPos5;
	private int tabPos6;
	//private int tabPos7;
	//private int tabPos8;
	
	private MultipleOutputs<NullWritable, Text> mos;
	
	private Text valueOut = new Text();
	
	@Override
	public void setup(Context context) {
	      
		URI[] distPaths;
	    BufferedReader br = null;
	    String distFileName;

	    try {
	    	mos = new MultipleOutputs<NullWritable, Text>(context);
	    	
	    	fileTypes = context.getConfiguration().get(ExtractRawXML.CONFIG_SETTING_FILE_TYPES);
	    	ctryToISO = context.getConfiguration().get(ExtractRawXML.CONFIG_SETTING_CTRY_ISO2_LOOKUP);
	    	timestamp = context.getConfiguration().get(ExtractRawXML.CONFIG_SETTING_TIMESTAMP);
	    	
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
			    	  
			    	  if( distPaths[i].toString().contains(ExtractRawXML.DIST_CACHE_SELECTION_LIST) ) {
			    		  		      	  
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
					parts = line.split(ExtractRawXML.SEPARATOR_CHARACTER, -1);

					terrCd          = parts[0];
					lgcyLclRfrDefCd = parts[1];
					posBusnDt       = parts[2];
					
					selectedValueListMap.put(terrCd + ExtractRawXML.SEPARATOR_CHARACTER + lgcyLclRfrDefCd + ExtractRawXML.SEPARATOR_CHARACTER + posBusnDt, "");
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
		//tabPos7 = getNextTabPos(value,tabPos6+1);
		//tabPos8 = getNextTabPos(value,tabPos7+1);

		if ( tabPos6 > 0 ) {
			
			fileType = getTextSubString(value,0,tabPos1);
			posBusnDt = getTextSubString(value,tabPos1+1,tabPos2);
			terrCd = getTextSubString(value,tabPos4+1,tabPos5);
			lgcyLclRfrDefCd = getTextSubString(value,tabPos5+1,tabPos6);

			context.getCounter("DaaS","Read XML File: " + fileType).increment(1);
			
			if ( fileTypes.contains(fileType) && selectedValueListMap.containsKey(terrCd + ExtractRawXML.SEPARATOR_CHARACTER + lgcyLclRfrDefCd + ExtractRawXML.SEPARATOR_CHARACTER + posBusnDt) ) {
				parts = value.toString().split(ExtractRawXML.SEPARATOR_CHARACTER);
				
				if ( parts.length >= 8 ) {
					terrCdLookup = (terrCd + "   ").substring(0, 3);
					
					terrCdPos = ctryToISO.indexOf(terrCdLookup);
					
					fileName = "POS_" + fileType + "_" + ctryToISO.substring(terrCdPos-2, terrCdPos) + "_" + lgcyLclRfrDefCd + "_" + posBusnDt + "." + timestamp + ".xml~";
					
					valueOut.clear();
					valueOut.set(parts[7]);

					context.getCounter("DaaS","Write XML File: " + fileType).increment(1);
					
					mos.write(NullWritable.get(), valueOut, fileName);
				} else {
					context.getCounter("DaaS","Invalid XML File: " + fileType).increment(1);
				}
			} else {
				context.getCounter("DaaS","Skip XML File: " + fileType).increment(1);
			}
		} else {
			context.getCounter("DaaS","Invlid XML File").increment(1);
		}
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

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

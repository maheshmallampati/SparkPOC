package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.abac.ABaCList;
import com.mcd.gdw.daas.abac.ABaCListItem;

import com.mcd.gdw.daas.util.HDFSUtil;

public class TextCopyFileMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static String[] parts = null;
	private static Text currentTextKey = new Text();
	private static HashMap<String, ABaCListItem> fileHashMap = new HashMap<String, ABaCListItem>();
	private static ABaCListItem currentFileListItem = null;

	private static String _fileSeparatorCharacter = "\\x09";
	private static int _terrFieldPosition = 0;
	private static int _lcatFieldPosition = 0;
	private static int _busnDtFieldPosition = 0;
	
	
	@Override
	public void setup(Context context) {

		String parmValue = "";
		int parmIntValue = 0; 
		
		Configuration hdfsConfig = context.getConfiguration();
		
		parmValue = getParmStringValue(hdfsConfig,HDFSUtil.DAAS_MAPRED_FILE_SEPARATOR_CHARACTER);
		
		if ( parmValue.length() > 0 ) {
			_fileSeparatorCharacter = parmValue;
		}
		
		parmIntValue = getParmIntValue(hdfsConfig,HDFSUtil.DAAS_MAPRED_TERR_FIELD_POSITION);
		
		if ( parmIntValue > 0 ) {
			_terrFieldPosition = parmIntValue; 
		}
		
		parmIntValue = getParmIntValue(hdfsConfig,HDFSUtil.DAAS_MAPRED_LCAT_FIELD_POSITION);
		
		if ( parmIntValue > 0 ) {
			_lcatFieldPosition = parmIntValue; 
		}
		
		parmIntValue = getParmIntValue(hdfsConfig,HDFSUtil.DAAS_MAPRED_BUSINESSDATE_FIELD_POSITION);
		
		if ( parmIntValue > 0 ) {
			_busnDtFieldPosition = parmIntValue; 
		}

		//System.out.println("SETUP="+ _fileSeparatorCharacter + " " + _terrFieldPosition + " " + _lcatFieldPosition + " " + _busnDtFieldPosition);
		
		
	//BufferedReader br = null;
		
		try {
			
			    FileSystem fs = FileSystem.get(context.getConfiguration());
				//ABaC2List abac2List = ABaC2.readList(fs,new Path(context.getConfiguration().get("path.to.cache")));

			    FileStatus[] status = null;

				Path cachePath = new Path(context.getConfiguration().get("path.to.cache"));
				
				status = fs.listStatus(cachePath);
			
				if ( status != null ) {
					for (int idx=0; idx < status.length; idx++ ) {
						ABaCListItem  abac2ListItem = ABaC.readList(fs, cachePath, status[idx].getPath().getName());
						fileHashMap.put(abac2ListItem.getTerrCd() + "_" + abac2ListItem.getLgcyLclRfrDefCd() + "_" + abac2ListItem.getBusnDt(),abac2ListItem);
					}
				}
				
			    /*
				for(Map.Entry<String,ABaC2ListItem> entry : abac2List){
					ABaC2ListItem  abac2ListItem = entry.getValue();
					
					if ( abac2ListItem != null ) {				
						fileHashMap.put(abac2ListItem.getTerrCd() + "_" + abac2ListItem.getLgcyLclRfrDefCd() + "_" + abac2ListItem.getBusnDt(),abac2ListItem);
					}
				
				
					}	
				*/

		}catch (Exception e) {
			System.err.println("Exception occured on read distributed cache file:");
			e.printStackTrace();
			System.exit(8);
		}
		
		
	}

	private String getParmStringValue(Configuration hdfsConfig
			                         ,String parameterName) {

		String parmValue = "";
		String retParmValue = "";
		
		parmValue = hdfsConfig.get(parameterName);
		
		if ( parmValue != null && parmValue.length() > 0 ) {
			retParmValue = parmValue;
		}
		
		return(retParmValue);
		
	}
	
	private int getParmIntValue(Configuration hdfsConfig
                               ,String parameterName) { 

		String parmValue = "";
		int parmIntValue = 0;
		int retParmValue = 0;
		
		parmValue = hdfsConfig.get(parameterName);
		
		if ( parmValue != null && parmValue.length() > 0 ) {
			try {
				parmIntValue = Integer.parseInt(parmValue);
				
				if ( parmIntValue >= 0 ) {
					retParmValue = parmIntValue;
				}
			} catch (Exception ex) {
				
			}
		}
		
		return(retParmValue);
		
	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
	    	parts = value.toString().split(_fileSeparatorCharacter);
	    	
	    	//for (int idx=0; idx < parts.length; idx++ ) {
	    	//	System.out.println(idx + ") " + parts[idx]);
	    	//}

	    	if ( parts.length >= _terrFieldPosition && parts.length >= _lcatFieldPosition && parts.length >= _busnDtFieldPosition ) {
	    		if ( parts[_terrFieldPosition].length() > 0 && parts[_lcatFieldPosition].length() > 0 && parts[_busnDtFieldPosition].length() > 0 ) {
	    			
	    			//System.out.println("Lookup");
	    			//System.out.println(parts[_terrFieldPosition] + "_" + parts[_lcatFieldPosition] + "_" + parts[_busnDtFieldPosition]);
	    			
			    	currentFileListItem = fileHashMap.get(parts[_terrFieldPosition] + "_" + parts[_lcatFieldPosition] + "_" + parts[_busnDtFieldPosition]);
			    	
					if (currentFileListItem == null) {
						
						//System.out.println("NOT FOUND");
						
				    	currentTextKey.clear();
				    	currentTextKey.set(parts[_terrFieldPosition] + "_" + parts[_busnDtFieldPosition] + "_" + parts[_lcatFieldPosition].substring(parts[_lcatFieldPosition].length()-1));
				    	context.write(currentTextKey,value);
				    	
					} else {
				    	//System.out.println("FOUND");
				    	return;
					}
	    		} else {
			    	//System.out.println("IMVALID 1");
	    			return;
	    		}
	    	} else {
				//System.out.println("IMVALID 2");
	    		return;
	    	}
		    	
	    } catch (Exception ex) {
	    	System.err.println("Exception occured in map:");
	    	ex.printStackTrace(System.err);
	    	System.exit(8);
	    }
	}
}

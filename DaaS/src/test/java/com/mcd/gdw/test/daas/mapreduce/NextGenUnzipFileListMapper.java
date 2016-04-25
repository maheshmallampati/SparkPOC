package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.test.daas.driver.NextGenUnzipFile;

public class NextGenUnzipFileListMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String splitFileName;
	private String fullFileName;
	private HashMap<String,String> locList = new HashMap<String,String>();
	private String[] parts;
	private int recNum;
	
	private StringBuffer textBuffer = new StringBuffer();
	private String posBusnDt;
	private String terrCd;
	private String lgcyLclRfrDefCd;
	
	private int tabPos1;
	private int tabPos2;
	private int tabPos3;
	private int tabPos4;
	private int tabPos5;
	private int tabPos6;

	
	private Text keyText = new Text(); 
	private Text valueText = new Text();
	
	@Override
	public void setup(Context context) {
		
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;
	    String line;
	    String terrCd;
	    String posBusnDt;
	    Path path;
	    
	    try {

	    	recNum = 1;
	    	locList.clear();
	    	
	    	path = ((FileSplit) context.getInputSplit()).getPath();
	    	fullFileName = path.toString();
	    	splitFileName = path.getName();
	    	
	    	parts = splitFileName.split("~");
	    	
	    	terrCd = parts[1];
	    	posBusnDt = parts[2];
	    	
	    	keyText.clear();
	    	keyText.set("MAX" + parts[0] + "~" + terrCd + "~" + posBusnDt);
	    	
	    	valueText.clear();
	    	valueText.set(parts[3].substring(0,parts[3].length()-3));
	    	
	    	context.write(keyText, valueText);

	    	distPaths = context.getCacheFiles();
	    
	    	if ( distPaths == null ) {
	    		System.err.println("distpath is null");
	    		System.exit(8);
	    	}
	    	
	    	if ( distPaths != null && distPaths.length > 0 )  {
	    	  
	    		System.out.println(" number of distcache files : " + distPaths.length);
	    	  
	    		for ( int i=0; i<distPaths.length; i++ ) {
			     
	    			System.out.println("distpaths:" + distPaths[i].toString());
		    	  
	    			distPathParts = distPaths[i].toString().split("#");
		    	  
	    			if( distPaths[i].toString().contains(NextGenUnzipFile.DIST_CACHE_ABAC) ) {
	    				
	    				br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
	    				
	    				while ( (line=br.readLine()) != null ) {
	    					parts = line.split("\t");

	    					if ( parts[1].equals(terrCd) && parts[3].substring(0, 8).equals(posBusnDt) ) {
	    						locList.put(parts[1] + "\t" + parts[3].substring(0, 8) + "\t" + parts[2],"");
	    						//System.err.println(parts[1] + "\t" + parts[3].substring(0, 8) + "\t" + parts[2]);
	    					}
	    				}
	    				
	    				br.close();
	    			}
	    		}
	    	}
	    	
	    } catch (Exception ex) {
	    	ex.printStackTrace(System.err);
	    	System.exit(8);
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
			posBusnDt = getTextSubString(value,tabPos1+1,tabPos2);
			terrCd = getTextSubString(value,tabPos4+1,tabPos5);
			lgcyLclRfrDefCd = getTextSubString(value,tabPos5+1,tabPos6);


			if ( locList.containsKey(terrCd + "\t" + posBusnDt + "\t" + lgcyLclRfrDefCd) ) {
		    	keyText.clear();
		    	keyText.set("REMOVE");

		    	valueText.clear();
				valueText.set(fullFileName + "\t" + recNum);

				context.write(keyText, valueText);

				keyText.clear();
		    	keyText.set("COUNT" + fullFileName);

		    	valueText.clear();
				valueText.set("1\t1");

				context.write(keyText, valueText);
			} else {
				keyText.clear();
		    	keyText.set("COUNT" + fullFileName);

		    	valueText.clear();
				valueText.set("1\t0");

				context.write(keyText, valueText);
			}
		}
		
		recNum++;
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

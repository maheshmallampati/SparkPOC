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
import com.mcd.gdw.test.daas.driver.FindFileIds;

public class FindFileIdsMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String[] parts; 
	private Path path;
	private String findKey;
	
	private String fileName;

	private static HashMap<String, String> listHashMap = new HashMap<String, String>();
	
	private Text keyOut = new Text();
	private Text valueOut = new Text();

	@Override
	public void setup(Context context) { 

		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;

	    path = ((FileSplit)context.getInputSplit()).getPath();
		fileName = path.getName();
		
	    try {
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
			    	  
			    	  if( distPaths[i].toString().contains(FindFileIds.FIND_LIST) ) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
			    		  addFindIdListtoMap(br);
				      	  System.out.println("Loaded " + FindFileIds.FIND_LIST);
			    	  }
		    	}
		    }
	    	
		} catch (Exception ex) {
			System.err.println("Error in initializing FindFileIdsMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}

	private void addFindIdListtoMap(BufferedReader br) {
	
		String line = null;
		
		try {
			while ((line = br.readLine()) != null) {

				if (line != null && !line.isEmpty()) {
					
					if ( !listHashMap.containsKey(line) ) {
						listHashMap.put(line, line);
					}
				}
			}
			
			br.close();
			
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
	    	parts = value.toString().split("\\t");

	    	if ( parts.length >= 7 ) {
	    		findKey = parts[DaaSConstants.XML_REC_TERR_CD_POS] + "\t" + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS] + "\t" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS];
	    		if ( listHashMap.containsKey(findKey) ) {
	    			keyOut.clear();
	    			keyOut.set(findKey);
	    			
	    			valueOut.clear();
	    			valueOut.set(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + "\t" + fileName);
	    			
	    			context.write(keyOut, valueOut);
	    		}
	    	}
		} catch (Exception ex) {
	    	System.err.println("Error occured in FindFileIdsMapper:");
	    	ex.printStackTrace(System.err);			
		}
	}
}

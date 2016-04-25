package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.driver.RebuildWorkLayer;

public class RebuildWorkLayerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private String[] parts;
	private String key;
	
	private static HashMap<String, String> listHashMap = new HashMap<String, String>();
	
	private MultipleOutputs<NullWritable, Text> mos;
	
	@Override
	public void setup(Context context) { 
		
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;
	    
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
			    	  
			    	  if( distPaths[i].toString().contains(RebuildWorkLayer.CACHE_INCLUDE_LIST) ) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
			    		  addIncludeListtoMap(br);
				      	  System.out.println("Loaded " + RebuildWorkLayer.CACHE_INCLUDE_LIST);
			    	  }
		    	}
		    }
			
		    mos = new MultipleOutputs<NullWritable, Text>(context);
		    
		} catch (Exception ex) {
			System.err.println("Error in initializing AsterFormatMapper2:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	    
	}

	private void addIncludeListtoMap(BufferedReader br) {
	
		String line = null;
		
		try {
			while ((line = br.readLine()) != null) {

				if (line != null && !line.isEmpty()) {

					parts = line.split("\t");
				
					key = parts[0] + "_" + parts[1];
					
					if ( !listHashMap.containsKey(key) ) {
						listHashMap.put(key, key);
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
	    	
	    	if ( listHashMap.containsKey(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS] + "_" +  parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]) ) {
	    		mos.write(NullWritable.get(), value,HDFSUtil.replaceMultiOutSpecialChars(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + HDFSUtil.FILE_PART_SEPARATOR+ parts[DaaSConstants.XML_REC_TERR_CD_POS] + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]));
	    		context.getCounter("DaaS Counters",parts[DaaSConstants.XML_REC_FILE_TYPE_POS]).increment(1);
	    	}
	    	
		} catch (Exception ex) {
	    	System.err.println("Error occured in RebuildWorkLayerMapper:");
	    	ex.printStackTrace(System.err);			
		}
	}
	
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
}

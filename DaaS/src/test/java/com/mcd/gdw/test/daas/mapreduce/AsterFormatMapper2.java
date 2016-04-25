package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.test.daas.driver.GenerateAsterFormat2;

public class AsterFormatMapper2  extends Mapper<LongWritable, Text, NullWritable, Text> {

	private String[] parts; 
	private static String key;
	private static String lookupKey;
	private static String dates;

	private String owshFltr = "*";
	
	private boolean keepValue;

	private static HashMap<String, ArrayList<String>> listHashMap = new HashMap<String, ArrayList<String>>();
	private static HashMap<String, Integer> listOnlyHashMap = new HashMap<String, Integer>();
	private static ArrayList<String> dateList;

	private static Calendar busnDate = Calendar.getInstance();
	private static Calendar fromDate = Calendar.getInstance();
	private static Calendar toDate = Calendar.getInstance();
	private static String dateItem;
	private static int idx;
	
	@Override
	public void setup(Context context) { 
		
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;
	    
	    owshFltr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER);
	    
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
			    	  
			    	  if( distPaths[i].toString().contains(GenerateAsterFormat2.CACHE_ASTER_INCLUDE_LIST) ) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
			    		  addIncludeListtoMap(br);
				      	  System.out.println("Loaded " + GenerateAsterFormat2.CACHE_ASTER_INCLUDE_LIST);
			    	  }
		    	}
		    }
			
		} catch (Exception ex) {
			System.err.println("Error in initializing AsterFormatMapper2:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	    
	}

	private void addIncludeListtoMap(BufferedReader br) {
	
		String line = null;
		String[] parts;
		
		try {
			while ((line = br.readLine()) != null) {

				if (line != null && !line.isEmpty()) {

					parts = line.split("\t");
				
					key = parts[0] + "_" + parts[1];
				
					dates = parts[2].substring(0, 4) + parts[2].substring(5, 7) + parts[2].substring(8, 10) + "|" +
							parts[3].substring(0, 4) + parts[3].substring(5, 7) + parts[3].substring(8, 10);
					
					if ( listHashMap.containsKey(key) ) {
						dateList = listHashMap.get(key);
						dateList.add(dates);
					} else {
						dateList = new ArrayList<String>();
						dateList.add(dates);
						listHashMap.put(key, dateList);
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
	    		if ( listOnlyHashMap.size() > 0 ) {
	    			lookupKey = parts[DaaSConstants.XML_REC_TERR_CD_POS] + "_" + parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS] + "_" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS];
    				keepValue = false;
	    			
	    			if ( listOnlyHashMap.containsKey(lookupKey) ) {
	    				keepValue = true;
	    			}
	    		} else {
		    		if ( owshFltr.equals("*") ) {
		    			keepValue = true;
		    		} else {
		    			if ( owshFltr.equalsIgnoreCase(parts[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS]) ) {
		    				keepValue = true;
		    			} else {
		    				if ( lcatInList(parts[DaaSConstants.XML_REC_TERR_CD_POS]
		    						       ,parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]
		    						       ,parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]) ) {
		    					keepValue = true;
		    				} else {
		    					keepValue = false;
		    				}
		    			}
		    		}
	    		}
	    		
	    		if ( keepValue ) {
	    			context.write(NullWritable.get(), value);
	    			context.getCounter("DaaS Counters", "Extracted XML file type: " + parts[DaaSConstants.XML_REC_FILE_TYPE_POS]).increment(1);
	    			context.getCounter("DaaS Counters", "Skipped XML file type  : " + parts[DaaSConstants.XML_REC_FILE_TYPE_POS]).increment(0);
	    		} else { 
	    			context.getCounter("DaaS Counters", "Extracted XML file type: " + parts[DaaSConstants.XML_REC_FILE_TYPE_POS]).increment(0);
	    			context.getCounter("DaaS Counters", "Skipped XML file type  : " + parts[DaaSConstants.XML_REC_FILE_TYPE_POS]).increment(1);
	    		}
	    	}
	    
		} catch (Exception ex) {
	    	System.err.println("Error occured in AsterFormatMapper2:");
	    	ex.printStackTrace(System.err);			
		}
	}
	
	private boolean lcatInList(String terrCode
            ,String lcat
            ,String busnDt) {

		boolean inList = false;

		key = terrCode + "_" + lcat;

		busnDate.set(Integer.parseInt(busnDt.substring(0,4)), Integer.parseInt(busnDt.substring(4,6))-1, Integer.parseInt(busnDt.substring(6,8)));

		if ( listHashMap.containsKey(key) ) {
			dateList = listHashMap.get(key);

			for ( idx=0; idx < dateList.size(); idx++ ) {
				dateItem = dateList.get(idx);
				fromDate.set(Integer.parseInt(dateItem.substring(0,4)), Integer.parseInt(dateItem.substring(4,6))-1, Integer.parseInt(dateItem.substring(6,8)));
				toDate.set(Integer.parseInt(dateItem.substring(9,13)), Integer.parseInt(dateItem.substring(13,15))-1, Integer.parseInt(dateItem.substring(15,17)));
				if ( (busnDate.after(fromDate) || busnDate.equals(fromDate)) && (busnDate.before(toDate) || busnDate.equals(toDate)) ) {
					inList = true;
				}
			}
		}

		return(inList);
	}

}

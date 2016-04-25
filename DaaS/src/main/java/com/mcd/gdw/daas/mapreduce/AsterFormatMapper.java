package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.GenerateAsterFormat;
import com.mcd.gdw.daas.util.HDFSUtil;

public class AsterFormatMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static String[] parts = null;
	private static Text currentKey = new Text();
	private static Text currentValue = new Text();
	
	//private static int id = 0;
	private String lcatKey;
	private String owshFltr = "*";
	
	private static HashMap<String, ArrayList<String>> listHashMap = new HashMap<String, ArrayList<String>>();
	private static ArrayList<String> dateList;

	private static HashMap<String, Integer> listOnlyHashMap = new HashMap<String, Integer>();
	
	private static Calendar busnDate = Calendar.getInstance();
	private static Calendar fromDate = Calendar.getInstance();
	private static Calendar toDate = Calendar.getInstance();
	private static String key;
	private static String lookupKey;
	private static String dates;
	private static String dateItem;
	private static int idx;

	@Override
	public void setup(Context context) {
	
		owshFltr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER);
		
		try {			
			//AWS START
			//FileSystem fs = FileSystem.get(context.getConfiguration());
			FileSystem fs = HDFSUtil.getFileSystem(context.getConfiguration().get(DaaSConstants.HDFS_ROOT_CONFIG), context.getConfiguration());
			//AWS END
			
			BufferedReader br = null;
				
			try {
				URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
				Path path = new Path(uris[0]);
					
				String line;
				
				if ( path.getName().equals("aster_include_list.txt") ) {
					System.out.println("Processed Include List");
					
					br = new BufferedReader(new InputStreamReader(fs.open(path)));
					
					while ((line = br.readLine()) != null) {
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
						
					br.close();
				} 
				
				if ( path.getName().equals("aster_load_list.txt") ) {
					System.out.println("Processed History Load List");
					br = new BufferedReader(new InputStreamReader(fs.open(path)));
					
					while ((line = br.readLine()) != null) {
						parts = line.split("\t");
						
						key = parts[0] + "_" + parts[1] + "_" + parts[2];
							
						if ( !listOnlyHashMap.containsKey(key) ) {
							listOnlyHashMap.put(key, 1);
						}
					}
						
					br.close();
				} 
				

			}catch(Exception ex){
				ex.printStackTrace();
			}finally{
				try{
					if(br != null)
						br.close();
				}catch(Exception ex){
						
				}
			}
		}catch (Exception e) {
			System.err.println("Exception occured on read distributed cache file:");
			e.printStackTrace();
			System.exit(8);
		}
		
	}
	
	StringBuilder sb = new StringBuilder();
	String emptyStr = "";
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
	
		boolean keepValue = true;
		
		sb.setLength(0);
		String valuestr = value.toString();
		
		try {
//	    	parts = value.toString().split("\\t");
	    	
	    	parts = valuestr.split("\\t");

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
			    	currentKey.clear();
			    	lcatKey = "00"+parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
			    	currentKey.set(lcatKey.substring(lcatKey.length()-2,lcatKey.length()));
			    	//id++;
			    	
			    	currentValue.clear();
			    	
			    	sb.append(parts[DaaSConstants.XML_REC_FILE_TYPE_POS]).append("\t");
			    	sb.append(parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS]).append("\t");
			    	sb.append(parts[DaaSConstants.XML_REC_TERR_CD_POS]).append("\t");
			    	sb.append(parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]).append("\t");
			    	sb.append(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0,4)).append( "-").append(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4,6)).append("-").append(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6,8)).append("\t" );
			    	sb.append(parts[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS]).append("\t");
			    	sb.append(parts[DaaSConstants.XML_REC_XML_TEXT_POS]).append("\t");
			    	
			    	currentValue.set(sb.toString());
			    	
//			    	currentValue.set(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + "\t" + 
//			    	                 parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS] + "\t" + 
//			    			         parts[DaaSConstants.XML_REC_TERR_CD_POS] + "\t" + 
//			    	                 parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS] + "\t" + 
//			    			         parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(0,4) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(4,6) + "-" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS].substring(6,8) + "\t" +
//			    	                 parts[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS] + "\t" +
//			    	                 parts[DaaSConstants.XML_REC_XML_TEXT_POS]);
			    	context.write(currentKey,currentValue);
				    context.getCounter("DaaS Counters", "LOCATION WRITTEN TO EXTRACT").increment(1);
	    		} else {
				    context.getCounter("DaaS Counters", "LOCATION SKIPPED OWNERSHIP OR LIST ONLY FILTER").increment(1);
	    			return;
	    		}
	    	} else {
			    context.getCounter("DaaS Counters", "MAP ERROR - INPUT INVALID").increment(1);
	    	}
	    } catch (Exception ex) {
	    	System.err.println("Error occured in AsterFormatMapper:");
	    	ex.printStackTrace(System.err);
	    }finally{
	    	if(valuestr != null)
	    		valuestr = null;
	    	if(parts != null){
	    		Arrays.fill(parts, emptyStr);
	    		parts = null;
	    	}
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

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}
	
	
}

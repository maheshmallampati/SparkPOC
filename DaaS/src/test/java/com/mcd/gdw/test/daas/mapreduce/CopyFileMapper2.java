package com.mcd.gdw.test.daas.mapreduce;

//import java.io.BufferedReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
//import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
//import com.mcd.gdw.daas.abac.ABaCList;
import com.mcd.gdw.daas.abac.ABaCListItem;
import com.mcd.gdw.daas.util.HDFSUtil;

//public class CopyFileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	public class CopyFileMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private static String[] parts = null;
	private static Text currentTextKey = new Text();
	private static HashMap<String, ABaCListItem> fileHashMap = new HashMap<String, ABaCListItem>();


	HashMap<String,String> terrCdMcdGblBusLcatBusDtMap = new HashMap<String,String>();
	
//	private MultipleOutputs<NullWritable, Text> mos;
	

	@Override
	public void setup(Context context) {
		
		try {
//				mos = new MultipleOutputs<NullWritable,Text>(context);
			
			    FileSystem fs = FileSystem.get(context.getConfiguration());

			
				BufferedReader br = null;
				
				try {
					URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
					Path path = new Path(uris[0]);
					
					String line;
					boolean found = false;
					String[] parts;
				
					
					br = new BufferedReader(new InputStreamReader(fs.open(path)));
					StringBuffer sbf = new StringBuffer();
					String emptyStr = new String("");
					
					while ( (line=br.readLine()) != null && !found ) {
						
						sbf.setLength(0);
						parts = line.split("\t");
						String terrCd = parts[4];
						String mcdGbalBusnLcat = parts[6];
						String busnDt = parts[3];
						
						sbf.append(terrCd).append("_");
						sbf.append(mcdGbalBusnLcat).append("_");
						sbf.append(busnDt).append("_");
						
						
						terrCdMcdGblBusLcatBusDtMap.put(sbf.toString(),emptyStr);
						
						
						
						sbf.setLength(0);
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
//				URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
//				Path path;
//				for(int i=0;i<uris.length;i++){
//					path = new Path(uris[i]);
//					status = fs.listStatus(path);
//					for (int idx=0; idx < status.length; idx++ ) {
//						ABaCListItem  abac2ListItem = ABaC.readList(fs, path, status[idx].getPath().getName());
//						fileHashMap.put(abac2ListItem.getMcdGbalBusnLcat().toString() + "_" + abac2ListItem.getBusnDt(),abac2ListItem);
////						System.out.println(" adding to hashmap");
//					}
//					
//				}

//				Path cachePath = new Path(context.getConfiguration().get("path.to.cache"));
//				
//				status = fs.listStatus(cachePath);
//			
//				if ( status != null ) {
//					for (int idx=0; idx < status.length; idx++ ) {
//						ABaCListItem  abac2ListItem = ABaC.readList(fs, cachePath, status[idx].getPath().getName());
//						fileHashMap.put(abac2ListItem.getMcdGbalBusnLcat().toString() + "_" + abac2ListItem.getBusnDt(),abac2ListItem);
//					}
//				}

		}catch (Exception e) {
			System.err.println("Exception occured on read distributed cache file:");
			e.printStackTrace();
			System.exit(8);
		}
	}
	

	StringBuffer sbftext = new StringBuffer();
	StringBuffer sbf = new StringBuffer();

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
	
		try {
	
			String valstr = value.toString();
			sbftext.setLength(0);
			sbftext.append(valstr);
			
			boolean foundStartTag = false;
			sbf.setLength(0);
			
			int i = 0;
			int len = sbftext.length();
			
			while(foundStartTag == false && i < len){
				
				if(sbftext.charAt(i) == '<'){
					foundStartTag = true;
					
				}else{
					sbf.append(sbftext.charAt(i));
				}
				i++;
				
			}
			
			
			
//	    	parts = valstr.split("\\t");
	    	parts = sbf.toString().split("\\t");

//	    	currentXmlFileListItem = fileHashMap.get(parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS] + "_" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
	    	String mapKey = parts[DaaSConstants.XML_REC_TERR_CD_POS]+"_"+parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS] + "_" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]+"_";
	    	
	    
	    	context.getCounter("Count", "TotalGoldLayer").increment(1);
	    	
			if (!terrCdMcdGblBusLcatBusDtMap.containsKey(mapKey)) {
		    	currentTextKey.clear();
		    	if(parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD"))
		    		currentTextKey.set(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + "_" + parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS].substring(parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS].length()-1)+"_"+parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
		    	else
		    		currentTextKey.set(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + "_" + parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS].substring(parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS].length()-1));
		    	context.write(currentTextKey,value);
		    	
		    	value.clear();
		    	
		    	parts = null;
		    	valstr = null;
		    	
//		    	String multikey = HDFSUtil.replaceMultiOutSpecialChars(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_TERR_CD_POS] + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
//		    	
//		    	mos.write(NullWritable.get(), value, multikey);
		    	context.getCounter("Count", "TotalGoldLayerunChanged").increment(1);
			} else {
				context.getCounter("Count", "TotalGoldLayerunSkipper").increment(1);
		    	return;
			}
	    } catch (Exception ex) {
	      System.err.println(ex.toString());
	    }
	}
	
//	  @Override 
//	  protected void cleanup(Context contect) throws IOException, InterruptedException {
//	    mos.close();
//	  }
}

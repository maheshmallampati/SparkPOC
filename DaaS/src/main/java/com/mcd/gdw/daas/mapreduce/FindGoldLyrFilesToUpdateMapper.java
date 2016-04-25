package com.mcd.gdw.daas.mapreduce;

//import java.io.BufferedReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaCListItem;
import com.mcd.gdw.daas.driver.MergeToFinal;
import com.mcd.gdw.daas.util.HDFSUtil;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//public class CopyFileMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	public class FindGoldLyrFilesToUpdateMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static String[] parts = null;
	private static Text currentTextKey = new Text();
	private static HashMap<String, ABaCListItem> fileHashMap = new HashMap<String, ABaCListItem>();


	HashMap<String,String> terrCdMcdGblBusLcatBusDtMap = new HashMap<String,String>();
	
//	private MultipleOutputs<NullWritable, Text> mos;
	
	String fileName;
	String filePath;
	@Override
	public void setup(Context context) {
		
		try {
//				mos = new MultipleOutputs<NullWritable,Text>(context);
			
				//AWS START
			    //FileSystem fs = FileSystem.get(context.getConfiguration());
			    FileSystem fs = HDFSUtil.getFileSystem(context.getConfiguration().get(DaaSConstants.HDFS_ROOT_CONFIG), context.getConfiguration());
				//AWS END

			
				BufferedReader br = null;
				Path inputpath = ((FileSplit)context.getInputSplit()).getPath();
				fileName = inputpath.getName();
				filePath = inputpath.toString();
				
				
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
						
//						System.out.println(" Adding to Map " + sbf.toString());
						
						
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

		}catch (Exception e) {
			System.err.println("Exception occured on read distributed cache file:");
			e.printStackTrace();
			System.exit(8);
		}
	}
	

	StringBuffer sbftext = new StringBuffer();
	StringBuffer sbf = new StringBuffer();
	int countChanged = 0;
	int countUnChanged = 0;
	
	

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		   setup(context);
		   int i=0;
		   while (context.nextKeyValue()) {
			   
			   if(countChanged > 0){
				   System.out.println(" stopping reading file " +filePath + " after " + i + " lines ");
				   break;
			   }
			   i++;
			   
			   map(context.getCurrentKey(), context.getCurrentValue(), context);
		   }
		   cleanup(context);
		   
	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
	
		try {
			
			if(Boolean.parseBoolean(context.getConfiguration().get(MergeToFinal.NONMERGE_FILE_TYPE)))
			{
				currentTextKey.set("");
		    	value.clear();
		    	value.set(filePath);	
		    	context.write(currentTextKey,value);
				return;
			}
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
	    	
//	    	System.out.println(" mapKey " + mapKey);
	    	
	    	context.getCounter("Count", "TotalGoldLayer").increment(1);
	    	
			if (terrCdMcdGblBusLcatBusDtMap.containsKey(mapKey)) {
		    	currentTextKey.clear();
		    	
//		    	if(parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD"))
//		    		currentTextKey.set(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + "_" + parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS].substring(parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS].length()-1)+"_"+parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
//		    	else
//		    		currentTextKey.set(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + "_" + parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS].substring(parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS].length()-1));
//		    	context.write(currentTextKey,value);
		    	
		    	currentTextKey.set("");
		    	value.clear();
		    	value.set(filePath);
		    	
		    	
		    	context.write(currentTextKey,value);
		    	
		    	
		    	parts = null;
		    	valstr = null;
		    	
//		    	String multikey = HDFSUtil.replaceMultiOutSpecialChars(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_TERR_CD_POS] + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
//		    	
//		    	mos.write(NullWritable.get(), value, multikey);
		    	context.getCounter("Count", "TotalGoldLayerChanged").increment(1);
		    	countChanged++;
		    
		    	return;
			} else {
				countUnChanged++;
				context.getCounter("Count", "TotalGoldLayerUnChanged").increment(1);
//				return;
			}
			
	    } catch (Exception ex) {
	      System.err.println(ex.toString());
	    }
	}
	
	  @Override 
	  protected void cleanup(Context context) throws IOException, InterruptedException {
		  if(countChanged == 0)
				context.getCounter("Count","NumofMappersWrtngUnchgdDataToGLyr").increment(1);
		  super.cleanup(context);
		  
//	    mos.close();
	  }
}

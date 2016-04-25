package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;

public class CopyFileReducer2 extends Reducer<Text, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;
	private static String[] parts = null;

	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	  
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Text value = new Text();
		Iterator<Text> valIt = values.iterator();
		
		
//    	parts = valstr.split("\\t");
    	
		StringBuffer sbftext = new StringBuffer();
		StringBuffer sbf = new StringBuffer();
 	
		while (valIt.hasNext()){
 			
 			value = valIt.next();
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
 			
//	    	parts = value.toString().split("\\t");
 			
	    	
	    	parts = sbf.toString().split("\\t");
		    
	    	
	    	
	    	mos.write(NullWritable.get(), value, HDFSUtil.replaceMultiOutSpecialChars(parts[DaaSConstants.XML_REC_FILE_TYPE_POS] + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_TERR_CD_POS] + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]));
	     
	    	value.clear();
	    	valstr = null;
	    	sbftext.setLength(0);
 		}
	}

	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
}

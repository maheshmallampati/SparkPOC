package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;

/**
 * 
 * @author Sateesh Pula
 *
 * The reducer writes custom output files using the FileType, TerrCd and BusinessDate (ex: STLD~840~20140708)
 */
public class UnzipInputFileReducer extends Reducer<Text, Text, NullWritable, Text> {
	
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
//		StringBuffer sbf = new StringBuffer();
//		byte[] valuebytes = null;
		String[] keyparts;
		
 		while (valIt.hasNext()){
//	     for (Text value : values ) {
			value.clear();
//			valuebytes = null;
//			sbf.setLength(0);
			
			value = valIt.next();
//			valuebytes = value.getBytes();
			String curkey = key.toString();
			 
			keyparts = curkey.split("_");
			String fname = keyparts[0];

			 if ( fname.equals("FileStatus") ) {
		    	 mos.write(NullWritable.get(), value,fname );
	    	 }else{
			
	    		 String terrCd = keyparts[2];
	    		 String busDt  = keyparts[3];
			
	    		 mos.write(NullWritable.get(), value,HDFSUtil.replaceMultiOutSpecialChars(fname + HDFSUtil.FILE_PART_SEPARATOR+ terrCd + HDFSUtil.FILE_PART_SEPARATOR + busDt));
	    	 }
		
//	    	 if ( fname.equals("FileStatus") ) {
//		    	 mos.write(NullWritable.get(), value,fname );
//	    	 } else {
//		    	 parts = value.toString().split("\\t");
//		    	 
//		    	 if ( parts.length < 5 ) {
//		    		 System.err.println(curkey + " value len=" + parts.length);
//		    		 for (int idx=0; idx < parts.length; idx++) {
//		    			 System.err.println(idx + ") " + parts[idx]);
//		    		 }
//		    	 } else {
//			    	 mos.write(NullWritable.get(), value,HDFSUtil.replaceMultiOutSpecialChars(fname + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_TERR_CD_POS] + HDFSUtil.FILE_PART_SEPARATOR + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]));
//		    	 }
//		       
//	    	 }
	    	 
	    	
	    	 parts = null;
	     }
	}

	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
}
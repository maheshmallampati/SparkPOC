package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AsterFormatReducer extends Reducer<Text, Text, NullWritable, Text> {

	private final static int FILE_TYPE_POS = 0;
	private final static int MCD_GBAL_LCAT_ID_NU_POS = 1;
	private final static int TERR_CD_POS = 2;
	private final static int LGCY_LCL_RFR_DEF_CD_POS = 3;
	private final static int POS_BUSN_DT_POS = 4;
	private final static int REST_OWSH_TYP_SHRT_DS_POS = 5;
	private final static int XML_TEXT_POS = 6;
	
	private static String[] parts = null;
	private static Text currentValue = new Text();

	Text value = null;
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		 StringBuffer sbf = new StringBuffer();
		 
		 Iterator<Text> itvalues = values.iterator();

//	     for ( value : values ) {
		  while(itvalues.hasNext()){
			
			 value = itvalues.next();

	    	 parts = value.toString().split("\\t");
		     
	    	 if ( parts.length >= 6 ) {
		    	 currentValue.clear();
		    	 
		    	 
		    	 sbf.setLength(0);
		    	 
		    	 sbf.append("\"").append(parts[FILE_TYPE_POS]).append("\",");
		    	 sbf.append(parts[MCD_GBAL_LCAT_ID_NU_POS]).append(",");
		    	 sbf.append(parts[TERR_CD_POS]).append(",");
		    	 sbf.append("\"").append(parts[LGCY_LCL_RFR_DEF_CD_POS] ).append("\",");
		    	 sbf.append("\"").append(parts[POS_BUSN_DT_POS]).append("\",");
		    	 sbf.append("\"").append(parts[REST_OWSH_TYP_SHRT_DS_POS]).append("\",");
		    	 sbf.append("\"").append( parts[XML_TEXT_POS].replaceAll("\"", "\"\"")).append("\"");
		    	 
		    	 currentValue.set(sbf.toString());
		    	 		
		    	 currentValue.set("\"" + parts[FILE_TYPE_POS] + "\"," +
		    	                  parts[MCD_GBAL_LCAT_ID_NU_POS] + "," + 
		    			          parts[TERR_CD_POS] + "," + 
		    	                  "\"" + parts[LGCY_LCL_RFR_DEF_CD_POS] + "\"," + 
		    			          "\"" + parts[POS_BUSN_DT_POS] + "\"," + 
		    			          "\"" + parts[REST_OWSH_TYP_SHRT_DS_POS] + "\"," + 
		    	                  "\"" + parts[XML_TEXT_POS].replaceAll("\"", "\"\"") + "\"");
//		    	 
		    	 /*
		    	 currentValue.set(parts[FILE_TYPE_POS] + "\t" +   	
   	                  parts[MCD_GBAL_LCAT_ID_NU_POS] + "\t" + 
   			          parts[TERR_CD_POS] + "\t" + 
   	                  parts[LGCY_LCL_RFR_DEF_CD_POS] + "\t" + 
   			          parts[POS_BUSN_DT_POS] + "\t" + 
   			          parts[REST_OWSH_TYP_SHRT_DS_POS] + "\t" +
   			          parts[SEQ] + "\t" +
   	                  parts[XML_TEXT_POS] + "\t");
   	             */
		    	 
		    	 context.write(NullWritable.get(), currentValue);
		    	 
		    	 //context.getCounter("Debug", key.toString()).increment(1);
	    	 } else {
		    	 context.getCounter("Debug", "REDUCE_ERROR").increment(1);
	    	 }
	     }
	}
}

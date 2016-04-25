package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;

public class PMIXMultiFileCombinerReducer extends Reducer<Text,Text,NullWritable,Text>{
	
	 private MultipleOutputs<NullWritable, Text> mos;
	 @Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		
			
		}
		
	
	
	
	 StringBuffer mosKey = new StringBuffer();
	 
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
//		String keyStr = key.toString();
//		String[] keyParts = keyStr.split(DaaSConstants.TILDE_DELIMITER);
//		
//		String terrCd = keyParts[0];
//		String busDt  = keyParts[1];
		
//		mosKey.setLength(0);
//		mosKey.append("PMIX").append(DaaSConstants.TILDE_DELIMITER);
//		mosKey.append(terrCd).append(DaaSConstants.TILDE_DELIMITER);
//		mosKey.append(busDt);
		
		
		for(Text value:values){
		
			mos.write(HDFSUtil.replaceMultiOutSpecialChars(key.toString()), NullWritable.get(), value);
		}
		
	}





	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		  mos.close();
	}

}

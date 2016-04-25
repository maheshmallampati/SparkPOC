package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;

/**
 * 
 * @author Sateesh Pula
 * A mapper that combines multiple PMIX files by business date
 */
public class PMIXMultiFileCombinerMapper extends Mapper<LongWritable,Text,Text, Text>{

	
	 Text newkey = new Text();
	 
	 	String fileName;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			fileName = ( (FileSplit) context.getInputSplit()).getPath().getName();
			
			
			
		}
		
	int i =0;	
	String[] parts = null;
	StringBuffer sbf = new StringBuffer();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
			parts = value.toString().split("\\|",-1);
			String lgcyCode = parts[0];
			String terrCd   = parts[parts.length-1];
			String lgcyCode_lastDigit = lgcyCode.substring(lgcyCode.length()-1);
			
			String datepart = parts[2];//fileName.substring(4,12);
			
			if(i == 0){
				System.out.println("fileName " +fileName + " xxxx " + datepart );
				i++;
			}
			
			sbf.setLength(0);
			sbf.append("PMIX").append(DaaSConstants.SPLCHARTILDE_DELIMITER);
			sbf.append(terrCd).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
			sbf.append(datepart).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
//			if(Integer.parseInt(lgcyCode) == 156){//for China, use the whole store number
//				sbf.append(lgcyCode);
//			}else{
				sbf.append(lgcyCode_lastDigit);
//			}
			
			newkey.clear();
			newkey.set(sbf.toString());
			
			context.write(newkey, value);
		
	}

	


	
	

}

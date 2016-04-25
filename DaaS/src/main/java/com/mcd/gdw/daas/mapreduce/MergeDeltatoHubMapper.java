package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.util.HDFSUtil;




public class MergeDeltatoHubMapper extends Mapper<LongWritable,Text,NullWritable,Text>{

	private MultipleOutputs<NullWritable, Text> mos;
	
	
	String multioutbaseOutputPath= "";

	String fileName = "";
	Path fileSplit = null;
	
	

	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		fileSplit = ((FileSplit)context.getInputSplit()).getPath();
		
	    fileName = fileSplit.getName();
	    
	    mos = new MultipleOutputs<NullWritable, Text>(context);
   
	    multioutbaseOutputPath = context.getConfiguration().get("MULTIOUT_BASE_OUTPUT_PATH");
		
		
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		String valTxt = value.toString();
		String[] valParts = valTxt.split("\t",-1);
		
		String posBusnDt = valParts[0];
		String terrCd    = valParts[4];
		String storeId   = valParts[2];
//		String fileType = "STLD";
		String posOrdKey = valParts[1];
//		if(!"POS0003:287909656".equalsIgnoreCase(posOrdKey))
//			return;
//		if(fileSplit.toString().contains("STLD")){
//				fileType = "STLD";
//				posBusnDt = valParts[0];
//				terrCd    = valParts[4];
//				storeId   = valParts[2];
//		}
//		if(fileSplit.toString().contains("DetailedSOS")){
//				fileType="DetailedSOS";
//				posBusnDt = valParts[1];
//				terrCd    = valParts[0];
//				storeId   = valParts[3];
//		}
		

			
//		mos.write(HDFSUtil.replaceMultiOutSpecialChars(terrCd+posBusnDt), NullWritable.get(), value, baseOutputPath+"/"+fileType+"/terr_cd="+terrCd+"/pos_busn_dt="+posBusnDt+"/"+HDFSUtil.replaceMultiOutSpecialChars(terrCd+"~"+posBusnDt));
		mos.write(HDFSUtil.replaceMultiOutSpecialChars(terrCd+posBusnDt), NullWritable.get(), value, multioutbaseOutputPath+"/terr_cd="+Integer.parseInt(terrCd)+"/pos_busn_dt="+posBusnDt+"/"+HDFSUtil.replaceMultiOutSpecialChars(terrCd+"~"+posBusnDt));
		
		
		
		if(fileSplit.toString().contains("delta")){
			context.getCounter("Count","ValidRecords_delta").increment(1);
//			context.getCounter("Count", fileName).increment(1);
		}

		if(fileSplit.toString().contains("tmp")){
			context.getCounter("Count","ValidRecords_tmp").increment(1);
//			context.getCounter("Count", fileName).increment(1);
		}
	
		
	}

	
	
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
}

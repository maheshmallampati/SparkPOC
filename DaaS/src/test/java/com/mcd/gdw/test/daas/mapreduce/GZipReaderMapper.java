package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;



public class GZipReaderMapper extends Mapper<LongWritable,Text,NullWritable,Text>{

	FileSystem fileSystem;
	Text newValue = new Text();
	
	private MultipleOutputs<NullWritable, Text> mos;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		fileSystem = FileSystem.get(context.getConfiguration());
		
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}


	String[] fileParts;
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		DataInputStream fileStatusStream = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		fileParts = value.toString().split("/");
		String[] fileNameParts = fileParts[fileParts.length-1].split("~");
		
		String datepart = fileNameParts[2];
		String fileType = HDFSUtil.replaceMultiOutSpecialChars(fileNameParts[0]);
 		String terrCd   = fileNameParts[1];
		newValue.clear();
		
		try{
			isr=new InputStreamReader(new GZIPInputStream(fileSystem.open(new Path(value.toString()))));
			br=new BufferedReader(isr);
			int numLines = 0;
			String line;
			while( (line = br.readLine()) != null){
				
				numLines++;				
				
			}
			
			newValue.set(value.toString()+"-Successful -numlines- "+ numLines);
			mos.write("GoodFiles"+DaaSConstants.SPLCHARTILDE_DELIMITER+terrCd+DaaSConstants.SPLCHARTILDE_DELIMITER+fileType+DaaSConstants.SPLCHARTILDE_DELIMITER+datepart, NullWritable.get(), newValue);
//			context.write(NullWritable.get(), newValue);
		}catch(Exception ex){
//			System.out.println(" found a corrupt file  ********************");
			newValue.set(value.toString());
			
			mos.write("CorruptFiles"+DaaSConstants.SPLCHARTILDE_DELIMITER+terrCd+DaaSConstants.SPLCHARTILDE_DELIMITER+fileType+DaaSConstants.SPLCHARTILDE_DELIMITER+datepart, NullWritable.get(), newValue);
			
//			mos.close();
			
			ex.printStackTrace();
		}finally{
			try{
				if(br != null){
					br.close();
				}
				if(isr != null)
					isr.close();
				
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
		
		
	}
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		mos.close();
		
	}
	
	
	
	
	

}

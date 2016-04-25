package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.mcd.gdw.daas.abac.ABaCListItem;
import com.mcd.gdw.daas.util.HDFSUtil;

public class TextInputFileMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static ABaCListItem currentFileListItem = null;
	private static Text currentTextKey = new Text();
	//private static Text currentText = new Text();

	private static FileSplit fileSplit = null;
	private static String fileName = "";
	private static BufferedReader cacheReader = null;
	private static String cacheLine = "";
	
	//private static boolean setFileStatus = false;

	@Override
	public void setup(Context context) {

        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();

        BufferedReader br = null;
		
//		try {
//			
//			    FileSystem fs = FileSystem.get(context.getConfiguration());
//				ABaC2List abac2List = ABaC2..readList(fs,new Path(context.getConfiguration().get("path.to.cache")));
//				
//				for(Map.Entry<String,ABaC2ListItem> entry : abac2List){
//					ABaC2ListItem  abac2ListItem = entry.getValue();
//					
//					if ( abac2ListItem != null && abac2ListItem.getFileName().equalsIgnoreCase(fileName) ) {				
//						currentFileListItem = abac2ListItem;
//						break;
//					}
//				
//				
//					}	
//				
//
//		}catch (Exception e) {
//			System.err.println("Exception occured on read distributed cache file:");
//			e.printStackTrace();
//			System.exit(8);
//		}
		

	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
	
		try {
			currentTextKey.clear();
			currentTextKey.set(currentFileListItem.getTerrCd() + HDFSUtil.FILE_PART_SEPARATOR + currentFileListItem.getBusnDt() + "_" + currentFileListItem.getLgcyLclRfrDefCd().substring(currentFileListItem.getLgcyLclRfrDefCd().length()-1));
			context.write(currentTextKey, value);

		} catch (Exception ex) {
	      System.err.println(ex.toString());
	    }
	}

}

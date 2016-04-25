package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GenericGoldFileListMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Path splitPath;
	private String fullFileName;
	private int tabPos1;
	private int tabPos2;
	private int tabPos3;
	private int tabPos4;
	private int tabPos5;
	private int tabPos6;
	private String posBusnDt;
	private String fileType;
	private String fileId;
	private String terrCd;
	private String lgcyLclRfrDefCd;
	private StringBuffer textBuffer = new StringBuffer();
	
	private Text keyText = new Text();
	private Text valueText = new Text();
	
	
	@Override
	public void setup(Context context) {

		splitPath = ((FileSplit) context.getInputSplit()).getPath();
    	fullFileName = splitPath.toString();
	
	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		tabPos1 = getNextTabPos(value,0);
		tabPos2 = getNextTabPos(value,tabPos1+1);
		tabPos3 = getNextTabPos(value,tabPos2+1);
		tabPos4 = getNextTabPos(value,tabPos3+1);
		tabPos5 = getNextTabPos(value,tabPos4+1);
		tabPos6 = getNextTabPos(value,tabPos5+1);

		if ( tabPos6 > 0 ) {
			fileType = getTextSubString(value,0,tabPos1);
			fileId = getTextSubString(value,tabPos2+1,tabPos3);
			posBusnDt = getTextSubString(value,tabPos1+1,tabPos2);
			terrCd = getTextSubString(value,tabPos4+1,tabPos5);
			lgcyLclRfrDefCd = getTextSubString(value,tabPos5+1,tabPos6);
			
			keyText.clear();
			keyText.set(fileType + "\t" + terrCd + "\t" + lgcyLclRfrDefCd + "\t" + posBusnDt);
			
			valueText.clear();
			valueText.set(fileId + "\t" + fullFileName);
			
			context.write(keyText, valueText);
		}
		
	}
	
	private int getNextTabPos(Text value
			                 ,int strtPos) {

		int retPos = strtPos;
		int charValue;
		
		try {
			
			charValue = value.charAt(retPos);
			
			while ( charValue > -1 && charValue != '\t' ) {
				retPos++;
				charValue = value.charAt(retPos);
			}
			
		} catch (Exception ex) {
			retPos = -1;
		}

		return(retPos);
	}
	
	private String getTextSubString(Text value
			                       ,int strtPos
			                       ,int endPos) {
		
		textBuffer.setLength(0);
				
		for ( int pos=strtPos; pos < endPos; pos++ ) {
			textBuffer.append((char)value.charAt(pos));
		}
		
		return(textBuffer.toString());
		
	}
	
}

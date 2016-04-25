package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.mcd.gdw.daas.util.SimpleEncryptAndDecrypt;



public class ZipFileWriterMapper  extends Mapper<Text,BytesWritable,Text,Text>{


	private SimpleEncryptAndDecrypt encypt = new SimpleEncryptAndDecrypt();

	StringBuffer xmlBuffer = new StringBuffer();
	byte[] valuebytes;
	
	Text outputKey = new Text();
	Text outputValue = new Text();
	
	String fileName;
	
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		
		fileName = ( (FileSplit)context.getInputSplit()).getPath().getName();
	}


	@Override
	protected void map(Text key, BytesWritable value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		try{
		valuebytes = value.getBytes();
		

		String valuebytesstr = new String(valuebytes,"UTF-8");
		
 
		xmlBuffer.setLength(0);
		xmlBuffer.append(valuebytesstr);
		
		
		replaceName(xmlBuffer,"CrewID",false);
        replaceName(xmlBuffer,"CrewId",false);
        replaceName(xmlBuffer,"CrewName",false);
        replaceName(xmlBuffer,"ManagerID",false);
        replaceName(xmlBuffer,"ManagerId",false);
        replaceName(xmlBuffer,"ManagerName",false);
		
        
        outputKey.clear();
        outputKey.set(fileName);
        
        outputValue.clear();
        outputValue.set(xmlBuffer.toString());

        
        context.write(outputKey, outputValue);
        
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			valuebytes = null;	
		}
	}
	
	
	private void replaceName(StringBuffer xmlText
            ,String tagName
            ,boolean encrypt) throws Exception {

		int pos = 0;
		int nameStart = 0;
		int nameEnd = 0;
		
			pos = xmlText.indexOf("<" + tagName, pos);
			
			while ( pos > 0 ) {
			nameStart = xmlText.indexOf(">", pos+1);
			
			if ( nameStart > 0 ) {
			nameStart++;
			
			nameEnd = xmlText.indexOf("<", nameStart);
			
			if ( (nameEnd-nameStart) > 0 ) {
				if ( encrypt ) {
					xmlText.replace(nameStart, nameEnd, encypt.encryptAsHexString(xmlText.substring(nameStart, nameEnd)));
				} else {
					xmlText.replace(nameStart, nameEnd, "***");
				}
			}
			
			pos = xmlText.indexOf("<" + tagName, pos+1);
			}
		}
	}
	
	

}

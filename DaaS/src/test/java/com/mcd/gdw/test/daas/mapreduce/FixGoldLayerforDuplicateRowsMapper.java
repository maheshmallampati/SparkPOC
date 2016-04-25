package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.util.HDFSUtil;

public class FixGoldLayerforDuplicateRowsMapper extends Mapper<LongWritable, Text, Text, Text>{

	HashSet<Integer> ignoreFileIds = new HashSet<Integer>();
	String fileName;
	 private MultipleOutputs<NullWritable, Text> mos;
	 
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		Path path = ((FileSplit)context.getInputSplit()).getPath();
		fileName = path.getName();
		
		mos = new MultipleOutputs<NullWritable, Text>(context);
		
		ignoreFileIds.add(new Integer(1830220));
		ignoreFileIds.add(new Integer(1830474));
		ignoreFileIds.add(new Integer(1830628));
		ignoreFileIds.add(new Integer(1833443));
		ignoreFileIds.add(new Integer(1834032));
		ignoreFileIds.add(new Integer(1834043));
		ignoreFileIds.add(new Integer(1834524));
		ignoreFileIds.add(new Integer(1835695));
		ignoreFileIds.add(new Integer(1835997));
		ignoreFileIds.add(new Integer(1836410));
		ignoreFileIds.add(new Integer(1836608));
		ignoreFileIds.add(new Integer(1838777));
		ignoreFileIds.add(new Integer(1839960));
		ignoreFileIds.add(new Integer(1840053));
		ignoreFileIds.add(new Integer(1840935));
		ignoreFileIds.add(new Integer(1842097));
		ignoreFileIds.add(new Integer(1844687));
		ignoreFileIds.add(new Integer(1845197));


	
		
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		
		String valstr = value.toString();
		String[] valparts = valstr.split("\t");
		
		
		Integer fileId = new Integer(valparts[2]);
	
		String[] fnameparts = fileName.split("~");
		String outputtype = fnameparts[0];
		String terrcd = fnameparts[1];
		String busdt = fnameparts[2];
		
		if(!ignoreFileIds.contains(fileId)){
			mos.write(HDFSUtil.replaceMultiOutSpecialChars(outputtype+"~"+terrcd+"~"+busdt), NullWritable.get(), value);
			context.getCounter("Count","ValidFileIds").increment(1);
		}else{
			
			context.getCounter("Count","Ignoring"+fileId).increment(1);
			mos.write("IgnoredFiles",NullWritable.get(), new Text(fileId.intValue() + " - "+fileName));
		}
		
		
	}

	
	  
	  @Override 
	  protected void cleanup(Context contect) throws IOException, InterruptedException {
	    mos.close();
	  }
	
	
	

}

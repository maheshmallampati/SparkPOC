package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FindGoldLyrFilesToUpdateReducer extends Reducer<Text,Text,NullWritable,Text>{
	
	MultipleOutputs<NullWritable, Text> mos = null ;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		
		mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> it = values.iterator();
		HashSet<String> fileNameSet = new HashSet<String>();
		String filetxt ;
		while(it.hasNext()){
			
			filetxt = it.next().toString();
//			System.out.println(" reducer received " + filetxt + " to string " + filetxt);
			fileNameSet.add(filetxt);
			
		}
		
//		System.out.println(" fileNameSet length " + fileNameSet.size());
		
		if(fileNameSet != null){
			Iterator<String> filenameit = fileNameSet.iterator();
			Text outfile = new Text();
			while(filenameit.hasNext()){
				outfile.clear();
				outfile.set(filenameit.next());
//				System.out.println(" reducer writing to goldLayerFilesToupdateForthisRun " + outfile);
				mos.write("goldLayerFilesToupdateForthisRun", NullWritable.get(), outfile);
				
			}
		}
	}

	

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		mos.close();
	}
	
	

}

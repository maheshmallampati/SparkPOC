package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;

public class GZipReaderDriver extends Configured implements Tool {

	public static void main(String[] argsAll) throws Exception{
		Configuration conf = new Configuration();
		
		int ret = ToolRunner.run(conf, new GZipReaderDriver(),argsAll);
		
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		BufferedWriter br = null;
		try{
		
//		getConf().setInt(NLineInputFormat.LINES_PER_MAP,10);
		FileSystem fileSystem = FileSystem.get(getConf());
		
		Job job = new Job(getConf(),"FindCorruptFiles");
		
		job.setJarByClass(GZipReaderDriver.class);
		
		job.setMapperClass(GZipReaderMapper.class);
		
		job.setNumReduceTasks(0);
		
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, new Path(args[1]), true);
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, new Path("/daastest/testcorruptfiles/filelist.txt"), true);
		
		br=new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path("/daastest/testcorruptfiles/filelist.txt"),true)));

		FileStatus[] fstat = fileSystem.globStatus(new Path(args[0]));
		
		FileStatus[] fstatus = null;
		FileStatus[] fstatustmp = null;
		int totalInputFileCount = 0;
		String[] inputpathstrs = args[0].split(",");
		
		for(String inputpaths:inputpathstrs){
			System.out.println(" inputpathstrs "+ inputpaths);
		fstatustmp = fileSystem.globStatus(new Path(inputpaths+"/*.gz"));
			fstatus = (FileStatus[])ArrayUtils.addAll(fstatus, fstatustmp);

		}
		
		String[] fileNameParts;
		String datepart;
		HashSet<String> uniqueDates = new HashSet<String>();
		String fileType ="";
		String terrCdfrmFileName ="";
		String moutfilename = "";
		for(FileStatus fstat2:fstatus){
			
			String fileNameorg = fstat2.getPath().toString();
			
			String fileNamelc = fstat2.getPath().getName();
			
			String fileName = fstat2.getPath().getName().toUpperCase();
			
			String fileNamePartsDelimiter = "~";
			
			
			if(fileName.indexOf("RXD126") > 0){
				fileNamePartsDelimiter = "RXD126";
			}
			fileNameParts = fileNamelc.split(fileNamePartsDelimiter);
			fileType          = HDFSUtil.replaceMultiOutSpecialChars(fileNameParts[0]);
			terrCdfrmFileName = fileNameParts[1];
			datepart = fileNameParts[2].substring(0,8);
			moutfilename = terrCdfrmFileName+DaaSConstants.SPLCHARTILDE_DELIMITER+fileType+DaaSConstants.SPLCHARTILDE_DELIMITER+datepart;
			uniqueDates.add(moutfilename);
			
			moutfilename = "";
			
//			System.out.println( "fileName  " + fileName);
//			if(fileNameorg.contains("STLD~840~20141028~0001252.gz"))
				br.write(fileNameorg +"\n");
		}
		br.close();
		Iterator<String> it = uniqueDates.iterator();
		
		while(it.hasNext()){
			moutfilename = it.next();
			System.out.println(" addding " +moutfilename);
			
			MultipleOutputs.addNamedOutput(job,"CorruptFiles"+DaaSConstants.SPLCHARTILDE_DELIMITER+moutfilename,TextOutputFormat.class, NullWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(job,"GoodFiles"+DaaSConstants.SPLCHARTILDE_DELIMITER+moutfilename,TextOutputFormat.class, NullWritable.class, Text.class);
		
		}
		
		
		FileInputFormat.addInputPath(job,new Path("/daastest/testcorruptfiles/filelist.txt"));
		job.setInputFormatClass(NLineInputFormat.class);
//		NLineInputFormat.addInputPath(job,new Path("/daastest/testcorruptfiles/filelist.txt"));
		NLineInputFormat.setNumLinesPerSplit(job, 10);
		
//		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean retval = job.waitForCompletion(true);
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				if(br != null)
					br.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
		return 0;
	}
	
	

}

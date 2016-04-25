package com.mcd.gdw.test.daas.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.FixGoldLayerforDuplicateRowsMapper;



public class FixGoldLayerforDuplicateRowsDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		int retval = ToolRunner.run(new Configuration(),new FixGoldLayerforDuplicateRowsDriver(), args);

		System.out.println(" return value : " + retval);
	}

	
	@Override
	public int run(String[] argsall) throws Exception {
		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		
		String[] args = gop.getRemainingArgs();
		
		Configuration hdfsConfig = getConf();
		
		hdfsConfig.set("mapred.compress.map.output", "true");
		hdfsConfig.set("mapred.output.compress", "true");
		hdfsConfig.set("mapred.output.compression.type", "RECORD"); 
		hdfsConfig.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		
		
		Job job = new Job(hdfsConfig,"FixGoldLayer");
		
		
		job.setJarByClass(FixGoldLayerforDuplicateRowsDriver.class);
		job.setMapperClass(FixGoldLayerforDuplicateRowsMapper.class);
	
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		//job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		MultipleOutputs.addNamedOutput(job, "IgnoredFiles", TextOutputFormat.class, NullWritable.class, Text.class);
		
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("STLD~840~20140207"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("STLD~840~20140208"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("STLD~840~20140209"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("STLD~840~20140210"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("STLD~840~20140211"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("STLD~840~20140212"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("STLD~840~20140213"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("STLD~840~20140215"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("DetailedSOS~840~20140207"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("DetailedSOS~840~20140208"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("DetailedSOS~840~20140209"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("DetailedSOS~840~20140210"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("DetailedSOS~840~20140211"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("DetailedSOS~840~20140212"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("DetailedSOS~840~20140213"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("DetailedSOS~840~20140215"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("MenuItem~840~20140207"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("MenuItem~840~20140208"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("MenuItem~840~20140209"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("MenuItem~840~20140210"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("MenuItem~840~20140211"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("MenuItem~840~20140212"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("MenuItem~840~20140213"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("MenuItem~840~20140215"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("SecurityData~840~20140207"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("SecurityData~840~20140208"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("SecurityData~840~20140209"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("SecurityData~840~20140210"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("SecurityData~840~20140211"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("SecurityData~840~20140212"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("SecurityData~840~20140213"), TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars("SecurityData~840~20140215"), TextOutputFormat.class, NullWritable.class, Text.class);


		FileSystem fileSystem = FileSystem.get(hdfsConfig);

		
		FileInputFormat.addInputPaths(job, args[0]);
		
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, new Path(args[1]),true);
		
		TextOutputFormat.setOutputPath(job, new Path(args[1]+"/"+args[2]));
		
		
		final String outputtype = args[2];
		
		 job.waitForCompletion(true);

		 
		 FileStatus[] outfiles = fileSystem.listStatus(new Path(args[1]+"/"+args[2]),new PathFilter() {
			
			@Override
			public boolean accept(Path arg0) {
				if(arg0.getName().startsWith(outputtype))
					return true;
				return false;
			}
		});
		
		 String fileName; 
		 String[] fileNameParts;
		 
		 String destFileName;
		 String dt;
		 
		 for(int i=0;i<outfiles.length;i++){
			 fileName = HDFSUtil.restoreMultiOutSpecialChars(outfiles[i].getPath().getName());
//			 System.out.println( " fileName " +fileName );
			 fileNameParts = fileName.split("~");
			 dt = fileNameParts[2].split("-")[0];
			 destFileName = fileNameParts[0]+"~"+fileNameParts[1]+"~"+dt+"~"+String.format("%07d", i+1)+".gz";
			 
			 String destPath = "/daastest/goldtest/np_xml/"+fileNameParts[0]+"/"+fileNameParts[1]+"/"+dt;
			 
			 if(!fileSystem.exists(new Path(destPath))){
				fileSystem.mkdirs(new Path(destPath));
			 }

			 fileSystem.rename(outfiles[i].getPath(), new Path(destPath+"/"+destFileName));

			 System.out.println( " hadoop fs -mv "+  "/daas/gold/np_xml/"+fileNameParts[0]+"/"+fileNameParts[1]+"/"+dt + "/daas/goldbackup/np_xml/"+fileNameParts[0]+"/"+fileNameParts[1]+"/"+dt);
			 
			 System.out.println( " hadoop fs -mv "+ destPath+"/"+destFileName + " /daas/gold/np_xml/"+fileNameParts[0]+"/"+fileNameParts[1]+"/"+dt+"/"+destFileName);
		 }
		return 0;
	}
	

}

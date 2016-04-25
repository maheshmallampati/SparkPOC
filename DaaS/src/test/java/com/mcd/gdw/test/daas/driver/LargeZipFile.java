package com.mcd.gdw.test.daas.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.mapreduce.ZipFileInputFormat;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.LargeZipFileMapper;

public class LargeZipFile extends Configured implements Tool {
	
	DaaSConfig daasConfig;
	Path baseOutputPath;
	FileSystem fileSystem;
	Configuration hdfsConfig;
	
	public static void main(String[] args) throws Exception {

		int retval = ToolRunner.run(new Configuration(),new LargeZipFile(), args);
		
		System.exit(retval);
		
	}
	
	public int run(String[] argsall) throws Exception {
		
		String configXmlFile = "";
		String fileType = ""; 
		String inputPath = "";
		
		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		
		String[] args = gop.getRemainingArgs();
		
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-t") && (idx+1) < args.length ) {
				fileType = args[idx+1];
			}			

			if ( args[idx].equals("-i") && (idx+1) < args.length ) {
				inputPath = args[idx+1];
			}			
		}
		
		daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		baseOutputPath = new Path("/daastest/work/LargeZipFile/out");
		
		hdfsConfig = getConf();
		fileSystem = FileSystem.get(hdfsConfig);
		
		if ( daasConfig.configValid() ) {
			runJob(daasConfig,fileType,hdfsConfig,inputPath);
		} else {
			System.err.println("Invalid config/file type");
			System.exit(8);
		}
		
		return(0);
	}

	private void runJob(DaaSConfig daasConfig
                       ,String fileType
                       ,Configuration hdfsConfig
                       ,String inputPath) throws Exception {
	
		Job job;
		
		job = Job.getInstance(hdfsConfig, "Large Zip File");

		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath, daasConfig.displayMsgs());
		
		ZipFileInputFormat.setLenient(true);
		ZipFileInputFormat.setInputPaths(job, new Path(inputPath));
		
		job.setJarByClass(LargeZipFile.class);
		job.setMapperClass(LargeZipFileMapper.class);
		job.setInputFormatClass(ZipFileInputFormat.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(ZipFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job,baseOutputPath);
		
		if ( !job.waitForCompletion(true) ) {
			System.err.println("Job Error");
			System.exit(8);
		}
		
	}
}

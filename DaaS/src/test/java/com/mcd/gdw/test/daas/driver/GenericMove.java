package com.mcd.gdw.test.daas.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.GenericMoveMapper;

public class GenericMove extends Configured implements Tool {

	ABaC abac; 
	
	public static void main(String[] args) throws Exception {

		int retval = ToolRunner.run(new Configuration(),new GenericMove(), args);

		System.out.println(" return value : " + retval);		
	}

	@Override
	public int run(String[] argsall) throws Exception {

		String configXmlFile = "";
		String inputPath = "";
		String[] args;

		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		
		args = gop.getRemainingArgs();
		
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-i") && (idx+1) < args.length ) {
				inputPath = args[idx+1];
			}
		}
		
		if ( configXmlFile.length() == 0 || inputPath.length() == 0  )  {
			System.err.println("Invalid parameters");
			System.err.println("Usage: GenericMove -c config.xml -i inputPath");
			System.exit(8);
		}
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile);
		
		if ( daasConfig.configValid() ) {
			if ( daasConfig.displayMsgs() ) {
				System.out.println(daasConfig.toString());
			}
			
			runMrMove(daasConfig,getConf(),inputPath);
		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.exit(8);
		}

		return(0);
		
	}

	private void runMrMove(DaaSConfig daasConfig
                          ,Configuration hdfsConfig
                          ,String inputPath) {

		Job job;
		
		try {
			FileSystem fileSystem = FileSystem.get(hdfsConfig);
			
			Path outPath= new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "generic_move");
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, outPath,daasConfig.displayMsgs());
			
			job = Job.getInstance(hdfsConfig, "Generic File Mover");

			FileInputFormat.addInputPath(job, new Path(inputPath));
			
			job.setJarByClass(GenericMove.class);
			job.setMapperClass(GenericMoveMapper.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			TextOutputFormat.setOutputPath(job, outPath);

			if ( !job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
}

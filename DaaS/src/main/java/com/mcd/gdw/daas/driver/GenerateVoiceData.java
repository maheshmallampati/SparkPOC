package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.mapreduce.VoiceDataMapper;
import com.mcd.gdw.daas.mapreduce.VoiceDataReducer;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

public class GenerateVoiceData extends Configured implements Tool {
	
	private static final String JOB_GRP_DESC = "Generate Voice Data Formats";
	
	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
	
	private FsPermission newFilePremission;
	private Job job;
	
	public static void main(String[] args) throws Exception {
		
		Configuration hdfsConfig = new Configuration();
				
		int retval = ToolRunner.run(hdfsConfig,new GenerateVoiceData(), args);
		
		System.exit(retval);
		
	}
	
	public int run(String[] args) throws Exception {

		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
		boolean compressOut = false;
		boolean helpRequest = false;

		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-t") && (idx+1) < args.length ) {
				fileType = args[idx+1];
			}

			if ( args[idx].equals("-d") && (idx+1) < args.length ) {
				terrDate = args[idx+1];
			}
			
			if ( args[idx].equals("-z") && (idx+1) < args.length ) {
				if ( args[idx+1].equalsIgnoreCase("FALSE") || args[idx+1].equalsIgnoreCase("NO") ) {
					compressOut = false;
				}
			}
			
			if ( args[idx].toUpperCase().equals("-H") || args[idx].toUpperCase().equals("-HELP")  ) {
				helpRequest = true;
			}
		}
		
		if ( helpRequest ) {
			System.out.println("Usage: GenerateVoiceData -c config.xml -t filetype -d territoryDateParms ");
			System.out.println("where territoryDateParm is a comma separated list of territory codes and dates separated by colons(:)");
			System.out.println("for example, 840:2012-07-01:2012-07-07 is territory 840 from July 1st, 2012 until July 7th, 2012.");
			System.out.println("the date format is either ISO YYYY-MM-DD or YYYYMMDD (both are valid)");
			System.out.println("If only one date is supplied then a single day is used for that territory");
			System.out.println("Multiple territoryDateParm can be specified as comma separated values: 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01");
			System.out.println("This will get a total of 3 days for 840 and 1 day from 250");
			System.exit(0);
		}

		if ( configXmlFile.length() == 0 || fileType.length() == 0 || terrDate.length() == 0 ) {
			System.err.println("Missing config.xml (-c), filetype (t), territoryDateParms (-d)");
			System.err.println("Usage: GenerateVoiceData -c config.xml -t filetype -d territoryDateParms");
			System.err.println("where territoryDateParm is a comma separated list of territory codes and dates separated by colons(:)");
			System.err.println("for example, 840:2012-07-01:2012-07-07 is territory 840 from July 1st, 2012 until July 7th, 2012. YYYY-MM-DD");
			System.err.println("the date format is either ISO YYYY-MM-DD or YYYYMMDD (both are valid)");
			System.err.println("If only one date is supplied then a single day is used for that territory");
			System.err.println("Multiple territoryDateParm can be specified as comma separated values: 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01");
			System.err.println("This will get a total of 3 days for 840 and 1 day from 250");
			System.exit(8);
		}
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		
		if ( daasConfig.configValid() ) {
			
			if ( daasConfig.displayMsgs()  ) {
				System.out.println(daasConfig.toString());
			}

			hdfsConfig = getConf();
			fileSystem = FileSystem.get(hdfsConfig);

			newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);
			
			runJob(daasConfig,fileType,terrDate,compressOut);
			
		} else {
			System.err.println("Invalid Config XML file, stopping");
			System.err.println(daasConfig.errText());
			System.exit(8);
		}
	
		return(0);
	}

	private void runJob(DaaSConfig daasConfig
            ,String fileType
            ,String terrDate
            ,boolean compressOut) {
		
		try {
			
			hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm());
			//Testing Only
			//baseOutputPath = new Path("/daastest/voice/output/");
			baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "voiceextracts");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");
			}

			if ( compressOut ) {
				hdfsConfig.set("mapreduce.map.output.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
				hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			}
			
			ArrayList<Path> requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDate);

			job = Job.getInstance(hdfsConfig, JOB_GRP_DESC);
	
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}
			
			//Testing Only
			//Path questionDistCache = new Path("/daastest/distcachefiles/VOICE_DATA_QUESTION.txt");
			Path questionDistCache = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "VOICE_DATA_QUESTION.txt");
			job.addCacheFile(new URI(questionDistCache.toString() + "#" + questionDistCache.getName()));
			
			//Testing Only
			//Path questionTextDistCache = new Path("/daastest/distcachefiles/VOICE_DATA_QUESTION_TEXT.txt");
			Path questionTextDistCache = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "VOICE_DATA_QUESTION_TEXT.txt");
			job.addCacheFile(new URI(questionTextDistCache.toString() + "#" + questionTextDistCache.getName()));
			
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			job.setJarByClass(GenerateVoiceData.class);
			job.setMapperClass(VoiceDataMapper.class);
			job.setReducerClass(VoiceDataReducer.class);
			job.setNumReduceTasks(1);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			TextOutputFormat.setOutputPath(job, baseOutputPath);			
			
			MultipleOutputs.addNamedOutput(job, "HEADER", TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "DETAIL", TextOutputFormat.class, Text.class, Text.class);

			job.waitForCompletion(true);
			
			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}
			
			SimpleDateFormat sdfDestination = new SimpleDateFormat("yyyyMMddhhmmss");
			String s = sdfDestination.format(new Date());
			
			SimpleDateFormat sdfTodayDestination = new SimpleDateFormat("yyyyMMdd");
			String date = sdfTodayDestination.format(new Date());
			
			FileStatus[] fstats = fileSystem.listStatus(baseOutputPath);
			
			for ( int idx=0; idx < fstats.length; idx++ ) {
				
				if (fstats[idx].getPath().getName().startsWith("HEADER")) {
					fileSystem.rename(fstats[idx].getPath(), new Path(baseOutputPath + "/GRSD_CustSurvRsndInfo_US_0_" + date + "_" + s + ".psv")); //Header
				} else if (fstats[idx].getPath().getName().startsWith("DETAIL")) {
					fileSystem.rename(fstats[idx].getPath(), new Path(baseOutputPath + "/GRSD_CustSurvRsndRspn_US_0_" + date + "_" + s + ".psv"));	//Detail
				} else if (fstats[idx].getPath().getName().startsWith("_")) {
					fileSystem.delete(fstats[idx].getPath(), false);
				} else {
					fileSystem.delete(fstats[idx].getPath(), false);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}
	
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig,String fileType,String requestedTerrDateParms) {

		ArrayList<Path> retPaths = new ArrayList<Path>();
		
		try {
			Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem,daasConfig, requestedTerrDateParms, "Header", "Detail");

			if ( requestPaths == null ) {
				System.err.println("Stopping, No valid territory/date params provided");
				System.exit(8);
			}

			int validCount = 0;

			for ( int idx=0; idx < requestPaths.length; idx++ ) {
				if ( fileSystem.exists(requestPaths[idx]) ) {
					retPaths.add(requestPaths[idx]);
					validCount++;

					if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
						System.out.println("Found valid path = " + requestPaths[idx].toString());
					}
			} else {
					System.err.println("Invalid path \"" + requestPaths[idx].toString() + "\" skipping.");
				}
			}
				
			if ( validCount == 0 ) {
				System.err.println("Stopping, No valid files found");
				System.exit(8);
			}

			if ( daasConfig.displayMsgs() ) {
				System.out.print("\nFound " + validCount + " HDFS path");
				if ( validCount > 1 ) {
					System.out.print("s");
				}
				System.out.print(" from " + requestPaths.length + " path");
				if ( requestPaths.length > 1 ) {
					System.out.println("s.");
				} else {
					System.out.println(".");
				}
			}

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\n");
			}

		} catch (Exception ex) {
			System.err.println("Error occured in GenerateVoiceData.getVaildFilePaths:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}

		return(retPaths);
			
	}

}

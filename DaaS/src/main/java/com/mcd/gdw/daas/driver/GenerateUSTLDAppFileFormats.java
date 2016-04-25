package com.mcd.gdw.daas.driver;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.mapreduce.USTLDAppFileFormatsMapper;
import com.mcd.gdw.daas.mapreduce.USTLDAppFileFormatsReducer;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

public class GenerateUSTLDAppFileFormats extends Configured implements Tool {
	
	private final static String JOB_GRP_DESC = "Extracting US TLD";
	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
	private FsPermission newFilePremission;
	
	private Job job;
	
	private ArrayList<Path> requestedPaths;
	
	public static void main(String[] args) throws Exception {
		
		Configuration hdfsConfig = new Configuration();
				
		int retval = ToolRunner.run(hdfsConfig,new GenerateUSTLDAppFileFormats(), args);

		System.out.println(" return value : " + retval);
	}
	
	public int run(String[] args) throws Exception {
		
		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
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
			
			if ( args[idx].toUpperCase().equals("-H") || args[idx].toUpperCase().equals("-HELP")  ) {
				helpRequest = true;
			}
		}

		if ( helpRequest ) {
			System.out.println("Usage: GenerateUSTLDAppFileFormats -c config.xml -t filetype -d territoryDateParms ");
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
			System.err.println("Usage: USTLDReformat -c config.xml -t filetype -d territoryDateParms");
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
			//AWS START
			//fileSystem = FileSystem.get(hdfsConfig);
			fileSystem = HDFSUtil.getFileSystem(daasConfig, hdfsConfig);
			//AWS END
			
			newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);
			
			runJob(daasConfig,fileType,terrDate,false);
			
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

			if ( compressOut ) {
				hdfsConfig.set("mapreduce.map.output.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
				hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			}
			
			// requestedPaths: [/daas/gold/np_xml/STLD/840/20151105]
			requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDate,"STLD");
			
			job = Job.getInstance(hdfsConfig, JOB_GRP_DESC);
			job.setJarByClass(GenerateUSTLDAppFileFormats.class);
			job.setMapOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(USTLDAppFileFormatsMapper.class);
			job.setReducerClass(USTLDAppFileFormatsReducer.class);
			job.setNumReduceTasks(1);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			SimpleDateFormat sdfDestination = new SimpleDateFormat("dd-MMM-yy");
			String s = sdfDestination.format(new Date());
			
			//AWS START
			//Production Path /tld/files/TLD/transData/DATE/data/input/asciioutput
			//baseOutputPath = new Path("/tld/sridhar1/files/TLD/transData" + Path.SEPARATOR + s + Path.SEPARATOR + "data/input/asciioutput");
			baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "tld" + Path.SEPARATOR + "files" + Path.SEPARATOR + "TLD" + Path.SEPARATOR + "transData" + Path.SEPARATOR + s + Path.SEPARATOR + "input" + Path.SEPARATOR + "asciioutput"); 
			//AWS END
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath, daasConfig.displayMsgs());

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");
			}
			
			FileOutputFormat.setOutputPath(job, baseOutputPath);
			
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}
			
			Path daypartDistCache = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "DayPart_ID.psv");
			job.addCacheFile(new URI(daypartDistCache.toString() + "#" + daypartDistCache.getName()));
			
			//sri_run_test.sh
			//Path usTLDStoreListDistCache = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "USTLDStoreList.txt");
			//TLDreprocess.sh
			Path usTLDStoreListDistCache = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "USTLDStoreList_reprocess.txt");
			job.addCacheFile(new URI(usTLDStoreListDistCache.toString() + "#" + usTLDStoreListDistCache.getName()));
			
			MultipleOutputs.addNamedOutput(job, "MENU", TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "TRANS", TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "TRANSITEM", TextOutputFormat.class, Text.class, Text.class);
			
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
                      
            job.waitForCompletion(true);
			
			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}
			
			FileStatus[] fstats = fileSystem.listStatus(baseOutputPath);
			
			for ( int idx=0; idx < fstats.length; idx++ ) {
				
				if (fstats[idx].getPath().getName().startsWith("TRANSITEM")) {
					fileSystem.rename(fstats[idx].getPath(), new Path(baseOutputPath + "/TRANSITEM.txt-m-00000"));
				} else if (fstats[idx].getPath().getName().startsWith("MENU")) {
					fileSystem.rename(fstats[idx].getPath(), new Path(baseOutputPath + "/MENU.TXT-m-00000"));
				} else if (fstats[idx].getPath().getName().startsWith("TRANS")) {
					fileSystem.rename(fstats[idx].getPath(), new Path(baseOutputPath + "/TRANS.txt-m-00000"));
				} else if (fstats[idx].getPath().getName().startsWith("_")) {
					fileSystem.delete(fstats[idx].getPath(), false);
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateUSTLDAppFileFormats.runJob:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}
	}
		
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig
                                             ,String fileType
                                             ,String requestedTerrDateParms
                                             ,String xmlType) {

		ArrayList<Path> retPaths = new ArrayList<Path>();

		try {

			Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem, daasConfig, requestedTerrDateParms, "STLD");

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
			System.err.println("Error occured in GenerateUSTLDAppFileFormats.getVaildFilePaths:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}

		return(retPaths);

	}
}

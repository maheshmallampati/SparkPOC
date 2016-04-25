package com.mcd.gdw.test.daas.driver;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.permission.FsAction;
//import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

import com.mcd.gdw.test.daas.mapreduce.CompressGoldLayerMapper;
import com.mcd.gdw.test.daas.mapreduce.CompressGoldLayerReducer;

public class CompressGoldLayer extends Configured implements Tool {

	public static final String SEPARATOR_CHARACTER = "\t";
	
	//private FsPermission newFilePremission;
	
	private FileSystem fileSystem;

	public static void main(String[] args) throws Exception {

		int retval = ToolRunner.run(new Configuration(),new CompressGoldLayer(), args);
			
		System.out.println(" return value : " + retval);
			
	}

	@Override
	public int run(String[] argsall) throws Exception {

		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
		String[] args;

		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		
		args = gop.getRemainingArgs();
		
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
		}
		
		if ( configXmlFile.length() == 0 || fileType.length() == 0 || terrDate.length() == 0 )  {
			System.err.println("Invalid parameters");
			System.err.println("Usage: CompressGoldLayer -c config.xml -t filetype -d territoryDateParms");
			System.exit(8);
		}
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		if ( daasConfig.configValid() ) {
			
			//newFilePremission = new FsPermission(FsAction.READ_WRITE,FsAction.READ_WRITE,FsAction.READ_WRITE);
			
			fileSystem = FileSystem.get(getConf());
			
			runExtract(daasConfig,fileType,getConf(),terrDate,true);
			
		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.err.println("File Type   = " + fileType);
			System.exit(8);
		}
		
		return(0);
	}

	private void runExtract(DaaSConfig daasConfig
            ,String fileType
            ,Configuration hdfsConfig
            ,String terrDate
            ,boolean compressOut) {

		Job job;
		ArrayList<Path> requestedPaths = null;
		ArrayList<String> subTypeList;
		ABaC abac = null;


		try {
			abac = new ABaC(daasConfig);
			subTypeList = abac.getSubFileTypeCodes();
			abac.dispose();

			hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm()); 

			if ( compressOut ) {
				hdfsConfig.set("mapreduce.map.output.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
				hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			}

			job = Job.getInstance(hdfsConfig, "Compress Gold Layer Files");

			Path outPath= new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "compress");

			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, outPath,daasConfig.displayMsgs());

			requestedPaths = getVaildFilePaths(daasConfig,fileSystem,fileType,terrDate,subTypeList);

			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			job.setJarByClass(CompressGoldLayer.class);
			job.setMapperClass(CompressGoldLayerMapper.class);
			job.setReducerClass(CompressGoldLayerReducer.class);
			job.setMapOutputKeyClass(Text.class);
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

	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig
                       ,FileSystem fileSystem
                       ,String fileType
                       ,String requestedTerrDateParms
                       ,ArrayList<String> subTypeCodes) {

		ArrayList<Path> retPaths = new ArrayList<Path>();

		try {

			Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem, daasConfig, requestedTerrDateParms, subTypeCodes);

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
			System.err.println("Error occured in GenerateAsterFormat.getVaildFilePaths:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

		return(retPaths);
	}

}

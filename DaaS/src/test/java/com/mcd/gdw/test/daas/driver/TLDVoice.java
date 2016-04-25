package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.TLDVoiceMapper;
import com.mcd.gdw.test.daas.mapreduce.TLDVoicePreMapper;
import com.mcd.gdw.test.daas.mapreduce.TLDVoicePreReducer;
import com.mcd.gdw.test.daas.mapreduce.TLDVoiceReducer;
import com.mcd.gdw.test.daas.mapreduce.TLDVoicePostMapper;

public class TLDVoice extends Configured implements Tool {
	
	public static final String CACHE_FILE_NAME = "TLDVoice/voicetranslist.txt";
	
	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
	
	private Path cachePath;

	public static void main(String[] args) throws Exception {
		
		Configuration hdfsConfig = new Configuration();
		
		int retval = ToolRunner.run(hdfsConfig,new TLDVoice(),args);
		
		System.exit(retval);
				
	}
	
	public int run(String[] args) throws Exception {

		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
		boolean compressOut = true;
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
			System.out.println("Usage: " + this.getClass().getSimpleName() + " -c config.xml -t filetype -d territoryDateParms ");
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
			System.err.println("Usage: " + this.getClass().getSimpleName() + " -c config.xml -t filetype -d territoryDateParms");
			System.err.println("where territoryDateParm is a comma separated list of territory codes and dates separated by colons(:)");
			System.err.println("for example, 840:2012-07-01:2012-07-07 is territory 840 from July 1st, 2012 until July 7th, 2012.");
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

			//runPreJob(daasConfig,compressOut);
			//runJob(daasConfig,fileType,terrDate,compressOut);

			baseOutputPath = new Path("/daastest" + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "TLDVoice");

			cachePath = new Path("/daastest" + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + CACHE_FILE_NAME);
			
			FileStatus[] fstatustmp = null;
			String[] parts;
			fstatustmp = fileSystem.listStatus(baseOutputPath,new PathFilter() {
					
				@Override
				public boolean accept(Path pathname) {
					if(!pathname.getName().startsWith("_SUCCESS"))
						return true;
					return false;
				}
			});
			
			InputStreamReader isr; 
			DataInputStream fileStatusStream; 
			BufferedReader br;
			int cnt =0; 
			
			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(cachePath,true)));
			
			for(FileStatus fstat : fstatustmp) {
				if ( fstat.getPath().getName().toUpperCase().endsWith("GZ") ) {
					isr=new InputStreamReader(new GZIPInputStream(fileSystem.open(fstat.getPath())));
					br=new BufferedReader(isr);
				} else {
					fileStatusStream = new DataInputStream(fileSystem.open(fstat.getPath()));
					br = new BufferedReader(new InputStreamReader(fileStatusStream));
				}
			
				String line;
				line=br.readLine();
				while (line != null) {
					parts = line.split("\t");
					
					if ( parts.length >= 10 && !parts[8].equalsIgnoreCase("NULL") ) {
						bw.write(parts[0] + "\t" + parts[1] + "\t" + parts[8] + "\t" + parts[9] + "\t" + parts[6] + "\n");
						line=br.readLine();
						cnt++;
					}
				}
				
				br.close();
			}

			bw.close();
			
			if ( cnt > 0 ) {
				runPostJob(daasConfig,fileType,terrDate,compressOut);
			}
			
		} else {
			System.err.println("Invalid Config XML file, stopping");
			System.err.println(daasConfig.errText());
			System.exit(8);
		}
	
		return(0);
		
	}

	private void runPreJob(DaaSConfig daasConfig
                          ,boolean compressOut) {

		Job job;
		ArrayList<Path> requestedPaths;

		try {
			baseOutputPath = new Path("/daastest" + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "TLDVoicePre");
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

			requestedPaths = new ArrayList<Path>();
			
			for (int idx=1; idx <= 30; idx++ ) {
				Path addPath = new Path("/daastest/hive/SMG/SurveyHeader/terr_cd=840/cal_dt=2015-09-" + String.format("%02d", idx));
				requestedPaths.add(addPath);
				
				//System.out.println(addPath.toString());
				
				addPath = new Path("/daastest/hive/SMG/SurveyDetail/terr_cd=840/cal_dt=2015-09-" + String.format("%02d", idx));
				requestedPaths.add(addPath);
				
				//System.out.println(addPath.toString());
			}

			job = Job.getInstance(hdfsConfig, "TLD Voice Pre");
	
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			job.setJarByClass(TLDVoice.class);
			job.setMapperClass(TLDVoicePreMapper.class);
			job.setReducerClass(TLDVoicePreReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void runJob(DaaSConfig daasConfig
                       ,String fileType
                       ,String terrDate
                       ,boolean compressOut) {

		Job job;
		ArrayList<Path> requestedPaths;

		try {
			//newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

			baseOutputPath = new Path("/daastest" + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "TLDVoice");
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

			requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDate);
			
			requestedPaths.add(new Path("/daastest" + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "TLDVoicePre"));

			job = Job.getInstance(hdfsConfig, "TLD Voice");
	
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			job.setJarByClass(TLDVoice.class);
			job.setMapperClass(TLDVoiceMapper.class);
			job.setReducerClass(TLDVoiceReducer.class);
			//job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			//job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void runPostJob(DaaSConfig daasConfig
                           ,String fileType
                           ,String terrDate
                           ,boolean compressOut) {

		Job job;
		ArrayList<Path> requestedPaths;

		try {
			//newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

			baseOutputPath = new Path("/daastest" + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "TLDVoicePost");
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

			requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDate);

			job = Job.getInstance(hdfsConfig, "TLD Voice Post");
	
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			job.addCacheFile(new URI(cachePath.toString() + "#" + cachePath.getName()));

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			job.setJarByClass(TLDVoice.class);
			job.setMapperClass(TLDVoicePostMapper.class);
			//job.setReducerClass(TLDVoicePostReducer.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//job.setOutputKeyClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig
                                             ,String fileType
                                             ,String requestedTerrDateParms) {

		ArrayList<Path> retPaths = new ArrayList<Path>();
		
		if ( daasConfig.displayMsgs() ) {
			System.out.println("RequestedTerrDateParms = " + requestedTerrDateParms);
		}

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
			ex.printStackTrace(System.err);
			System.exit(8);
		}

		return(retPaths);

	}
	
}

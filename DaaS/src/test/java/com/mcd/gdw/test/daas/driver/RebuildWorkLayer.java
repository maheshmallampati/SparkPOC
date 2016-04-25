package com.mcd.gdw.test.daas.driver;

import java.net.URI;
import java.util.ArrayList;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.RebuildWorkLayerMapper;

public class RebuildWorkLayer extends Configured implements Tool {
	
	public class ResultListFilter implements PathFilter {
	    public boolean accept(Path path) {
	    	return(!path.getName().contains("RxD"));
	    }
	}
	
	public static final String CACHE_INCLUDE_LIST = "work.txt";
	
	private Path cachePath; 
	private FileSystem fileSystem;
	
	public static void main(String[] args) throws Exception {

		int retval = ToolRunner.run(new Configuration(),new RebuildWorkLayer(), args);
		
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
			System.err.println("Usage: RebuildWorkLayer -c config.xml -t filetype -d territoryDateParms");
			System.exit(8);
		}
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		if ( daasConfig.configValid() ) {
			
			//newFilePremission = new FsPermission(FsAction.READ_WRITE,FsAction.READ_WRITE,FsAction.READ_WRITE);
			//cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + CACHE_INCLUDE_LIST);
			cachePath = new Path("/user/mc05112/" + CACHE_INCLUDE_LIST);
			
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
		Path outPath;

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
		
			job = Job.getInstance(hdfsConfig, "Rebuild Work Layer");
			
			outPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "rebuild_out");;

			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, outPath,daasConfig.displayMsgs());

			requestedPaths = getVaildFilePaths(daasConfig,fileSystem,fileType,terrDate,subTypeList);
			
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			job.addCacheFile(new URI(cachePath.toString() + "#" + cachePath.getName()));

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			job.setJarByClass(RebuildWorkLayer.class);
			job.setMapperClass(RebuildWorkLayerMapper.class);
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
			
			FileStatus[] status = fileSystem.listStatus(outPath, new ResultListFilter());
			
			if ( status != null ) {
				for (int idx=0; idx < status.length; idx++ ) {
					fileSystem.delete(status[idx].getPath(),false);
				}
			}

			status = fileSystem.listStatus(outPath);
			String[] parts;
			Path newName;
			
			if ( status != null ) {
				for (int idx=0; idx < status.length; idx++ ) {
					parts = status[idx].getPath().toString().split("-");
					newName = new Path(parts[0] + "-r-" + parts[2]);
					fileSystem.rename(status[idx].getPath(), newName);
				}
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

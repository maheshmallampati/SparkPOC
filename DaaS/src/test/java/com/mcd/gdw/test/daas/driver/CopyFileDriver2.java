package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
//import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.abac.ABaCList;
import com.mcd.gdw.daas.abac.ABaCListItem;
import com.mcd.gdw.daas.driver.MergeToFinal;
import com.mcd.gdw.daas.mapreduce.CopyFileMapper;
import com.mcd.gdw.daas.mapreduce.CopyFileReducer;
import com.mcd.gdw.daas.util.CopyMoveNFileMapper;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.CopyFileMapper2;
import com.mcd.gdw.test.daas.mapreduce.CopyFileReducer2;

public class CopyFileDriver2 extends Configured implements Tool {
	FileSystem fileSystem = null;

	String fileListJobId = "";
	String updateAbac = "false";
	String moveFiles  = "false";
	String submitjob  = "true";
	String jobgroupId = "";
	String numreducers = "";
	DaaSConfig daasConfig = null;
	ABaC abac = null;
	
	
	public static void main(String[] args) {
		try {
			int retval = ToolRunner.run(new Configuration(),
					new CopyFileDriver2(), args);

			System.out.println(" return value : " + retval);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public int run(String[] argsall) throws Exception {

		GenericOptionsParser gop = new GenericOptionsParser(argsall);

		String[] args = gop.getRemainingArgs();

		if (args.length < 6) {
			System.err.println("Missing one or more parameters");
			System.err.println("Usage: " + CopyFileDriver2.class.getName()
					+ " -c DaaSConfFile.xml -t FileType -j fileListJobId [-s true -u false -m false]");
			if (args.length > 0) {
				System.err.print("Suppled parameters =");
				for (int idx = 0; idx < args.length; idx++) {
					System.err.print(" " + args[idx]);
				}
			} else {
				System.err.println("Suppled parameters = NONE");
			}
			System.err.println("\nStopping");
			System.exit(8);
		}

		String configXmlFile = "";
		String fileType = "";

		for (int idx = 0; idx < args.length; idx++) {
			if (args[idx].equals("-c") && (idx + 1) < args.length) {
				configXmlFile = args[idx + 1];
			}

			if (args[idx].equals("-t") && (idx + 1) < args.length) {
				fileType = args[idx + 1];
			}
			if (args[idx].equals("-j") && (idx + 1) < args.length) {
				fileListJobId = args[idx + 1];
			}
			if (args[idx].equals("-s") && (idx + 1) < args.length) {
				submitjob = args[idx + 1];
			}
			if (args[idx].equals("-u") && (idx + 1) < args.length) {
				updateAbac = args[idx + 1];
			}
			if (args[idx].equals("-m") && (idx + 1) < args.length) {
				moveFiles = args[idx + 1];
			}
			if (args[idx].equals("-jg") && (idx + 1) < args.length) {
				jobgroupId = args[idx + 1];
			}
			if (args[idx].equals("-r") && (idx + 1) < args.length) {
				numreducers = args[idx + 1];
			}
		}

		if (configXmlFile.length() == 0 || fileType.length() == 0) {
			System.err.println("Missing config.xml and/or filetype");
			System.err.println("Usage: MergeToFinal -c config.xml -t filetype -j 12345 -s true -u false -m false");
			System.exit(8);
		}

		daasConfig = new DaaSConfig(configXmlFile, fileType);
		
		abac = new ABaC(daasConfig);

		runMrCopyWithFilter(daasConfig, fileType, getConf());
		return 0;
	}

	private void runMrCopyWithFilter(DaaSConfig daasConfig, String fileType,
			Configuration jobConfig) throws Exception {

		fileSystem = FileSystem.get(jobConfig);

		try {
			System.out.println("\nCopy Files with Filter\n");
			int updateAbaCJobSeqNbr = 7;
			MergeToFinal mergeToFinal = new MergeToFinal(daasConfig,fileType,fileSystem,updateAbaCJobSeqNbr);
			
			Path baseOutputPath = new Path(daasConfig.hdfsRoot()+ Path.SEPARATOR + daasConfig.hdfsWorkSubDir()+ Path.SEPARATOR + daasConfig.fileSubDir());

			Path step1OutputPath = new Path(baseOutputPath.toString()+ Path.SEPARATOR + "step1");
			Path step2OutputPath = new Path(baseOutputPath.toString()+ Path.SEPARATOR + "step2");
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, step2OutputPath,daasConfig.displayMsgs());
			Path step3OutputPath = new Path(baseOutputPath.toString()+ Path.SEPARATOR + "step3");
			
			
			if(submitjob.equalsIgnoreCase("true")){
				
				
				jobConfig.set("mapred.cluster.map.memory.mb", "4096");
				jobConfig.set("mapred.job.map.memory.mb", "4096");
				jobConfig.set("io.sort.mb", "672");
				jobConfig.set("mapred.map.child.java.opts", "-Xmx3072m");
				jobConfig.set("mapred.reduce.child.java.opts", "-Xmx3072m");
				
				jobConfig.set("mapred.compress.map.output", "true");
				jobConfig.set("mapred.output.compress", "true");
//				jobConfig.set("mapred.output.compression.type", "BLOCK"); 
				jobConfig.set("mapred.output.compression.type", "RECORD"); 
				jobConfig.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//				hdfsConfig.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec"); 
				jobConfig.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
				
				
				jobConfig.set("mapred.child.java.opts", "-server -Xmx3072m -Djava.net.preferIPv4Stack=true");
				Job job = new Job(jobConfig,"Processing MapReduce Copy with Filter - " + fileType);

				job.setJarByClass(CopyFileMapper.class);
				job.setMapperClass(CopyFileMapper.class);
//				job.setReducerClass(CopyFileReducer2.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(NullWritable.class);
				// job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setNumReduceTasks(0);
				// job.setOutputFormatClass(TextOutputFormat.class);
				
				
				
				FileStatus[] fstatusarr = fileSystem.listStatus(step2OutputPath);
				TreeSet<String> uniquedates = new TreeSet<String>();
				
				String fileName;
				String[] fileParts;
				String date;
				for(FileStatus fstatus:fstatusarr){
					
					if(!fstatus.isDir()){
						fileName  = fstatus.getPath().getName();
						fileParts = fileName.split("~");
						
						date = fileParts[2];
						
						uniquedates.add(date);
					}
				}
				
				if(uniquedates.size() > 0){
					Iterator<String> it = uniquedates.iterator();
					while(it.hasNext()){
						date = it.next();
						
						System.out.println("Adding multioutput ProductRxD045DbRxD126840RxD126" +date);
						System.out.println("Adding multioutput StoreRxD045DbRxD126" +date);
						System.out.println("Adding multioutput STLDRxD126840RxD126" +date);
						System.out.println("Adding multioutput DetailedSOSRxD045dbRxD126" +date);
						System.out.println("Adding multioutput MenuItemRxD045dbRxD126" +date);
						System.out.println("Adding multioutput SecurityDataRxD126840RxD126" +date);
						System.out.println("\n--------------------------------------------------");
								
						MultipleOutputs.addNamedOutput(job,"ProductRxD045DbRxD126840RxD126"+date,TextOutputFormat.class, Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"StoreRxD045DbRxD126"+date,TextOutputFormat.class, Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"STLDRxD126840RxD126"+date,TextOutputFormat.class, Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"DetailedSOSRxD045dbRxD126"+date,TextOutputFormat.class, Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"MenuItemRxD045dbRxD126"+date, TextOutputFormat.class,Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"SecurityDataRxD126840RxD126"+date,TextOutputFormat.class, Text.class, Text.class);
					}
				}
						
				HDFSUtil.removeHdfsSubDirIfExists(fileSystem, step3OutputPath, true);

//				Path cachePath = new Path("/daas/lz/4902/cache");
				Path cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "cache");
				
				FileInputFormat.setInputPaths(job, step2OutputPath);//+"/STLD~840~20140606~0000609.gz"
				TextOutputFormat.setOutputPath(job, step3OutputPath);
				
				LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

				DistributedCache.addCacheFile(new Path(cachePath.toString() + Path.SEPARATOR+ daasConfig.abacSqlServerCacheFileName()).toUri(),job.getConfiguration());
				job.waitForCompletion(true);
			}
			
			
			if(daasConfig == null){
				System.out.println( " daasConfig is null ");
				return;
			}
			if(updateAbac.equalsIgnoreCase("true")){// update file/subfile status'
				mergeToFinal.updABaC (Integer.parseInt(jobgroupId),daasConfig,fileSystem);
				abac.closeJobGroup(Integer.parseInt(jobgroupId),DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
			}
			
			if(moveFiles.equalsIgnoreCase("true")){// move files from step1 and step3 to the gold layer
				String fileSuffix = "gz";
				Path baseFinalPath = new Path(daasConfig.hdfsRoot()
						+ Path.SEPARATOR + daasConfig.hdfsFinalSubDir()
						+ Path.SEPARATOR + daasConfig.fileSubDir());
	
				mergeToFinal.moveFiles(step1OutputPath, baseFinalPath, fileSuffix, true, true,jobConfig,Integer.parseInt(fileListJobId));
				mergeToFinal.moveFiles(step3OutputPath, baseFinalPath, fileSuffix, true, true,jobConfig,Integer.parseInt(fileListJobId));
			}

		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}
	
	
}


package com.mcd.gdw.daas.driver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.ArrayUtils;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.mapreduce.QuarterHourDaypartPMIXMapper;
import com.mcd.gdw.daas.mapreduce.QuarterHourDaypartPMIXReducer;
import com.mcd.gdw.daas.util.DaaSConfig;

public class GenerateQuarterHourDaypartPMIXFromStld extends Configured implements Tool {

	int prevJobGroupId = -1;
	//int prevjobSeqNbr = -1;
	int prevjobSeqNbr = 99;
	DaaSConfig daasConfig = null;
	
	private Set<Integer> validTerrCdSet = new HashSet<Integer>();
	
	String numtasks = "1";
	
	ABaC  abac = null;
	String owshFltr = "*";
	
	public static void main(String[] args) throws Exception {
		
		
		
		Configuration conf1 = new Configuration();
		System.out.println(" property value  " +conf1.get("io.sort.mb"));
		int retval = ToolRunner.run(conf1,new GenerateQuarterHourDaypartPMIXFromStld(), args);

		System.out.println(" return value : " + retval);

	}

	@Override
	public int run(String[] argsall) throws Exception {
		
			System.out.println(" property value  " + getConf().get("io.sort.mb"));
		
			GenericOptionsParser gop = new GenericOptionsParser(argsall);
			String[] args = gop.getRemainingArgs();
		
		System.out.println( " gop.getRemainingArgs().length " + gop.getRemainingArgs().length);
		System.out.println(args.length);
		
		for (int idx2 = 0; idx2 < args.length; idx2++) {
			System.out.println (idx2 + " : "+args[idx2]);
		}
		
		int idx;
		String inputRootDir = "";

		String outputDir = "";

		String cacheFile = "";

		String propertiesstr = "";

		String queuename ="default";
		
		String configXmlFile = "";
		String fileType = "";
		String vldTerrCdsStr = "";
		try {
			for (idx = 0; idx < args.length; idx++) {
				if ((idx % 2) != 0) {
					if (args[idx - 1].equals("-r")) {
						inputRootDir = args[idx];
					}

					if (args[idx - 1].equals("-o")) {
						outputDir = args[idx];
					}
					if (args[idx - 1].equals("-dc")) {
						cacheFile = args[idx];
					}
					if (args[idx - 1].equals("-q")) {
						queuename = args[idx];
					}
					if ( args[idx-1].equals("-c") ) {
						configXmlFile = args[idx];
					}

					if ( args[idx-1].equals("-t")) {
						fileType = args[idx];
					}
					if ( args[idx-1].equals("-prevJobGroupId")  ) {
						prevJobGroupId = Integer.parseInt(args[idx]);
					}
					if (args[idx-1].equals("-seqNbr")) {
						prevjobSeqNbr = Integer.parseInt(args[idx]);
					}
					if ( args[idx-1].equals("-owshfltr")) {
						owshFltr = args[idx];
					}
					if ( args[idx-1].equals("-numtasks")) {
						numtasks = args[idx];
					}
					if ( args[idx-1].equalsIgnoreCase("-vldTerrCodes") ) {
						vldTerrCdsStr = args[idx];
						
						String[] terrCds = vldTerrCdsStr.split(",");
						for(String terrCd:terrCds){
							validTerrCdSet.add(Integer.parseInt(terrCd));
						}
					}
					

				}
			}
			
			daasConfig = new DaaSConfig(configXmlFile,fileType);
			abac = new ABaC(daasConfig);
			

//			if(prevJobGroupId == -1){
//				prevJobGroupId= abac.getOpenJobGroupId(DaaSConstants.TDA_EXTRACT_JOBGROUP_NAME+"test");
//				System.out.println( " ABAC query returned " + prevJobGroupId);
//				if(prevJobGroupId == -1)
//						prevJobGroupId = abac.createJobGroup(DaaSConstants.TDA_EXTRACT_JOBGROUP_NAME+"test");
//			}
//			
//			if(prevjobSeqNbr == -1){
//				prevjobSeqNbr = 4;
//			}
			if (inputRootDir.length() > 0 && outputDir.length() > 0) {
				return runJob(inputRootDir, outputDir, cacheFile, propertiesstr,queuename);
			} else {
				Logger.getLogger(GenerateQuarterHourDaypartPMIXFromStld.class.getName())
						.log(Level.SEVERE,
								"Missing input root directory, sub directory or output directory arguments");
				Logger.getLogger(GenerateQuarterHourDaypartPMIXFromStld.class.getName())
						.log(Level.INFO,
								"Usage "
										+ GenerateQuarterHourDaypartPMIXFromStld.class
												.getName()
										+ " -r rootdirectory -o outputdirectory -c distcachefile");
				System.exit(8);
			}
		} catch (Exception ex) {
			Logger.getLogger(GenerateQuarterHourDaypartPMIXFromStld.class.getName()).log(
					Level.SEVERE, null, ex);
		}finally{
			if(abac != null)
				abac.dispose();
		}

		return 0;
	}

	public int runJob(String inputRootDir, String outputDir,
			String cacheFile, String propertiesstr,String queuename) throws Exception {

		Configuration conf = this.getConf();
		Job job;
		FileSystem hdfsFileSystem;
		Path hdfsOutputPath;
		ArrayList<String> inputDirList = new ArrayList<String>();
		ArrayList<Path> inputDirPathList = new ArrayList<Path>();
		int jobId = 0;
		try {
			
			conf.set("mapred.job.queue.name", queuename);
			
			hdfsFileSystem = FileSystem.get(conf);

			job = new Job(conf, "Generate Quarter Hour PMIX");
			
//			abac = new ABaC(daasConfig);
//			jobId = abac.createJob(prevJobGroupId, ++prevjobSeqNbr, job.getJobName());
//			jobId = abac.createJob(prevJobGroupId, 99, job.getJobName());

//			FileInputFormat.addInputPath(job, new Path(inputRootDir));

			job.setJarByClass(GenerateQuarterHourDaypartPMIXFromStld.class);
			job.setMapperClass(QuarterHourDaypartPMIXMapper.class);
			job.setReducerClass(QuarterHourDaypartPMIXReducer.class);
			
			job.setNumReduceTasks(Integer.parseInt(numtasks));
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			String[] cachefilestrs = cacheFile.split(",");

			for (String cachepathstr : cachefilestrs) {

				System.out.println("adding " + cachepathstr + " to dist cache");
				DistributedCache.addCacheFile(new Path(cachepathstr).toUri(),
						job.getConfiguration());
			}

			hdfsOutputPath = new Path(outputDir);
			
			

			FileOutputFormat.setOutputPath(job, hdfsOutputPath);
			hdfsFileSystem = FileSystem.get(hdfsOutputPath.toUri(), conf);

			
			if (hdfsFileSystem.exists(hdfsOutputPath)) {
				hdfsFileSystem.delete(hdfsOutputPath, true);
				Logger.getLogger(GenerateQuarterHourDaypartPMIXFromStld.class.getName())
						.log(Level.INFO,
								"Removed existing output path = " + outputDir);
			}
			
			
			FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;
		
		
			int totalInputFileCount = 0;
			String[] inputpathstrs = inputRootDir.split(",");
			
			
			for(String inputpaths:inputpathstrs){
				fstatustmp = hdfsFileSystem.globStatus(new Path(inputpaths+"/STLD*"));
				fstatus = (FileStatus[])ArrayUtils.addAll(fstatus, fstatustmp);
	
			}
			String filepath;
			String datepart;
			String terrCdDatepart;
			HashSet<String> terrCdDtset = new HashSet<String>();
			String[] fileNameParts;
			for(FileStatus fstat:fstatus){
				String fileName = fstat.getPath().getName().toUpperCase();
				
				String fileNamePartsDelimiter = "~";
				
				if(fileName.indexOf("RXD126") > 0){
					fileNamePartsDelimiter = "RXD126";
				}
				fileNameParts = fileName.split(fileNamePartsDelimiter);
				String terrCdfrmFileName = fileNameParts[1];
				
				if(validTerrCdSet != null && validTerrCdSet.contains(Integer.parseInt(terrCdfrmFileName))){
					FileInputFormat.addInputPath(job, fstat.getPath());
					
					totalInputFileCount++;
					
					datepart = fileNameParts[2].substring(0,8);
					terrCdDtset.add(terrCdfrmFileName+DaaSConstants.SPLCHARTILDE_DELIMITER+datepart);
				 
				}
				}
			
				if(! (totalInputFileCount > 0)){
					System.out.println(" There are no input files to process; exiting");
					System.exit(1);
				}
				Iterator<String> it = terrCdDtset.iterator();
			
				while(it.hasNext()){
					terrCdDatepart = it.next();
					System.out.println(" addding " +terrCdDatepart);
					MultipleOutputs.addNamedOutput(job,"15MINDAYPART"+DaaSConstants.SPLCHARTILDE_DELIMITER+terrCdDatepart,TextOutputFormat.class, Text.class, Text.class);
				}
			
		
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			int retCode= job.waitForCompletion(true) ? 0 : 1;
			
//			abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
//			abac.closeJobGroup(prevJobGroupId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);

		} catch (InterruptedException ex) {
			Logger.getLogger(GenerateQuarterHourDaypartPMIXFromStld.class.getName()).log(
					Level.SEVERE, null, ex);
			throw ex;
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(GenerateQuarterHourDaypartPMIXFromStld.class.getName()).log(
					Level.SEVERE, null, ex);
			throw ex;
		} catch (Exception ex) {
			Logger.getLogger(GenerateQuarterHourDaypartPMIXFromStld.class.getName()).log(
					Level.SEVERE, null, ex);
			throw ex;
		}finally{
			if(abac != null)
				abac.dispose();
		}

		return 0;
	}


}

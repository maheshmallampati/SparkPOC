package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
import com.mcd.gdw.daas.abac.ABaCListSubItem;
import com.mcd.gdw.daas.driver.MergeToFinal;

//import com.mcd.gdw.daas.abac.ABaC2.CodeType;

//import com.mcd.gdw.daas.mapreduce.CopyFileMapper;
import com.mcd.gdw.daas.mapreduce.CopyFileMapper;
import com.mcd.gdw.daas.mapreduce.CopyFileReducer;
import com.mcd.gdw.daas.mapreduce.TextCopyFileReducer;
//import com.mcd.gdw.daas.mapreduce.UnzipInputFileMapper;
import com.mcd.gdw.daas.mapreduce.ABaCMapper;
import com.mcd.gdw.daas.mapreduce.TextCopyFileMapper;
import com.mcd.gdw.daas.mapreduce.UnZipCompositeKeyComparator;
import com.mcd.gdw.daas.mapreduce.UnZipGroupComparator;
import com.mcd.gdw.daas.mapreduce.UnZipPartitioner;
import com.mcd.gdw.daas.mapreduce.UnzipInputFileMapper;
import com.mcd.gdw.daas.mapreduce.UnzipInputFileReducer;
import com.mcd.gdw.daas.mapreduce.TextInputFileMapper;
import com.mcd.gdw.daas.mapreduce.TextInputFileReducer;

//import com.mcd.gdw.daas.mapreduce.UnzipInputFileGroupComparator;
//import com.mcd.gdw.daas.mapreduce.UnzipInputFileCompKeyComparator;
//import com.mcd.gdw.daas.mapreduce.UnzipInputfilePartitioner;
import com.mcd.gdw.daas.mapreduce.ZipFileInputFormat;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
/**
 * 
 * @author Sateesh Pula
 * 
 * run unzip
 * hadoop jar /home/mc32445/scripts/daasmapreduce.jar com.mcd.gdw.daas.driver.MergeToFinal -Dio.sort.mb=1024 -c config.xml -t POS_XML
 */
public class UnzipDriver extends Configured implements Tool {

	private int jobGroupId = 0;
	private int fileListJobId = 0;
//	private int jobId = 0;
	private ABaC abac = null;
	private ABaCList abac2List = null;
	private String[] fileSubTypes = null; 
	private HashMap<String,Integer> terrDateMap = new HashMap<String,Integer>();
	private FileSystem fileSystem = null;
	//private Configuration hdfsConfig = null;
	
	private Path cachePath = null;
	private Path baseOutputPath = null;
	private Path step0OutputPath = null;
	private Path step1OutputPath = null;
	private Path step2OutputPath = null;
	private Path step3OutputPath = null;
	private Path baseFinalPath = null;

	private int nextFileNum = 1;
	private int jobSeqNbr   = 1;
	
	

	private static ArrayList<String> overrides = new ArrayList<String>();
	
	

public static void main(String[] args) throws Exception {
				
		int retval = ToolRunner.run(new Configuration(),new UnzipDriver(), args);

		System.out.println(" return value : " + retval);
	}

	public int run(String[] argsall) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		
		String[] args = gop.getRemainingArgs();
		
		if ( args.length < 2 ) {
			System.err.println("Missing one or more parameters");
			System.err.println("Usage: " + UnzipDriver.class.getName() + " DaaSConfFile.xml FileType");
			if ( args.length > 0 ) {
				System.err.print("Suppled parameters =");
				for (int idx=0; idx < args.length; idx++ ) {
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
		
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-t") && (idx+1) < args.length ) {
				fileType = args[idx+1];
			}
		}
		
		if ( configXmlFile.length() == 0 || fileType.length() == 0 ) {
			System.err.println("Missing config.xml and/or filetype");
			System.err.println("Usage: MergeToFinal -c config.xml -t filetype");
			System.exit(8);
		}

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		if ( daasConfig.configValid() ) {
			
			runJob(daasConfig,fileType,getConf());
			
		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.err.println("File Type   = " + fileType);
			System.exit(8);
		}
		
		return(0);
	}
	
	private void runJob(DaaSConfig daasConfig
                       ,String fileType
                       ,Configuration hdfsConfig) throws Exception {

//		int mergejobid = 0;
		int jobId = 0;
		String fileSuffix = "";

		terrDateMap = new HashMap<String,Integer>();

		if ( overrides.size() > 0 ) {
			System.out.println("\nHadoop Overridden Paramerters:");

			for ( String overrideParm : overrides ) {
				System.out.println("   " +overrideParm+"="+hdfsConfig.get(overrideParm));	
			}
			
			System.out.println("");
		}

		fileSystem = FileSystem.get(hdfsConfig);
		
		abac = new ABaC(daasConfig);
		
		jobGroupId = abac.createJobGroup("Unzip Process");
		fileListJobId = abac.createJob(jobGroupId, jobSeqNbr++, "Get File List");
		
		hdfsConfig.set("job.group.id", ""+jobGroupId);
		//cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "cache"+Path.SEPARATOR+daasConfig.abacCacheFileName());
		cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "cache");
		hdfsConfig.set("path.to.cache", cachePath.toString());

		hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_ABAC_FROM_PATH,daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.hdfsLandingZoneArrivalSubDir());
		hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_ABAC_TO_PATH,daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "source");
		hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_ABAC_TO_REJECT_PATH,daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "reject");

		getCacheInfo(daasConfig,fileType,fileListJobId,fileSystem);
		
		baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir());
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

		step0OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step0");
		runABaCMapper(daasConfig,fileType,hdfsConfig,fileListJobId,fileSystem);
		
		if ( daasConfig.fileCompressOutput() ) {
			
			System.out.println("Setting Compress Output2");
			
			fileSuffix = "gz";
			
			hdfsConfig.set("mapred.compress.map.output", "true");
			hdfsConfig.set("mapred.output.compress", "true");
//			hdfsConfig.set("mapred.output.compression.type", "BLOCK"); 
			hdfsConfig.set("mapred.output.compression.type", "RECORD"); 
			hdfsConfig.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//			hdfsConfig.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec"); 
			hdfsConfig.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		}


//		hdfsConfig.set("mapred.child.java.opts", "-server -Djava.net.preferIPv4Stack=true -verbose:gc -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="+daasConfig.heapDumpPath()+" "+daasConfig.fileMapReduceJavaHeapSizeParm()); 
		hdfsConfig.set("mapred.child.java.opts", "-server -Djava.net.preferIPv4Stack=true "+daasConfig.fileMapReduceJavaHeapSizeParm());

//		hdfsConfig.set("mapred.child.java.opts", "-server -Djava.net.preferIPv4Stack=true -verbose:gc -XX:+PrintGCDetails "+daasConfig.fileMapReduceJavaHeapSizeParm());
		
		
		step1OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step1");
		step2OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step2");
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,step2OutputPath,daasConfig.displayMsgs());
		step3OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step3");
		//Path mergeOutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "merge");
		//HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,mergeOutputPath,daasConfig.displayMsgs());
		
		baseFinalPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir());
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,baseFinalPath,daasConfig.displayMsgs());
		
		Path baseFinalPathSubType = null;

		if (fileSubTypes != null && fileSubTypes.length > 0 ) {
			for (int idx=0; idx < fileSubTypes.length; idx++ ) {
				baseFinalPathSubType = new Path(baseFinalPath.toString() + Path.SEPARATOR + fileSubTypes[idx]);
				HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,baseFinalPathSubType,daasConfig.displayMsgs());
			}
		}
		
		System.out.println( " daasConfig.fileInputIsZipFormat() " + daasConfig.fileInputIsZipFormat());
		if ( daasConfig.fileInputIsZipFormat() ) {
			runMrUnzip(daasConfig,fileType,hdfsConfig);
		}

		updABaC(jobGroupId, daasConfig, fileSystem);
		
		
//		abac.closeJob(mergejobid, (short)1,"successful");
		abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);

		abac.dispose();

		if ( !daasConfig.hdfsKeepTempDirs() ) {
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem,baseOutputPath,daasConfig.displayMsgs());
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem,new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.fileSubDir()),daasConfig.displayMsgs());
		}
		
	
		
	}

	private void updABaC(int jobGroupId
            ,DaaSConfig daasConfig
            ,FileSystem fileSystem) {
	
		Path hdfsSubItemList = null; 
	
	
		if ( daasConfig.fileInputIsZipFormat() ) {
			hdfsSubItemList = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + "step1" );
		}
	
		DataInputStream fileStatusStream = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
	
		try {
	
			int jobId = abac.createJob(jobGroupId, jobSeqNbr++,"Update ABaC");
		
			Timestamp jobGroupStartTS = getJobGroupStartTime(jobGroupId,daasConfig);
			
			abac2List.clear();
		
			if ( daasConfig.fileInputIsZipFormat() ) {
				FileStatus[] status = fileSystem.listStatus(hdfsSubItemList);
	
				for ( int i=0; i < status.length; i++ ) {
					if ( status[i].getPath().getName().startsWith("FileStatus-r") ) {
					
					
					
						if ( daasConfig.fileInputIsZipFormat() ) {
							isr=new InputStreamReader(new GZIPInputStream(fileSystem.open(status[i].getPath())));
							br=new BufferedReader(isr);
						} else {
							fileStatusStream = new DataInputStream(fileSystem.open(status[i].getPath()));
							br = new BufferedReader(new InputStreamReader(fileStatusStream));
						}
					
						String line;
						line=br.readLine();
//						String[] lineparts = null;
						ABaCListItem  abac2ListItem = null;
//						Iterator<Entry<String, ABaC2ListSubItem>>  subItemIterator = null;
//						ABaC2ListSubItem abac2ListSubItem = null;
						while (line != null){
						
							abac2ListItem = ABaC.deserializeListItemFromHexString(line);
							
							abac2ListItem.setFileProcessStartTimestamp(jobGroupStartTS);
							
							abac2List.addItem(abac2ListItem);
						
//							lineparts = line.split("\\|");
//		
//							abac2ListItem = abac2List.getItem(lineparts[0]);
////						subItemIterator = abac2ListItem.iterator();
//						
//						
//						
//						
//							abac2ListSubItem = abac2ListItem.getSubItem(lineparts[2]);
//						
//							abac2ListSubItem.setStatusTypeId(abac.getCodeType(Integer.parseInt(lineparts[3])));
//							abac2ListSubItem.setFileSizeNum(Float.parseFloat(lineparts[5]));
							line=br.readLine();
						}
	
					
					}
				}
			}
	
			abac.updateFileList(abac2List);
	
			abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
	
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err.println("ABaC update list failed");
			System.err.println(ex.toString());
			System.exit(8);
		}finally{
			try{
				if(br != null)
					br.close();
			
				if(isr != null)
					isr.close();
			
			
			
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
	
	public Timestamp getJobGroupStartTime(int jobGroupId,DaaSConfig daasConfig) throws Exception{
		StringBuffer sql = new StringBuffer();
	
		if(abac  == null)
			abac = new ABaC(daasConfig);
		
		sql.setLength(0);
		sql.append("select\n");
		sql.append("   a." + ABaC.COL_DW_JOB_GRP_STRT_TS + "\n");
		sql.append("from " + daasConfig.abacSqlServerDb() + "." + ABaC.TBL_DW_JOB_GRP_XECT + " a with (NOLOCK)\n");
		sql.append("where a." + ABaC.COL_DW_JOB_GRP_XECT_ID  + " = " +  jobGroupId+ "\n" );
		sql.append("and   a." + ABaC.COL_DW_JOB_GRP_END_TS + " is null\n");
		try{
			
			ResultSet rs = abac.resultSet(sql.toString());
			
			if(rs != null){
				rs.next();
				return rs.getTimestamp(1);
			}
			
		}catch(Exception ex){
			ex.printStackTrace();
			throw new Exception (ex.getMessage());
		}
		return new Timestamp(System.currentTimeMillis());
	}
	
	
	private void getCacheInfo(DaaSConfig daasConfig
                             ,String fileType
                             ,int jobId
                             ,FileSystem fileSystem) {

		try {
			//abac2List = abac.setupList(jobId, fileSystem);
			abac2List = abac.setupList2(jobId, fileSystem,false,"840");
			
			//abac.saveList(jobId, abac2List, fileSystem);
			
			String fileSubTypeText = "";
			String mapKey = "";
			boolean firstLineFl = true;
			for(Map.Entry<String,ABaCListItem> entry : abac2List){
				
				ABaCListItem  abac2ListItem = entry.getValue();
				if ( firstLineFl ) {
					firstLineFl = false;
					Iterator<Entry<String, ABaCListSubItem>>  subItemIterator = abac2ListItem.iterator();
					
					while(subItemIterator.hasNext()){
						ABaCListSubItem abac2ListSubItem = subItemIterator.next().getValue();
						
						if ( fileSubTypeText.length() > 0 ) {
							fileSubTypeText += "|";
						}
						fileSubTypeText += abac2ListSubItem.getSubFileDataTypeCd();
						
					}
					
					if ( fileSubTypeText.length() > 0 ) {
						fileSubTypes = fileSubTypeText.split("\\|");
					}
				}
					
					mapKey = abac2ListItem.getTerrCd()+"|"+abac2ListItem.getBusnDt();
					if  ( terrDateMap.containsKey(mapKey) ) {
						terrDateMap.put(mapKey, (int)terrDateMap.get(mapKey)+1);
					} else {
						terrDateMap.put(mapKey, 1);
					}
				
			}
	
		} catch (Exception ex) {
			System.err.println("Reading cache file error:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void runMrUnzip(DaaSConfig daasConfig
	                       ,String fileType
	                       ,Configuration hdfsConfig) throws Exception{

		
//		int jobId = 0;
		Job job;
		
		System.out.println( " unzip called jobGroupId " + jobGroupId );
		System.out.println("mapred.child.ulimit " + hdfsConfig.get("mapred.child.ulimit"));
		
//		jobId = abac.createJob(jobGroupId, jobSeqNbr++, "MapReduce Unzip");

		try {
			System.out.println("\nUnzipping files\n");

			hdfsConfig.set("skipFilesonSize", daasConfig.skipFilesonSize());
			hdfsConfig.set("MAX_FILE_SIZE", daasConfig.maxFileSize());
			
			job = new Job(hdfsConfig, "Processing - " + fileType);

			ZipFileInputFormat.setLenient(true);
//			ZipFileInputFormat.setInputPaths(job, new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + "*.zip"));
//			ZipFileInputFormat.setInputPaths(job, new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + daasConfig.filePattern()));
			ZipFileInputFormat.setInputPaths(job, new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "source" ));
			
			job.setJarByClass(UnzipDriver.class);
			job.setMapperClass(UnzipInputFileMapper.class);
			job.setReducerClass(UnzipInputFileReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			//job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(daasConfig.numberofReducers());
			job.setInputFormatClass(ZipFileInputFormat.class);
//			job.setOutputFormatClass(TextOutputFormat.class);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			job.setPartitionerClass(UnZipPartitioner.class);
			job.setGroupingComparatorClass(UnZipGroupComparator.class);
			job.setSortComparatorClass(UnZipCompositeKeyComparator.class);
	
			if ( fileSubTypes != null ) {

				String[] keyValueParts = {"",""};
		
				for ( Map.Entry<String, Integer> entry : terrDateMap.entrySet()) {
		
					try {
						keyValueParts = (entry.getKey()).split("\\|");
				
						Path finalPathSubTypeDt = null;

						for ( int idx=0; idx < fileSubTypes.length; idx++ ) {
							MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(fileSubTypes[idx] + HDFSUtil.FILE_PART_SEPARATOR + keyValueParts[0] + HDFSUtil.FILE_PART_SEPARATOR + keyValueParts[1]),TextOutputFormat.class, Text.class, Text.class);
					
							finalPathSubTypeDt = new Path(baseFinalPath.toString() + Path.SEPARATOR + fileSubTypes[idx] + Path.SEPARATOR + keyValueParts[0] + Path.SEPARATOR + keyValueParts[1]);
							HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,finalPathSubTypeDt,daasConfig.displayMsgs());
					
							System.out.println("Added Multiple Output for " + fileSubTypes[idx] + " | " + keyValueParts[0] + " | " + keyValueParts[1]);
						}
					} catch(Exception ex) {
						System.err.println("Error occured in building MultipleOutputs File Name");
						System.err.println(ex.toString());
						System.exit(8);
					}
				}
			}

			MultipleOutputs.addNamedOutput(job,"FileStatus" ,TextOutputFormat.class, Text.class, Text.class);
			System.out.println("Added Multiple Output for FileStatus\n");
	
			TextOutputFormat.setOutputPath(job, step1OutputPath);

			DistributedCache.addCacheFile(new Path(cachePath.toString() + Path.SEPARATOR + daasConfig.abacSqlServerCacheFileName()).toUri(),job.getConfiguration());
	
			if ( job.waitForCompletion(true) ) {
				
//				abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
				
			} else {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}


		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}
	}
	
	private void runABaCMapper(DaaSConfig daasConfig
            ,String fileType
            ,Configuration hdfsConfig
            ,int jobId
            ,FileSystem fileSystem) throws Exception{

Job job;

try {
job = new Job(hdfsConfig, "ABaC - Move Files");

FileInputFormat.addInputPath(job, new Path(cachePath.toString() + Path.SEPARATOR + "move*"));

job.setJarByClass(MergeToFinal.class);
job.setMapperClass(ABaCMapper.class);
job.setMapOutputKeyClass(NullWritable.class);
job.setMapOutputValueClass(Text.class);
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);
job.setNumReduceTasks(1);

TextOutputFormat.setOutputPath(job, step0OutputPath);

if ( job.waitForCompletion(true) ) {

abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
} else {
System.err.println("Error occured in MapReduce process, stopping");
System.exit(8);
}

} catch (Exception ex) {
System.err.println("Error occured in MapReduce process:");
System.err.println(ex.toString());
System.exit(8);
}
}

	
}
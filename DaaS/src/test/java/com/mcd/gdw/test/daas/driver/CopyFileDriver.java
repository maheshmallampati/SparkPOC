package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import org.apache.hadoop.fs.permission.FsPermission;
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

public class CopyFileDriver extends Configured implements Tool {
	FileSystem fileSystem = null;

	String fileListJobId = "";
	String updateAbac = "false";
	String moveFiles  = "false";
	String submitjob  = "true";
	String jobgroupId = "";
	String numreducers = "";
	
	DaaSConfig daasConfig ;
	public static void main(String[] args) {
		try {
			int retval = ToolRunner.run(new Configuration(),
					new CopyFileDriver(), args);

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
			System.err.println("Usage: " + CopyFileDriver.class.getName()
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

		runMrCopyWithFilter(daasConfig, fileType, getConf());
		return 0;
	}

	private void runMrCopyWithFilter(DaaSConfig daasConfig, String fileType,
			Configuration jobConfig) throws Exception {

		fileSystem = FileSystem.get(jobConfig);

		try {
			System.out.println("\nCopy Files with Filter\n");
		
			
			Path baseOutputPath = new Path(daasConfig.hdfsRoot()+ Path.SEPARATOR + daasConfig.hdfsWorkSubDir()+ Path.SEPARATOR + daasConfig.fileSubDir());

			Path step1OutputPath = new Path(baseOutputPath.toString()+ Path.SEPARATOR + "step1");
			Path step2OutputPath = new Path(baseOutputPath.toString()+ Path.SEPARATOR + "step2");
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, step2OutputPath,daasConfig.displayMsgs());
			Path step3OutputPath = new Path(baseOutputPath.toString()+ Path.SEPARATOR + "step3");
			
			
			if(submitjob.equalsIgnoreCase("true")){
				
				jobConfig.set("dfs.umaskmode", "002");
		
				jobConfig.set("mapred.cluster.map.memory.mb", "4096");
				jobConfig.set("mapred.job.map.memory.mb", "4096");
				jobConfig.set("io.sort.mb", "672");
				jobConfig.set("mapred.map.child.java.opts", "-Xmx3072m");
				jobConfig.set("mapred.reduce.child.java.opts", "-Xmx3072m");
//				
//				jobConfig.set("mapred.cluster.map.memory.mb","2048");
//				jobConfig.set("mapred.job.map.memory.mb","2048");
//				jobConfig.set("mapred.map.child.java.opts","-Xmx2048m");
//				jobConfig.set("mapred.reduce.child.java.opts", "2048");

				
				jobConfig.set("mapred.compress.map.output", "true");
				jobConfig.set("mapred.output.compress", "true");
				jobConfig.set("mapred.output.compression.type", "BLOCK"); 
//				jobConfig.set("mapred.output.compression.type", "RECORD"); 
				jobConfig.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//				hdfsConfig.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec"); 
				jobConfig.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
				
				
				jobConfig.set("mapred.child.java.opts", "-server -Xmx3072m -Djava.net.preferIPv4Stack=true");
				Job job = new Job(jobConfig,"Processing MapReduce Copy with Filter - " + fileType);

				job.setJarByClass(CopyFileMapper.class);
				job.setMapperClass(CopyFileMapper.class);
				job.setReducerClass(CopyFileReducer.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(NullWritable.class);
				// job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setNumReduceTasks(Integer.parseInt(numreducers));
				// job.setOutputFormatClass(TextOutputFormat.class);
				
				
				
				FileStatus[] fstatusarr = fileSystem.globStatus(new Path(step2OutputPath.toString()+"/*/*.gz"));
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
						
						System.out.println("Adding multioutput productRxD045dbRxD126840RxD126" +date);
						System.out.println("Adding multioutput storeRxD045dbRxD126" +date);
						System.out.println("Adding multioutput STLDRxD126840RxD126" +date);
						System.out.println("Adding multioutput DetailedSOSRxD045dbRxD126" +date);
						System.out.println("Adding multioutput MenuItemRxD045dbRxD126" +date);
						System.out.println("Adding multioutput SecurityDataRxD126840RxD126" +date);
						System.out.println("\n--------------------------------------------------");
								
						MultipleOutputs.addNamedOutput(job,"productRxD045dbRxD126840RxD126"+date,TextOutputFormat.class, Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"storeRxD045dbRxD126"+date,TextOutputFormat.class, Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"STLDRxD126840RxD126"+date,TextOutputFormat.class, Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"DetailedSOSRxD045dbRxD126"+date,TextOutputFormat.class, Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"MenuItemRxD045dbRxD126"+date, TextOutputFormat.class,Text.class, Text.class);
						MultipleOutputs.addNamedOutput(job,"SecurityDataRxD126840RxD126"+date,TextOutputFormat.class, Text.class, Text.class);
					}
				}
						
				HDFSUtil.removeHdfsSubDirIfExists(fileSystem, step3OutputPath, true);

//				Path cachePath = new Path("/daas/lz/4902/cache");
				Path cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "cache");
				
				FileInputFormat.setInputPaths(job, new Path(step2OutputPath.toString()+"/*/*.gz"));//+"/STLD~840~20140606~0000609.gz"
				TextOutputFormat.setOutputPath(job, step3OutputPath);
				
				LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

				DistributedCache.addCacheFile(new Path(cachePath.toString() + Path.SEPARATOR+ daasConfig.abacSqlServerCacheFileName()).toUri(),job.getConfiguration());
				job.waitForCompletion(true);
			}
			if(updateAbac.equalsIgnoreCase("true")){// update file/subfile status'
				updABaC (Integer.parseInt(jobgroupId),daasConfig,fileSystem);
			}
			
			if(moveFiles.equalsIgnoreCase("true")){// move files from step1 and step3 to the gold layer
				String fileSuffix = "gz";
				Path baseFinalPath = new Path(daasConfig.hdfsRoot()
						+ Path.SEPARATOR + daasConfig.hdfsFinalSubDir()
						+ Path.SEPARATOR + daasConfig.fileSubDir());
	
				moveFiles(step1OutputPath, baseFinalPath, fileSuffix, true, true,jobConfig,Integer.parseInt(fileListJobId));
				moveFiles(step3OutputPath, baseFinalPath, fileSuffix, true, true,jobConfig,Integer.parseInt(fileListJobId));
			}

		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}
	
	
	//the below methods are duplicated from mergetofinal class
	
	private ABaC abac = null;
	private ABaCList abac2List = null;
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
			abac = new ABaC(daasConfig);
			abac2List = new ABaCList(daasConfig.fileFileSeparatorCharacter());
			
			int jobId = abac.createJob(jobGroupId, 5,"Update ABaC");
		
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
	

	
	private int nextFileNum = 1;

	private int moveFiles(Path sourcePath
            ,Path targetPath
            ,String fileSuffix
            ,boolean separateDir
            ,boolean keepCopy
            ,Configuration hdfsConfig
            ,Integer fileListJobId) {

FileStatus[] status = null;
String[] fileParts = null;

String targetPathText = targetPath.toString();
String fullTargetPathText = null;
String targetName = null;

String fullFileSuffix = ""; 

if ( fileSuffix.length() > 0 ) {
fullFileSuffix = "." + fileSuffix;
} else {
fullFileSuffix = "";
}

int retCnt = 0;

HashMap<String,String> sourcePathDestPathMap = new HashMap<String,String>();
BufferedWriter bwForListofFilestoCopy = null;
try {
status = fileSystem.listStatus(sourcePath);
fileParts = null;

String fileType = "";
String terrCd = "";
String businessDt = "";

Path newName = null;
FsPermission newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

String nameFromPath = "";

HashMap<String,Integer> maxFileNumMap = new HashMap<String,Integer>();

if ( status != null ) {
	for (int idx=0; idx < status.length; idx++ ) {
		
		if(status[idx].getLen() == 0)
			continue;

		nameFromPath = HDFSUtil.restoreMultiOutSpecialChars(status[idx].getPath().getName());

		if ( nameFromPath.contains(HDFSUtil.FILE_PART_SEPARATOR) ) {

			fileParts = nameFromPath.split(HDFSUtil.FILE_PART_SEPARATOR);
	
			//System.out.println(fileParts.length);
			//for (int iii=0; iii < fileParts.length; iii++ ) {
			//	System.out.println(iii + ")" + fileParts[iii]);
			//}
			
			if ( !Character.isDigit(fileParts[0].charAt(0)) ) {
				fileType = fileParts[0];
				terrCd = fileParts[1];
				businessDt = fileParts[2].substring(0,8);
			} else {
				fileType = "";
				terrCd = fileParts[0];
				businessDt = fileParts[1].substring(0,8);
			}
	
			if ( separateDir ) {
				if ( fileType.length() > 0 ) {
					fullTargetPathText = targetPathText + Path.SEPARATOR + fileType + Path.SEPARATOR + terrCd + Path.SEPARATOR + businessDt;
				} else {
					fullTargetPathText = targetPathText + Path.SEPARATOR + terrCd + Path.SEPARATOR + businessDt;
				}
			} else {
				fullTargetPathText = targetPathText;
			}
	
			
			
			if(maxFileNumMap.get(fullTargetPathText) == null){
				int maxFileNum = 0;
				
				
				FileStatus[] fstats = fileSystem.listStatus(new Path(fullTargetPathText));
				if(fstats != null && fstats.length > 0){
					int fileNum = 0;
					for(int i =0;i<fstats.length;i++){
						String fileName = fstats[i].getPath().getName();
//						System.out.println(" fileName " + fileName);
						int extIndx = fileName.lastIndexOf(".gz");
						String fileNumVal = fileName.substring(extIndx-5,extIndx);
						
						fileNum = Integer.parseInt(fileNumVal);
						if(fileNum > maxFileNum)
							maxFileNum = fileNum;
						
					}
				}
					
				nextFileNum = maxFileNum+1;
				
				maxFileNumMap.put(fullTargetPathText, new Integer(nextFileNum));
				
			}else{
				nextFileNum++;
			}
//			System.out.println(" maxFileNum for  " + fullTargetPathText + " is " + maxFileNum);
//				new Path(fileType + HDFSUtil.FILE_PART_SEPARATOR + terrCd + HDFSUtil.FILE_PART_SEPARATOR + businessDt));;
			
			if ( fileType.length() > 0 ) {
				targetName = fileType + HDFSUtil.FILE_PART_SEPARATOR + terrCd + HDFSUtil.FILE_PART_SEPARATOR + businessDt + HDFSUtil.FILE_PART_SEPARATOR + String.format("%07d", nextFileNum) + fullFileSuffix;
			} else {
				targetName = terrCd + HDFSUtil.FILE_PART_SEPARATOR + businessDt + HDFSUtil.FILE_PART_SEPARATOR + String.format("%07d", nextFileNum) + fullFileSuffix;
			}
	
			newName = new Path(fullTargetPathText + Path.SEPARATOR + targetName);
			
			if ( keepCopy ) {
				System.out.print("MOVE:"+status[idx].getPath() + " TO:"+newName.getName() + "...");
//				FileUtil.copy(fileSystem, status[idx].getPath(), fileSystem, newName, false, hdfsConfig);
				sourcePathDestPathMap.put(status[idx].getPath().toString(),newName.toString());
				
				System.out.println("done");
			} else {
				fileSystem.rename(status[idx].getPath(), newName);
				fileSystem.setPermission(newName,newFilePremission);
			}
			

			
			
			retCnt++;
//			nextFileNum++;
		}
	}

//	call mapreduce to copy files
	if(keepCopy){
		Path destPathRoot = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId);
		Path destPathCache = new Path(destPathRoot.toString() + Path.SEPARATOR + "cache");
		String sourcePathLastDir = sourcePath.getName().substring(sourcePath.getName().lastIndexOf("/") + 1);
		String outputPath = destPathRoot.toString()+ Path.SEPARATOR + "CopyMoveFileList" +Path.SEPARATOR+sourcePathLastDir+ Path.SEPARATOR+ "filelist_tocopymoveto_goldlayer.txt";

		
		bwForListofFilestoCopy =new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(outputPath),true)));
		
		Iterator<String> keyIt = sourcePathDestPathMap.keySet().iterator();
		StringBuffer sbf = new StringBuffer();
		int i =0;
		while(keyIt.hasNext()){
			String key = keyIt.next();
			
			sbf.setLength(0);
			if(i > 0)
				sbf.append("\n");
			sbf.append(key).append("\t").append(sourcePathDestPathMap.get(key));
			
			bwForListofFilestoCopy.write(sbf.toString());
			i++;
		}
		
		bwForListofFilestoCopy.close();
		
		FileStatus stat = fileSystem.getFileStatus(new Path(outputPath));
		if(stat.getLen() > 0){
			
			hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_COPY_OR_MOVE_FILES,"COPY");
			Job moveFileJob = new Job(hdfsConfig,"Copy/Move "+sourcePathLastDir+" files to Gold Layer");
			
			moveFileJob.setJarByClass(MergeToFinal.class);
			
			moveFileJob.setMapperClass(CopyMoveNFileMapper.class);
			moveFileJob.setNumReduceTasks(0);
			
			moveFileJob.setInputFormatClass(NLineInputFormat.class);
//			moveFileJob.setInputFormatClass(TextInputFormat.class);
			moveFileJob.setOutputFormatClass(TextOutputFormat.class);
			
			moveFileJob.setMapOutputKeyClass(NullWritable.class);
			moveFileJob.setMapOutputValueClass(Text.class);
			
			moveFileJob.setOutputKeyClass(NullWritable.class);
			moveFileJob.setOutputValueClass(Text.class);
			
		
			FileInputFormat.addInputPath(moveFileJob, new Path(outputPath));
			NLineInputFormat.setNumLinesPerSplit(moveFileJob, 10);
			FileOutputFormat.setOutputPath(moveFileJob, new Path(destPathRoot.toString()+Path.SEPARATOR+"CopyMoveFileStatus"+Path.SEPARATOR+sourcePathLastDir));
			
			MultipleOutputs.addNamedOutput(moveFileJob, "CopyMoveFileStatus", TextOutputFormat.class, NullWritable.class, Text.class);
			
			moveFileJob.waitForCompletion(true);
		}
		
		
		
	}
}

} catch (Exception ex) {
System.err.println("Error occured in move files:");
System.err.println(ex.toString());
ex.printStackTrace();
System.exit(8);
}finally{
try{
if(bwForListofFilestoCopy != null)
	bwForListofFilestoCopy.close();
}catch(Exception ex){
	
}
}

return(retCnt);
}
}

package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
//import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tools.ant.taskdefs.SendEmail;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.abac.ABaCList;
import com.mcd.gdw.daas.abac.ABaCListItem;
import com.mcd.gdw.daas.abac.ABaCListSubItem;

//import com.mcd.gdw.daas.abac.ABaC2.CodeType;

//import com.mcd.gdw.daas.mapreduce.CopyFileMapper;
import com.mcd.gdw.daas.mapreduce.CopyFileMapper;
import com.mcd.gdw.daas.mapreduce.CopyFileReducer;
import com.mcd.gdw.daas.mapreduce.TextCopyFileReducer;
//import com.mcd.gdw.daas.mapreduce.UnzipInputFileMapper;
import com.mcd.gdw.daas.mapreduce.ABaCMapper;
import com.mcd.gdw.daas.mapreduce.FindGoldLyrFilesToUpdateMapper;
import com.mcd.gdw.daas.mapreduce.FindGoldLyrFilesToUpdateReducer;
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
import com.mcd.gdw.daas.util.CopyMoveNFileMapper;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.SendMail;
/**
 * 
 * @author Sateesh Pula
 * 
 * run unzip
 * hadoop jar /home/mc32445/scripts/daasmapreduce.jar com.mcd.gdw.daas.driver.MergeToFinal -Dio.sort.mb=1024 -c config.xml -t POS_XML
 */
public class MergeToFinal extends Configured implements Tool {

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
	private Path archivePath = null;
	
	//@mc41946: Merge Process for Multiple runs
	private Path step1CurrentOutputPath = null;
	

	private int nextFileNum = 1;
	private int jobSeqNbr   = 1;
	
	

	private static ArrayList<String> overrides = new ArrayList<String>();
	
	DaaSConfig daasConfig;
	public MergeToFinal(){
		
	}
	public MergeToFinal(DaaSConfig daaSConfig,String type,FileSystem fileSystem,int jobSeqNbr){
		try{
			this.daasConfig = daaSConfig;
			fileType = type;
			this.fileSystem = fileSystem;
			abac = new ABaC(daasConfig);
			this.jobSeqNbr = jobSeqNbr;
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

	String configXmlFile = "";
	String fileType = "";
	//@mc41946: Merge Process for Multiple runs
	//String processUSStores="false";
	boolean multipleInject=false;
	String terrCDList="";

	String selectSubTypes="";
	SendMail sendMail;
	
	String fromAddress = "";
	String toAddress = "";	
	String subject = "";
	String emailText    = "";
	
	//@mc41946: Merge Process for Multiple runs
	
public static void main(String[] args) throws Exception {
				
		int retval = ToolRunner.run(new Configuration(),new MergeToFinal(), args);

		System.out.println(" return value : " + retval);
	}

	public int run(String[] argsall) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		
		String[] args = gop.getRemainingArgs();
		
		if ( args.length < 2 ) {
			System.err.println("Missing one or more parameters");
			System.err.println("Usage: " + MergeToFinal.class.getName() + " DaaSConfFile.xml FileType");
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
		
		
		
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-t") && (idx+1) < args.length ) {
				fileType = args[idx+1];
			}
			//@mc41946: Merge Process for Multiple runs
			if ( args[idx].equalsIgnoreCase("-f") && (idx+1) < args.length ) {
				//processUSStores = args[idx+1];
				multipleInject=Boolean.parseBoolean(args[idx+1]);
			}
			if ( args[idx].equalsIgnoreCase("-terrCDList") && (idx+1) < args.length ) {
				terrCDList = args[idx+1];
				
			}

			if (args[idx].equals("-selectsubtypes") && (idx + 1) < args.length) {
				selectSubTypes = args[idx + 1];
			}
			if (args[idx].equals("-fromAddress") && (idx + 1) < args.length) {
				fromAddress = args[idx + 1];
			}
			if (args[idx].equals("-toAddress") && (idx + 1) < args.length) {
				toAddress = args[idx + 1];
			}
			if (args[idx].equals("-subject") && (idx + 1) < args.length) {
				subject = args[idx + 1];
			}
			if (args[idx].equals("-emailText") && (idx + 1) < args.length) {
				emailText = args[idx + 1];
			}
			//@mc41946: Merge Process for Multiple runs
		}
		
		//AWS START
		//if ( configXmlFile.length() == 0 || fileType.length() == 0 || selectSubTypes.length()==0 || fromAddress.length()==0 || toAddress.length()==0 || subject.length()==0 || emailText.length()==0 || terrCDList.length()==0) {
		if ( configXmlFile.length() == 0 || fileType.length() == 0 || fromAddress.length()==0 || toAddress.length()==0 || subject.length()==0 || emailText.length()==0 || terrCDList.length()==0) {
		//AWS END
			System.err.println("Missing config.xml and/or filetype");
			System.err.println("Usage: MergeToFinal -c config.xml -t filetype");
			System.exit(8);
		}

		daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		if ( daasConfig.configValid() ) {
			sendMail = new SendMail(daasConfig);
			
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

		//AWS START
		//fileSystem = FileSystem.get(hdfsConfig);
		fileSystem = HDFSUtil.getFileSystem(daasConfig, hdfsConfig);
		//AWS END
		
		abac = new ABaC(daasConfig);
		jobGroupId = abac.createJobGroup("NewPOS Merge Process");
		fileListJobId = abac.createJob(jobGroupId, jobSeqNbr++, "Get File List");
		
		hdfsConfig.set("job.group.id", ""+jobGroupId);
		//cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "cache"+Path.SEPARATOR+daasConfig.abacCacheFileName());
		cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "cache");
		//cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir()+ Path.SEPARATOR + "current" + Path.SEPARATOR + "cache");
		//cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "cache");
		hdfsConfig.set("path.to.cache", cachePath.toString());
		
//				
////		mergejobid = abac.createJob(jobGroupId, 1, "MergetoFinal");
//
//		System.out.println( " jobGroupId " + jobGroupId + " jobId " +jobId);
//		cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + daasConfig.abacCacheFileName());

		hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_ABAC_FROM_PATH,daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.hdfsLandingZoneArrivalSubDir());
		hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_ABAC_TO_PATH,daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "source");
		hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_ABAC_TO_REJECT_PATH,daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "reject");

		//@mc41946: Merge Process for Multiple runs
		getCacheInfo(daasConfig,fileType,fileListJobId,fileSystem);
		
		baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir());
		//HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

		step0OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step0");
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, step0OutputPath,daasConfig.displayMsgs());
		
		runABaCMapper(daasConfig,fileType,hdfsConfig,fileListJobId,fileSystem);
		
//		abac.closeJob(fileListJobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
		
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
		
		//@mc41946: Merge Process for Multiple runs
		 step1OutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "current"+ Path.SEPARATOR +"step1");
		 HDFSUtil.removeHdfsSubDirIfExists(fileSystem,step1OutputPath,true);
		//step1OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step1");
		//mc41946: yet to implement move and rename.
		step2OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step2");
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem,step2OutputPath,daasConfig.displayMsgs());
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,step2OutputPath,daasConfig.displayMsgs());
		step3OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step3");
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem,step3OutputPath,true);
		//Path mergeOutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "merge");
		//HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,mergeOutputPath,daasConfig.displayMsgs());
		//@mc41946: Merge Process for Multiple runs
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
		} else {
			runMrFile(daasConfig,fileType,hdfsConfig);
		}
        //@mc41946: Merge Process for Multiple runs
		if(multipleInject && !(daasConfig.nonmergedZipFile()))
		{
			System.out.println("Processed Current Work Layer for USA stores for file Type"+fileType);
			abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
			return;
		}
		
		//@mc41946: Merge Process for Multiple runs
		
		
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, new Path(daasConfig.hdfsRoot()+Path.SEPARATOR+daasConfig.hdfsWorkSubDir()+Path.SEPARATOR+daasConfig.fileSubDir()+"/MergeProcessStep1Status/_SUCCESS"), true);
		
		//@mc41946: Merge Process for Multiple runs
		step1CurrentOutputPath=step1OutputPath;
		step1OutputPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + "step1");
		
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,step1OutputPath,true);
		ArrayList<String> subTypeList;
		
			try {
			if (selectSubTypes.length() > 0) {
				subTypeList = new ArrayList<String>();
				//System.out.println("Select Sub Types:");
				String[] parts = selectSubTypes.split(",");
				for (String addSubType : parts) {
					System.out.println("   " + addSubType);
					subTypeList.add(addSubType);
				}
				subTypeList.add("FileStatus");
			} else {
				//abac = new ABaC(daasConfig);
				subTypeList = abac.getSubFileTypeCodes();
				subTypeList.add("FileStatus");
				//abac.dispose();
			}

			 for (int idxCode=0; idxCode < subTypeList.size(); idxCode++) {
					String subTypeCode=subTypeList.get(idxCode);
					moveFilesinHDFS(daasConfig, step1CurrentOutputPath, step1OutputPath, fileSystem, HDFSUtil.replaceMultiOutSpecialChars(subTypeCode));
				}
			//}
		}
		 catch (Exception ex) {
			// TODO Auto-generated catch block
			
			System.err.println("Error occured in Moving Files , stopping");
			ex.printStackTrace(System.err);
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			System.exit(8);
			//throw new InterruptedException("Error occured in Moving Files:"+ex.toString());		 
			
		}

		
		System.out.println("Files Moved sucessfully from current to work layer");
		/*if(multipleInject)
		{
			System.out.println("Processed Current Work Layer for USA stores");
			abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
			return;
		}*/
		
		//@mc41946: Merge Process for Multiple runs
		updateABaCCacheInfo(daasConfig,fileType,fileListJobId,fileSystem);
		
		jobId = abac.createJob(jobGroupId, jobSeqNbr++,"Move existing gold files into work layer");
		
		
		Path finalSourcePath = null;
		String targetPathText = baseFinalPath.toString();

		int existingCnt = 0;
		int newCnt = 0;
		int existingToFinalCnt = 0;
		String[] keyValueParts = {"",""};
		
		HashSet<String> listOfExistingGoldLyrFiles = new HashSet<String>();
		
		
		if ( terrDateMap !=null && terrDateMap.size() > 0 ) {
			
			for ( Map.Entry<String, Integer> entry : terrDateMap.entrySet()) {
				try {
					keyValueParts = (entry.getKey()).split("\\|");

					if ( fileSubTypes != null ) {
						for ( int idx=0; idx < fileSubTypes.length; idx++ ) {
							finalSourcePath = new Path(targetPathText + Path.SEPARATOR + fileSubTypes[idx] + Path.SEPARATOR + keyValueParts[0] + Path.SEPARATOR + keyValueParts[1]);
					
//							System.out.println("Moved files for " + fileSubTypes[idx] + "--" + keyValueParts[0] + "--" + keyValueParts[1] + " from " + finalSourcePath.toString() + " to " + step2OutputPath.toString());
//							
//							existingCnt += moveFiles(finalSourcePath,step2OutputPath,fileSuffix,false);
							listOfExistingGoldLyrFiles.add(finalSourcePath.toString());
						}
					} else {
						finalSourcePath = new Path(targetPathText + Path.SEPARATOR + keyValueParts[0] + Path.SEPARATOR + keyValueParts[1]);
						
//						System.out.println("Moved files for " + keyValueParts[0] + "--" + keyValueParts[1] + " from " + finalSourcePath.toString() + " to " + step2OutputPath.toString());
//						existingCnt += moveFiles(finalSourcePath,step2OutputPath,fileSuffix,false);
						listOfExistingGoldLyrFiles.add(finalSourcePath.toString());
					}
				} catch(Exception ex) {
					System.err.println("Error occured in moving files to work");
					System.err.println(ex.toString());
					sendEmailToSupport(fromAddress, toAddress, subject, emailText);
					System.exit(8);
				}
			}
		}

		System.out.println("listOfExistingGoldLyrFiles "+listOfExistingGoldLyrFiles);

//		System.out.println("\nMoved " + existingCnt + " existing files(s) from gold layer to work layer");
		
		//new logic to copy only the required files to step2outputpath instead of all the files from the gold layer
		//uses the list of files creating above 'listOfExistingGoldLyrFiles'
		if(listOfExistingGoldLyrFiles != null && listOfExistingGoldLyrFiles.size() > 0){
			Iterator<String> fileListIt = listOfExistingGoldLyrFiles.iterator();
			Path goldLayerAllFileListPath =  new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "CopyMoveFileList"+Path.SEPARATOR+"goldLayerFiles_all_forthisrun.txt");
			Path goldLayerFileListtoUpdate =  new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "CopyMoveFileList"+Path.SEPARATOR+"GoldLayerUpdateFileList");
			
			//"goldLayerFiles_toupdate_forthisrun.txt");
			
			BufferedWriter bwForListofAllGoldFiles = null;
			int cnt = 0;
			StringBuffer goldFileListBf = new StringBuffer();
			bwForListofAllGoldFiles =new BufferedWriter(new OutputStreamWriter(fileSystem.create(goldLayerAllFileListPath,true)));
		
			
			while(fileListIt.hasNext()){
				
				
				goldFileListBf.setLength(0);
				String filePath = fileListIt.next();
				
				if(cnt > 0)
					goldFileListBf.append("\n");
				
				goldFileListBf.append(filePath);
				
				bwForListofAllGoldFiles.write(goldFileListBf.toString());

				cnt++;
			}
			
			if(bwForListofAllGoldFiles != null)
				bwForListofAllGoldFiles.close();
			
			
			
			runFindGoldLyrFilesToUpdateMapper(daasConfig,fileType,hdfsConfig,goldLayerAllFileListPath,goldLayerFileListtoUpdate);
			
			Path fileToMovetoStep2 = new Path(goldLayerFileListtoUpdate.toString()+Path.SEPARATOR+"goldLayerFilesToupdateForthisRun-r-00000.gz");
			
			if(fileSystem.exists(fileToMovetoStep2)){
			
				InputStreamReader insr = null;
				BufferedReader br = null;
//				CompressionCodecFactory  compressionCodecFactory = new CompressionCodecFactory(hdfsConfig);
//				CompressionCodec codec = compressionCodecFactory.getCodec(fileToMovetoStep2);
				
				insr = new InputStreamReader(new GZIPInputStream(fileSystem.open(fileToMovetoStep2)));
					
				br = new BufferedReader(insr);
//				InputStream in = fileSystem.open(fileToMovetoStep2);
//				
//				InputStream filein = codec.createInputStream(in);
//				byte[] contents = new byte[(int)fileSystem.getLength(fileToMovetoStep2)];
//				
//				org.apache.hadoop.io.IOUtils.readFully(filein,contents, 0,contents.length);
//				
//				org.apache.hadoop.io.IOUtils.closeStream(filein);
//				in.close();
//				String temp = new String(contents);
				
			
				if(br != null){
					String line = null;
					
					while( (line = br.readLine()) != null){
						
						existingCnt += moveFiles(new Path(line),step2OutputPath,fileSuffix,false);
						
						System.out.println(" New Move : from " + line + " to " + step2OutputPath.toString());
					}
					
					
					br.close();
					insr.close();
				}
			}
		}
		
		
		System.out.println(" New Move : moved " + existingCnt + " from Gold Layer to " + step2OutputPath.toString());
		
		abac.closeJob(jobId,DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
		
//		hdfsConfig.set("mapred.cluster.map.memory.mb","2048");
//		hdfsConfig.set("mapred.job.map.memory.mb","2048");
//		hdfsConfig.set("mapred.map.child.java.opts","-Xmx2048m");
//		hdfsConfig.set("mapred.reduce.child.java.opts", "2048");

		
		if ( existingCnt > 0 ) {
			
			if(daasConfig.nonmergedZipFile())
			{
				HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,step3OutputPath,true);
				System.out.println("Skipping mapreduce copy for Non Merged Zip  Files");
				
			}else if ( daasConfig.fileInputIsZipFormat() ) {
				System.out.println("Running MR copy with Filter");
				runMrCopyWithFilter(daasConfig,fileType,hdfsConfig);
			} else {
				System.out.println("Running MR File copy with Filter");
				runMrCopyFileWithFilter(daasConfig,fileType,hdfsConfig);
			}
		} else {
			System.out.println("Skipping mapreduce copy with filter because there are no existing gold layer files");
		}

		jobId = abac.createJob(jobGroupId,jobSeqNbr++, "Move new files into gold layer");
		
		
		if ( daasConfig.hdfsKeepTempDirs() ) {
			newCnt = moveFiles(step1OutputPath,baseFinalPath,fileSuffix,true,true,hdfsConfig,fileListJobId);
			
		} else {
			newCnt = moveFiles(step1OutputPath,baseFinalPath,fileSuffix,true);
			
		}
		
		System.out.println("\nMoved " + newCnt + " new files(s) to gold layer");
		
		abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
		
		if ( existingCnt > 0 ) {
			jobId = abac.createJob(jobGroupId,jobSeqNbr++, "Move existing files back into gold layer");
			if ( daasConfig.hdfsKeepTempDirs() ) {
				existingToFinalCnt = moveFiles(step3OutputPath,baseFinalPath,fileSuffix,true,true,hdfsConfig,fileListJobId);
			} else { 
				existingToFinalCnt = moveFiles(step3OutputPath,baseFinalPath,fileSuffix,true);
			}
			System.out.println("\nMoved " + existingToFinalCnt + " existing (filtered) files(s) back to gold layer");
			abac.closeJob(jobId,(short)1 ,"successful");
		} else {
			System.out.println("Skipping move existing files back to gold layer because there are no existing gold layer files");
		}

		
		updABaC(jobGroupId, daasConfig, fileSystem);
		
//		abac.closeJob(mergejobid, (short)1,"successful");
		abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);

		abac.dispose();

		if ( !daasConfig.hdfsKeepTempDirs() ) {
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem,baseOutputPath,daasConfig.displayMsgs());
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem,new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.fileSubDir()),daasConfig.displayMsgs());
		}
		
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, new Path(daasConfig.hdfsRoot()+Path.SEPARATOR+daasConfig.hdfsWorkSubDir()+Path.SEPARATOR+daasConfig.fileSubDir()+"/MergeProcessStatus/_SUCCESS"), true);
		//pass last job sequence number to next job
//		String ooziePropFile = System.getProperty(DaaSConstants.OOZIE_ACTION_OUTPUT_PROPERTIES);
//		if(ooziePropFile != null){
//			File propFile = new File(ooziePropFile);
//			
//			Properties props = new Properties();
//			props.setProperty(DaaSConstants.LAST_JOB_SEQ_NBR, ""+jobSeqNbr);
//			props.setProperty(DaaSConstants.TDA_EXTRACTS_INPUT_PATH,step1OutputPath.toString());
//			props.setProperty(DaaSConstants.TDA_EXTRACTS_OUTPUT_BASEPATH,new Path(daasConfig.hdfsRoot() + Path.SEPARATOR+daasConfig.hdfsWorkSubDir()+Path.SEPARATOR +jobGroupId).toString());
//			
//			System.out.println( " daasConfig.hdfsRoot() + Path.SEPARATOR+daasConfig.hdfsWorkSubDir()+Path.SEPARATOR +jobGroupId " + daasConfig.hdfsRoot() + Path.SEPARATOR+daasConfig.hdfsWorkSubDir()+Path.SEPARATOR +jobGroupId);
//		
//			OutputStream outputStream = new FileOutputStream(propFile);
//			props.store(outputStream, "custom props");
//			
//			outputStream.close();
//			
//		}
		
	 //@mc41946: Merge Process for Multiple runs
		
		 archivePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "archive"+ Path.SEPARATOR +"step1");
		 //System.out.println("Archive path is -->"+archivePath);
		 if(!(daasConfig.nonmergedZipFile())){
		 HDFSUtil.removeHdfsSubDirIfExists(fileSystem,archivePath,true);
			}// Added for Injecting Non Merged Zip 
		 HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,archivePath,true);
	
		// System.out.println("Move files from work directory to archive directory");
		//move files
		 FileStatus[] fstatustmp = null;
		 fstatustmp = fileSystem.listStatus(step1OutputPath,new PathFilter() {
				
				@Override
				public boolean accept(Path pathname) {
					if(!pathname.getName().startsWith("_SUCCESS"))
						return true;
					return false;
				}
			});
		 FsPermission fspermission =  new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
		 for(FileStatus fstat:fstatustmp){
			 String fileName  = fstat.getPath().getName();
//				fileName  = fstat.getPath().getName().replace("PMIX", "PMIX-");
			 //@mc41946: Adding file type to FileStatus file before moving to archive path to distinguish the files after merge process.
			 if(daasConfig.nonmergedZipFile()){
				 fileName  = fstat.getPath().getName().replace("FileStatus", "FileStatus-"+fileType);
			 }
				
				
			
				if(!fileSystem.rename(new Path(fstat.getPath().toString()), new Path(archivePath+Path.SEPARATOR+fileName))){
					System.out.println("could not move " + fstat.getPath().toString() + " to " +archivePath+Path.SEPARATOR+fileName);
				}else{
					fileSystem.setPermission(new Path(archivePath+Path.SEPARATOR+fileName), fspermission);
					System.out.println(" moved " + fstat.getPath().toString() + " to " +archivePath+Path.SEPARATOR+fileName);
				}
				
			}
		//Removing step1 folder for Non Merged Zip  files as it has to run multiple times in a day.
		 if(daasConfig.nonmergedZipFile()){
			 HDFSUtil.removeHdfsSubDirIfExists(fileSystem, step1OutputPath,daasConfig.displayMsgs());
		 }
			
		
	}

	private void getCacheInfo(DaaSConfig daasConfig
                             ,String fileType
                             ,int jobId
                             ,FileSystem fileSystem) throws Exception{

		try {
			//abac2List = abac.setupList(jobId, fileSystem);
			//@mc41946: Merge Process for Multiple runs
			abac2List = abac.setupList2(jobId, fileSystem,multipleInject,terrCDList);
			
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
//			System.exit(8);
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			throw new InterruptedException("Reading cache file error:"+ex.toString());
		}
	}
	
	
	private void updateABaCCacheInfo(DaaSConfig daasConfig, String fileType,
			int jobId, FileSystem fileSystem) throws Exception {

		try {
			// abac2List = abac.setupList(jobId, fileSystem);
			// @mc41946: Merge Process for Multiple runs
			System.out.println("Updating ABaCCacheInfo File");
			abac2List = abac.updateABaCFileList(jobId, fileSystem, multipleInject,
					terrCDList);

			// abac.saveList(jobId, abac2List, fileSystem);

			String fileSubTypeText = "";
			String mapKey = "";
			boolean firstLineFl = true;
			for (Map.Entry<String, ABaCListItem> entry : abac2List) {

				ABaCListItem abac2ListItem = entry.getValue();
				if (firstLineFl) {
					firstLineFl = false;
					Iterator<Entry<String, ABaCListSubItem>> subItemIterator = abac2ListItem
							.iterator();

					while (subItemIterator.hasNext()) {
						ABaCListSubItem abac2ListSubItem = subItemIterator
								.next().getValue();

						if (fileSubTypeText.length() > 0) {
							fileSubTypeText += "|";
						}
						fileSubTypeText += abac2ListSubItem
								.getSubFileDataTypeCd();

					}

					if (fileSubTypeText.length() > 0) {
						fileSubTypes = fileSubTypeText.split("\\|");
					}
				}

				mapKey = abac2ListItem.getTerrCd() + "|"
						+ abac2ListItem.getBusnDt();
				 //terrDateMap.clear();
				if (terrDateMap.containsKey(mapKey)) {
					terrDateMap.put(mapKey, (int) terrDateMap.get(mapKey) + 1);
				} else {
					terrDateMap.put(mapKey, 1);
				}

			}

		} catch (Exception ex) {
			System.err.println("Reading updateABaCCacheInfo file error:");
			ex.printStackTrace(System.err);
			// System.exit(8);
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			throw new InterruptedException("Reading updateABaCCacheInfo file error:"
					+ ex.toString());
		}
	}

	private void runABaCMapper(DaaSConfig daasConfig
                              ,String fileType
                              ,Configuration hdfsConfig
                              ,int jobId
                              ,FileSystem fileSystem) throws Exception{

		Job job;
		
		try {
			//AWS START
			hdfsConfig.set(DaaSConstants.HDFS_ROOT_CONFIG, daasConfig.hdfsRoot());
			
			//job = new Job(hdfsConfig, "ABaC - Move Files");
			job = Job.getInstance(hdfsConfig, "ABaC - Move Files");
			//AWS END
			//@mc41946:Path cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "cache");
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
				if ( !updABaCReason(daasConfig, fileSystem) ) {
					System.err.println("Error occured in MapReduce process");
					System.err.println("1 or more Move File errors encountered, stopping");
					sendEmailToSupport(fromAddress, toAddress, subject, emailText);
					//System.exit(8);
				}
				abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
			} else {
				System.err.println("Error occured in MapReduce process, stopping");
				sendEmailToSupport(fromAddress, toAddress, subject, emailText);
				System.exit(8);
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
//			System.exit(8);
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			 throw new InterruptedException("Error occured in MapReduce process:"+ex.toString());
		}
	}
    public static final String FILE_TYPE="FILE_TYPE";
    public static final String NONMERGE_FILE_TYPE="NONMERGE_FILE_TYPE";
	private void runMrUnzip(DaaSConfig daasConfig
	                       ,String fileType
	                       ,Configuration hdfsConfig) throws Exception{

		
		int jobId = 0;
		Job job;
		
		System.out.println( " unzip called jobGroupId " + jobGroupId );
		System.out.println("mapred.child.ulimit " + hdfsConfig.get("mapred.child.ulimit"));
		
		jobId = abac.createJob(jobGroupId, jobSeqNbr++, "MapReduce Unzip");

		try {
			System.out.println("Unzipping files of File Type "+fileType);

			hdfsConfig.set("skipFilesonSize", daasConfig.skipFilesonSize());
			hdfsConfig.set("MAX_FILE_SIZE", daasConfig.maxFileSize());
			hdfsConfig.set(FILE_TYPE,fileType);
			hdfsConfig.set(NONMERGE_FILE_TYPE, Boolean.toString(daasConfig.nonmergedZipFile()));
			job = new Job(hdfsConfig, "Processing - " + fileType);

			ZipFileInputFormat.setLenient(true);
//			ZipFileInputFormat.setInputPaths(job, new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + "*.zip"));
//			ZipFileInputFormat.setInputPaths(job, new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + daasConfig.filePattern()));
			ZipFileInputFormat.setInputPaths(job, new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + fileListJobId + Path.SEPARATOR + "source" ));

			job.setJarByClass(MergeToFinal.class);
			if(daasConfig.nonmergedZipFile())
			{
				System.out.println("Unzipping files of File Type "+fileType);
				job.setMapperClass(UnzipInputFileMapper.class);
				//hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm()); 
				//System.out.println("Heap Space for Non Merged Zip  files"+ daasConfig.fileMapReduceJavaHeapSizeParm());
				job.setNumReduceTasks(0);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(ZipFileInputFormat.class);
				LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
				
			}
			else
			{
				job.setMapperClass(UnzipInputFileMapper.class);
				job.setReducerClass(UnzipInputFileReducer.class);
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(NullWritable.class);
				//job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setNumReduceTasks(daasConfig.numberofReducers());
				job.setInputFormatClass(ZipFileInputFormat.class);
//				job.setOutputFormatClass(TextOutputFormat.class);
				LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
				
				job.setPartitionerClass(UnZipPartitioner.class);
				job.setGroupingComparatorClass(UnZipGroupComparator.class);
				job.setSortComparatorClass(UnZipCompositeKeyComparator.class);
			}
			
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
						sendEmailToSupport(fromAddress, toAddress, subject, emailText);
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
				
				abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
				
			} else {
				System.err.println("Error occured in MapReduce process, stopping");
				sendEmailToSupport(fromAddress, toAddress, subject, emailText);
				System.exit(8);
			}


		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
//			System.exit(8);
			 throw new InterruptedException("Error occured in MapReduce process:"+ex.toString());
		}
	}

	private void runMrFile(DaaSConfig daasConfig
                          ,String fileType
                          ,Configuration hdfsConfig) throws Exception{
		
		
		int jobId = 0;
		Job job;

		jobId = abac.createJob(jobGroupId,jobSeqNbr++, "MapReduce Generic File");
			
		try {
			System.out.println("\nProcessing files\n");

			job = new Job(hdfsConfig, "Processing - " + fileType);

			FileStatus[] status = null;

			status = fileSystem.listStatus(new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.fileSubDir()));
		
			if ( status != null ) {
				for (int idx=0; idx < status.length; idx++ ) {
					if ( !status[idx].getPath().getName().equals(daasConfig.abacSqlServerCacheFileName()) ) {
						FileInputFormat.addInputPath(job, status[idx].getPath());
					}
				}
			}

			job.setJarByClass(MergeToFinal.class);
			job.setMapperClass(TextInputFileMapper.class);
			job.setReducerClass(TextInputFileReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(10);
			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			String[] keyValueParts = {"",""};

			for ( Map.Entry<String, Integer> entry : terrDateMap.entrySet()) {

				try {
					keyValueParts = (entry.getKey()).split("\\|");
	
					Path finalPathSubTypeDt = null;

					MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(keyValueParts[0] + HDFSUtil.FILE_PART_SEPARATOR + keyValueParts[1]),TextOutputFormat.class, Text.class, Text.class);
		
					finalPathSubTypeDt = new Path(baseFinalPath.toString() + Path.SEPARATOR + keyValueParts[0] + Path.SEPARATOR + keyValueParts[1]);
					HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,finalPathSubTypeDt,daasConfig.displayMsgs());
		
					System.out.println("Added Multiple Output for " + keyValueParts[0] + " | " + keyValueParts[1]);
				} catch(Exception ex) {
					System.err.println("Error occured in building MultipleOutputs File Name");
					System.err.println(ex.toString());
					sendEmailToSupport(fromAddress, toAddress, subject, emailText);
					System.exit(8);
				}
			}

			//MultipleOutputs.addNamedOutput(job,"FileStatus" ,TextOutputFormat.class, Text.class, Text.class);
			//System.out.println("Added Multiple Output for FileStatus\n");
			System.out.println("");

			TextOutputFormat.setOutputPath(job, step1OutputPath);

			DistributedCache.addCacheFile(cachePath.toUri(),job.getConfiguration());

			if ( job.waitForCompletion(true) ) {
				abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
			} else {
				System.err.println("Error occured in MapReduce process, stopping");
				sendEmailToSupport(fromAddress, toAddress, subject, emailText);
				System.exit(8);
			}

		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
			ex.printStackTrace();
//			System.exit(8);
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			 throw new InterruptedException("Error occured in MapReduce process:"+ex.toString());
		}
	}
	
	private void runMrCopyWithFilter(DaaSConfig daasConfig
	                                ,String fileType
	                                ,Configuration hdfsConfig) throws Exception{

		int jobId = 0;
		Job job;

		jobId = abac.createJob(jobGroupId, jobSeqNbr++,"MapReduce Copy with Filter");

		try {
			System.out.println("\nCopy Files with Filter\n");

			//AWS START
			hdfsConfig.set(DaaSConstants.HDFS_ROOT_CONFIG, daasConfig.hdfsRoot());
			//AWS END

			job = new Job(hdfsConfig, "Processing MapReduce Copy with Filter - " + fileType);

			job.setJarByClass(MergeToFinal.class);
			job.setMapperClass(CopyFileMapper.class);
//			job.setReducerClass(CopyFileReducer.class);
			
//			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(NullWritable.class);
			//job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(0);
//			job.setOutputFormatClass(TextOutputFormat.class);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			
	
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
						sendEmailToSupport(fromAddress, toAddress, subject, emailText);
//						System.exit(8);
						 throw new InterruptedException("Error occured in building MultipleOutputs File Name"+ex.toString());
					}
				}
			}
	
			System.out.println("");
			
			FileInputFormat.setInputPaths(job, step2OutputPath);
			TextOutputFormat.setOutputPath(job, step3OutputPath);

			
			
//			DistributedCache.addCacheFile(cachePath.toUri(),job.getConfiguration());
			
			DistributedCache.addCacheFile(new Path(cachePath.toString() + Path.SEPARATOR + daasConfig.abacSqlServerCacheFileName()).toUri(),job.getConfiguration());
	
			if ( job.waitForCompletion(true) ) {
				abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
			} else {
				System.err.println("Error occured in MapReduce process, stopping");
//				System.exit(8);
				sendEmailToSupport(fromAddress, toAddress, subject, emailText);
				 throw new InterruptedException("Error occured in MapReduce process, stopping");
			}

		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
//			System.exit(8);
			 throw new InterruptedException("Error occured in MapReduce process, stopping" + ex.toString());
		}
	}
	
	private void runMrCopyFileWithFilter(DaaSConfig daasConfig
	                                    ,String fileType
	                                    ,Configuration hdfsConfig) throws Exception{

		int jobId = 0;
		Job job;

		jobId = abac.createJob(jobGroupId, jobSeqNbr++,"MapReduce Copy with Filter");
		try {
			System.out.println("\nCopy Files with Filter\n");

			hdfsConfig.set(HDFSUtil.DAAS_MAPRED_FILE_SEPARATOR_CHARACTER, daasConfig.fileFileSeparatorRegxSplit());
			hdfsConfig.set(HDFSUtil.DAAS_MAPRED_TERR_FIELD_POSITION, Integer.toString(daasConfig.fileTerrCdFieldPosition()));
			hdfsConfig.set(HDFSUtil.DAAS_MAPRED_LCAT_FIELD_POSITION, Integer.toString(daasConfig.fileLgcyLclRfrDefCdFieldPosition()));
			hdfsConfig.set(HDFSUtil.DAAS_MAPRED_BUSINESSDATE_FIELD_POSITION, Integer.toString(daasConfig.fileBusinessDateFieldPosition()));
			
			job = new Job(hdfsConfig, "Processing MapReduce Copy with Filter - " + fileType);

			job.setJarByClass(MergeToFinal.class);
			job.setMapperClass(TextCopyFileMapper.class);
			job.setReducerClass(TextCopyFileReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(60);
//			job.setOutputFormatClass(TextOutputFormat.class);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			String[] keyValueParts = {"",""};
		
			for ( Map.Entry<String, Integer> entry : terrDateMap.entrySet()) {
		
				try {
					keyValueParts = (entry.getKey()).split("\\|");
			
					Path finalPathSubTypeDt = null;

					MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(keyValueParts[0] + HDFSUtil.FILE_PART_SEPARATOR + keyValueParts[1]),TextOutputFormat.class, Text.class, Text.class);
					
					finalPathSubTypeDt = new Path(baseFinalPath.toString() + Path.SEPARATOR + keyValueParts[0] + Path.SEPARATOR + keyValueParts[1]);
					HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,finalPathSubTypeDt,daasConfig.displayMsgs());
					
					System.out.println("Added Multiple Output for " + keyValueParts[0] + " | " + keyValueParts[1]);
					
				} catch(Exception ex) {
					System.err.println("Error occured in building MultipleOutputs File Name");
					System.err.println(ex.toString());
					sendEmailToSupport(fromAddress, toAddress, subject, emailText);
					System.exit(8);
				}
			}
	
			System.out.println("");
			
			FileInputFormat.setInputPaths(job, step2OutputPath);
			TextOutputFormat.setOutputPath(job, step3OutputPath);

//			DistributedCache.addCacheFile(cachePath.toUri(),job.getConfiguration());
			DistributedCache.addCacheFile(new Path(cachePath.toString() + Path.SEPARATOR + daasConfig.abacSqlServerCacheFileName()).toUri(),job.getConfiguration());
	
			if ( job.waitForCompletion(true) ) {
				abac.closeJob(jobId,DaaSConstants.JOB_SUCCESSFUL_ID,DaaSConstants.JOB_SUCCESSFUL_CD);
			} else {
				System.err.println("Error occured in MapReduce process, stopping");
				sendEmailToSupport(fromAddress, toAddress, subject, emailText);
				System.exit(8);
			}

		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
//			System.exit(8);
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			throw new InterruptedException("Error occured in MapReduce process, stopping" + ex.toString());
		}
	}

	public int moveFiles(Path sourcePath
                         ,Path targetPath
                         ,String fileSuffix
                         ,boolean separateDir) throws Exception{
		
		return(moveFiles(sourcePath,targetPath,fileSuffix,separateDir,false,null,null));
		
	}

	public int moveFiles(Path sourcePath
	                     ,Path targetPath
	                     ,String fileSuffix
	                     ,boolean separateDir
	                     ,boolean keepCopy
	                     ,Configuration hdfsConfig
	                     ,Integer fileListJobId) throws Exception {

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
//			FsPermission newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);
	
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
//									System.out.println(" fileName " + fileName);
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
//						System.out.println(" maxFileNum for  " + fullTargetPathText + " is " + maxFileNum);
//							new Path(fileType + HDFSUtil.FILE_PART_SEPARATOR + terrCd + HDFSUtil.FILE_PART_SEPARATOR + businessDt));;
						
						if ( fileType.length() > 0 ) {
							targetName = fileType + HDFSUtil.FILE_PART_SEPARATOR + terrCd + HDFSUtil.FILE_PART_SEPARATOR + businessDt + HDFSUtil.FILE_PART_SEPARATOR + String.format("%07d", nextFileNum) + fullFileSuffix;
						} else {
							targetName = terrCd + HDFSUtil.FILE_PART_SEPARATOR + businessDt + HDFSUtil.FILE_PART_SEPARATOR + String.format("%07d", nextFileNum) + fullFileSuffix;
						}
				
						newName = new Path(fullTargetPathText + Path.SEPARATOR + targetName);
						
						if ( keepCopy ) {
							System.out.print("MOVE:"+status[idx].getPath() + " TO:"+newName.getName() + "...");
//							FileUtil.copy(fileSystem, status[idx].getPath(), fileSystem, newName, false, hdfsConfig);
							sourcePathDestPathMap.put(status[idx].getPath().toString(),newName.toString());
							
							System.out.println("done");
						} else {
							
							System.out.println(" simple move " + status[idx].getPath() + "  to " + newName);
							fileSystem.rename(status[idx].getPath(), newName);
//							fileSystem.setPermission(newName,newFilePremission);
						}
						

						
						
						retCnt++;
						
					}
				}
			
//				call mapreduce to copy files
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
						//AWS START
						hdfsConfig.set(DaaSConstants.HDFS_ROOT_CONFIG, daasConfig.hdfsRoot());
						//AWS END
						
						Job moveFileJob = new Job(hdfsConfig,"Copy/Move "+sourcePathLastDir+" files to Gold Layer");
						
						moveFileJob.setJarByClass(MergeToFinal.class);
						
						moveFileJob.setMapperClass(CopyMoveNFileMapper.class);
						moveFileJob.setNumReduceTasks(0);
						
						moveFileJob.setInputFormatClass(NLineInputFormat.class);
//						moveFileJob.setInputFormatClass(TextInputFormat.class);
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
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
//			System.exit(8);
			 throw new InterruptedException("Error occured in move files:" + ex.toString());
		}finally{
			try{
			if(bwForListofFilestoCopy != null)
				bwForListofFilestoCopy.close();
			}catch(Exception ex){
				
			}
		}

		return(retCnt);
	}
	
	public void updABaC(int jobGroupId
            ,DaaSConfig daasConfig
            ,FileSystem fileSystem) throws Exception {
	
		Path hdfsSubItemList = null; 
	
		if(daasConfig == null){
			System.out.println(" daasConfig is null in mergetofinal " );
			return;
		}
			
		if ( daasConfig.fileInputIsZipFormat() ) {
			hdfsSubItemList = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + "step1" );
		}
	
		DataInputStream fileStatusStream = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
	
		try {
	
			int jobId = abac.createJob(jobGroupId, jobSeqNbr++,"Update ABaC");
		
			Timestamp jobGroupStartTS = getJobGroupStartTime(jobGroupId,daasConfig);
			
			if(abac2List == null)
				abac2List = new ABaCList(daasConfig.fileFileSeparatorCharacter());
			abac2List.clear();
		
			if ( daasConfig.fileInputIsZipFormat() ) {
				FileStatus[] status = fileSystem.listStatus(hdfsSubItemList);
	
				for ( int i=0; i < status.length; i++ ) {
					//if ( status[i].getPath().getName().startsWith("FileStatus-r") ) {
						if ( status[i].getPath().getName().startsWith("FileStatus-") ) {
					
					
					
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
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
//			System.exit(8);
			 throw new InterruptedException("ABaC update list failed" + ex.toString());
			
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
	
	private boolean updABaCReason(DaaSConfig daasConfig
                                 ,FileSystem fileSystem) throws Exception{
	
		boolean successFl = true;
		Path hdfsReasonOutput = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + "step0" );
	
		DataInputStream fileStatusStream = null;
		BufferedReader br = null;
		String line;
		String[] parts;
		ArrayList<Integer> missingList = new ArrayList<Integer>();
	
		try {
			FileStatus[] status = fileSystem.listStatus(hdfsReasonOutput);
	
			for ( int i=0; i < status.length; i++ ) {
				if ( status[i].getPath().getName().startsWith("part-") ) {
					fileStatusStream = new DataInputStream(fileSystem.open(status[i].getPath()));
					br = new BufferedReader(new InputStreamReader(fileStatusStream));
					
					while ( (line=br.readLine()) != null ) {
						parts = line.split("\\|");
						
						if ( parts[0].equalsIgnoreCase("FAIL")) {
							System.err.println("Move error found for file DW_FILE_ID = " + parts[1]);
							successFl = false;
						}
						
						if ( parts[0].equalsIgnoreCase("MISSING") ) {
							if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
								System.out.println("Found missing file for DW_FILE_ID = " + parts[1]);
							}
							
							missingList.add(Integer.parseInt(parts[1]));
						}
					}
					
					if(br != null)
						br.close();
				}
				
				
			}
			
			if ( successFl && missingList.size() > 0 ) {
				abac.updateReasonToMissing(missingList);
			}
		} catch (Exception ex) {
			System.err.println("ABaC update reason failed");
			ex.printStackTrace();
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
//			System.exit(8);
			throw new InterruptedException("ABaC update reason failed" + ex.toString());
		}finally{
			try{
				if(br != null) { 
					br.close();
				}
			}catch(Exception ex){
				System.err.println("ABaC update reason failed");
				ex.printStackTrace();
				sendEmailToSupport(fromAddress, toAddress, subject, emailText);
				System.exit(8);
			}
		}
		
		return(successFl);
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
	
	
	
	public void runFindGoldLyrFilesToUpdateMapper(DaaSConfig daasConfig,String fileType,Configuration conf,Path sourcePath,Path outputPath) throws Exception{
		
		BufferedReader br = null;
		try{
		
			
//			conf.set("mapreduce.map.memory.mb ", "3584");
//			conf.set("mapreduce.reduce.memory.mb","3584");
//			conf.set("mapreduce.map.java.opts","-Xmx2867m");
//			conf.set("mapreduce.cluster.mapmemory.mb", "3584");
//			conf.set("mapreduce.reduce.java.opts","-Xmx2867m");
			//AWS START
			conf.set(DaaSConstants.HDFS_ROOT_CONFIG, daasConfig.hdfsRoot());
			//AWS END
			



            conf.set(FILE_TYPE,fileType);
			conf.set(NONMERGE_FILE_TYPE, Boolean.toString(daasConfig.nonmergedZipFile()));
			Job findFilesjob = new Job(conf,"FindGoldLayerFilesToUpdate");
			
			findFilesjob.setJarByClass(MergeToFinal.class);
			findFilesjob.setMapperClass(FindGoldLyrFilesToUpdateMapper.class);
			findFilesjob.setReducerClass(FindGoldLyrFilesToUpdateReducer.class);
			
			findFilesjob.setNumReduceTasks(1);
			
			findFilesjob.setMapOutputKeyClass(Text.class);
			findFilesjob.setMapOutputValueClass(Text.class);
			
			findFilesjob.setOutputKeyClass(NullWritable.class);
			findFilesjob.setOutputValueClass(Text.class);
			
			br = new BufferedReader(new InputStreamReader(fileSystem.open(sourcePath)));
			String line;
			FileStatus[] fstat = null;
			StringBuffer inputPaths = new StringBuffer();
			int cnt =0;
			int totalFileCount = 0;
			while( (line = br.readLine()) != null){
				if (cnt > 0)
					inputPaths.append(",");
				inputPaths.append(line);
				
				fstat = fileSystem.listStatus(new Path(line));
				totalFileCount += fstat.length;
//				
//				if(fstat != null ){
//					for(int i=0;i<fstat.length;i++){
//						if (i > 0)
//							inputPaths.append(",");
//						
//						inputPaths.append(fstat[i].getPath().getParent().toString());
//					}
//				}
				cnt++;
				
			}
			
			
//			findFilesjob.setInputFormatClass(NLineInputFormat.class);
			findFilesjob.setInputFormatClass(TextInputFormat.class);
			findFilesjob.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(findFilesjob,inputPaths.toString());
//			NLineInputFormat.setInputPaths(findFilesjob,inputPaths.toString());
//			NLineInputFormat.setNumLinesPerSplit(findFilesjob, 10);
			
			FileOutputFormat.setOutputPath(findFilesjob, outputPath);
			MultipleOutputs.addNamedOutput(findFilesjob, "goldLayerFilesToupdateForthisRun", TextOutputFormat.class, NullWritable.class, Text.class);
			
			DistributedCache.addCacheFile(new Path(cachePath.toString() + Path.SEPARATOR + daasConfig.abacSqlServerCacheFileName()).toUri(),findFilesjob.getConfiguration());
			
			LazyOutputFormat.setOutputFormatClass(findFilesjob, TextOutputFormat.class);
			
			if(totalFileCount > 0)
				findFilesjob.waitForCompletion(true);
			
			
		}catch(Exception ex){
//			ex.printStackTrace();
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			throw new InterruptedException("runFindGoldLyrFilesToUpdateMapper failed" + ex.toString());
		}finally{
			try{
				if(br != null)
					br.close();
			}catch(Exception ex){
				
			}
		}
	}
	
	//@mc41946: Merge Process for Multiple runs
	//moveFilesFromCurrent(fileSystem,step1CurrentOutputPath,step1OutputPath);
	public void moveFilesinHDFS(DaaSConfig daasConfig, Path step1CurrentOutputPath,
			Path step1OutputPath, FileSystem hdfsFileSystem,
			final String subTypeCode) throws InterruptedException {
		
		try {
			//FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;
			HDFSUtil.createHdfsSubDirIfNecessary(hdfsFileSystem,
					step1OutputPath, daasConfig.displayMsgs());
			
			//Checking the files in Current Path
			fstatustmp = hdfsFileSystem.listStatus(
					step1CurrentOutputPath, new PathFilter() {
				@Override
				public boolean accept(Path pathname) {
					if (pathname.getName().contains(subTypeCode))
						return true;
					return false;
				}
			});
			/*System.out.println(" num of output files for "+subTypeCode+"at "
					+ step1CurrentOutputPath.toString() + " "
					+ fstatustmp.length);*/
			String fileName;
			FsPermission fspermission = new FsPermission(FsAction.ALL,
					FsAction.ALL, FsAction.ALL);
			
			//Checking the files in existing Step1 Path
			FileStatus[] fstus = hdfsFileSystem.listStatus(step1OutputPath, new PathFilter() {
				@Override
				public boolean accept(Path pathname) {
					if (pathname.getName().contains(subTypeCode))
						return true;
					return false;
				}
			});
			int subTypeFileCount=fstus.length;
				for (FileStatus fstat : fstatustmp) {
					fileName = fstat.getPath().getName();
					//System.out.println(" fileName  " + fileName);
					
					String filename=fileName.replace(".gz", "");
					String[] fileContent=null;
					String newfileName="";					
					if(daasConfig.nonmergedZipFile())
					{
						fileContent=filename.split("m-");
						newfileName=fileContent[0]+"m-"+String.format("%05d",(++subTypeFileCount))+".gz";
					}else
					{
						fileContent=filename.split("r-");
						newfileName=fileContent[0]+"r-"+String.format("%05d",(++subTypeFileCount))+".gz";
					}

					if (!hdfsFileSystem.rename(
							new Path(fstat.getPath().toString()), new Path(
									step1OutputPath + Path.SEPARATOR + newfileName))) {
						System.out.println("could not rename "
								+ fstat.getPath().toString() + " to "
								+ step1OutputPath + Path.SEPARATOR + newfileName);
					} else {
						hdfsFileSystem.setPermission(new Path(step1OutputPath
								+ Path.SEPARATOR + newfileName), fspermission);
						System.out.println(" renamed " + fstat.getPath().toString()
								+ " to " + step1OutputPath + Path.SEPARATOR
								+ newfileName);
					}
					
				}
		
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
	        throw new InterruptedException("moveFilesinHDFS failed" + ex.toString());
		}
	}
	
	public void sendEmailToSupport(String fromAddress,String toAddress,String subject,String body)
	{
		sendMail.SendEmail(fromAddress, toAddress, subject, body);
	}
}
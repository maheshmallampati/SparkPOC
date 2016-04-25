package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.mapreduce.FilterOutRestatementsMapper;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

public class FilterOutRestatementsDriver extends Configured implements Tool{
	
	public static void main(String[] argsAll){
		
		try{
			int ret = ToolRunner.run(new Configuration(), new FilterOutRestatementsDriver(),argsAll);
		}catch(Exception ex){
			
		}
	}

	private static final String JOB_DESC = "Filterout restatements for HUB";

	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
//	private Path baseHivePath = null;
//	FsPermission newFilePremission;
	String createJobDetails = "TRUE";
	String multioutBaseOutputPath = "";
	String storeFilterFilePath = "" ;
	Path workPath = null;
	@Override
	public int run(String[] argsAll) throws Exception {
	
		GenericOptionsParser gop = new GenericOptionsParser(argsAll);
		String[] args = gop.getRemainingArgs();
		
		
		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
		boolean helpRequest = false;
		String useStoreFilter = "FALSE";
		
		
		

		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}
			else
			if ( args[idx].equals("-t") && (idx+1) < args.length ) {
				fileType = args[idx+1];
			}
			else
			if ( args[idx].equals("-d") && (idx+1) < args.length ) {
				terrDate = args[idx+1];
			}
			else
			if ( args[idx].toUpperCase().equals("-H") || args[idx].toUpperCase().equals("-HELP")  ) {
				helpRequest = true;
			}
			else if ( args[idx].equals("-createJobDetails") ) {
				createJobDetails = args[idx+1];
				if(StringUtils.isBlank(createJobDetails)){
					createJobDetails = "TRUE";
				}
			}else if ( args[idx].equalsIgnoreCase("-usestorefilter") ) {
				useStoreFilter = args[idx+1].toUpperCase();
			}if ( args[idx].equalsIgnoreCase("-storeFilterFilePath") ) {
				storeFilterFilePath = args[idx+1];
			}
			//AWS START
			//else if(args[idx].equalsIgnoreCase("-multioutBaseOuputPath")){
			//	multioutBaseOutputPath = args[idx+1];
			//}
			//AWS END
		}

		if ( helpRequest ) {
			System.out.println("Usage: FilterOutRestatementsDriver -c config.xml -t filetype -d territoryDateParms ");
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
			System.err.println("Usage: FilterOutRestatementsDriver -c config.xml -t filetype -d territoryDateParms");
			System.err.println("where territoryDateParm is a comma separated list of territory codes and dates separated by colons(:)");
			System.err.println("for example, 840:2012-07-01:2012-07-07 is territory 840 from July 1st, 2012 until July 7th, 2012.");
			System.err.println("the date format is either ISO YYYY-MM-DD or YYYYMMDD (both are valid)");
			System.err.println("If only one date is supplied then a single day is used for that territory");
			System.err.println("Multiple territoryDateParm can be specified as comma separated values: 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01");
			System.err.println("This will get a total of 3 days for 840 and 1 day from 250");
			System.exit(8);
		}

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		
		hdfsConfig = getConf();
		//AWS START
		//fileSystem = FileSystem.get(hdfsConfig);
		//AWS END
		
		if ( daasConfig.configValid() ) {
			
			if ( daasConfig.displayMsgs()  ) {
				System.out.println(daasConfig.toString());
			}
			//@mc41946: AWS Migration
			multioutBaseOutputPath = daasConfig.hdfsRoot() +Path.SEPARATOR +daasConfig.hdfsHiveSubDir() +Path.SEPARATOR + new Path( "datahub_tmp/tld");
			
			hdfsConfig = getConf();
			//AWS START
			//fileSystem = FileSystem.get(hdfsConfig);
			fileSystem = HDFSUtil.getFileSystem(daasConfig, hdfsConfig);
			//AWS END 
			
			hdfsConfig.set("USE_STORE_FILTER",useStoreFilter);

			runJob(daasConfig,fileType,terrDate,true);
			
		} else {
			System.err.println("Invalid Config XML file, stopping");
			System.err.println(daasConfig.errText());
			System.exit(8);
		}
		
		
		
		return 0;
	}
	
	private void runJob(DaaSConfig daasConfig
            ,String fileType
            ,String terrDate
            ,boolean compressOut) {
		
		int jobId = 0;
		ABaC abac = null;
		int prevJobGroupId = 0;
		try{
			//AWS START
			hdfsConfig.set(DaaSConstants.HDFS_ROOT_CONFIG, daasConfig.hdfsRoot());
			//AWS END 
			
			baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "NewTLDDataHub");
			
			//AWS START
			multioutBaseOutputPath = daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsHiveSubDir() + Path.SEPARATOR + "datahub_tmp" + Path.SEPARATOR + "tld";
			//AWS END
			
			hdfsConfig.set("MULTIOUT_BASE_OUTPUT_PATH", multioutBaseOutputPath);
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, new Path(multioutBaseOutputPath), true);
			

			//AWS START
			//hdfsConfig.set("mapred.compress.map.output", "true");
			//hdfsConfig.set("mapred.output.compress", "true"); 
			//hdfsConfig.set("mapred.output.compression.type", "BLOCK");  
//			hdfsConfig.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
			//hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			hdfsConfig.set("mapreduce.map.output.compress", "true");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
			hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
//			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			//AWS END 
			
			abac = new ABaC(daasConfig);
			if("TRUE".equalsIgnoreCase(createJobDetails)){
				prevJobGroupId= abac.getOpenJobGroupId(DaaSConstants.DATAHUB_EXTRACT_JOBGROUP_NAME);
				jobId = abac.createJob(prevJobGroupId, 2, JOB_DESC);
			}
			Job job = Job.getInstance(getConf());
			job.setJobName("FilterRS");
			
			job.setMapperClass(FilterOutRestatementsMapper.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setJarByClass(FilterOutRestatementsMapper.class);
			
			job.setNumReduceTasks(0);
			
			Path currentRunTerrCdBusnDtStoreIdList = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "terrcd_busndt_storeid_list.txt.gz");
//			System.out.println ( " currentRunTerrCdBusnDtStoreIdList " + currentRunTerrCdBusnDtStoreIdList.toString());
			HashSet<String> uniqueTerrCdBusndtStoreIds = new HashSet<String>();
			
			String[] parts;
			String terrcd;
			String busndt;
			String storeid;
			
			if(fileSystem.exists(currentRunTerrCdBusnDtStoreIdList)){
				
				InputStreamReader insr = null;
				BufferedReader br = null;
				insr = new InputStreamReader(new GZIPInputStream(fileSystem.open(currentRunTerrCdBusnDtStoreIdList)));
//				insr = new InputStreamReader(fileSystem.open(currentRunTerrCdBusnDtStoreIdList));
					
				br = new BufferedReader( insr);
				
				
				if(br != null){
					String line = null;
					
					while( (line = br.readLine()) != null){
						parts = line.split("~");
						terrcd = parts[0];
						busndt = parts[1];
						storeid = parts[2];
						uniqueTerrCdBusndtStoreIds.add(terrcd+"\t"+busndt);
//						System.out.println( " line " + line);
					}
					
					br.close();
					insr.close();
				}
			}
			
//			System.out.println(" done adding keys " + uniqueTerrCdBusndtStoreIds.size());
			
			job.addCacheFile(new URI(currentRunTerrCdBusnDtStoreIdList.toString() + "#" + currentRunTerrCdBusnDtStoreIdList.getName()));
//			job.addCacheArchive(new URI(currentRunTerrCdBusnDtStoreIdList.toString() + "#" + currentRunTerrCdBusnDtStoreIdList.getName()) );
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath, true);
			
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
//			LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);
//			SequenceFileOutputFormat.setOutputPath(job, baseOutputPath);
			
			//set the input paths
			if(uniqueTerrCdBusndtStoreIds == null || uniqueTerrCdBusndtStoreIds.isEmpty()){
				System.out.println("Exiting without submitting the job. No input paths to process");
				System.exit(1);
			}
			
			Iterator<String > uniqueTerrCdBusndtStoreIdsIt = uniqueTerrCdBusndtStoreIds.iterator();
			StringBuilder inputpaths = new StringBuilder();
			
			//AWS START
			int pathCnt = 0;
			//AWS END
			int i = 0;
			
			String unqkey ;
			Path inputpath;
			while(uniqueTerrCdBusndtStoreIdsIt.hasNext()){
				unqkey = uniqueTerrCdBusndtStoreIdsIt.next();
				parts = unqkey.split("\t");
//				System.out.println (" unqkey " + unqkey);
				terrcd = parts[0];
				busndt = parts[1];
//				storeid = parts[2];
//				if(i > 0) 
//					inputpaths.append(",");
//				inputpaths.append("/daas/hive/datahub/STLD/terr_cd="+terrcd+"/pos_busn_dt="+busndt);
//				inputpath = new Path("/daas/hive/datahub/STLD/terr_cd="+terrcd+"/pos_busn_dt="+busndt);
				//AWS START
				//inputpath = new Path("/daas/hive/datahub/tld/terr_cd="+terrcd+"/pos_busn_dt="+busndt);
				inputpath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsHiveSubDir() + Path.SEPARATOR + "datahub" + Path.SEPARATOR + "tld" + Path.SEPARATOR + "terr_cd=" + terrcd + Path.SEPARATOR + "pos_busn_dt=" + busndt);
				//AWS END 
				if(fileSystem.exists(inputpath )){
					FileInputFormat.addInputPath(job, inputpath);
					//AWS START
					pathCnt++;
					//AWS END
//					SequenceFileInputFormat.addInputPath(job, inputpath);
				}
//				inputpath = new Path("/daas/hive/datahub/DetailedSOS/terr_cd="+terrcd+"/pos_busn_dt="+busndt);
//				if(fileSystem.exists(inputpath )){
//					FileInputFormat.addInputPath(job, inputpath);
//				}
				i++;
				MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars(terrcd+busndt), TextOutputFormat.class, NullWritable.class, Text.class);
//				MultipleOutputs.addNamedOutput(job, HDFSUtil.replaceMultiOutSpecialChars(terrcd+busndt), SequenceFileOutputFormat.class, NullWritable.class, Text.class);
				
			}
			
//			FileInputFormat.addInputPath(job, new Path("/daas/hive/datahub/STLD/terr_cd=840/pos_busn_dt=2015-03-30"));
//			System.out.println(" done adding inputpaths " + inputpaths.substring(0,100));
//			FileInputFormat.addInputPaths(job, inputpaths.toString());
			
			//AWS START
			if ( pathCnt > 0 ) {
				if ( ! job.waitForCompletion(true) ) {
					System.err.println("Error occured in MapReduce process, stopping");
					System.exit(8);
				}
			} else {
				HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, baseOutputPath, daasConfig.displayMsgs());
			}
			//AWS END
				
			if("TRUE".equalsIgnoreCase(createJobDetails)){
				abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
				
			}
			
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			if(abac != null)
				abac.dispose();
		}
	}

}

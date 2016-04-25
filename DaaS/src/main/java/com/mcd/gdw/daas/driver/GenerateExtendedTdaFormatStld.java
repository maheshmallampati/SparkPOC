package com.mcd.gdw.daas.driver;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
import org.apache.hadoop.fs.PathFilter;
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
import com.mcd.gdw.daas.mapreduce.NpStldXmlExtendedMapper;
import com.mcd.gdw.daas.mapreduce.NpStldXmlExtendedReducer;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.SendMail;
/**
 * 
 * @author KhajaAsmath
 * Driver class to extract DTL from STLD files
 * Usage:
 * hadoop jar /home/mc32445/scripts/mcdonaldsmrnew.jar com.mcd.gdw.daas.driver.GenerateExtendedTdaFormatStld \
 *	-r /user/mc32445/poc/work/np_xml \
 *	-o /user/mc32445/poc/stldextract3 \
 *	-c /user/mc32445/tdaextract/distcache/TDA_CODE_VALUES.psv,/user/mc32445/tdaextract/distcache/ITEMCODE_MAPPING.psv,/user/mc32445/tdaextract/distcache/MenuPriceBasis.psv
 *  
 *  r - input path, -o for output path, -c all the distributed cache files, -p properties
 */

public class GenerateExtendedTdaFormatStld extends Configured implements Tool {
	private DaaSConfig daasConfig;
	
	String checkforCustInfo = "true";
	String outputFileFormat="JSON";
	private Set<Integer> validTerrCdSet = new HashSet<Integer>();	
	private ArrayList<String> workTerrCodeList = new ArrayList<String>();
	private int jobSeqNbr = 1;
	private int jobGroupId = 0;
	int jobId = 0;
	int totalRecordsInOutputFile = -1;
	private Set<String> terrCodes=new HashSet<String> ();
	private final static String JOB_SUCCESSFUL_CD  = "SUCCESSFUL";
	private final static String fileNameSeperator = "_";
	private final static String fileSeperator = "/";
	public  final static String OUTPUT_FILE_DETAIL_NAME="Tasseo Extract";
	public  final static String OUTPUT_FILE_TYPE="PSV";
	private final static String TDA_EXTRACT_JOBGROUP_NAME = "Generate Extended TDA Format STLD";
	private final static String TDA_EXTRACT_HISTORY_JOBGROUP_NAME = "Generate History Extended TDA Format STLD";
	private final static String JOB_OUTPUT_LOCATION="TasseoExtracts";
	private final static String US_OUTPUT_FILE_NAME="USRxD126TASSEORxD126TDARxD126";
	private final static String AU_OUTPUT_FILE_NAME="AURxD126TASSEORxD126TDARxD126";
	private final static String CN_OUTPUT_FILE_NAME="CNRxD126TASSEORxD126TDARxD126";
	private final static String US_OUTPUT_FILE_NAME_DB="USRxD126DBRxD126TDARxD126";
	private final static String AU_OUTPUT_FILE_NAME_DB="AURxD126DBRxD126TDARxD126";
	private final static String CN_OUTPUT_FILE_NAME_DB="CNRxD126DBRxD126TDARxD126";
	
	private final static String OUTPUT_FILE_NAME="MCDRxD126TMSRxD126";

	
	
	public static void main(String[] args) throws Exception {
		int retval = ToolRunner.run(new Configuration(),new GenerateExtendedTdaFormatStld(), args);

	}

	String filterOnStoreId = "FALSE";
	String filterOnLastMonth = "FALSE";
	String cacheFile = "";
	SendMail sendMail;
	
	String fromAddress = "";
	String toAddress = "";	
	String subject = "";
	String emailSuccessText    = "";
	String emailFailureText ="";
	String inputRootDir = "";

	
	String configXmlFile = "";
	String fileType = "";
	
	String vldTerrCdsStr = "";
	
	String terrDate = "";
	// String terrDateFile = "";
	String owshFltr = "*";
	String generateHeaders="TRUE";
	String numberOfReducers="10";
	String generateExtraFields="TRUE";
	
	Boolean compressOut=true;
	
	//AWS START
	FileSystem fileSystem;
	//AWS End
	
	//String terrCD="";
	
	@Override
	public int run(String[] argsall) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		String[] args = gop.getRemainingArgs();
		
		int idx;
	
		try {
			
			for ( idx = 0; idx < args.length; idx++) {
				if (args[idx].equals("-c") && (idx + 1) < args.length) {
					configXmlFile = args[idx + 1];
				}

				if (args[idx].equals("-t") && (idx + 1) < args.length) {
					fileType = args[idx + 1];
				}

				if (args[idx].equals("-d") && (idx + 1) < args.length) {
					terrDate = args[idx + 1];
				}
				if (args[idx].equals("-owshfltr") && (idx + 1) < args.length) {
					owshFltr = args[idx + 1];
				}
				if ( args[idx].equals("-ci")  && (idx + 1) < args.length) {
					checkforCustInfo = args[idx+1];
				}
				if ( args[idx].equalsIgnoreCase("-vldTerrCodes") && (idx + 1) < args.length)  {
					vldTerrCdsStr = args[idx+1];
					
					String[] terrCds = vldTerrCdsStr.split(",");
					for(String terrCd:terrCds){
						validTerrCdSet.add(Integer.parseInt(terrCd));
						System.out.println(" added " + terrCd +" to valid terrcd list");
					}
				}
				if ( args[idx].equalsIgnoreCase("-filterOnStoreId") && (idx + 1) < args.length)  {
					filterOnStoreId = args[idx+1];
					if(filterOnStoreId.equalsIgnoreCase("TRUE")){
						filterOnStoreId = "TRUE";
					}
				}
				if (args[idx].equalsIgnoreCase("-dc") && (idx + 1) < args.length) {
					cacheFile = args[idx+1];
				}
				if ( args[idx].equalsIgnoreCase("-f") && (idx + 1) < args.length) {
					outputFileFormat = args[idx+1];
					
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
				if (args[idx].equals("-emailSuccessText") && (idx + 1) < args.length) {
					emailSuccessText = args[idx + 1];
				}
				if (args[idx].equals("-emailFailureText") && (idx + 1) < args.length) {
					emailFailureText = args[idx + 1];
				}
				
				if (args[idx].equals("-generateHeaders") && (idx + 1) < args.length) {
					generateHeaders = args[idx + 1];
				}
				if (args[idx].equals("-numberOfReducers") && (idx + 1) < args.length) {
					numberOfReducers = args[idx + 1];
				}
				if (args[idx].equals("-generateAdditionalFields") && (idx + 1) < args.length) {
					generateExtraFields = args[idx + 1];
				}
				
				if (args[idx].equals("-compressOut") && (idx + 1) < args.length) {
					compressOut = Boolean.valueOf(args[idx + 1]);
				}

			}
			
				
			if (configXmlFile.length() == 0 || fileType.length() == 0
					|| terrDate.length() == 0 || fromAddress.length()==0 || toAddress.length()==0 || subject.length()==0 || subject.length()==0 || emailSuccessText.length()==0 || emailFailureText.length()==0) {
				System.err.println("Invalid parameters");
				System.err
						.println("Usage: GenerateExtendedTdaFormatStld -c config.xml -t filetype -d territoryDateParms -owshfltr ownershipFilter");
				System.exit(8);
			}
			daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		
		if (daasConfig.configValid()) {
			sendMail = new SendMail(daasConfig);
			runMrTMSExtract(daasConfig, fileType, getConf(), terrDate,
					owshFltr);

		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.err.println("File Type   = " + fileType);
			System.exit(8);
		}
		
		return 0;
		}catch (Exception ex) {
			Logger.getLogger(GenerateExtendedTdaFormatStld.class.getName()).log(
					Level.SEVERE, null, ex);
			throw ex;
		}
		
	}
	
	/**
	 * Method which runs Map Reduce job.
	 * 
	 * @param daasConfig
	 *            : Configuration object which loads ABaC, teradata and Hadoop
	 *            Paths
	 * @param fileType
	 *            : File type for this job. POS_XML
	 * @param hdfsConfig
	 *            : Map Reduce Configuration Object
	 * @param terrDate
	 *            : Parameter for generating Input Path i.e. either Work or Gold
	 *            Layer
	 * @param owshFltr
	 *            : Ownership filter
	 * @param haviOrderFilter
	 * @throws Exception
	 *             :
	 */
	private void runMrTMSExtract(DaaSConfig daasConfig, String fileType,
			Configuration hdfsConfig, String terrDate, String owshFltr) throws Exception {

		Job job;
		ArrayList<Path> requestedPaths = null;
		hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm()); 
/*		SimpleDateFormat sdf;
		Calendar dt;
		String subDir = "";*/

		// M-1939 Hadoop- Offer Redemption Extract changes for US Mobile Stores:
		// Havi Extract is done only on STLD files
		ArrayList<String> subTypeList = new ArrayList<String>();
		subTypeList.add("STLD");

		ABaC abac = null;
		try {

			hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER,
					owshFltr);
			String jobTitle = "";
			// M-1939 Hadoop- Offer Redemption Extract changes for US Mobile
			// Stores: Update Job Title based on extract.
			if (!terrDate.toUpperCase().startsWith("LAST")) {
				jobTitle = TDA_EXTRACT_HISTORY_JOBGROUP_NAME;
			} else {
				jobTitle = TDA_EXTRACT_JOBGROUP_NAME;
			}

			if ( outputFileFormat.equalsIgnoreCase("tsv") ) {
				System.out.println("Create Extended TDA File Format as Tab Seprated Value (TSV)");
			} else {
				System.out.println("Create TASSEO File Format");
			}

			//AWS START
            //fileSystem = FileSystem.get(hdfsConfig);
            fileSystem = HDFSUtil.getFileSystem(daasConfig, hdfsConfig);
            //AWS END
			/*sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			dt = Calendar.getInstance();
			subDir = sdf.format(dt.getTime());*/

			// M-1939 : Define output path based on input path i.e. Work or Gold
			// Layer.
			Path outPath;
			if ( outputFileFormat.equalsIgnoreCase("tsv") ) {
				outPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR+ daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "ExtendedTDAasTSV" );
			} else {
				outPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR+ daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + JOB_OUTPUT_LOCATION );
			}
			/*if (terrDate.toUpperCase().startsWith("WORK")) {
				outPath = new Path(outputDir);
			} else {
				outPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR
						+ daasConfig.hdfsWorkSubDir() + Path.SEPARATOR
						+ "TMSHaviExtract22" + Path.SEPARATOR + subDir);
			}*/
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, outPath,
					daasConfig.displayMsgs());

			/*
			 * M-1939 : Add Input Paths based on the Parameter. Path for Work
			 * Layer: WORK:840 Path for History Extract:
			 * 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01
			 */
			if (terrDate.toUpperCase().startsWith("LAST")) {
				String[] workParts = (terrDate + ":").split(":");
				String filterTerrCodeList = workParts[1];
				StringBuffer terrCDPaths=new StringBuffer();

				if (filterTerrCodeList.length() > 0) {
					System.out
							.println("TMS extract is perromed for the following Territory Codes:");
					String[] parts = filterTerrCodeList.split(",");
					for (String terrCD : parts) {
						
						if (terrCDPaths.length()>0) {
							terrCDPaths.append(",");
							}
						System.out.println("    " + terrCD);
						terrCDPaths.append(terrCD+":"+getPreviousMonth());
						workTerrCodeList.add(terrCD);
					}
				}
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem,
						fileType, terrCDPaths.toString(), subTypeList);
				System.out
						.println("Total number of Input Paths from Last Month : "
								+ requestedPaths.size());
				terrCodes=new HashSet<String>(workTerrCodeList);
			} 
			
			else if (terrDate.toUpperCase().startsWith("WORK")) {
					String[] workParts = (terrDate + ":").split(":");
					String filterTerrCodeList = workParts[1];

					if (filterTerrCodeList.length() > 0) {
						System.out
								.println("Work Layer using only the following Territory Codes:");
						String[] parts = filterTerrCodeList.split(",");
						for (String addTerrCode : parts) {
							System.out.println("    " + addTerrCode);
							workTerrCodeList.add(addTerrCode);
						}
					}
					requestedPaths = getVaildFilePaths(daasConfig, fileSystem,
							fileType, subTypeList);
					System.out
							.println("Total number of Input Paths from Work Layer : "
									+ requestedPaths.size());
					terrCodes=new HashSet<String>(workTerrCodeList);
				}
			
				else {
			
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem,
						fileType, terrDate, subTypeList);
				System.out
						.println("Total number of Input Paths from Gold Layer : "
								+ requestedPaths.size());
				ArrayList<String> goldTerrCodes=goldTerrCodeList(daasConfig, fileSystem,
						terrDate, subTypeList);
				terrCodes=new HashSet<String>(goldTerrCodes);
			}

			// M-1939 : Parameters required for Havi Extract.
			String filterCriteria = "FALSE";
			Configuration conf = getConf();
//			String[] temp = null;
		    SimpleDateFormat SDF_MMddyyyyHHmmssSSSnodashes = new SimpleDateFormat("MMddyyyyHHmmss");
			String timestamp = SDF_MMddyyyyHHmmssSSSnodashes.format(new Date(System.currentTimeMillis()));
			
			if ( compressOut ) {
				System.out.println("Setting output format to GZ");
				
				hdfsConfig.set("mapreduce.map.output.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
				hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
//				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
				
				conf.set("mapreduce.map.output.compress", "true");
				conf.set("mapreduce.output.fileoutputformat.compress", "true");
				conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
				conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
//				conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
				
			}
			//AWS START
            hdfsConfig.set(DaaSConstants.HDFS_ROOT_CONFIG, daasConfig.hdfsRoot());
            //AWS END
			
			conf.set("USE_FILTER_BY_STORE_ORDER", filterCriteria);
			conf.set("CHECK_FOR_CUSTINFO",checkforCustInfo);
			conf.set("OUTPUT_FILE_FORMAT",outputFileFormat);			
			conf.set(DaaSConstants.JOB_CONFIG_PARM_STORE_FILTER, filterOnStoreId);
			conf.set("TIMESTAMP", timestamp);
			conf.set("GENERATE_HEADERS",generateHeaders);
			conf.set("GENERATE_EXTRA_FIELDS",generateExtraFields);
			//conf.set("TERR_CD",terrCD);
			
			// conf.set("mapred.job.queue.name", queuename);

			// M-1939 : Job Parameters.
			job = Job.getInstance(conf, jobTitle);
			abac = new ABaC(daasConfig);
			job.setJarByClass(GenerateExtendedTdaFormatStld.class);
			job.setMapperClass(NpStldXmlExtendedMapper.class);
			if(generateHeaders.equalsIgnoreCase("TRUE"))
			{
				job.setReducerClass(NpStldXmlExtendedReducer.class);
				job.setNumReduceTasks(Integer.parseInt(numberOfReducers));
				
			}else
			{
				job.setNumReduceTasks(0);
			}
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		
			/*job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);*/
			HashSet<String> terrCdDtset = new HashSet<String>();
			
			
			
			String datepart;
			String terrCdDatepart;
//			HashSet<String> dtset = new HashSet<String>();
			String[] fileNameParts;
			
			//HashSet<String> terrCdDtset = new HashSet<String>();
//			FileInputFormat.addInputPath(job,new Path(""));
			
			FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;

			

			Path cachePath = new Path(cacheFile);
            for (String distCachePath: cacheFile.split(",")){
                System.out.println("Distributed Cache file "+distCachePath);
                cachePath=new Path(distCachePath);
                DistributedCache.addCacheFile(cachePath.toUri(),
                        job.getConfiguration());
            }
			
			for(FileStatus fstat:fstatus){
				String fileName = fstat.getPath().getName().toUpperCase();
				System.out.println(" file name " + fileName);
				
				if(fstat.isDirectory())
					continue;
				
				String fileNamePartsDelimiter = "~";
				
				if(fileName.indexOf("RXD126") > 0){
					fileNamePartsDelimiter = "RXD126";
				}
				fileNameParts = fileName.split(fileNamePartsDelimiter);
				String terrCdfrmFileName = fileNameParts[1];
				//System.out.println(" terrCdfrmFileName " + terrCdfrmFileName);
				
				
//				if(fileName.startsWith("STLDRXD12636RXD126") || fileName.startsWith("STLD~36~") ||
//						fileName.startsWith("STLDRXD126840RXD126") || fileName.startsWith("STLD~840~")){
				//if(validTerrCdSet != null && validTerrCdSet.contains(Integer.parseInt(terrCdfrmFileName))){
						
				
					FileInputFormat.addInputPath(job, fstat.getPath());
				
					datepart = fileNameParts[2].substring(0,8);
					terrCdDtset.add(terrCdfrmFileName+DaaSConstants.SPLCHARTILDE_DELIMITER+datepart);
					
//					int lastindx = fileName.lastIndexOf("~");
//					if(lastindx == -1)
//						lastindx = fileName.lastIndexOf("-R-");
//					
//					datepart = fileName.substring(lastindx-8,lastindx);
//					dtset.add(datepart);
				//}
			}
//			Iterator<String> it = dtset.iterator();
			if ( terrCdDtset.size() == 0 ) {
				System.err.println("Stopping, No valid files found");
				sendEmailToSupport(fromAddress, toAddress, subject, "No valid files found for TMS Extract ");
				System.exit(8);
			}

			Iterator<String> it = terrCdDtset.iterator();
			//conf.set("terrCdDtset",terrCdDtset);
			
			String terrCd= "";
			while(it.hasNext()){
//				datepart = it.next();
				terrCdDatepart = it.next();
				terrCd   = terrCdDatepart.split(DaaSConstants.SPLCHARTILDE_DELIMITER)[0];
				datepart = terrCdDatepart.split(DaaSConstants.SPLCHARTILDE_DELIMITER)[1];
				
//				System.out.println(" addding " +datepart);
//				MultipleOutputs.addNamedOutput(job,"HDR"+datepart,TextOutputFormat.class, Text.class, Text.class);
//				MultipleOutputs.addNamedOutput(job,"MCDRxD126TMSRxD126"+datepart,TextOutputFormat.class, Text.class, Text.class);
				
				if(Integer.parseInt(terrCd) == 36) {
					//System.out.println(" addding1 " +"MCDRxD126TMSRxD126"+datepart);
					MultipleOutputs.addNamedOutput(job,OUTPUT_FILE_NAME+datepart,TextOutputFormat.class, Text.class, Text.class);
				if ( outputFileFormat.equalsIgnoreCase("TSV") ) {
					System.out.println("MO="+AU_OUTPUT_FILE_NAME_DB+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp);
					MultipleOutputs.addNamedOutput(job,AU_OUTPUT_FILE_NAME_DB+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp,TextOutputFormat.class, Text.class, Text.class);
				} else {
					System.out.println("MO="+AU_OUTPUT_FILE_NAME+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp);
					MultipleOutputs.addNamedOutput(job,AU_OUTPUT_FILE_NAME+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp,TextOutputFormat.class, Text.class, Text.class);
				}
				//	MultipleOutputs.addNamedOutput(job,"AURxD126TASSEORxD126TDA"+datepart,TextOutputFormat.class, Text.class, Text.class);
				//	MultipleOutputs.addNamedOutput(job,"AURxD126TASSEORxD126TDA",TextOutputFormat.class, Text.class, Text.class);
				}
				else if(Integer.parseInt(terrCd) == 124) {
					//System.out.println(" addding1 " +"MCDRxD126TMSRxD126"+datepart);
					MultipleOutputs.addNamedOutput(job,OUTPUT_FILE_NAME+datepart,TextOutputFormat.class, Text.class, Text.class);
				if ( outputFileFormat.equalsIgnoreCase("TSV") ) {
					System.out.println("MO="+CN_OUTPUT_FILE_NAME_DB+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp);
					MultipleOutputs.addNamedOutput(job,CN_OUTPUT_FILE_NAME_DB+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp,TextOutputFormat.class, Text.class, Text.class);
				} else {
					System.out.println("MO="+CN_OUTPUT_FILE_NAME+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp);
					MultipleOutputs.addNamedOutput(job,CN_OUTPUT_FILE_NAME+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp,TextOutputFormat.class, Text.class, Text.class);
				}
				//	MultipleOutputs.addNamedOutput(job,"AURxD126TASSEORxD126TDA"+datepart,TextOutputFormat.class, Text.class, Text.class);
				//	MultipleOutputs.addNamedOutput(job,"AURxD126TASSEORxD126TDA",TextOutputFormat.class, Text.class, Text.class);
				}
				else{
					//System.out.println(" addding " +"MCDRxD126TMSRxD126"+terrCdDatepart);
					MultipleOutputs.addNamedOutput(job,OUTPUT_FILE_NAME+datepart,TextOutputFormat.class, Text.class, Text.class);
					MultipleOutputs.addNamedOutput(job,OUTPUT_FILE_NAME+terrCd+DaaSConstants.SPLCHARTILDE_DELIMITER+datepart,TextOutputFormat.class, Text.class, Text.class);
					//MultipleOutputs.addNamedOutput(job,"PIPERxD126DELIMRxD126"+terrCdDatepart,TextOutputFormat.class, Text.class, Text.class);
					if ( outputFileFormat.equalsIgnoreCase("TSV") ) {
						MultipleOutputs.addNamedOutput(job,US_OUTPUT_FILE_NAME_DB+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp,TextOutputFormat.class, Text.class, Text.class);
					} else {
						MultipleOutputs.addNamedOutput(job,US_OUTPUT_FILE_NAME+datepart+DaaSConstants.SPLCHARTILDE_DELIMITER+timestamp,TextOutputFormat.class, Text.class, Text.class);
					}
				}
			}
			
			//MultipleOutputs.addNamedOutput(job,"AURxD126TASSEORxD126TDARxD126"+timestamp,TextOutputFormat.class, Text.class, Text.class);

			
			FileOutputFormat.setOutputPath(job, outPath);
/*			Date current_timeStamp = new Date();
			SimpleDateFormat extractedDateFormat = new SimpleDateFormat(
					"yyyyMMdd");			
			String extractedDate = extractedDateFormat
					.format(current_timeStamp);
	        Iterator<String> it = terrCodes.iterator();*/
	        
	        if ( terrCdDtset.size() == 0 ) {
				System.err.println("Stopping, No valid files found");
				sendEmailToSupport(fromAddress, toAddress, subject, "No valid files found for TMS Extract ");
				System.exit(8);
			}
			
			
			
			/*while(it.hasNext()){
				String terrCd = it.next();
				
				MultipleOutputs.addNamedOutput(job,HAVI_OUTPUT_FILE_NAME_PREFIX+DaaSConstants.SPLCHARTILDE_DELIMITER+terrCd+DaaSConstants.SPLCHARTILDE_DELIMITER+extractedDate,TextOutputFormat.class, Text.class, Text.class);
				
			}*/
			//this prevents the creation of part* files when using MultiOutputformat
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            cachePath = new Path(cacheFile);			
			DistributedCache.addCacheFile(cachePath.toUri(),
					job.getConfiguration());
			jobGroupId = abac.createJobGroup(jobTitle);
			jobId = abac.createJob(jobGroupId, jobSeqNbr, job.getJobName());

			if (!job.waitForCompletion(true)) {
				System.err
						.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}
			
			if(compressOut)
			{
				System.out.println("Changing output format to GZ");
				outputFileFormat="gz";
			}
			renameMCDTMSFile(fileSystem,outPath,abac);
		 }catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			sendEmailToSupport(fromAddress, toAddress, subject, "Error occured in MapReduce processs "+ex.getMessage());
			ex.printStackTrace(System.err);
			System.exit(8);
		} finally {
			if (abac != null) {
				System.out
						.println("Inside Finally block to close abac connections : ");
				abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID,
						JOB_SUCCESSFUL_CD);
				abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_SUCCESSFUL_ID,
						JOB_SUCCESSFUL_CD);
				abac.dispose();

			}
		}

	}
	
	

/*
	public int runJob(String inputRootDir, String outputDir,Configuration conf) throws Exception {

		
		Job job;
		FileSystem hdfsFileSystem = null;
		Path hdfsOutputPath;
	
		
//		PathFilterByFileName stldPathFilter = new PathFilterByFileName("STLD");
	
		int jobId = 0;
		
		
			
		ABaC abac = null;
		try {
//			conf = new Configuration();
			
			abac = new ABaC(daasConfig);
			
			hdfsFileSystem = FileSystem.get(conf);
			
			conf.set("CHECK_FOR_CUSTINFO",checkforCustInfo);
			conf.set(DaaSConstants.JOB_CONFIG_PARM_STORE_FILTER, filterOnStoreId);
//			conf.set("VALID_STORES", vldStores);
			
			
			job = new Job(conf, "Generate TMS TDA Format STLD Header/Detail");

			int jobGroupId = abac.createJobGroup("Generate TMS TDA Extracts");
			int jobSeqNbr = 1;
			
			jobId = abac.createJob(jobGroupId, jobSeqNbr, job.getJobName());
			
//			FileInputFormat.setInputPathFilter(job, STLDPathFilter.class);
			
			job.setJarByClass(GenerateExtendedTdaFormatStld.class);
			job.setMapperClass(NpStldXmlExtendedMapper.class);
			
			job.setNumReduceTasks(0);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;
			String[] inputpathstrs = inputRootDir.split(",");
		
			for(String cachepathstr:inputpathstrs){
				//System.out.println( " cachepathstr  " +cachepathstr);
//				fstatustmp = hdfsFileSystem.listStatus(new Path(cachepathstr),new STLDPathFilter());
				fstatustmp = hdfsFileSystem.globStatus(new Path(cachepathstr+"/STLD*"));
//				fstatustmp = hdfsFileSystem.globStatus(new Path(cachepathstr));
				
				
					fstatus = (FileStatus[])ArrayUtils.addAll(fstatus, fstatustmp);
//					FileInputFormat.addInputPath(job, new Path(cachepathstr+"/STLDRxD12636RxD126*"));
					
				
	
			}
//			FileInputFormat.addInputPath(job, new Path("/daastest/work/np_xml/step1/STLDRxD12636RxD12620140721-r-00036.gz"));
		
			String datepart;
			String terrCdDatepart;
			//HashSet<String> dtset = new HashSet<String>();
			String[] fileNameParts;
			
			HashSet<String> terrCdDtset = new HashSet<String>();
//			FileInputFormat.addInputPath(job,new Path(""));
			
			for(FileStatus fstat:fstatus){
				
			
				
				String fileName = fstat.getPath().getName().toUpperCase();
				//System.out.println(" file name " + fileName);
				
				if(fstat.isDirectory())
					continue;
				
				String fileNamePartsDelimiter = "~";
				
				if(fileName.indexOf("RXD126") > 0){
					fileNamePartsDelimiter = "RXD126";
				}
				fileNameParts = fileName.split(fileNamePartsDelimiter);
				String terrCdfrmFileName = fileNameParts[1];
				//System.out.println(" terrCdfrmFileName " + terrCdfrmFileName);
				
				
//				if(fileName.startsWith("STLDRXD12636RXD126") || fileName.startsWith("STLD~36~") ||
//						fileName.startsWith("STLDRXD126840RXD126") || fileName.startsWith("STLD~840~")){
				if(validTerrCdSet != null && validTerrCdSet.contains(Integer.parseInt(terrCdfrmFileName))){
						
				
					FileInputFormat.addInputPath(job, fstat.getPath());
				
					datepart = fileNameParts[2].substring(0,8);
					terrCdDtset.add(terrCdfrmFileName+DaaSConstants.SPLCHARTILDE_DELIMITER+datepart);
					
//					int lastindx = fileName.lastIndexOf("~");
//					if(lastindx == -1)
//						lastindx = fileName.lastIndexOf("-R-");
//					
//					datepart = fileName.substring(lastindx-8,lastindx);
//					dtset.add(datepart);
				}
			}
//			Iterator<String> it = dtset.iterator();
			if ( terrCdDtset.size() == 0 ) {
				System.err.println("Stopping, No valid files found");
				System.exit(8);
			}

			Iterator<String> it = terrCdDtset.iterator();
			
			String terrCd= "";
			while(it.hasNext()){
//				datepart = it.next();
				terrCdDatepart = it.next();
				terrCd   = terrCdDatepart.split(DaaSConstants.SPLCHARTILDE_DELIMITER)[0];
				datepart = terrCdDatepart.split(DaaSConstants.SPLCHARTILDE_DELIMITER)[1];
				
//				System.out.println(" addding " +datepart);
//				MultipleOutputs.addNamedOutput(job,"HDR"+datepart,TextOutputFormat.class, Text.class, Text.class);
//				MultipleOutputs.addNamedOutput(job,"MCDRxD126TMSRxD126"+datepart,TextOutputFormat.class, Text.class, Text.class);
				
				if(Integer.parseInt(terrCd) == 36) {
					//System.out.println(" addding1 " +"MCDRxD126TMSRxD126"+datepart);
					MultipleOutputs.addNamedOutput(job,"MCDRxD126TMSRxD126"+datepart,TextOutputFormat.class, Text.class, Text.class);
					//MultipleOutputs.addNamedOutput(job,"AURxD126TasseoRxD126TDARxD126"+datepart,TextOutputFormat.class, Text.class, Text.class);
				}else{
					//System.out.println(" addding " +"MCDRxD126TMSRxD126"+terrCdDatepart);
					MultipleOutputs.addNamedOutput(job,"MCDRxD126TMSRxD126"+terrCdDatepart,TextOutputFormat.class, Text.class, Text.class);
					MultipleOutputs.addNamedOutput(job,"PIPERxD126DELIMRxD126"+terrCdDatepart,TextOutputFormat.class, Text.class, Text.class);
				}
			}
			
			hdfsOutputPath = new Path(outputDir );

			FileOutputFormat.setOutputPath(job, hdfsOutputPath);
//			hdfsFileSystem = FileSystem.get(hdfsOutputPath.toUri(), conf);

			if (hdfsFileSystem.exists(hdfsOutputPath)) {
				hdfsFileSystem.delete(hdfsOutputPath, true);
				Logger.getLogger(GenerateExtendedTdaFormatStld.class.getName()).log(
						Level.INFO,
						"Removed existing output path = " + outputDir);
			}
			
	
			//this prevents the creation of part* files when using MultiOutputformat
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			Path cachePath = new Path(cacheFile);
			
			DistributedCache.addCacheFile(cachePath.toUri(),
					job.getConfiguration());
			
			int retCode = 0;
			retCode = job.waitForCompletion(true) ? 0 : 1;
			
		
			
			abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
			abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
			
			
			renameMCDTMSFile(hdfsFileSystem,outputDir);
			
			return retCode;

		} catch (Exception ex) {
			sendEmailToSupport(fromAddress, toAddress, subject, "Error Occured in Map Reduce Process "+ex.getMessage());
			Logger.getLogger(GenerateExtendedTdaFormatStld.class.getName()).log(
					Level.SEVERE, null, ex);
			throw ex;
		}finally{
			if(abac != null)
				abac.dispose();
		}

	
	}*/

	private void renameMCDTMSFile(FileSystem hdfsFileSystem,Path path,ABaC abac){
		try{
			
			
			FileStatus fs[] = hdfsFileSystem.listStatus(path);
            
           for (int fileCounter = 0; fileCounter < fs.length; fileCounter++) {
        	     
                  int fileIndex=fileCounter + 1;
            	  String filename=fs[fileCounter].getPath().getName().split("-")[0].replaceAll(DaaSConstants.SPLCHARTILDE_DELIMITER, fileNameSeperator);
            	  hdfsFileSystem.rename(fs[fileCounter].getPath(), new Path(path+fileSeperator+filename+fileNameSeperator+fileIndex+"."+outputFileFormat));
            	  totalRecordsInOutputFile = totalLinesinOutPutFile(new Path(path+fileSeperator+filename+fileNameSeperator+fileIndex+"."+outputFileFormat), hdfsFileSystem);
            	  abac.insertExecutionTargetFile(jobId, fileIndex, filename+fileNameSeperator+fileIndex, OUTPUT_FILE_DETAIL_NAME, outputFileFormat, totalRecordsInOutputFile);
           }
              
		/*FileStatus[] fs = hdfsFileSystem.listStatus(path);
		
		//Path path = new Path(path);
		
	   // String parent = path.getParent().toString();
		
		for(FileStatus fstat:fs){
			
			String fileName = fstat.getPath().getName();
			
			String newfileName = fileName.replace("RxD126", "_")+".json";
			if(outputFileFormat.equalsIgnoreCase("psv"))
			{
				newfileName = fileName.replace("RxD126", "_")+".psv";
			}
			
			System.out.println(" moving file  "+ path.toString()+Path.SEPARATOR+fileName + " to "+new Path(path+Path.SEPARATOR+newfileName).toString());
			
			if(!hdfsFileSystem.rename(new Path(path.toString()+Path.SEPARATOR+fileName), new Path(path+Path.SEPARATOR+newfileName))){
				System.out.println(" Could not move" + path.toString()+"/"+fileName);
			}
		}*/
		
		
//		hdfsFileSystem.rename(arg0, arg1)
		}catch(Exception ex){
			sendEmailToSupport(fromAddress, toAddress, subject, "Error Occured in Map Reduce Process while renaming files "+ex.getMessage());
			ex.printStackTrace();
		}
	}
	
	
	/**
	 * @return This Method will return last Month in specified year.
	 */
	private String getPreviousMonth()
	{
		String month="";
		try{		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, -1);
		month= format.format(cal.getTime());
		return month;
		}catch(Exception ex)
		{
			sendEmailToSupport(fromAddress, toAddress, subject, "Error Occured in Map Reduce Process while getting previous month "+ex.getMessage());
			ex.printStackTrace();
			
		}
		return month;
	}
	public void sendEmailToSupport(String fromAddress,String toAddress,String subject,String body)
	{
		sendMail.SendEmail(fromAddress, toAddress, subject, body);
	}
	
	/**
	 * This Method gets valid File Paths from the Work layer. -d WORK:840 gets
	 * all the files from WorkLayer whose terrcode is 840.
	 * 
	 * @param daasConfig
	 *            :Configuration object which loads ABaC, teradata and Hadoop
	 *            Paths
	 * @param fileSystem
	 *            Hadoop FileSystem
	 * @param fileType
	 *            FileType for the paths i.e. POS_XML
	 * @param subTypeCodes
	 *            STLD files for Offer Redemption Extract. Other codes are
	 *            ignored.
	 * @return
	 */
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig,
			FileSystem fileSystem, String fileType,
			ArrayList<String> subTypeCodes) {

		ArrayList<Path> retPaths = new ArrayList<Path>();
		String filePath;
		boolean useFilePath;
		boolean removeFilePath;
		String[] fileNameParts;
		String fileTerrCode;

		Path listPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR
				+ daasConfig.hdfsWorkSubDir() + Path.SEPARATOR
				+ daasConfig.fileSubDir() + Path.SEPARATOR + "step1");

		try {
			FileStatus[] fstus = fileSystem.listStatus(listPath);

			for (int idx = 0; idx < fstus.length; idx++) {
				filePath = HDFSUtil.restoreMultiOutSpecialChars(fstus[idx]
						.getPath().getName());

				useFilePath = false;
				for (int idxCode = 0; idxCode < subTypeCodes.size(); idxCode++) {
					if (filePath.startsWith(subTypeCodes.get(idxCode))) {
						useFilePath = true;
					}
				}

				if (useFilePath && workTerrCodeList.size() > 0) {
					fileNameParts = filePath.split("~");
					fileTerrCode = fileNameParts[1];

					removeFilePath = true;

					for (String checkTerrCode : workTerrCodeList) {
						if (fileTerrCode.equals(checkTerrCode)) {
							removeFilePath = false;
						}
					}

					if (removeFilePath) {
						useFilePath = false;
					}
				}

				// if ( filePath.startsWith("STLD") ||
				// filePath.startsWith("DetailedSOS") ||
				// filePath.startsWith("MenuItem") ||
				// filePath.startsWith("SecurityData") ||
				// filePath.startsWith("store-db") ||
				// filePath.startsWith("product-db") ) {
				if (useFilePath) {
					retPaths.add(fstus[idx].getPath());

					if (daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum) {
						System.out.println("Added work source file ="
								+ filePath);
					}
				}
			}

		} catch (Exception ex) {
			System.err
					.println("Error occured in TMSHaviXmlDriver.getVaildFilePaths:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

		if (retPaths.size() == 0) {
			System.err.println("Stopping, No valid files found");
			System.exit(8);
		}

		return (retPaths);
	}

	/**
	 * This Method gets valid File Paths from the Gold layer.
	 * 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01. This will get a
	 * total of 3 days for 840 and 1 day from 250
	 * 
	 * @param daasConfig
	 *            Configuration object which loads ABaC, teradata and Hadoop
	 *            Paths
	 * @param fileSystem
	 *            Hadoop FileSystem
	 * @param fileType
	 *            FileType for the paths i.e. POS_XML
	 * @param requestedTerrDateParms
	 * @param subTypeCodes
	 * @return
	 */
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig,
			FileSystem fileSystem, String fileType,
			String requestedTerrDateParms, ArrayList<String> subTypeCodes) {

		ArrayList<Path> retPaths = new ArrayList<Path>();

		try {

			// Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem,
			// daasConfig, requestedTerrDateParms, "STLD", "DetailedSOS",
			// "MenuItem", "SecurityData","store-db","product-db");
			Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem,
					daasConfig, requestedTerrDateParms, subTypeCodes);

			if (requestPaths == null) {
				System.err
						.println("Stopping, No valid territory/date params provided");
				System.exit(8);
			}

			int validCount = 0;

			for (int idx = 0; idx < requestPaths.length; idx++) {
				if (fileSystem.exists(requestPaths[idx])) {
					retPaths.add(requestPaths[idx]);
					validCount++;

					if (daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum) {
						System.out.println("Found valid path = "
								+ requestPaths[idx].toString());
					}
				} else {
					System.err.println("Invalid path \""
							+ requestPaths[idx].toString() + "\" skipping.");
				}
			}

			if (validCount == 0) {
				System.err.println("Stopping, No valid files found");
				System.exit(8);
			}

			if (daasConfig.displayMsgs()) {
				System.out.print("\nFound " + validCount + " HDFS path");
				if (validCount > 1) {
					System.out.print("s");
				}
				System.out.print(" from " + requestPaths.length + " path");
				if (requestPaths.length > 1) {
					System.out.println("s.");
				} else {
					System.out.println(".");
				}
			}

			if (daasConfig.displayMsgs()) {
				System.out.println("\n");
			}

		} catch (Exception ex) {
			System.err
					.println("Error occured in getVaildFilePaths:"+ex.toString());
			sendEmailToSupport(fromAddress, toAddress, subject, "Error Occured in Map Reduce Process while getting valid file paths "+ex.getMessage());
			ex.printStackTrace(System.err);
			System.exit(8);
		}

		return (retPaths);
	}
	
	/** This method stores the territory codes of gold layer in list.
	 * 
	 * @param daasConfig
	 * @param fileType
	 * @param requestedTerrDateParms
	 * @param subTypeCodes
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private ArrayList<String> goldTerrCodeList(DaaSConfig daasConfig,
			FileSystem fileSystem,
			String requestedTerrDateParms, ArrayList<String> subTypeCodes) throws FileNotFoundException, IOException
	{
		ArrayList<String> terrCodes= new ArrayList<String>();
		ArrayList<String> allTerrCodes = new ArrayList<String>();
		
		Path listPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + subTypeCodes.get(0));
		
		FileStatus[] fstus = fileSystem.listStatus(listPath);
		
		for (int idx=0; idx < fstus.length; idx++ ) {
			allTerrCodes.add(fstus[idx].getPath().getName());
		}
		 String[] listParts;
		 String[] argParts;
		listParts = requestedTerrDateParms.split(",");

	    for ( int idxList=0; idxList < listParts.length; idxList++ ) {

	    	argParts = (listParts[idxList]).split(":");
	      
	    	if ( argParts[0].equals("*") ) {
	    		terrCodes = allTerrCodes;
	    	} else {
	    		terrCodes = new ArrayList<String>();
	    		terrCodes.add(argParts[0]);
	    	}
		
		
	}
	    return terrCodes;	
}
	
	/**
	 * This method gets Number of Lines in merged output file.
	 * 
	 * @param path
	 *            : Path where merged output file is present.
	 * @param fs
	 *            : Hadoop file system
	 * @return countOfLinesInOutputFile : Returns count of lines in output file
	 * @throws IOException
	 *             : Throws Exception when file is not found.
	 * 
	 * */
	private int totalLinesinOutPutFile(Path path, FileSystem fs)
			throws IOException {
		BufferedReader br = null;
		String line;
		int count = 0;
		if (path.getName().contains(".psv") || path.getName().contains(".tsv")) {
			try {
				br = new BufferedReader(new InputStreamReader(fs.open(path)));
				while ((line = br.readLine()) != null) {
					count++;
				}

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			}
		}
		return count;
	}
	
}

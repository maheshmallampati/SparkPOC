package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
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
import com.mcd.gdw.test.*;
import com.mcd.gdw.test.daas.mapreduce.GDWMetricsMapper;
import com.mcd.gdw.test.daas.mapreduce.GDWMetricsReducer;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;




/**
 * @author KhajaAsmath This MapReduce job extracts the Cook and Wait Time for US
 *         Stores. It uses NpAPTSosXmlMapper and NpAPTSosXmlReducer to implement
 *         the Map and Reduce steps, respectively. When you run this class you
 *         must supply it with three parameters: -c config.xml,-t POS_XML and -d
 *         WORK:840 The output will be stored in Pipe seperated file format.
 */

public class GenerateGDWMetrics extends Configured implements Tool {

	private ArrayList<String> workTerrCodeList = new ArrayList<String>();
	private Set<String> goldTerrCdBusnDtList = new HashSet<String>();
	// private String createJobDetails = "FALSE";
	private final static String GDW_EXTRACT_JOBGROUP_NAME = "GDW Metric Extract";
	private final static String GDW_EXTRACT_HISTORY_JOBGROUP_NAME = "GDW Metric Extract";
	private String fileSeperator = "/";
	private String fileNameSeperator = "_";
	private int jobSeqNbr = 1;
	private int jobGroupId = 0;
	int jobId = 0;
	int totalRecordsInOutputFile = -1;
	//Set<String> terrCodes = new HashSet<String>();
	static final String JOB_SUCCESSFUL_CD = "SUCCESSFUL";
	String cacheFile = "";
	private FsPermission newFilePremission = null;
	private int jobStatus = 0;
	public static String timeStamp;
	public static String dateInOutputPath;
	public static final String OUTPUT_FILE_TYPE = "Pipe Separated File";
	public static final String OUTPUT_FILE_DETAIL_NAME = "APT - Cook and Wait Time Detail";
	public static final String APTUS_STLD_TDA_Header = "APTSTLDHDR";
	Path gdwMetricsOutputPath = null;
	String filterOnStoreId = "FALSE";
	public static final String STORE_FILTER_LIST = "STORE_FILTER.txt";
	public static final String APTCOOK_COUNTER = "count";
	public static String UNDERSCORE_DELIMITER   = "_";
	public static String HYPHEN_DELIMITER="-";
	public static String TILDE_DELIMITER   = "~";
	public static final String GDW_METRICS_FILE = "GDWMetrics";
	public static final String GDW_FILE_STORE_COUNTS="GDWStoreCounts";
	HashMap<String, String> recordCTStoreMap = new HashMap<String, String>();
	String configXmlFile = "";
	String configXml="";
	String fileType = "";
	String terrDate = "";
	// String terrDateFile = "";
	String owshFltr = "*";
	String[] args;
	

	public GenerateGDWMetrics() {
		Date currentDate = new Date();
		SimpleDateFormat customDateTimeFormat = DaaSConstants.SDF_yyyyMMddHHmmssSSSnodashes;
		timeStamp = customDateTimeFormat.format(currentDate);

		SimpleDateFormat customDateFormat = DaaSConstants.SDF_yyyyMMdd;
		dateInOutputPath = customDateFormat.format(currentDate);
	}

	public static void main(String[] args) throws Exception {

		ToolRunner.run(new Configuration(), new GenerateGDWMetrics(), args);
	}

	@Override
	public int run(String[] argsall) throws Exception {
		
		//-c config.xml -t POS_XML -d WORK

		GenericOptionsParser gop = new GenericOptionsParser(argsall);

		args = gop.getRemainingArgs();

		for (int idx = 0; idx < args.length; idx++) {
			if (args[idx].equals("-c") && (idx + 1) < args.length) {
				System.out.println("configxml");
				configXmlFile = args[idx + 1];
			}
			
			if (args[idx].equals("-cg") && (idx + 1) < args.length) {
				System.out.println("configxml");
				configXml = args[idx + 1];
			}
			

			if (args[idx].equals("-t") && (idx + 1) < args.length) {
				System.out.println(fileType);
				fileType = args[idx + 1];
			}

			if (args[idx].equals("-d") && (idx + 1) < args.length) {
				System.out.println(terrDate);
				terrDate = args[idx + 1];
			}			
				
			if (args[idx].equalsIgnoreCase("-dc")
					&& (idx + 1) < args.length) {
				cacheFile = args[idx + 1];
			}
			if (args[idx].equals("-owshfltr") && (idx + 1) < args.length) {
				owshFltr = args[idx + 1];
			}
			if (args[idx].equals("-filterOnStoreId") && (idx + 1) < args.length) {
				filterOnStoreId = args[idx + 1];
			}

		}

		if (configXmlFile.length() == 0 || fileType.length() == 0
				|| terrDate.length() == 0) {
			System.err.println("Invalid parameters");
			System.err
					.println("Usage: GenerateAPTFormatSos -c config.xml -t filetype -d territoryDateParms");
			System.exit(8);
		}

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		newFilePremission = new FsPermission(FsAction.ALL, FsAction.ALL,
				FsAction.READ_EXECUTE);

		if (daasConfig.configValid()) {

			runMrGDWWorkLayerMetricsExtract(daasConfig, fileType, getConf(), terrDate,
					owshFltr);
			
			
			ArrayList < String > goldTerrCdBusnDt = new ArrayList < String > ();
			goldTerrCdBusnDt.addAll(goldTerrCdBusnDtList);
			String goldTerrCdBusnDtStrng = StringUtils.join(goldTerrCdBusnDt,",");
			
			System.out.println("Gold List -->"+goldTerrCdBusnDtStrng.toString());
			
			daasConfig = new DaaSConfig(configXml, fileType);
						
			runMrGDWGoldLayerMetricsExtract(daasConfig, fileType, getConf(), goldTerrCdBusnDtStrng.toString(),
					owshFltr);
			

		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.err.println("File Type   = " + fileType);
			System.exit(8);
		}

		return (0);
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
	 * @throws Exception
	 *             :
	 */
	private void runMrGDWWorkLayerMetricsExtract(DaaSConfig daasConfig, String fileType,
			Configuration hdfsConfig, String terrDate, String owshFltr)
			throws Exception {

		Job job;
		ArrayList<Path> requestedPaths = null;

		ArrayList<String> subTypeList = new ArrayList<String>();
		 //subTypeList.add("DetailedSOS");
		 subTypeList.add("STLD");

	
		try {

			hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER,
					owshFltr);
			String jobTitle = "";

			if (!terrDate.toUpperCase().startsWith("WORK")) {
				jobTitle = GDW_EXTRACT_HISTORY_JOBGROUP_NAME;
			} else {
				jobTitle = GDW_EXTRACT_JOBGROUP_NAME;
			}

			System.out.println("\nCreate APT File Format\n");

			FileSystem fileSystem = FileSystem.get(hdfsConfig);

			// Define output path based on input path i.e. Work or Gold Layer.

			gdwMetricsOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR
					+ daasConfig.hdfsWorkSubDir() + Path.SEPARATOR
					+ "GDWMetrics");

			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, gdwMetricsOutputPath,
					daasConfig.displayMsgs());

			/*
			 * M-1939 : Add Input Paths based on the Parameter. Path for Work
			 * Layer: WORK:840 Path for History Extract:
			 * 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01
			 */
			
			if (terrDate.toUpperCase().startsWith("WORK")) {
				
				String[] workParts = (terrDate + ":").split(":");
				String filterTerrCodeList = (workParts.length)>1?workParts[1]:"";

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
				/*System.out
						.println("Total number of Input Paths from Work Layer : "
								+ requestedPaths.size());
				terrCodes = new HashSet<String>(workTerrCodeList);*/
			} else {
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem,
						fileType, terrDate, subTypeList);
				System.out
						.println("Total number of Input Paths from Gold Layer : "
								+ requestedPaths.size());
			/*	ArrayList<String> goldTerrCodes = goldTerrCodeList(daasConfig,
						fileSystem, terrDate, subTypeList);
				terrCodes = new HashSet<String>(goldTerrCodes);*/
			}

			Configuration conf = getConf();
			conf.set(DaaSConstants.JOB_CONFIG_PARM_STORE_FILTER,
					filterOnStoreId);
			
			job = Job.getInstance(conf, jobTitle);
			//job = new Job();
			Path storeFilerDistCache = new Path(daasConfig.hdfsRoot()
					+ Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR
					+ "STORE_FILTER.txt");
			
			job.setJarByClass(GenerateGDWMetrics.class);
			job.setMapperClass(GDWMetricsMapper.class);
			job.setReducerClass(GDWMetricsReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			TextOutputFormat.setOutputPath(job, gdwMetricsOutputPath);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			

			/*HashSet<String> terrCdDtset = new HashSet<String>();

			String datepart;
			String terrCdDatepart;
			String[] fileNameParts;
			FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;

			for (Path cachepathstr : requestedPaths) {
				FileInputFormat.addInputPath(job, cachepathstr);
				fstatustmp = fileSystem.globStatus(new Path(cachepathstr
						+ "/DetailedSOS*"));
				fstatus = (FileStatus[]) ArrayUtils.addAll(fstatus, fstatustmp);

			}

			for (FileStatus fstat : fstatus) {
				String fileName = fstat.getPath().getName().toUpperCase();
				System.out.println(" file name " + fileName);

				if (fstat.isDirectory())
					continue;
				fileNameParts = HDFSUtil.restoreMultiOutSpecialChars(fileName).split(TILDE_DELIMITER);
				String terrCdfrmFileName = fileNameParts[1];
				datepart = fileNameParts[2].substring(0, 8);
				terrCdDtset.add(terrCdfrmFileName
						+ TILDE_DELIMITER + datepart);

			}

			if (terrCdDtset.size() == 0) {
				System.err.println("Stopping, No valid files found");
				System.exit(8);
			}

			Iterator<String> it = terrCdDtset.iterator();

			String terrCd = "";
			while (it.hasNext()) {
				// datepart = it.next();
				terrCdDatepart = it.next();
				terrCd = terrCdDatepart
						.split(TILDE_DELIMITER)[0];
				datepart = terrCdDatepart
						.split(TILDE_DELIMITER)[1];
				
				MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(APTUS_SOS_TDA_Detail
								+ UNDERSCORE_DELIMITER + terrCd
								+ UNDERSCORE_DELIMITER 
								+ datepart), TextOutputFormat.class, Text.class,
						Text.class);

			}		*/
			for (Path outPath : requestedPaths) {
				FileInputFormat.addInputPath(job, outPath);
			}
			
			MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(GDW_METRICS_FILE), TextOutputFormat.class, Text.class,
			Text.class);
			MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(GDW_FILE_STORE_COUNTS), TextOutputFormat.class, Text.class,
					Text.class);
			jobStatus = job.waitForCompletion(true) ? 0 : 1;

			

		} catch (Exception ex) {
			ex.printStackTrace();
			
			System.out.println("Exception occured");

			System.exit(1);
		} finally {
			System.out.println("Inside Finally block to close the rest of the connections");
			
		}

	}

	
	private void runMrGDWGoldLayerMetricsExtract(DaaSConfig daasConfig, String fileType,
			Configuration hdfsConfig, String terrDate, String owshFltr)
			throws Exception {

		Job job;
		ArrayList<Path> requestedPaths = null;

		ArrayList<String> subTypeList = new ArrayList<String>();
		 //subTypeList.add("DetailedSOS");
		 subTypeList.add("STLD");

	
		try {

			hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER,
					owshFltr);
			String jobTitle = "";

			if (!terrDate.toUpperCase().startsWith("WORK")) {
				jobTitle = GDW_EXTRACT_HISTORY_JOBGROUP_NAME;
			} else {
				jobTitle = GDW_EXTRACT_JOBGROUP_NAME;
			}

			System.out.println("\nCreate APT File Format\n");

			FileSystem fileSystem = FileSystem.get(hdfsConfig);

			// Define output path based on input path i.e. Work or Gold Layer.

			gdwMetricsOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR
					+ daasConfig.hdfsWorkSubDir() + Path.SEPARATOR
					+ "GDWMetricsGold");

			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, gdwMetricsOutputPath,
					daasConfig.displayMsgs());

			/*
			 * M-1939 : Add Input Paths based on the Parameter. Path for Work
			 * Layer: WORK:840 Path for History Extract:
			 * 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01
			 */
			
			if (terrDate.toUpperCase().startsWith("WORK")) {
				
				String[] workParts = (terrDate + ":").split(":");
				String filterTerrCodeList = (workParts.length)>1?workParts[1]:"";

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
				/*System.out
						.println("Total number of Input Paths from Work Layer : "
								+ requestedPaths.size());
				terrCodes = new HashSet<String>(workTerrCodeList);*/
			} else {
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem,
						fileType, terrDate, subTypeList);
				System.out
						.println("Total number of Input Paths from Gold Layer : "
								+ requestedPaths.size());
			/*	ArrayList<String> goldTerrCodes = goldTerrCodeList(daasConfig,
						fileSystem, terrDate, subTypeList);
				terrCodes = new HashSet<String>(goldTerrCodes);*/
			}

			Configuration conf = getConf();
			conf.set(DaaSConstants.JOB_CONFIG_PARM_STORE_FILTER,
					filterOnStoreId);
			
			job = Job.getInstance(conf, jobTitle);
			//job = new Job();
			Path storeFilerDistCache = new Path(daasConfig.hdfsRoot()
					+ Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR
					+ "STORE_FILTER.txt");
			
			job.setJarByClass(GenerateGDWMetrics.class);
			job.setMapperClass(GDWMetricsMapper.class);
			job.setReducerClass(GDWMetricsReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			TextOutputFormat.setOutputPath(job, gdwMetricsOutputPath);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			

			/*HashSet<String> terrCdDtset = new HashSet<String>();

			String datepart;
			String terrCdDatepart;
			String[] fileNameParts;
			FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;

			for (Path cachepathstr : requestedPaths) {
				FileInputFormat.addInputPath(job, cachepathstr);
				fstatustmp = fileSystem.globStatus(new Path(cachepathstr
						+ "/DetailedSOS*"));
				fstatus = (FileStatus[]) ArrayUtils.addAll(fstatus, fstatustmp);

			}

			for (FileStatus fstat : fstatus) {
				String fileName = fstat.getPath().getName().toUpperCase();
				System.out.println(" file name " + fileName);

				if (fstat.isDirectory())
					continue;
				fileNameParts = HDFSUtil.restoreMultiOutSpecialChars(fileName).split(TILDE_DELIMITER);
				String terrCdfrmFileName = fileNameParts[1];
				datepart = fileNameParts[2].substring(0, 8);
				terrCdDtset.add(terrCdfrmFileName
						+ TILDE_DELIMITER + datepart);

			}

			if (terrCdDtset.size() == 0) {
				System.err.println("Stopping, No valid files found");
				System.exit(8);
			}

			Iterator<String> it = terrCdDtset.iterator();

			String terrCd = "";
			while (it.hasNext()) {
				// datepart = it.next();
				terrCdDatepart = it.next();
				terrCd = terrCdDatepart
						.split(TILDE_DELIMITER)[0];
				datepart = terrCdDatepart
						.split(TILDE_DELIMITER)[1];
				
				MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(APTUS_SOS_TDA_Detail
								+ UNDERSCORE_DELIMITER + terrCd
								+ UNDERSCORE_DELIMITER 
								+ datepart), TextOutputFormat.class, Text.class,
						Text.class);

			}		*/
			for (Path outPath : requestedPaths) {
				FileInputFormat.addInputPath(job, outPath);
			}
			
			MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(GDW_METRICS_FILE), TextOutputFormat.class, Text.class,
			Text.class);
			MultipleOutputs.addNamedOutput(job,HDFSUtil.replaceMultiOutSpecialChars(GDW_FILE_STORE_COUNTS), TextOutputFormat.class, Text.class,
					Text.class);
			jobStatus = job.waitForCompletion(true) ? 0 : 1;

			

		} catch (Exception ex) {
			ex.printStackTrace();
			
			System.out.println("Exception occured");

			System.exit(1);
		} finally {
			System.out.println("Inside Finally block to close the rest of the connections");
			
		}

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
		String busnDate;

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

				if (useFilePath){
					fileNameParts = filePath.split("~");
					fileTerrCode = fileNameParts[1];
					busnDate=fileNameParts[2];
					busnDate=busnDate.substring(0, 4)+"-"+busnDate.substring(4, 6)+"-"+busnDate.substring(6, 8);
					goldTerrCdBusnDtList.add(fileTerrCode+":"+busnDate);
					if (workTerrCodeList.size() > 0) {
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
				}/////

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
					.println("Error occured in GenerateAPTFormatSos.getVaildFilePaths:");
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
					.println("Error occured in GenerateAPTFormatSos.getVaildFilePaths:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

		return (retPaths);
	}

	
	
	//Adds key values pairs to the provided hashmap and closes the buffered reader
	  private void addKeyValuestoMap(HashMap<String,String> keyvalMap,BufferedReader br){
		  
		  
		  try { 
              
		      String line = "";
		      while ( (line = br.readLine() )!= null) {
		        String[] flds = line.split("\\|");
		        if (flds.length == 3) {
		        	keyvalMap.put(flds[0].trim(), flds[1].trim()+DaaSConstants.PIPE_DELIMITER+flds[2].trim());
		        }   
		      }
		    } catch (IOException e) { 
		      e.printStackTrace();
		      System.out.println("read from distributed cache: read length and instances");
		    }finally{
		    	try{
		    		if(br != null)
		    			br.close();
		    	}catch(Exception ex){
		    		ex.printStackTrace();
		    	}
		    }
	  }


}

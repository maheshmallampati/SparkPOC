package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

/**
 * @author Naveen Kumar B.V
 * 
 * Driver class for TDA. Its uses TDAStldMapper, TDAMenuItemMapper, TDAReducer and 
 * TDAUtil to generate output for order header and order detail.
 */
public class TDADriver extends Configured implements Tool {
	
	public static final BigDecimal DECIMAL_ZERO = new BigDecimal("0.00");
	public static final String DECIMAL_ZERO_STR = "0.00";
	
	public static final String GDW_DATE_SEPARATOR = "-";
	public static final String HAVI_DATE_SEPARATOR = "/";
	public static final String REGEX_PIPE_DELIMITER = "\\|";
	public static final String COLON_SEPARATOR = ":";
	public static final String SEMI_COLON_SEPARATOR = ";";

	public static final String NET_SELLING_PRICE = "Net Selling Price";
	
	public static final String STLD_FILE_TYPE = "STLD";
	public static final String MENU_ITEM_FILE_TYPE = "MenuItem";
	public static final String STLD_TAG_INDEX = "S";
	public static final String MENU_ITEM_TAG_INDEX = "M";
	
	public static final String STORE_DESCRIPTION = "NSN";
	public static final String US_CURRENCY_CODE = "USD";
	public static final String RSTMT_FL = "N";
	public static final String MIX_SRCE_TYP_CD ="P";
	public static final String PROM_KEY = "0";
	public static final String ORIGINAL_MENU_ITEM = "0";
	public static final String HAPPY_MEAL_TOY_CD = "800";
	public static final String PMIX_UNIT_TYPE_SOLD = "1";
	public static final String PMIX_UNIT_TYPE_PROMO	= "7";
	
	public static final String UNKNOWN_EVT_CODE = "4";
	public static final String UNKNOWN_POD_CODE = "10007";
	public static final String UNKNOWN_ORD_KIND_CODE = "6";
	public static final String UNKNOWN_SALE_TYPE_CODE = "20004";
	public static final String UNKNOWN_PMT_CODE = "-1";
	
	public static final String TDAUS_Order_Header = "TDAUSorderHeader";
	public static final String TDAUS_Order_Detail = "TDAUSOrderDetail";
	
	public static final String Invalid_Input_Log = "InvalidInputLog";
	public static final String Output_File_Stats = "OutputFileStats";
	
	private ABaC abac = null;
	private int jobStatus = 0;
	private int jobGroupId = 0;
	private int jobId = 0;
	private int jobSeqNbr = 1;
	private int recordCount;
	private int storeCount;
	private String uniqueFileId;
	
	public static final Logger LOGGER = Logger.getLogger(TDADriver.class.getName());
	public static Handler fileHandler  = null;
	public static SimpleFormatter simpleFormatter = null;
	public static String timeStamp;
	
	private ArrayList<Path> requestedPaths = null;
	private ArrayList<String> subTypeList = new ArrayList<String>();
	private ArrayList<String> workTerrCodeList = new ArrayList<String>();
	
	private Configuration hdfsConfig = null;
	private FileSystem fileSystem = null;
	private FsPermission newFilePremission = null;
	private DaaSConfig daasConfig = null;
	
	private Map<String, String> outFileStatsMap = new HashMap<String,String>();
	
	public TDADriver() {
		Date currentDate = new Date();	
		SimpleDateFormat customDateFormat = DaaSConstants.SDF_yyyyMMddHHmmssSSSnodashes;
		timeStamp = customDateFormat.format(currentDate);
	}
	
	public enum StldItemType {
		ITEM_AT_LEVEL0, CMP_IN_CMB_LKP, CMP_NOT_IN_CMB_LKP
	}
	
	/**
	 * Main Method for TDADriver.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Configuration hdfsConfig = new Configuration();
		fileHandler  = new FileHandler("./TDA.log");
		simpleFormatter = new SimpleFormatter();

		LOGGER.addHandler(fileHandler);
		fileHandler.setFormatter(simpleFormatter);
		fileHandler.setLevel(Level.ALL); 
		LOGGER.setLevel(Level.ALL);		
		
		int status = ToolRunner.run(hdfsConfig, new TDADriver(), args);
		System.exit(status);
	}
	
	/**
	 * Run Method for TDA.
	 * All the input arguments are processed in this method 
	 */
	@Override
	public int run(String[] args) throws Exception {
		
		LOGGER.log(Level.INFO, "In Run : Job Started");
		String inputDir = "";
		String outputDir = "";
		String lookupFilesDir = "";
		String dateRange = "";
		String configXmlFile = "";
		String fileType = "";
		String lookUpFiles = "";
		String[] inputPathsStr = new String[2];
		String jobName = "";
		
		try {
			for (int idx = 0; idx < args.length; idx++) {
				
				if (args[idx].equals("-jobName") && (idx + 1) < args.length) {
					jobName = args[idx + 1];
				}
				if (args[idx].equals("-inputDir") && (idx + 1) < args.length) {
					inputDir = args[idx + 1];
				}
				if (args[idx].equals("-outputDir") && (idx + 1) < args.length) {
					outputDir = args[idx + 1];
				}
				if (args[idx].equals("-lookupFilesDir") && (idx + 1) < args.length) {
					lookupFilesDir = args[idx + 1];
				}
				
				// This path is used to accept input from any custom location(both STLD & MenuItem)
				if (args[idx].equals("-i") && (idx + 1) < args.length) {
					inputPathsStr[0] = args[idx + 1];
				}
				
				// This path is used explicitly for MenuItem input location
				if (args[idx].equals("-m") && (idx + 1) < args.length) {
					inputPathsStr[1] = args[idx + 1];
				}
				
				// Actual parameter used in production to accept a date range/work layer/custom location. (Eg: -d 840:2015-09-20:2015-09-21)
				if (args[idx].equals("-d") && (idx + 1) < args.length) {
					dateRange = args[idx + 1];
				}
				
				if (args[idx].equals("-dc") && (idx + 1) < args.length) {
					lookUpFiles = args[idx + 1];
				}
				
				if (args[idx].equals("-c") && (idx + 1) < args.length) {
					configXmlFile = args[idx + 1];
				}
				if (args[idx].equals("-t") && (idx + 1) < args.length) {
					fileType = args[idx + 1];
				}
			}
			
			if (configXmlFile.length() == 0 || fileType.length() == 0) {	
				LOGGER.log(Level.INFO, "Missing config.xml and/or filetype");
				System.exit(1);
			}
			
			hdfsConfig = getConf();
			fileSystem = FileSystem.get(hdfsConfig);
			daasConfig = new DaaSConfig(configXmlFile, fileType);

			newFilePremission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_EXECUTE);
			Path outputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + outputDir);
			//Path outputPath = new Path("/daastest" + Path.SEPARATOR + "sales" + Path.SEPARATOR + outputDir);
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, outputPath, true);
			LOGGER.log(Level.INFO, "Output Path : " + outputPath.toString());
			abac = new ABaC(daasConfig);
			
			subTypeList.add(STLD_FILE_TYPE);
			subTypeList.add(MENU_ITEM_FILE_TYPE);
			
			if(dateRange.toUpperCase().startsWith("DEV")) {
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem, inputPathsStr, subTypeList);
			} else if(dateRange.toUpperCase().startsWith("WORK")) {
				String[] workParts = (dateRange).split(":");
				String filterTerrCodeList = workParts[1];
				if (filterTerrCodeList.length() > 0) {
					String[] parts = filterTerrCodeList.split(",");
					for (String addTerrCode : parts) {
						workTerrCodeList.add(addTerrCode);
					}
				}
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem, fileType, subTypeList, inputDir);
			} else {	
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem, fileType, dateRange, subTypeList);
			}
			
			Job job = Job.getInstance(hdfsConfig, jobName);
			job.setJarByClass(com.mcd.gdw.daas.driver.TDADriver.class);
			
			String[] fileLocationNames = lookUpFiles.split(",");
			Path cacheFilePath = null;
			String[] fileLocParts = null;
			for (String eachFileLoc : fileLocationNames) {
				LOGGER.log(Level.INFO, "Adding " + eachFileLoc + " to distributed cache");
				cacheFilePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + lookupFilesDir + Path.SEPARATOR + eachFileLoc);
				fileLocParts = eachFileLoc.split("/");
				job.addCacheFile(new URI(cacheFilePath.toUri()+"#"+fileLocParts[1]));
			}
			
			try {
				jobGroupId = abac.createJobGroup(jobName);
			} catch (Exception ex) {				
				LOGGER.log(Level.INFO, "Job Group Id Is Open");					
				LOGGER.log(Level.INFO,ex.toString());
				LOGGER.log(Level.INFO, ex.getMessage(),ex);
				abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_FAILURE_ID, DaaSConstants.JOB_FAILURE_CD);
				System.exit(1);
			}

			try {
				jobId = abac.createJob(jobGroupId, jobSeqNbr, job.getJobName());
			} catch (Exception ex) { 
				LOGGER.log(Level.INFO, "Job Is Open");				
				LOGGER.log(Level.INFO, ex.toString());		
				LOGGER.log(Level.INFO, ex.getMessage(),ex);
				abac.closeJob(jobId, DaaSConstants.JOB_FAILURE_ID, DaaSConstants.JOB_FAILURE_CD);
				abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_FAILURE_ID, DaaSConstants.JOB_FAILURE_CD);
				System.exit(1);
			}
			
			job.setMapperClass(com.mcd.gdw.daas.mapreduce.TDAStldMapper.class);
			job.setMapperClass(com.mcd.gdw.daas.mapreduce.TDAMenuItemMapper.class);
			job.setReducerClass(com.mcd.gdw.daas.mapreduce.TDAReducer.class);
			
			for (Path addPath : requestedPaths) {				
				if(addPath.toString().contains(STLD_FILE_TYPE)) {
					MultipleInputs.addInputPath(job, addPath, org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,com.mcd.gdw.daas.mapreduce.TDAStldMapper.class);
				} else if(addPath.toString().contains(MENU_ITEM_FILE_TYPE)) {
					MultipleInputs.addInputPath(job, addPath, org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, com.mcd.gdw.daas.mapreduce.TDAMenuItemMapper.class);
				}					
			}
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, outputPath);	  
			
	        MultipleOutputs.addNamedOutput(job, TDAUS_Order_Header, TextOutputFormat.class, NullWritable.class, Text.class);
	        MultipleOutputs.addNamedOutput(job, TDAUS_Order_Detail, TextOutputFormat.class, NullWritable.class, Text.class);
	       	       
	        MultipleOutputs.addNamedOutput(job, Invalid_Input_Log, TextOutputFormat.class, NullWritable.class, Text.class);
	        MultipleOutputs.addNamedOutput(job, Output_File_Stats, TextOutputFormat.class, NullWritable.class, Text.class);
	        
	        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	        jobStatus = job.waitForCompletion(true) ? 0 : 1;
	        
	        if (jobStatus == 0) {
	            FileStatus fs[] = fileSystem.listStatus(outputPath);
	            
	            String[] stats = null;
	            String outFileName = "";
	            // To add output file stats (record and store count) to a Map.
	            addOutFileStatsToMap(fs);
	            
	            for (int fileCounter = 0; fileCounter < fs.length; fileCounter++) {
	              
	              if (fs[fileCounter].getPath().getName().contains(TDAUS_Order_Header)) {
	            	  
	            	  stats = outFileStatsMap.get(fs[fileCounter].getPath().getName()).split(REGEX_PIPE_DELIMITER);
		              recordCount = Integer.parseInt(stats[0]);
		              storeCount = Integer.parseInt(stats[1]);
		              uniqueFileId = stats[2];
		              outFileName = "TDA_Ord_Hdr."+timeStamp+fileCounter+".psv";
		              
	            	  fileSystem.rename(fs[fileCounter].getPath(), new Path(outputPath + Path.SEPARATOR + outFileName));
					  abac.insertExecutionTargetFile(jobId, fileCounter + 1, outFileName, "TDA - Order Header", "Pipe Separated File", recordCount, storeCount, uniqueFileId);
					  
	              } else if (fs[fileCounter].getPath().getName().contains(TDAUS_Order_Detail)) {
	            	  stats = outFileStatsMap.get(fs[fileCounter].getPath().getName()).split(REGEX_PIPE_DELIMITER);
		              recordCount = Integer.parseInt(stats[0]);
		              storeCount = Integer.parseInt(stats[1]);
		              uniqueFileId = stats[2];
		              outFileName = "TDA_Ord_Dtl."+timeStamp+fileCounter+".psv";
		              
	            	  fileSystem.rename(fs[fileCounter].getPath(), new Path(outputPath + Path.SEPARATOR + outFileName));
					  abac.insertExecutionTargetFile(jobId, fileCounter + 1, outFileName, "TDA - Order Detail", "Pipe Separated File", recordCount, storeCount, uniqueFileId);
	              }
	            }
	            
	            fileSystem.setPermission(outputPath, newFilePremission);
	            abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
				abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
				LOGGER.log(Level.INFO, "In Run : Job Completed");
	        }
	        
		} catch (Exception ex) { 
			LOGGER.log(Level.INFO, "Error occured in MapReduce process");			
			LOGGER.log(Level.INFO, ex.toString());
			LOGGER.log(Level.INFO, ex.getMessage(),ex);
			abac.closeJobGroup(jobGroupId, DaaSConstants.JOB_FAILURE_ID, DaaSConstants.JOB_FAILURE_CD);
			abac.closeJob(jobId, DaaSConstants.JOB_FAILURE_ID, DaaSConstants.JOB_FAILURE_CD);
			System.exit(1);
		} finally {
			if (abac != null) {
				abac.dispose();
			}
		}
		return jobStatus;
	}
	
	/**
	 * Method to process date range as input from Gold layer.
	 * This method is called when input : -d 840:2015-10-10:2015-10-11
	 * 
	 * @param daasConfig
	 * @param fileSystem
	 * @param fileType
	 * @param requestedTerrDateParms
	 * @param subTypeCodes
	 * @return
	 */
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig,FileSystem fileSystem, String fileType,String requestedTerrDateParms, ArrayList<String> subTypeCodes) {		
			ArrayList<Path> retPaths = new ArrayList<Path>();
			
			try {
				Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem,daasConfig, requestedTerrDateParms, subTypeCodes);
				if (requestPaths == null) {
					LOGGER.log(Level.INFO, "Stopping, No valid territory/date params provided");					
					System.exit(8);
				}
				
				for (int idx = 0; idx < requestPaths.length; idx++) {
					if (fileSystem.exists(requestPaths[idx])) {
						retPaths.add(requestPaths[idx]);
						if (daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum) {
							LOGGER.log(Level.INFO, "Found valid path = "+ requestPaths[idx].toString());
						}
					} else {
							LOGGER.log(Level.INFO, "Invalid path \""+ requestPaths[idx].toString() + "\" skipping.");
					}
				}

				if (retPaths.size() == 0) {
					LOGGER.log(Level.INFO,"Stopping, No valid files found");
					System.exit(8);
				}

				if (daasConfig.displayMsgs()) {
					LOGGER.log(Level.INFO,"\n Output path count : " + retPaths.size());
					LOGGER.log(Level.INFO,"\n Input path count :  " + requestPaths.length);
				}
				
			} catch (Exception ex) {
				LOGGER.log(Level.INFO, "Error occured in TDADriver.getVaildFilePaths:");					
				LOGGER.log(Level.INFO,ex.toString());
				LOGGER.log(Level.INFO, ex.getMessage(),ex);
				System.exit(1);
			}

			return (retPaths);
	}
	
	/**
	 * Method to process work layer as input
	 * This method is called when input : -d WORK:840
	 * 
	 * @param daasConfig
	 * @param fileSystem
	 * @param fileType
	 * @param subTypeCodes
	 * @return
	 */
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig, FileSystem fileSystem, 
										String fileType, ArrayList<String> subTypeCodes, String inputDir) {

		ArrayList<Path> retPaths = new ArrayList<Path>();
		String filePath;
		boolean useFilePath;
		boolean removeFilePath;
		String[] fileNameParts;
		String fileTerrCode;
		
		Path listPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR	+ daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + inputDir);
		LOGGER.log(Level.INFO,"Input Path : " + listPath.toString());
		try {
			FileStatus[] fstus = fileSystem.listStatus(listPath);

			for (int idx = 0; idx < fstus.length; idx++) {
				filePath = HDFSUtil.restoreMultiOutSpecialChars(fstus[idx].getPath().getName());

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
				if (useFilePath) {
					retPaths.add(fstus[idx].getPath());
					if (daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum) {
						//LOGGER.log(Level.INFO,"Added work source file ="+ filePath);
					}
				}
			}

		} catch (Exception ex) {
			LOGGER.log(Level.INFO, "Error occured in TDADriver.getVaildFilePaths:");			
			LOGGER.log(Level.INFO, ex.toString());
			LOGGER.log(Level.INFO, ex.getMessage(),ex);
			System.exit(1);
		}

		if (retPaths.size() == 0) {
			LOGGER.log(Level.INFO, "Stopping, No valid files found");			
			System.exit(1);
		}

		return (retPaths);
	}
	
	/**
	 * Method to process input from any custom location. All the files under the locations passed with -i (STLD) and -m (MenuItem)
	 * are accepted as input.
	 * This method is called when input : -d DEV:840
	 * Please note that its mandatory to pass the input locations with -i and -m to process input from any custom location.
	 * 
	 * @param daasConfig
	 * @param fileSystem
	 * @param inputPaths
	 * @param subTypeCodes
	 * @return
	 */
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig, FileSystem fileSystem, 
												String[] inputPaths, ArrayList<String> subTypeCodes) {

		ArrayList<Path> retPaths = new ArrayList<Path>();
		String filePath;
		boolean useFilePath;
		boolean removeFilePath;
		String[] fileNameParts;
		String fileTerrCode;

		Path listPath = null;
		try {
			for (int i = 0; i < inputPaths.length; i++) {
				listPath = new Path(inputPaths[i]);		
				FileStatus[] fstus = fileSystem.listStatus(listPath);

				for (int idx = 0; idx < fstus.length; idx++) {
					filePath = HDFSUtil.restoreMultiOutSpecialChars(fstus[idx].getPath().getName());

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

					if (useFilePath) {
						retPaths.add(fstus[idx].getPath());

						if (daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum) {
							// LOGGER.log(Level.INFO, "Added work source file =" + filePath);
						}
					}
				}
			}
		} catch (Exception ex) {
			LOGGER.log(Level.INFO, "Error occured in getVaildFilePaths:");			
			LOGGER.log(Level.INFO, ex.toString());
			LOGGER.log(Level.INFO, ex.getMessage(),ex);
			System.exit(1);
		}

		if (retPaths.size() == 0) {
			LOGGER.log(Level.INFO, "Stopping, No valid files found");			
			System.exit(1);
		}
		return (retPaths);
	}

	/**
	 * To add record count, store count and unique fileId to a map.
	 * @param fs
	 * @throws IOException
	 */
	private void addOutFileStatsToMap(FileStatus[] fs) throws IOException {
		BufferedReader br = null;
		String line = "";
		String[] stats = null;
		for (int fileCounter = 0; fileCounter < fs.length; fileCounter++) {
			if (fs[fileCounter].getPath().getName().contains(Output_File_Stats)) {
				try {
					br = new BufferedReader(new InputStreamReader(fileSystem.open(fs[fileCounter].getPath())));
					while ((line = br.readLine()) != null) {
						stats = line.split("\\|");
						if (stats.length == 4) {
							outFileStatsMap.put(stats[0].trim(), stats[1].trim() + DaaSConstants.PIPE_DELIMITER + stats[2].trim() + DaaSConstants.PIPE_DELIMITER + stats[3].trim());
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					br.close();
				}
			}
		}
	}
}

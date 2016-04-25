package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
 * APTDriver Class
 */
public class APTDriver extends Configured implements Tool {

	//AWS START 
	public static final String CACHE_FILE_EMIX_CMBCMP = "EMIX_CMBCMP.psv";
	public static final String CACHE_FILE_EMIX_CMIM   = "EMIX_CMIM.psv";
	//AWS END 
	
	private static final String JOB_NAME = "Generate APT Data Feed - PROD";
	private static final String APT_OUTPUT_DIR = "apt_extract";
	
	public static final BigDecimal DECIMAL_ZERO = new BigDecimal("0.00");
	
	public static final String EMPTY_STR = "";
	public static final String GDW_DATE_SEPARATOR = "-";
	public static final String HAVI_DATE_SEPARATOR = "/";
	public static final String TIME_SEPARATOR = ":";
	public static final String ZERO_MINUTE_OR_SECOND = "00";
	public static final String THIRTY_MINUTE_STR = "30";
	public static final String TENTH_HOUR_STR = "10";
	public static final String HAVI_OVR_STR = "OVR";
	public static final String HAVI_BKOVR_STR = "BKOVR";
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
	
	public static final String DRIVE_THRU="Drive Thru";
	public static final String FRONT_COUNTER="Counter";
	public static final String PRODUCT="Product";
	public static final String NON_PRODUCT="Non-Product";
	public static final String TOTAL="Total";
	public static final String COUPONS_REDEEMED = "Coupons Redeemed";
	public static final String GIFT_CERT_REDEEMED = "Gift Certificates Redeemed";
	public static final String GIFT_CERT_SOLD = "Gift Certificates Sold";
	
	public static final String APTUS_TDA_Header = "APTUSTDAHeader";
	public static final String APTUS_TDA_Detail = "APTUSTDADetail";
	
	private int jobStatus = 0;
	
	private ABaC abac = null;
	private int totalRecordsInOutputFile = -1;
	private int jobGroupId;
	private int jobId;
	private int jobSeqNbr = 1;
	
	public static final Logger LOGGER = Logger.getLogger(APTDriver.class.getName());
	public static Handler fileHandler  = null;
	public static SimpleFormatter simpleFormatter = null;
	public static String timeStamp;
	public static String dateInOutputPath;
	
	private ArrayList<Path> requestedPaths = null;
	private ArrayList<String> subTypeList = new ArrayList<String>();
	private ArrayList<String> workTerrCodeList = new ArrayList<String>();
	
	private Configuration hdfsConfig = null;
	private FileSystem fileSystem = null;
	private FsPermission newFilePremission = null;
	private DaaSConfig daasConfig = null;
	
	public APTDriver() {
		Date currentDate = new Date();	
		SimpleDateFormat customDateTimeFormat = DaaSConstants.SDF_yyyyMMddHHmmssSSSnodashes; //TODO: Report that variable name is misleading, need to remove milliseconds part.
		timeStamp = customDateTimeFormat.format(currentDate);
		
		SimpleDateFormat customDateFormat = DaaSConstants.SDF_yyyyMMdd;
		dateInOutputPath = customDateFormat.format(currentDate);
	}
	
	public enum StldItemType {
		ITEM_AT_LEVEL0, CMP_IN_CMB_LKP, CMP_NOT_IN_CMB_LKP
	}
	
	/**
	 * Main Method
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration hdfsConfig = new Configuration();

		fileHandler  = new FileHandler("./apt_prod"+".log");
		simpleFormatter = new SimpleFormatter();

		LOGGER.addHandler(fileHandler);
		fileHandler.setFormatter(simpleFormatter);
		fileHandler.setLevel(Level.ALL); 
		LOGGER.setLevel(Level.ALL);		
		
		int status = ToolRunner.run(hdfsConfig, new APTDriver(), args);
		System.exit(status);
	}
	
	@Override
	public int run(String[] args) throws Exception {

		LOGGER.log(Level.INFO, "In Run : Job Started");
		
		String dateRange = "";
		String configXmlFile = "";
		String fileType = "";
		String[] inputPathsStr = new String[2];
		
		try {
			for (int idx = 0; idx < args.length; idx++) {
				if (args[idx].equals("-i") && (idx + 1) < args.length) {
					inputPathsStr[0] = args[idx + 1];
				}
				
				if (args[idx].equals("-m") && (idx + 1) < args.length) {
					inputPathsStr[1] = args[idx + 1];
				}
				
				if (args[idx].equals("-d") && (idx + 1) < args.length) {
					dateRange = args[idx + 1];
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
			//AWS START
			//fileSystem = FileSystem.get(hdfsConfig);
			//AWS END
			daasConfig = new DaaSConfig(configXmlFile, fileType);
			fileSystem = HDFSUtil.getFileSystem(daasConfig, hdfsConfig);
			
			newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);
			Path outputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + APT_OUTPUT_DIR);
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, outputPath, true);
			LOGGER.log(Level.INFO, "Output Path : " + outputPath.toString());
			
			abac = new ABaC(daasConfig);
			
			subTypeList.add(STLD_FILE_TYPE);
			subTypeList.add(MENU_ITEM_FILE_TYPE);
			
			if(args[0].equals("-i")) {
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
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem, fileType, subTypeList);
			} else {	
				requestedPaths = getVaildFilePaths(daasConfig, fileSystem, fileType, dateRange, subTypeList);
			}
			
			Job job = Job.getInstance(hdfsConfig, JOB_NAME);
			job.setJarByClass(com.mcd.gdw.daas.driver.APTDriver.class);
			
			//AWS START
			//job.addCacheFile(new Path("EMIX_CMBCMP.psv").toUri()); // To add ComboComponents data to distributed cache.
			//job.addCacheFile(new Path("EMIX_CMIM.psv").toUri()); // To add Primary-Secondary mapping data to distributed cache.
			Path cacheFile;
			cacheFile = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + CACHE_FILE_EMIX_CMBCMP);
			job.addCacheFile(new URI(cacheFile.toString() + "#" + cacheFile.getName())); // To add ComboComponents data to distributed cache.
			cacheFile = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + CACHE_FILE_EMIX_CMIM);
			job.addCacheFile(new URI(cacheFile.toString() + "#" + cacheFile.getName())); // To add Primary-Secondary mapping data to distributed cache.
			//AWS END 
			
			try {
				jobGroupId = abac.createJobGroup(JOB_NAME);
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
			
			job.setNumReduceTasks(10);
			
			job.setMapperClass(com.mcd.gdw.daas.mapreduce.APTStldMapper.class);
			job.setMapperClass(com.mcd.gdw.daas.mapreduce.APTMenuItemMapper.class);
			job.setReducerClass(com.mcd.gdw.daas.mapreduce.APTReducer.class);
			
			for (Path addPath : requestedPaths) {				
				if(addPath.toString().contains(STLD_FILE_TYPE)) {
					MultipleInputs.addInputPath(job, addPath, org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,com.mcd.gdw.daas.mapreduce.APTStldMapper.class);
				} else if(addPath.toString().contains(MENU_ITEM_FILE_TYPE)) {
					MultipleInputs.addInputPath(job, addPath, org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, com.mcd.gdw.daas.mapreduce.APTMenuItemMapper.class);
				}					
			}
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, outputPath);	  
	        
			MultipleOutputs.addNamedOutput(job, APTUS_TDA_Header, TextOutputFormat.class, NullWritable.class, Text.class);
	        MultipleOutputs.addNamedOutput(job, APTUS_TDA_Detail, TextOutputFormat.class, NullWritable.class, Text.class);
	        
	        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	        jobStatus = job.waitForCompletion(true) ? 0 : 1;
	        
	        if (jobStatus == 0) {
	            FileStatus fs[] = fileSystem.listStatus(outputPath);
	           for (int fileCounter = 0; fileCounter < fs.length; fileCounter++) {
	              if (fs[fileCounter].getPath().getName().contains(APTUS_TDA_Header)) {
	            	  fileSystem.rename(fs[fileCounter].getPath(), new Path(outputPath+"/APTUS_TDA_Header."+timeStamp+fileCounter+".psv"));
	            	  totalRecordsInOutputFile = totalLinesinOutPutFile(new Path(outputPath+"/APTUS_TDA_Header."+timeStamp+fileCounter+".psv"), fileSystem);
					  abac.insertExecutionTargetFile(jobId, fileCounter + 1, "APTUS_TDA_Header."+timeStamp+fileCounter, "APT - Order Header", "Pipe Separated File", totalRecordsInOutputFile);
	              } else if (fs[fileCounter].getPath().getName().contains(APTUS_TDA_Detail)) {
	            	  fileSystem.rename(fs[fileCounter].getPath(), new Path(outputPath+"/APTUS_TDA_Detail."+timeStamp+fileCounter+".psv"));
	            	  totalRecordsInOutputFile = totalLinesinOutPutFile(new Path(outputPath+"/APTUS_TDA_Detail."+timeStamp+fileCounter+".psv"), fileSystem); 
					  abac.insertExecutionTargetFile(jobId, fileCounter + 1, "APTUS_TDA_Detail."+timeStamp+fileCounter, "APT - Order Detail", "Pipe Separated File", totalRecordsInOutputFile);
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
	 * Method to process a date range as input
	 * @param daasConfig
	 * @param fileSystem
	 * @param inputPaths
	 * @param subTypeCodes
	 * @return
	 */
	// TODO: Check if this method can be moved to ABaC and used by all projects.
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
					LOGGER.log(Level.INFO,"Output path count : " + retPaths.size());
					LOGGER.log(Level.INFO,"Input path count :  " + requestPaths.length);
				}
				
			} catch (Exception ex) {
				LOGGER.log(Level.INFO, "Error occured in APTDriver.getVaildFilePaths:");					
				LOGGER.log(Level.INFO,ex.toString());
				LOGGER.log(Level.INFO, ex.getMessage(),ex);
				System.exit(1);
			}

			return (retPaths);
	}
	
	/**
	 * Method to process work layer as input
	 * @param daasConfig
	 * @param fileSystem
	 * @param fileType
	 * @param subTypeCodes
	 * @return
	 */
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig,FileSystem fileSystem, String fileType, ArrayList<String> subTypeCodes) {

		ArrayList<Path> retPaths = new ArrayList<Path>();
		String filePath;
		boolean useFilePath;
		boolean removeFilePath;
		String[] fileNameParts;
		String fileTerrCode;

		Path listPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR	+ daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + "step1");
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
			LOGGER.log(Level.INFO, "Error occured in APTDriver.getVaildFilePaths:");			
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
	 * Only for developer testing, to run for a single .gz file
	 * @param daasConfig
	 * @param fileSystem
	 * @param inputPaths
	 * @param subTypeCodes
	 * @return
	 */
	// TODO: Check if this method can be moved to ABaC and used by all projects.
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
	 * Method to calculate number of lines in an output file
	 * @param path
	 * @param fs
	 * @return
	 * @throws IOException
	 */
	private int totalLinesinOutPutFile(Path path, FileSystem fs) throws IOException {
		BufferedReader br = null;
		String line = "";
		int count = 0;
		if (path.getName().contains(".psv")) {
			try {
				br = new BufferedReader(new InputStreamReader(fs.open(path)));
				while ((line = br.readLine()) != null) {
					count++;
				}
			} catch (FileNotFoundException e) {
				LOGGER.log(Level.INFO, "Error in reading the number of records");
				LOGGER.log(Level.INFO, e.getMessage(), e);
				e.printStackTrace();
			} finally {
				br.close();
			}
		}
		return count;
	}
}
package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.RDBMS;
import com.mcd.gdw.test.daas.mapreduce.AsterFormatMapper2;

public class GenerateAsterFormat2  extends Configured implements Tool {

	public static final String CACHE_ASTER_INCLUDE_LIST = "new_aster_include_list.txt";
	
	private String selectSubTypes = "";
	private ArrayList<String> workTerrCodeList = new ArrayList<String>();
	private FsPermission newFilePremission;
	
	private Path cachePath; 
	private FileSystem fileSystem;

	public static void main(String[] args) throws Exception {

		int retval = ToolRunner.run(new Configuration(),new GenerateAsterFormat2(), args);
		
		System.out.println(" return value : " + retval);
	}

	@Override
	public int run(String[] argsall) throws Exception {

		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
		String terrDateFile = "";
		String owshFltr = "*";
		String[] args;

		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		
		args = gop.getRemainingArgs();
		
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-t") && (idx+1) < args.length ) {
				fileType = args[idx+1];
			}

			if ( args[idx].equals("-d") && (idx+1) < args.length ) {
				terrDate = args[idx+1];
			}

			if ( args[idx].equals("-df") && (idx+1) < args.length ) {
				terrDateFile = args[idx+1];
			}

			if ( args[idx].equals("-owshfltr") && (idx+1) < args.length ) {
				owshFltr = args[idx+1];
			}

			if ( args[idx].equals("-selectsubtypes") && (idx+1) < args.length ) {
				selectSubTypes = args[idx+1];
			}
		}
		
		if ( configXmlFile.length() == 0 || fileType.length() == 0 || (terrDate.length() == 0 && terrDateFile.length() == 0) || (terrDate.length() > 0 && terrDateFile.length() > 0) )  {
			System.err.println("Invalid parameters");
			System.err.println("Usage: GenerateAsterFormat -c config.xml -t filetype -d territoryDateParms|-df territoryLocationDateFile -owshfltr ownershipFilter");
			System.exit(8);
		}
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		if ( daasConfig.configValid() ) {

			if ( terrDateFile.length() > 0 ) {
				terrDate = getTerrDateFromFile(terrDateFile);
			}
			
			newFilePremission = new FsPermission(FsAction.READ_WRITE,FsAction.READ_WRITE,FsAction.READ_WRITE);
			cachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + CACHE_ASTER_INCLUDE_LIST);
			
			fileSystem = FileSystem.get(getConf());
			
			createAsterIncludeList(daasConfig);
			runMrAsterExtract(daasConfig,fileType,getConf(),terrDate,terrDateFile,owshFltr,true);
			
		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.err.println("File Type   = " + fileType);
			System.exit(8);
		}
		
		return(0);
	}

	private void runMrAsterExtract(DaaSConfig daasConfig
                                  ,String fileType
                                  ,Configuration hdfsConfig
                                  ,String terrDate
                                  ,String terrDateFile
                                  ,String owshFltr
                                  ,boolean compressOut) {

		Job job;
		ArrayList<Path> requestedPaths = null;
		SimpleDateFormat sdf;
		Calendar dt;
		String subDir = "";
		ArrayList<String> subTypeList;
		ABaC abac = null;
		

		try {
			if ( selectSubTypes.length() > 0 ) {
				subTypeList = new ArrayList<String>();
				System.out.println("Select Sub Types:");
				String[] parts = selectSubTypes.split(",");
				for ( String addSubType : parts ) {
					System.out.println("   " + addSubType);
					subTypeList.add(addSubType);
				}
			} else {
				abac = new ABaC(daasConfig);
				subTypeList = abac.getSubFileTypeCodes();
				abac.dispose();
			}
			
			hdfsConfig.set(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER, owshFltr);
			
			hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm()); 

			if ( compressOut ) {
				hdfsConfig.set("mapreduce.map.output.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
				hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			}
			
			String jobTitle = "";
			
			if ( terrDateFile.length() > 0 ) {
				jobTitle = "Processing MapReduce Aster History Extract";
			} else {
				jobTitle = "Processing MapReduce Aster Extract";
			}
			job = Job.getInstance(hdfsConfig, jobTitle);
			
			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nCreate Aster Loader File Format\n");
			}
			
			sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			dt = Calendar.getInstance();
			subDir = sdf.format(dt.getTime());
			
			Path outPath=null;

			if ( terrDateFile.length() > 0 ) {
				outPath= new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "new_asterextract_hist_extract");
			} else {
				outPath= new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "new_asterextract" + Path.SEPARATOR + subDir);
			}
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, outPath,daasConfig.displayMsgs());

			if ( terrDate.toUpperCase().startsWith("WORK") ) {
				String[] workParts = (terrDate + ":").split(":");
				
				String filterTerrCodeList = workParts[1];

				if ( filterTerrCodeList.length() > 0 ) {
					System.out.println("Work Layer using only the following Territory Codes:");
					String[] parts = filterTerrCodeList.split(",");
					for ( String addTerrCode : parts ) {
						System.out.println("    " + addTerrCode);
						workTerrCodeList.add(addTerrCode);
					}
				}
				
				requestedPaths = getVaildFilePaths(daasConfig,fileSystem,fileType,subTypeList);
			} else {
				requestedPaths = getVaildFilePaths(daasConfig,fileSystem,fileType,terrDate,subTypeList);
			}

			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			if ( terrDateFile.length() > 0 ) {
				//Path cacheFile = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + "aster_load_list.txt");
				
				//if ( fileSystem.exists(cacheFile)) {
				//	fileSystem.delete(cacheFile, false);
				//}
				
				//fileSystem.copyFromLocalFile(new Path(terrDateFile), cacheFile);
				
				//DistributedCache.addCacheFile(cacheFile.toUri(),job.getConfiguration());
			} else {
				//DistributedCache.addCacheFile(new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + "aster_include_list.txt").toUri(),job.getConfiguration());
				job.addCacheFile(new URI(cachePath.toString() + "#" + cachePath.getName()));
			}

			job.setJarByClass(GenerateAsterFormat2.class);
			job.setMapperClass(AsterFormatMapper2.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			TextOutputFormat.setOutputPath(job, outPath);

			if ( !job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig
                                             ,FileSystem fileSystem
                                             ,String fileType
                                             ,ArrayList<String> subTypeCodes) {

		ArrayList<Path> retPaths = new ArrayList<Path>();
		String filePath;
		boolean useFilePath;
		boolean removeFilePath;
		String[] fileNameParts;
		String fileTerrCode;
		
		Path listPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + "step1");
		
		try {
			FileStatus[] fstus = fileSystem.listStatus(listPath);
			
			for (int idx=0; idx < fstus.length; idx++ ) {
				filePath = HDFSUtil.restoreMultiOutSpecialChars(fstus[idx].getPath().getName());
				
				useFilePath = false;
				for (int idxCode=0; idxCode < subTypeCodes.size(); idxCode++) {
					if ( filePath.startsWith(subTypeCodes.get(idxCode)) ) {
						useFilePath = true;
					}
				}
				
				if ( useFilePath && workTerrCodeList.size() > 0 ) {
					fileNameParts = filePath.split("~");
					fileTerrCode = fileNameParts[1];
					
					removeFilePath = true;
					
					for ( String checkTerrCode : workTerrCodeList ) {
						if ( fileTerrCode.equals(checkTerrCode) ) {
							removeFilePath = false;
						}
					}
					
					if ( removeFilePath ) {
						useFilePath = false;
					}
				}
				
				if ( useFilePath ) {
					retPaths.add(fstus[idx].getPath());
					
					if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
						System.out.println("Added work source file =" + filePath);
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateAsterFormat.getVaildFilePaths:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

		if ( retPaths.size() == 0 ) {
			System.err.println("Stopping, No valid files found");
			System.exit(8);
		}

		return(retPaths);
	}
	
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig
			                                 ,FileSystem fileSystem
                                             ,String fileType
                                             ,String requestedTerrDateParms
                                             ,ArrayList<String> subTypeCodes) {

		ArrayList<Path> retPaths = new ArrayList<Path>();

		try {

			Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem, daasConfig, requestedTerrDateParms, subTypeCodes);

			if ( requestPaths == null ) {
				System.err.println("Stopping, No valid territory/date params provided");
				System.exit(8);
			}

			int validCount = 0;

			for ( int idx=0; idx < requestPaths.length; idx++ ) {
				if ( fileSystem.exists(requestPaths[idx]) ) {
					retPaths.add(requestPaths[idx]);
					validCount++;

					if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
						System.out.println("Found valid path = " + requestPaths[idx].toString());
					}
				} else {
					System.err.println("Invalid path \"" + requestPaths[idx].toString() + "\" skipping.");
				}
			}

			if ( validCount == 0 ) {
				System.err.println("Stopping, No valid files found");
				System.exit(8);
			}

			if ( daasConfig.displayMsgs() ) {
				System.out.print("\nFound " + validCount + " HDFS path");
				if ( validCount > 1 ) {
					System.out.print("s");
				}
				System.out.print(" from " + requestPaths.length + " path");
				if ( requestPaths.length > 1 ) {
					System.out.println("s.");
				} else {
					System.out.println(".");
				}
			}

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\n");
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateAsterFormat.getVaildFilePaths:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

		return(retPaths);
	}
	
	private String getTerrDateFromFile(String terrDateFile) {
		
		HashMap<String,Integer> terrDateMap = new HashMap<String,Integer>();
		String returnValue = "";
		String line = "";
		String[] parts;
		String mapKey;
		
		String[] keyValueParts = {"",""};

		try {
			BufferedReader br = new BufferedReader(new FileReader(terrDateFile));
			 
			while ((line = br.readLine()) != null) {
				parts = line.split("\\t");

				mapKey = parts[0] + "|" + parts[2];
				
				if  ( terrDateMap.containsKey(mapKey) ) {
					terrDateMap.put(mapKey, (int)terrDateMap.get(mapKey)+1);
				} else {
					terrDateMap.put(mapKey, 1);
				}
			}
			
			br.close();
			
			for ( Map.Entry<String, Integer> entry : terrDateMap.entrySet()) {
				keyValueParts = (entry.getKey()).split("\\|");
				if ( returnValue.length() > 0 ) {
					returnValue += ",";
				}
				
				returnValue += keyValueParts[0] + ":" + keyValueParts[1]; 
			}

		} catch(Exception ex) {
			System.err.println("Error occured in GenerateAsterFormat.getTerrDateFromFile:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
		return(returnValue);
	}

	private void createAsterIncludeList(DaaSConfig daasConfig) throws Exception {
		
		ResultSet rset;
		StringBuffer sql = new StringBuffer();

		RDBMS sqlServer = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());

		sql.setLength(0);
		sql.append("select\n");
		sql.append("   a.CTRY_ISO_NU\n");
		sql.append("  ,a.LGCY_LCL_RFR_DEF_CD\n");
		sql.append("  ,a.INCL_STRT_DT\n");
		sql.append("  ,a.INCL_END_DT\n");
		sql.append("from " + daasConfig.abacSqlServerDb() + ".aster_include" + " a with (NOLOCK)\n");
	
		BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(cachePath,true)));
	
		rset = sqlServer.resultSet(sql.toString());
	
		while ( rset.next() ) {
			bw.write(rset.getString("CTRY_ISO_NU") + "\t" + rset.getString("LGCY_LCL_RFR_DEF_CD") + "\t" + rset.getString("INCL_STRT_DT") + "\t" + rset.getString("INCL_END_DT") + "\n");
		}
	
		rset.close();
		bw.close();
		
		fileSystem.setPermission(cachePath,newFilePremission);
	}
}

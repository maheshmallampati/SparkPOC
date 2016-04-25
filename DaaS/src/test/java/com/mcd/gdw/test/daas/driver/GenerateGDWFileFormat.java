package com.mcd.gdw.test.daas.driver;

import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.GDWFileFormatMapper;
import com.mcd.gdw.test.daas.mapreduce.GDWFileFormatPreMapper;
import com.mcd.gdw.test.daas.mapreduce.GDWFileFormatPreReducer;

public class GenerateGDWFileFormat extends Configured implements Tool {

	public final static String SEPARATOR_CHARACTER                = "\t";
	public final static String COMPONENT_ITEM_SEPARATOR_CHARACTER = "|";
	public final static String COMPONENT_SEPARATOR_CHARACTER      = "~";
	
	public final static String CACHE_POD_TYPES             = "pod_types.txt";
	public final static String CACHE_GIFT_CARD_ITEMS       = "gift_card_items.txt";
	public final static String CACHE_GIFT_CARD_TENDERS     = "gift_card_tenders.txt";
	public final static String CACHE_CTRY                  = "CTRY.txt";
	public final static String CACHE_VALUE_MEAL_COMPONENTS = "valuemealcomponents.txt";
	
	private final static String JOB_GRP_DESC = "Generate GDW File Format";
	
	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
	private FsPermission newFilePermission;

	private Job job;
	private Path distCache;
	private FileStatus[] fstats;
	private Path itmPath;
	private Path valueMealComponentsPath;

	private ArrayList<Path> requestedPaths;

	public static void main(String[] args) throws Exception {
		
		Configuration hdfsConfig = new Configuration();

		int retval = ToolRunner.run(hdfsConfig,new GenerateGDWFileFormat(), args);
		
		System.out.println(" return value : " + retval);
		
	}

	public int run(String[] args) throws Exception {

		ABaC abac;
		int jobGrpId;

		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
		boolean helpRequest = false;

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
			
			if ( args[idx].toUpperCase().equals("-H") || args[idx].toUpperCase().equals("-HELP")  ) {
				helpRequest = true;
			}
		}

		if ( helpRequest ) {
			System.out.println("Usage: GenerateGDWFileFormat -c config.xml -t filetype -d territoryDateParms ");
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
			System.err.println("Usage: GenerateGDWFileFormat -c config.xml -t filetype -d territoryDateParms");
			System.err.println("where territoryDateParm is a comma separated list of territory codes and dates separated by colons(:)");
			System.err.println("for example, 840:2012-07-01:2012-07-07 is territory 840 from July 1st, 2012 until July 7th, 2012.");
			System.err.println("the date format is either ISO YYYY-MM-DD or YYYYMMDD (both are valid)");
			System.err.println("If only one date is supplied then a single day is used for that territory");
			System.err.println("Multiple territoryDateParm can be specified as comma separated values: 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01");
			System.err.println("This will get a total of 3 days for 840 and 1 day from 250");
			System.exit(8);
		}

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		
		if ( daasConfig.configValid() ) {
			
			if ( daasConfig.displayMsgs()  ) {
				System.out.println(daasConfig.toString());
			}

			hdfsConfig = getConf();
			fileSystem = FileSystem.get(hdfsConfig);

			newFilePermission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

			abac = new ABaC(daasConfig);

			jobGrpId = abac.createJobGroup(JOB_GRP_DESC);

			runPreJob(daasConfig,fileType,terrDate);
			runJob(daasConfig,fileType,terrDate,true);

			abac.closeJobGroup(jobGrpId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
			abac.dispose();
			
		} else {
			System.err.println("Invalid Config XML file, stopping");
			System.err.println(daasConfig.errText());
			System.exit(8);
		}
	
		return(0);
		
	}
	
	private void runPreJob(DaaSConfig daasConfig
                          ,String fileType
                          ,String terrDate) {

		try {
			baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "GDWFileFormat" + Path.SEPARATOR + "Step1");
			
			valueMealComponentsPath = new Path(baseOutputPath.toString() + Path.SEPARATOR + CACHE_VALUE_MEAL_COMPONENTS);
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");
			}
		
			hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm()); 

			if ( terrDate.toUpperCase().startsWith("WORK") ) {
				requestedPaths = getVaildFilePaths(daasConfig,fileSystem,fileType,"Product-Db");
			} else {
				requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDate,"Product-Db");
			}

			job = Job.getInstance(hdfsConfig, JOB_GRP_DESC + " Part 1");
		
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			job.setJarByClass(GenerateGDWFileFormat.class);
			job.setMapperClass(GDWFileFormatPreMapper.class);
			job.setReducerClass(GDWFileFormatPreReducer.class);
			job.setNumReduceTasks(1);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputKeyClass(Text.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}
			
			boolean setName = false;
			
			fstats = fileSystem.listStatus(baseOutputPath);
			if(fstats != null && fstats.length > 0 && !setName) {
				for( int idx=0; idx < fstats.length; idx++ ) {
					itmPath = fstats[idx].getPath();
					
					if ( itmPath.getName().startsWith("part") ) {
						fileSystem.setPermission(itmPath, newFilePermission);
						
						if ( !setName ) {
							fileSystem.rename(itmPath, valueMealComponentsPath );
							setName = true;
						}
					}
				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateGDWFileFormat.runPreJob:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}
		
	}
	
	private void runJob(DaaSConfig daasConfig
                       ,String fileType
                       ,String terrDate
                       ,boolean compressOut) {


		try {
			baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "GDWFileFormat" + Path.SEPARATOR + "Step2");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");
			}
			
			hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm()); 

			if ( compressOut ) {
				hdfsConfig.set("mapreduce.map.output.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
				hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			}

			if ( terrDate.toUpperCase().startsWith("WORK") ) {
				requestedPaths = getVaildFilePaths(daasConfig,fileSystem,fileType,"STLD");
			} else {
				requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDate,"STLD");
			}

			job = Job.getInstance(hdfsConfig, JOB_GRP_DESC + " Part 2");
			
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			distCache = new Path("/user/mc05112/cache/pod_types.txt");
			job.addCacheFile(new URI(distCache.toString() + "#" + distCache.getName()));
			distCache = new Path("/user/mc05112/cache/gift_card_items.txt");
			job.addCacheFile(new URI(distCache.toString() + "#" + distCache.getName()));
			distCache = new Path("/user/mc05112/cache/gift_card_tenders.txt");
			job.addCacheFile(new URI(distCache.toString() + "#" + distCache.getName()));
			distCache = new Path("/daas/hive/Reference/CTRY/CTRY.txt");
			job.addCacheFile(new URI(distCache.toString() + "#" + distCache.getName()));
			distCache = valueMealComponentsPath; //new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "GDWFileFormatPre/part-r-00000");
			job.addCacheFile(new URI(distCache.toString() + "#" + distCache.getName()));
			
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			job.setJarByClass(GenerateGDWFileFormat.class);
			job.setMapperClass(GDWFileFormatMapper.class);
			//job.setReducerClass(GDWFileFormatReducer.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateGDWFileFormat.runJob:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}

	}

	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig
                                             ,FileSystem fileSystem
                                             ,String fileType
                                             ,String xmlType) {

		ArrayList<Path> retPaths = new ArrayList<Path>();
		String filePath;

		Path listPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + "step1");

		try {
			FileStatus[] fstus = fileSystem.listStatus(listPath);

			for (int idx=0; idx < fstus.length; idx++ ) {
				filePath = HDFSUtil.restoreMultiOutSpecialChars(fstus[idx].getPath().getName());

				if ( filePath.startsWith(xmlType) ) {
					retPaths.add(fstus[idx].getPath());

					if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
						System.out.println("Added work source file =" + filePath);
					}
				}
			}

		} catch (Exception ex) {
			System.err.println("Error occured in GenerateGDWFileFormat.getVaildFilePaths:");
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
                                             ,String fileType
                                             ,String requestedTerrDateParms
                                             ,String xmlType) {

		ArrayList<Path> retPaths = new ArrayList<Path>();

		try {

			Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem, daasConfig, requestedTerrDateParms, xmlType);

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
			System.err.println("Error occured in GenerateGDWFileFormat.getVaildFilePaths:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}

		return(retPaths);

	}
	
}

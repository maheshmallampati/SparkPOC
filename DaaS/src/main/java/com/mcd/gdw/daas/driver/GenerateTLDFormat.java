package com.mcd.gdw.daas.driver;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.mapreduce.TLDFormatMapper;
import com.mcd.gdw.daas.mapreduce.TLDFormatReducer;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

public class GenerateTLDFormat extends Configured implements Tool {

	public class ResultListFilter implements PathFilter {
	    public boolean accept(Path path) {
	    	return(!path.getName().startsWith("_"));
	    }
	}
	
	private class OutputFileName {
		
		private String terrCd;
		private String posBusnDt;
		private String prefix; 
		
		public OutputFileName(FileStatus fileStatus) {
			
			String[] parts = fileStatus.getPath().getName().split("_");
			String[] parts2 = null; 
			
			if ( parts.length > 1 ) {
				terrCd = parts[0];
				
				if ( parts.length == 2 ) {
					parts2 = parts[1].split("-");
					posBusnDt = "";
				} else {
					posBusnDt = parts[1];
					parts2 = parts[2].split("-");
				}
				
				prefix = parts2[0];
				
			} else {
				terrCd = "";
				posBusnDt = "";
				prefix = "";
			}
			
		}
		
		@SuppressWarnings("unused")
		public String getTerrCd() {
			return(terrCd);
		}
		
		@SuppressWarnings("unused")
		public String getPosBusnDt() {
			return(posBusnDt);
		}
		
		public String getPrefix() {
			return(prefix);
		}
		
		public String getKey() {

			String retKey = ""; 
			
			if ( prefix.equalsIgnoreCase("TLDTRNITM") ) {
				retKey = prefix + "_" + terrCd + "_" + posBusnDt; 
				
			} else if ( prefix.equalsIgnoreCase("MENUITM") ) {
				retKey = prefix + "_" + terrCd; 
			}
			
			return(retKey);
		}
		
		public Path getDirPath(Path basePath) {

			Path retPath = null;
			
			if ( posBusnDt.length() > 0 ) {
				retPath = new Path(basePath.toString() + Path.SEPARATOR + "terr_cd=" + terrCd + Path.SEPARATOR + "pos_busn_dt=" + posBusnDt); 
				
			} else {
				retPath = new Path(basePath.toString() + Path.SEPARATOR + "terr_cd=" + terrCd); 
			}
			
			return(retPath);
			
		}
		
		public Path getNewFilePath(Path basePath
				                  ,Path currPath) {

			Path retPath = null;
			
			if ( posBusnDt.length() > 0 ) {
				retPath = new Path(basePath.toString() + Path.SEPARATOR + "terr_cd=" + terrCd + Path.SEPARATOR + "pos_busn_dt=" + posBusnDt + Path.SEPARATOR + currPath.getName()); 
				
			} else {
				retPath = new Path(basePath.toString() + Path.SEPARATOR + "terr_cd=" + terrCd + Path.SEPARATOR + currPath.getName()); 
			}
			
			return(retPath);
			
		}
		
	}
	private static final String JOB_GRP_DESC = "Generate TLD Extracts";
	
	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
	private Path baseHivePath = null;
	FsPermission newFilePremission;

	public static void main(String[] args) throws Exception {
		
		final Configuration hdfsConfig = new Configuration();


//		final Configuration conf = new Configuration();
		hdfsConfig.set("fs.defaultFS", "hdfs://MCDHADOOPUDA");
		hdfsConfig.set("mapreduce.framework.name", "yarn");
		hdfsConfig.set("yarn.resourcemanager.address", "hdp001-7:8050");
		hdfsConfig.set("yarn.resourcemanager.scheduler.address", "hdp001-7:8030");
		hdfsConfig.set("yarn.resourcemanager.webapp.address", "hdp001-7:8088");
		hdfsConfig.set("mapreduce.jobhistory.webapp.address", "hdp001-7:19888");
		
		int retval = ToolRunner.run(hdfsConfig,new GenerateTLDFormat(), args);

		System.out.println(" return value : " + retval);

	}
	
	public int run(String[] args) throws Exception {

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
			System.out.println("Usage: GenerateTLDFormat -c config.xml -t filetype -d territoryDateParms ");
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
			System.err.println("Usage: GenerateTLDFormat -c config.xml -t filetype -d territoryDateParms");
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

			runJob(daasConfig,fileType,terrDate,true);
			
		} else {
			System.err.println("Invalid Config XML file, stopping");
			System.err.println(daasConfig.errText());
			System.exit(8);
		}
	
		return(0);
		
	}

	private void runJob(DaaSConfig daasConfig
                       ,String fileType
                       ,String terrDate
                       ,boolean compressOut) {

		ABaC abac;
		ArrayList<String> lastList;
		StringBuffer terrDateList = new StringBuffer();

//		Job job;

		int jobGrpId;

		ArrayList<Path> requestedPaths;

		Path baseTldTrnPath;
		Path baseTldMenuPath;

		try {
			newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

			baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "TLDOut");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");
			}

			baseHivePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsHiveSubDir() );
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, baseHivePath,daasConfig.displayMsgs());
			baseHivePath = new Path(baseHivePath.toString() + Path.SEPARATOR + "TLD" );
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, baseHivePath,daasConfig.displayMsgs());
	
			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nHive path = " + baseHivePath.toString() + "\n");
			}

			baseTldTrnPath = new Path(baseHivePath.toString() + Path.SEPARATOR + "TLD_ITM" );
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, baseTldTrnPath,daasConfig.displayMsgs());
			baseTldMenuPath = new Path(baseHivePath.toString() + Path.SEPARATOR + "TLD_MENU" );
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, baseTldMenuPath,daasConfig.displayMsgs());

			hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm()); 

			if ( compressOut ) {
				//hdfsConfig.set("mapred.compress.map.output", "true");
				//hdfsConfig.set("mapred.output.compress", "true");
				//hdfsConfig.set("mapred.output.compression.type", "RECORD");
				//hdfsConfig.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
				//hdfsConfig.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.GzipCodec");

				hdfsConfig.set("mapreduce.map.output.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
				hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");

			}
	
			abac = new ABaC(daasConfig);

			jobGrpId = abac.createJobGroup(JOB_GRP_DESC);

			if ( terrDate.toUpperCase().startsWith("LAST") ) {

				boolean force = terrDate.equalsIgnoreCase("LAST_FORCE");
		
				System.out.println("Getting list of Territory Codes / Business Days that have changed") ;

				if ( daasConfig.displayMsgs() ) {
			
					System.out.println("The following territory codes / busness days have changed since the last export");
			
					if ( force ) {
						System.out.println(" (FORCE LAST MergeToFinal run results):");
					} else {
						System.out.println(":");
					}
				}
		
				lastList = abac.getChangedTerrBusinessDatesSinceTs(JOB_GRP_DESC,force);
		
				if ( daasConfig.displayMsgs() ) {
					System.out.println("The following territory codes / busness days have changed since the last export:");
				}
			
				for (String itm : lastList) {
					if (itm.startsWith("840:") ) {
						if ( daasConfig.displayMsgs() ) {
							System.out.println(itm);
						}
						if ( terrDateList.length() > 0 ) {
							terrDateList.append(",");
						}
						terrDateList.append(itm);
					}
				}

				requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDateList.toString());
			} else {
				requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDate);
			}
	
			final Job job = new Job(hdfsConfig, "Creating TLD Format");
//			final Job job = Job.getInstance(hdfsConfig, "Creating TLD Format");

			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			Path daypartCachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "DayPart_ID.psv");
			//Path countryCachePath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "country.txt#country.txt");
			
			//DistributedCache.addCacheFile(new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "DayPart_ID.psv").toUri(), job.getConfiguration());
			job.addCacheFile(new URI(daypartCachePath.toUri()+"#DayPart_ID.psv"));
			//DistributedCache.addCacheFile(new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + "distcachefiles" + Path.SEPARATOR + "country.txt").toUri(), job.getConfiguration());
			//job.addCacheFile(countryCachePath.toUri());
			
			//DistributedCache.addCacheFile(new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + "aster_include_list.txt").toUri(), job.getConfiguration());
		
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	
			job.setJarByClass(GenerateTLDFormat.class);
			job.setMapperClass(TLDFormatMapper.class);
			job.setReducerClass(TLDFormatReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			//job.setNumReduceTasks(1);
			//job.setOutputFormatClass(TextOutputFormat.class);
			//job.setOutputFormat(NullOutputFormat.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

			abac.closeJobGroup(jobGrpId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
			abac.dispose();
			
			FileStatus[] status = fileSystem.listStatus(baseOutputPath, new ResultListFilter());      //.listStatus(baseOutputPath);
	
			//String fname;
			//String dt;
			//String prefix;
			Path newPath=null; 
			//String parts[];
			String key;
	
			OutputFileName outFile; 
			
			HashMap<String,Path> uniqueValues = new HashMap<String,Path>(); 

			if ( status != null ) {
				for (int idx=0; idx < status.length; idx++ ) {
					outFile = new OutputFileName(status[idx]);
					key = outFile.getKey();
					
					if ( !uniqueValues.containsKey(key) ) {
						if ( outFile.prefix.equalsIgnoreCase("TLDTRNITM") ) {
							uniqueValues.put(key, outFile.getDirPath(baseTldTrnPath));
						} else if ( outFile.prefix.equalsIgnoreCase("MENUITM") ) {
							uniqueValues.put(key, outFile.getDirPath(baseTldMenuPath));
						}
					}
					
				}
			}

			for (Map.Entry<String, Path> entry :  uniqueValues.entrySet()) {
				newPath = entry.getValue();
				
				//System.out.println("DIR->" + newPath.toString());
				
				HDFSUtil.removeHdfsSubDirIfExists(fileSystem, newPath, daasConfig.displayMsgs());
				HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, newPath, daasConfig.displayMsgs());
			}

			if ( status != null ) {
				for (int idx=0; idx < status.length; idx++ ) {
					outFile = new OutputFileName(status[idx]);
					if ( outFile.getPrefix().length() > 0 ) {
						if ( outFile.prefix.equalsIgnoreCase("TLDTRNITM") ) {
							newPath = outFile.getNewFilePath(baseTldTrnPath, status[idx].getPath());
						} else if ( outFile.prefix.equalsIgnoreCase("MENUITM") ) {
							newPath = outFile.getNewFilePath(baseTldMenuPath, status[idx].getPath());
						}
								
						System.out.println(status[idx].getPath() + " " + newPath);
						
						fileSystem.rename(status[idx].getPath(), newPath);
						fileSystem.setPermission(newPath,newFilePremission);						
					}
				}
			}
	
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateTLDFormat.runJob:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}

	}

	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig
                                             ,String fileType
                                             ,String requestedTerrDateParms) {

		ArrayList<Path> retPaths = new ArrayList<Path>();

		try {

			Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem, daasConfig, requestedTerrDateParms, "STLD", "MenuItem");
			//Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem, daasConfig, requestedTerrDateParms, "STLD");

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
			System.err.println("Error occured in GenerateTLDFormat.getVaildFilePaths:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}

		return(retPaths);
	
	}

}

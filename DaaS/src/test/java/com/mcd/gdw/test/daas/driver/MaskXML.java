package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.RDBMS;
import com.mcd.gdw.test.daas.mapreduce.MaskXMLMapper;
import com.mcd.gdw.test.daas.mapreduce.MaskXMLPartitioner;
import com.mcd.gdw.test.daas.mapreduce.MaskXMLReducer;
import com.mcd.gdw.test.daas.mapreduce.ZipFileOutputFormat;

public class MaskXML extends Configured implements Tool {

	public static final String CONFIG_CACHE_NAME  = "daas.dist_cache_file_name";
	public static final String CONFIG_PART_NAME   = "daas.dist_cache_part_file";
	public static final String CONFIG_CTRY_DECODE = "daas.ctry_decode";
	
	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
	private FsPermission cacheFilePermission;
	private Path partCache;

	private StringBuffer ctryDecode = new StringBuffer();

	public static void main(String[] args) throws Exception {

		Configuration hdfsConfig = new Configuration();

		int retval = ToolRunner.run(hdfsConfig,new MaskXML(), args);

		System.out.println(" return value : " + retval);
		
	}
	public int run(String[] args) throws Exception {

		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
		String maskFile = "";
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

			if ( args[idx].equals("-m") && (idx+1) < args.length ) {
				maskFile = args[idx+1];
			}
			
			if ( args[idx].toUpperCase().equals("-H") || args[idx].toUpperCase().equals("-HELP")  ) {
				helpRequest = true;
			}
		}

		if ( helpRequest ) {
			System.out.println("Usage: MaskXML -c config.xml -t filetype -d territoryDateParms -m maskFile ");
			System.out.println("where territoryDateParm is a comma separated list of territory codes and dates separated by colons(:)");
			System.out.println("for example, 840:2012-07-01:2012-07-07 is territory 840 from July 1st, 2012 until July 7th, 2012.");
			System.out.println("the date format is either ISO YYYY-MM-DD or YYYYMMDD (both are valid)");
			System.out.println("If only one date is supplied then a single day is used for that territory");
			System.out.println("Multiple territoryDateParm can be specified as comma separated values: 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01");
			System.out.println("This will get a total of 3 days for 840 and 1 day from 250");
			System.exit(0);
		}

		if ( configXmlFile.length() == 0 || fileType.length() == 0 || terrDate.length() == 0 || maskFile.length() == 0) {
			System.err.println("Missing config.xml (-c), filetype (t), territoryDateParms (-d), maskFile (-m)");
			System.err.println("Usage: GenerateOffersReportFormat -c config.xml -t filetype -d territoryDateParms -m maskFile");
			System.err.println("where territoryDateParm is a comma separated list of territory codes and dates separated by colons(:)");
			System.err.println("for example, 840:2012-07-01:2012-07-07 is territory 840 from July 1st, 2012 until July 7th, 2012.");
			System.err.println("the date format is either ISO YYYY-MM-DD or YYYYMMDD (both are valid)");
			System.err.println("If only one date is supplied then a single day is used for that territory");
			System.err.println("Multiple territoryDateParm can be specified as comma separated values: 840:20120701,840:2012-07-05:2012-07-08,250:2012-08-01");
			System.err.println("This will get a total of 3 days for 840 and 1 day from 250");
			System.exit(8);
		}
		
		if ( !new File(maskFile).isFile() ) {
			System.err.println("Mask file does not exist or is not a file");
			System.exit(8);
		}
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		
		if ( daasConfig.configValid() ) {
			
			if ( daasConfig.displayMsgs()  ) {
				System.out.println(daasConfig.toString());
			}

			hdfsConfig = getConf();
			fileSystem = FileSystem.get(hdfsConfig);
			
			try {
				String line;
				String[] parts;
				
				int terrCd;
				String ctryIsoAbbr2;
				
				BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsHiveSubDir() + Path.SEPARATOR + "Reference" + Path.SEPARATOR + "CTRY" + Path.SEPARATOR + "CTRY.txt"))));
				
				while ((line = br.readLine()) != null) {
					parts = line.split("\t");
					
					terrCd = Integer.parseInt(parts[0]);
					ctryIsoAbbr2 = parts[2];

					ctryDecode.append("|");
					ctryDecode.append(String.format("%03d", terrCd));
					ctryDecode.append(ctryIsoAbbr2);
				}
				
				br.close();

			} catch (Exception ex) {
				ex.printStackTrace(System.err);
				System.exit(8);
			}
			
			generatePartitions(daasConfig,terrDate,hdfsConfig.get("mapreduce.job.reduces"));
			
			runJob(daasConfig,fileType,terrDate,maskFile,true);
			
		} else {
			System.err.println("Invalid Config XML file, stopping");
			System.err.println(daasConfig.errText());
			System.exit(8);
		}
	
		return(0);
	}

	private void generatePartitions(DaaSConfig daasConfig
			                       ,String terrDate
			                       ,String numReducers) throws Exception {
		
		RDBMS db;
		StringBuffer sql = new StringBuffer();
		String[] terrDateParts;
		String[] parts;
		StringBuffer where = new StringBuffer();
		String frDt;
		String toDt;
		String fromDate;
		String toDate;
		
		partCache = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + "mask_part.txt");
		
		terrDateParts = terrDate.split(",");
		
		where.setLength(0);
		
		for ( int idx=0; idx < terrDateParts.length; idx++ ) {
			if ( where.length() == 0 ) {
				where.append("                 (a.TERR_CD = ");
			} else {
				where.append("          or     (a.TERR_CD = ");
			}
			
			parts = terrDateParts[idx].split(":");

			where.append(parts[0]);
			where.append(" and a.CAL_DT between '");
			
			frDt = parts[1];
			if ( parts.length == 2 ) {
				toDt = parts[1];
			} else {
				toDt = parts[2];
			}
			
			if ( frDt.length() == 8 ) {
				fromDate = frDt.substring(0, 4) + "-" + frDt.substring(4, 6) + "-" + frDt.substring(6, 8);
			} else {
				fromDate = frDt;
			}
			
			if ( toDt.length() == 8 ) {
				toDate = toDt.substring(0, 4) + "-" + toDt.substring(4, 6) + "-" + toDt.substring(6, 8);
			} else {
				toDate = toDt;
			}

			where.append(fromDate);
			where.append(" ' and '");
			where.append(toDate);
			where.append("')\n");
			
		}
		
		db = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());
		
		sql.setLength(0);
		sql.append("select\n");
		sql.append("   a.TERR_CD\n");
		sql.append("  ,a.LGCY_LCL_RFR_DEF_CD\n");
		sql.append("  ,a.CAL_DT\n");
		sql.append("  ,a.ROW_NUM % " + numReducers + " as PART\n");
		sql.append("from (\n");
		sql.append("     select\n");
		sql.append("        a.TERR_CD\n");
		sql.append("       ,a.LGCY_LCL_RFR_DEF_CD\n");
		sql.append("       ,a.CAL_DT\n");
		sql.append("       ,row_number() over(order by a.TERR_CD, a.LGCY_LCL_RFR_DEF_CD, a.CAL_DT) as ROW_NUM\n");
		sql.append("     from (\n");
		sql.append("          select distinct\n");
		sql.append("             a.TERR_CD\n");
		sql.append("            ,a.LGCY_LCL_RFR_DEF_CD\n");
		sql.append("            ,substring(cast(a.CAL_DT as varchar(10)),1,4) + substring(cast(a.CAL_DT as varchar(10)),6,2) + substring(cast(a.CAL_DT as varchar(10)),9,2) as CAL_DT\n");
		sql.append("          from ABaCProd.dbo.Vdw_file a\n");
		sql.append("          inner join ABaCProd.dbo.Vdw_audt_stus_typ b\n");
		sql.append("            on (b.DW_AUDT_STUS_TYP_ID = a.DW_AUDT_STUS_TYP_ID)\n");
		sql.append("          where (\n");
		sql.append(where);
		sql.append("                )\n");
		sql.append("          and   b.DW_AUDT_STUS_TYP_DS = 'SUCCESSFUL') a) a\n");

		BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(partCache,true)));
		
		ResultSet rset = db.resultSet(sql.toString());
		
		while ( rset.next() ) {
			bw.write(rset.getString("TERR_CD") + "\t" + rset.getString("LGCY_LCL_RFR_DEF_CD") + "\t" + rset.getString("CAL_DT") + "\t" + rset.getString("PART") + "\n");
		}
		
		bw.close();
		rset.close();
		
	}
	
	private void runJob(DaaSConfig daasConfig
            ,String fileType
            ,String terrDate
            ,String maskFile
            ,boolean compressOut) throws Exception  {

		Job job;		
		Path maskPath;

		ArrayList<Path> requestedPaths;
		
		cacheFilePermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE);
		
		maskPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + new File(maskFile).getName());
		
		fileSystem.copyFromLocalFile(false, true, new Path(maskFile), maskPath);
		fileSystem.setPermission(maskPath, cacheFilePermission);

		baseOutputPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "MaskedXML");
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

		if ( daasConfig.displayMsgs() ) {
			System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");
		}
		
		hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm());
		hdfsConfig.set(CONFIG_CTRY_DECODE, ctryDecode.toString());
		hdfsConfig.set(CONFIG_CACHE_NAME, maskPath.getName());
		hdfsConfig.set(CONFIG_PART_NAME, partCache.getName());

		if ( compressOut ) {
			hdfsConfig.set("mapreduce.map.output.compress", "true");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
			hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
		}

		requestedPaths = getVaildFilePaths(daasConfig,fileType,terrDate);

		job = Job.getInstance(hdfsConfig, "Creating Masked XML");
		
		job.addCacheFile(new URI(maskPath.toString() + "#" + maskPath.getName()));
		
		for (Path addPath : requestedPaths ) {
			FileInputFormat.addInputPath(job, addPath);
		}
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		job.setJarByClass(MaskXML.class);
		job.setMapperClass(MaskXMLMapper.class);
		job.setPartitionerClass(MaskXMLPartitioner.class);
		job.setReducerClass(MaskXMLReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputFormatClass(ZipFileOutputFormat.class);
		ZipFileOutputFormat.setOutputPath(job, baseOutputPath);
		
		MultipleOutputs.addNamedOutput(job, "ZIPOUTPUT", ZipFileOutputFormat.class, Text.class, Text.class);
		

		if ( ! job.waitForCompletion(true) ) {
			System.err.println("Error occured in MapReduce process, stopping");
			System.exit(8);
		}
		
		
	}
	
	private ArrayList<Path> getVaildFilePaths(DaaSConfig daasConfig
                                             ,String fileType
                                             ,String requestedTerrDateParms) {

		ArrayList<Path> retPaths = new ArrayList<Path>();

		try {

			Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem, daasConfig, requestedTerrDateParms, "STLD", "DetailedSOS", "MenuItem", "SecurityData","Store-Db","Product-Db");

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
			System.err.println("Error occured in GenerateOffersReport.getVaildFilePaths:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}

		return(retPaths);
	
	}
}

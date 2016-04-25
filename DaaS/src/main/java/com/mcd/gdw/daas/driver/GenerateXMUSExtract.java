package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.RDBMS;

import com.mcd.gdw.daas.util.SimpleEncryptAndDecrypt;

import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.mapreduce.XMUSExtractMapper;

public class GenerateXMUSExtract extends Configured implements Tool {

	public class ResultListFilter implements PathFilter {
		
		private String prefix; 
		
		public ResultListFilter(String prefix) {
			
			this.prefix = prefix;
			
		}
		
	    public boolean accept(Path path) {
	    	return(!path.getName().startsWith("_") && path.getName().startsWith(this.prefix));
	    }
	}

	public static final String LOCATION_INCLUDE_LIST        = "xmusextract_include_list.txt";
	public static final String LOCATION_HIST_INCLUDE_LIST   = "xmusextract_include_hist_list.txt";
	
	public static final String ORDER_FILE_PREFIX            = "Order";
	public static final String ORDER_DETAIL_FILE_PREFIX     = "OrderDetail";
	public static final String TIMINGS_FILE_PREFIX          = "Timings";
	public static final String MENU_FILE_PREFIX             = "Menu";

	public static final String ORDER_TABLE_NAME             = "FT_XML_SUMMARY";
	public static final String ORDER_DETAIL_TABLE_NAME      = "FT_XML_DETAIL";
	public static final String TIMINGS_TABLE_NAME           = "FT_XML_TIME";
	public static final String MENU_TABLE_NAME              = "FT_XML_MENU";
	
	public static final String JOB_DESC                     = "Generate XM US Extract Data";
	public static final String JOB_STEP1_DESC               = "MapReduce Extract XML Data";
	public static final String JOB_STEP2_DESC               = "Load Order Data";
	public static final String JOB_STEP3_DESC               = "Load Order Detail Data";
	public static final String JOB_STEP4_DESC               = "Load Timings Data";
	public static final String JOB_STEP5_DESC               = "Load Menu Data";
	
	private ArrayList<String> workTerrCodeList = new ArrayList<String>();
	private FileSystem fileSystem;
	private FsPermission newFilePremission;
	private RDBMS sqlServer;
	
	private HashMap<String,String> sqlStmt = new HashMap<String,String>();
	
	private int totCnt;
	
	public static void main(String[] args) throws Exception {
		
		int retval = ToolRunner.run(new Configuration(),new GenerateXMUSExtract(), args);
		
		System.out.println(" return value : " + retval); 
		
	}

	@Override
	public int run(String[] argsall) throws Exception {

		String configXmlFile = "";
		String fileType = "";
		String terrDate = "";
		String serverName = "";
		String database = "";
		String userId = "";
		String password = "";
		String preprocesssqltype = "none";
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
			
			if ( args[idx].equals("-server") && (idx+1) < args.length ) {
				serverName = args[idx+1];
			}
			
			if ( args[idx].equals("-database") && (idx+1) < args.length ) {
				database = args[idx+1];
			}
			
			if ( args[idx].equals("-userid") && (idx+1) < args.length ) {
				userId = args[idx+1];
			}
			
			if ( args[idx].equals("-password") && (idx+1) < args.length ) {
				password = args[idx+1];
			}
			
			if ( args[idx].equals("-preprocesssqltype") && (idx+1) < args.length ) {
				preprocesssqltype = args[idx+1];
			}
			
		}
		
		if ( configXmlFile.length() == 0 || fileType.length() == 0 || terrDate.length() == 0 || serverName.length() == 0 || database.length() == 0 || userId.length() == 0 || password.length() == 0 || preprocesssqltype.length() == 0 )  {
			System.err.println("Invalid parameters");
			System.err.println("Usage: GenerateXMUSExtract -c config.xml -t filetype -d territoryDateParms -server serverName -database databaseName -userid userId -password encryptedPasswordBytesAsText -preprocesssqltype preprocessSqlType");
			System.exit(8);
		}
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		if ( daasConfig.configValid() ) {

			//AWS START
			//fileSystem = FileSystem.get(getConf());
			fileSystem = HDFSUtil.getFileSystem(daasConfig, getConf());
			//AWS END
			
			initSqlStmtMap(database);
			
			try {
				
				if ( daasConfig.displayMsgs() ) {
					System.out.print("Connecting to SQL Server to get Location List ... ");
				}
		    
				SimpleEncryptAndDecrypt decrypt = new SimpleEncryptAndDecrypt();
			
				sqlServer = new RDBMS(RDBMS.ConnectionType.SQLServer,serverName,userId,decrypt.decryptFromHexString(password));
				
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.println("done");
			    }
				
			} catch (Exception ex) {
				System.err.println("Error connecting to SQL Server Target System");
				ex.printStackTrace(System.err);
				System.exit(8);
			}
			
			newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

			String priorWeek = "";
			
			if ( terrDate.toUpperCase().startsWith("PRIORWEEKENDINGTHURSDAY:") ) {
				int dayDiff =0;
				Calendar fromDt = Calendar.getInstance();
				Calendar toDt = Calendar.getInstance();
				SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
				String dtRange = "";

				if ( fromDt.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY ) {
					dayDiff = -9;
				} else if ( fromDt.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY ) {
					dayDiff = -10;
				} else if ( fromDt.get(Calendar.DAY_OF_WEEK) == Calendar.TUESDAY ) {
					dayDiff = -11;
				} else if ( fromDt.get(Calendar.DAY_OF_WEEK) == Calendar.WEDNESDAY ) {
					dayDiff = -12;
				} else if ( fromDt.get(Calendar.DAY_OF_WEEK) == Calendar.THURSDAY ) {
					dayDiff = -13;
				} else if ( fromDt.get(Calendar.DAY_OF_WEEK) == Calendar.FRIDAY ) {
					dayDiff = -7;
				} else if ( fromDt.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY ) {
					dayDiff = -8;
				}

				fromDt.add(Calendar.DAY_OF_MONTH, dayDiff);
				toDt.set(fromDt.get(Calendar.YEAR), fromDt.get(Calendar.MONTH), fromDt.get(Calendar.DAY_OF_MONTH));
				toDt.add(Calendar.DAY_OF_MONTH, 6);
				dtRange = ":" + fmt.format(fromDt.getTime()) + ":" + fmt.format(toDt.getTime());
				
				String[] terrCodeList = terrDate.split(":");
				
				for ( int i=1; i < terrCodeList.length; i++ ) {
					if ( priorWeek.length() > 0 ) {
						priorWeek += ",";
					}
					
					priorWeek += terrCodeList[i] + dtRange; 
				}
				
				if ( daasConfig.displayMsgs() ) {
					System.out.print("Prior Week Ending Thrusday generated parameter: " + priorWeek);
				}
				
			}

			Path locationList = null;
			ArrayList<String> histTerrDate = null;
			int selCnt = 0;

			if ( terrDate.toUpperCase().equals("NULL") ) {
				System.exit(0);
			}
			
			if ( terrDate.toUpperCase().equals("HIST") ) {
				locationList = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + LOCATION_HIST_INCLUDE_LIST);
				histTerrDate = createHistListDistCache(locationList,database);
				selCnt = histTerrDate.size();
			} else {
				locationList = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + LOCATION_INCLUDE_LIST);
				selCnt = createListDistCache(locationList,database);
			}
			fileSystem.setPermission(locationList,newFilePremission);

			if ( selCnt > 0 ) {
				if ( terrDate.toUpperCase().equals("HIST") ) {
					
					String deleteType = preprocesssqltype;
					
					int runCnt = 0;
					
					for ( String sliceTerrDate : histTerrDate ) {
						runCnt++;
						runMR(daasConfig,fileType,getConf(),sliceTerrDate,database,deleteType,locationList,false,JOB_DESC + " Hist run " + runCnt + " of " + histTerrDate.size() , JOB_DESC + " Hist");
						deleteType = "none"; 

						sqlServer.commit();
						sqlServer.dispose();
						SimpleEncryptAndDecrypt decrypt = new SimpleEncryptAndDecrypt();
					
						sqlServer = new RDBMS(RDBMS.ConnectionType.SQLServer,serverName,userId,decrypt.decryptFromHexString(password));

					}
				} else if ( priorWeek.length() > 0 ) {
					runMR(daasConfig,fileType,getConf(),priorWeek,database,preprocesssqltype,locationList,false,JOB_DESC,JOB_DESC);
				} else {
					runMR(daasConfig,fileType,getConf(),terrDate,database,preprocesssqltype,locationList,false,JOB_DESC,JOB_DESC);
				}
			} else {
				System.out.println("WARNING: No selected locations / History");
			}

			sqlServer.commit();
			sqlServer.dispose();
			
		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.err.println("File Type   = " + fileType);
			System.exit(8);
		}
		
		return(0);
	}

	private void initSqlStmtMap(String database) {
		
		StringBuffer sqlTarget = new StringBuffer();
		
		sqlTarget.setLength(0);
		sqlTarget.append("truncate table ");
		sqlTarget.append(database);
		sqlTarget.append(".");
		sqlTarget.append(ORDER_TABLE_NAME);
		
		sqlStmt.put("truncate|" + ORDER_TABLE_NAME, sqlTarget.toString());
		
		sqlTarget.setLength(0);
		sqlTarget.append("delete from ");
		sqlTarget.append(database);
		sqlTarget.append(".");
		sqlTarget.append(ORDER_TABLE_NAME);
		
		sqlStmt.put("delete|" + ORDER_TABLE_NAME, sqlTarget.toString());
		
	    sqlTarget.setLength(0);
	    sqlTarget.append("insert into ");
	    sqlTarget.append(database); 
	    sqlTarget.append(".");
	    sqlTarget.append(ORDER_TABLE_NAME);
	    sqlTarget.append(" (\n");
	    sqlTarget.append("    StoreId\n");
	    sqlTarget.append("   ,BusinessDate\n");
	    sqlTarget.append("   ,ORD_KEY\n");
	    sqlTarget.append("   ,OrderTimestamp\n");
	    sqlTarget.append("   ,POD\n");
	    sqlTarget.append("   ,SALE_TYPE\n");
	    sqlTarget.append("   ,SALE_KIND\n");
	    sqlTarget.append("   ,TOTAL_AMOUNT\n");
	    sqlTarget.append("   ,NON_PRODUCT_AMOUNT)\n");
	    sqlTarget.append("  values (?,?,?,?,?,?,?,?,?);\n");
	    
		sqlStmt.put("insert|" + ORDER_TABLE_NAME, sqlTarget.toString());
		
		sqlTarget.setLength(0);
		sqlTarget.append("truncate table ");
		sqlTarget.append(database);
		sqlTarget.append(".");
		sqlTarget.append(ORDER_DETAIL_TABLE_NAME);
		
		sqlStmt.put("truncate|" + ORDER_DETAIL_TABLE_NAME, sqlTarget.toString());
		
		sqlTarget.setLength(0);
		sqlTarget.append("delete from ");
		sqlTarget.append(database);
		sqlTarget.append(".");
		sqlTarget.append(ORDER_DETAIL_TABLE_NAME);
		
		sqlStmt.put("delete|" + ORDER_DETAIL_TABLE_NAME, sqlTarget.toString());

	    sqlTarget.setLength(0);
	    sqlTarget.append("insert into ");
	    sqlTarget.append(database); 
	    sqlTarget.append(".");
	    sqlTarget.append(ORDER_DETAIL_TABLE_NAME);
	    sqlTarget.append(" (\n");
	    sqlTarget.append("    WKTHUR\n");
		sqlTarget.append("   ,BusinessDate\n");
		sqlTarget.append("   ,StoreId\n");
		sqlTarget.append("   ,OrderKey\n");
		sqlTarget.append("   ,OrderKind\n");
		sqlTarget.append("   ,OrderSaleType\n");
		sqlTarget.append("   ,OrderLocation\n");
		sqlTarget.append("   ,OrderTimestamp\n");
		sqlTarget.append("   ,OrderTotalAmount\n");
		sqlTarget.append("   ,ItemLevel\n");
		sqlTarget.append("   ,ItemType\n");
		sqlTarget.append("   ,ItemAction\n");
		sqlTarget.append("   ,ItemCode\n");
		sqlTarget.append("   ,ItemSeq\n");
		sqlTarget.append("   ,ItemQty\n");
		sqlTarget.append("   ,ItemQtyPromo\n");
		sqlTarget.append("   ,ITEM_GRILL_QTY\n");
		sqlTarget.append("   ,ITEM_GRILL_MODIFIER\n");
		sqlTarget.append("   ,ItemTotalPrice)\n");
	    sqlTarget.append("  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);\n");
	    
		sqlStmt.put("insert|" + ORDER_DETAIL_TABLE_NAME, sqlTarget.toString());

		sqlTarget.setLength(0);
		sqlTarget.append("truncate table ");
		sqlTarget.append(database);
		sqlTarget.append(".");
		sqlTarget.append(TIMINGS_TABLE_NAME);
		
		sqlStmt.put("truncate|" + TIMINGS_TABLE_NAME, sqlTarget.toString());
		
		sqlTarget.setLength(0);
		sqlTarget.append("delete from ");
		sqlTarget.append(database);
		sqlTarget.append(".");
		sqlTarget.append(TIMINGS_TABLE_NAME);
		
		sqlStmt.put("delete|" + TIMINGS_TABLE_NAME, sqlTarget.toString());
		
	    sqlTarget.setLength(0);
	    sqlTarget.append("insert into ");
	    sqlTarget.append(database); 
	    sqlTarget.append(".");
	    sqlTarget.append(TIMINGS_TABLE_NAME);
	    sqlTarget.append(" (\n");
		sqlTarget.append("    linenum\n");
		sqlTarget.append("   ,TERR_CD\n");
		sqlTarget.append("   ,LGCY_LCL_RFR_DEF_CD\n");
		sqlTarget.append("   ,BusnessDate\n");
		sqlTarget.append("   ,ORD_KEY\n");
		sqlTarget.append("   ,TM_SEGMENT_ID\n");
		sqlTarget.append("   ,PROD_NODE_ID\n");
		sqlTarget.append("   ,TRAN_COUNT\n");
		sqlTarget.append("   ,ITEMS_COUNT\n");
		sqlTarget.append("   ,NUM_CARS\n");
		sqlTarget.append("   ,TIME_START_TO_TOTAL\n");
		sqlTarget.append("   ,TIME_START_TO_STORE\n");
		sqlTarget.append("   ,TIME_START_TO_RECALL\n");
		sqlTarget.append("   ,TIME_START_TO_CLOSEDRAWER\n");
		sqlTarget.append("   ,TIME_START_TO_PAY\n");
		sqlTarget.append("   ,TIME_START_TO_SERVE\n");
		sqlTarget.append("   ,TIME_ORDER_TOTAL\n");
		sqlTarget.append("   ,TOTAL_AMOUNT\n");
		sqlTarget.append("   ,CHECK_POINT)\n");
	    sqlTarget.append("  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);\n");
	    
		sqlStmt.put("insert|" + TIMINGS_TABLE_NAME, sqlTarget.toString());

		sqlTarget.setLength(0);
		sqlTarget.append("truncate table ");
		sqlTarget.append(database);
		sqlTarget.append(".");
		sqlTarget.append(MENU_TABLE_NAME);
		
		sqlStmt.put("truncate|" + MENU_TABLE_NAME, sqlTarget.toString());
		
		sqlTarget.setLength(0);
		sqlTarget.append("delete from ");
		sqlTarget.append(database);
		sqlTarget.append(".");
		sqlTarget.append(MENU_TABLE_NAME);
		
		sqlStmt.put("delete|" + MENU_TABLE_NAME, sqlTarget.toString());
		
	    sqlTarget.setLength(0);
	    sqlTarget.append("insert into ");
	    sqlTarget.append(database); 
	    sqlTarget.append(".");
	    sqlTarget.append(MENU_TABLE_NAME);
	    sqlTarget.append(" (\n");
	    sqlTarget.append("    STORE_ID\n");
	    sqlTarget.append("   ,TERR_CD\n");
	    sqlTarget.append("   ,POS_BUSN_DT\n");
	    sqlTarget.append("   ,POS_CREA_DT\n"); 
	    sqlTarget.append("   ,MENU_ITM_ID\n");
	    sqlTarget.append("   ,MENU_ITM_NAME\n");
	    sqlTarget.append("   ,MENU_ITM_FAMILY_GROUP\n");
	    sqlTarget.append("   ,MENU_ITM_GRILL_GROUP\n");
	    sqlTarget.append("   ,EAT_IN_PRICE_AM\n");
	    sqlTarget.append("   ,PROD_SHOW_ON_MAIN\n");
	    sqlTarget.append("   ,PROD_SHOW_ON_MFY\n");
	    sqlTarget.append("   ,PROD_DONOT_DECOMPVM\n");
	    sqlTarget.append("   ,PROD_SHOW_ON_SUMMARY\n");
	    sqlTarget.append("   ,PROD_GRILLABLE\n");
	    sqlTarget.append("   ,PROD_DONTPRINTGRILL)\n");
	    sqlTarget.append("  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);\n");
	    
		sqlStmt.put("insert|" + MENU_TABLE_NAME, sqlTarget.toString());
	    
	}
	
	private void runMR(DaaSConfig daasConfig
                      ,String fileType
                      ,Configuration hdfsConfig
                      ,String terrDate
                      ,String database
                      ,String preprocesssqltype
                      ,Path locationList
                      ,boolean compressOut
                      ,String jobDesc
                      ,String jobABaCDesc) {

		Job job;
		ArrayList<Path> requestedPaths = null;
		ArrayList<String> subTypeList = new ArrayList<String>();
		ABaC abac;
		int jobGrpId; 
		int jobId;

		subTypeList.add("STLD");
		subTypeList.add("DetailedSOS");
		subTypeList.add("MenuItem");

		try {
			abac = new ABaC(daasConfig);
			
			hdfsConfig.set("mapred.child.java.opts", daasConfig.fileMapReduceJavaHeapSizeParm()); 

			if ( compressOut ) {
				hdfsConfig.set("mapreduce.map.output.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
				hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
				hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			}
			
			jobGrpId = abac.createJobGroup(jobABaCDesc);
			jobId = abac.createJob(jobGrpId, 1, JOB_STEP1_DESC);
			
			job = Job.getInstance(hdfsConfig, jobDesc);
			
			if ( daasConfig.displayMsgs() ) {
				System.out.println("\n" + jobDesc + "\n");
			}
			Path outPath=null;

			outPath= new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "XMUSExtract");

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
			
			job.addCacheFile(new URI(locationList.toString() + "#" + locationList.getName()));
			
			for (Path addPath : requestedPaths ) {
				FileInputFormat.addInputPath(job, addPath);
			}

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
			job.setJarByClass(GenerateXMUSExtract.class);
			job.setMapperClass(XMUSExtractMapper.class);
			//job.setReducerClass(XMUSExtractReducer.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			//job.setOutputFormatClass(TextOutputFormat.class);
			
			TextOutputFormat.setOutputPath(job, outPath);

			if ( !job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

			abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
			
		    sqlServer.setBatchSize(10000);
		    sqlServer.setAutoCommit(false);

			jobId = abac.createJob(jobGrpId, 2, JOB_STEP2_DESC);
			updateSqlServer(outPath,daasConfig,ORDER_FILE_PREFIX,ORDER_TABLE_NAME,preprocesssqltype,compressOut);
			sqlServer.commit();
			abac.insertExecutionTargetFile(jobId, 1, ORDER_TABLE_NAME, "Order Table", "Table", totCnt);
			abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);

			jobId = abac.createJob(jobGrpId, 3, JOB_STEP3_DESC);
			updateSqlServer(outPath,daasConfig,ORDER_DETAIL_FILE_PREFIX,ORDER_DETAIL_TABLE_NAME,preprocesssqltype,compressOut);
			sqlServer.commit();
			abac.insertExecutionTargetFile(jobId, 1, ORDER_DETAIL_TABLE_NAME, "Order Detail Table", "Table", totCnt);
			abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);

			jobId = abac.createJob(jobGrpId, 4, JOB_STEP4_DESC);
			updateSqlServer(outPath,daasConfig,TIMINGS_FILE_PREFIX,TIMINGS_TABLE_NAME,preprocesssqltype,compressOut);
			sqlServer.commit();
			abac.insertExecutionTargetFile(jobId, 1, TIMINGS_TABLE_NAME, "Timings Table", "Table", totCnt);
			abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);

			jobId = abac.createJob(jobGrpId, 5, JOB_STEP5_DESC);
			updateSqlServer(outPath,daasConfig,MENU_FILE_PREFIX,MENU_TABLE_NAME,preprocesssqltype,compressOut);
			sqlServer.commit();
			abac.insertExecutionTargetFile(jobId, 1, MENU_TABLE_NAME, "Menu Table", "Table", totCnt);
			abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
			
			abac.closeJobGroup(jobGrpId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
			abac.dispose();
			
		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}		
	}
	
	private void updateSqlServer(Path outPath
			                    ,DaaSConfig daasConfig
			                    ,String filePrefix
			                    ,String tableName
			                    ,String preprocesssqltype
			                    ,boolean compressedOut) {
	
		FileStatus[] status;
		String line = "";
		String[] lineparts = null;
		int inscnt = 0;
		int rowCnt = 0;
		int fileCnt = 0;

		DataInputStream fileStatusStream = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		
		try {
		    totCnt = 0;
		    
		    if ( preprocesssqltype.equalsIgnoreCase("TRUNCATE") ) {
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.print("Truncate table "+ tableName + " ... ");
			    }
			    
			    sqlServer.executeUpdate(sqlStmt.get("truncate|" + tableName));
			    
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.println("done");
			    }
		    } else if ( preprocesssqltype.equalsIgnoreCase("DELETE") ) { 
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.print("Delete from table "+ tableName + " ... ");
			    }
			    
			    sqlServer.executeUpdate(sqlStmt.get("delete|" + tableName));
			    
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.println("done");
			    }
		    }
		    
		    sqlServer.setPreparedStatement(sqlStmt.get("insert|" + tableName));

			status = fileSystem.listStatus(outPath, new ResultListFilter(filePrefix + "-"));
			
			if ( status != null ) {
				for (int idx=0; idx < status.length; idx++ ) {

					fileCnt++;
					
					if ( compressedOut ) {
						isr=new InputStreamReader(new GZIPInputStream(fileSystem.open(status[idx].getPath())));
						br=new BufferedReader(isr);
					} else {
						fileStatusStream = new DataInputStream(fileSystem.open(status[idx].getPath()));
						br = new BufferedReader(new InputStreamReader(fileStatusStream));
					}
				
					line=br.readLine();
					while (line != null) {
						lineparts = line.split("\\t");
						rowCnt++;
						
						if ( tableName.equals(ORDER_TABLE_NAME) ) {
							inscnt = sqlServer.addBatch(Integer.parseInt(lineparts[0])
			 		                                   ,java.sql.Timestamp.valueOf(lineparts[1])
				                                       ,lineparts[2]
				                                       ,java.sql.Timestamp.valueOf(lineparts[3])
				                                       ,lineparts[4]
				                                       ,lineparts[5]
							                           ,lineparts[6]
	   						                           ,Float.parseFloat(lineparts[7])
	  						                           ,Float.parseFloat(lineparts[8])
					                                   );
							
						} else if ( tableName.equals(ORDER_DETAIL_TABLE_NAME) ) {
							inscnt = sqlServer.addBatch(java.sql.Date.valueOf(lineparts[0])
									                   ,java.sql.Timestamp.valueOf(lineparts[1])
	                                                   ,Integer.parseInt(lineparts[2])
                                                       ,lineparts[3]
                                                       ,lineparts[4]
                                                       ,lineparts[5]
                                                       ,lineparts[6]
			                                           ,java.sql.Timestamp.valueOf(lineparts[7])
			                                           ,Float.parseFloat(lineparts[8])
			                                           ,Integer.parseInt(lineparts[9])
			                                           ,lineparts[10]
			                                           ,lineparts[11]
			                                           ,Integer.parseInt(lineparts[12])
			                                           ,Integer.parseInt(lineparts[13])
			                                           ,Integer.parseInt(lineparts[14])
			                                           ,Integer.parseInt(lineparts[15])
			                                           ,Integer.parseInt(lineparts[16])
			                                           ,Integer.parseInt(lineparts[17])
			                                           ,Float.parseFloat(lineparts[18])
	                                                   );
							
						} else if ( tableName.equals(TIMINGS_TABLE_NAME) ) {
							inscnt = sqlServer.addBatch(Integer.parseInt(lineparts[0])
                                                       ,Integer.parseInt(lineparts[1])
                                                       ,Integer.parseInt(lineparts[2])
                                                       ,java.sql.Date.valueOf(lineparts[3])
                                                       ,lineparts[4]
                                                       ,Integer.parseInt(lineparts[5])
                                                       ,lineparts[6]
                                                       ,Integer.parseInt(lineparts[7])
                                                       ,Integer.parseInt(lineparts[8])
                                                       ,Integer.parseInt(lineparts[9])
                                                       ,Float.parseFloat(lineparts[10])
                                                       ,Float.parseFloat(lineparts[11])
                                                       ,Float.parseFloat(lineparts[12])
                                                       ,Float.parseFloat(lineparts[13])
                                                       ,Float.parseFloat(lineparts[14])
                                                       ,Float.parseFloat(lineparts[15])
                                                       ,Float.parseFloat(lineparts[16])
                                                       ,Float.parseFloat(lineparts[17])
                                                       ,Integer.parseInt(lineparts[18])
                                                       );
							
						} else if ( tableName.equals(MENU_TABLE_NAME) ) {
							inscnt = sqlServer.addBatch(Integer.parseInt(lineparts[0])
							 		                   ,lineparts[1]
	   							                       ,java.sql.Timestamp.valueOf(lineparts[2])
	  							                       ,java.sql.Timestamp.valueOf(lineparts[3])
	   							                       ,Integer.parseInt(lineparts[4])
	   							                       ,lineparts[5]
	   										           ,lineparts[6]
	   				   						           ,lineparts[7]
	   				  						           ,Float.parseFloat(lineparts[8])
	   				   						           ,lineparts[9]
	   				   						           ,lineparts[10]
	   		   				   				           ,lineparts[11]
	   		   	   				   			           ,lineparts[12]
	   		   	     				   		           ,lineparts[13]
	   		   	     	   				   	           ,lineparts[14]
									                   );
						}
						
						if ( inscnt > 0 ) {
							totCnt += inscnt;
							
							if ( daasConfig.displayMsgs() ) {
								System.out.println("row count " + totCnt);
							}
						}
						line=br.readLine();
					}

					br.close();
				}
				
			    totCnt = sqlServer.finalizeBatch();
				
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.println("row count " + totCnt + " read count = " + rowCnt + " file count = " + fileCnt);
			    }
			    
			    

				sqlServer.commit();
			}
			
		} catch (Exception ex) {
			System.err.println("Line=" + line);
			System.err.println("Error occured in MapReduce post-process:");
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
			System.err.println("Error occured in GenerateXMUSExtract.getVaildFilePaths:");
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
			System.err.println("Error occured in GenerateXMUSExtract.getVaildFilePaths:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
		return(retPaths);
	}
	
	private int createListDistCache(Path locationList
                                    ,String database) {

		int rowCnt = 0;
		
		try {
			StringBuffer sql = new StringBuffer();
			ResultSet rset;

			sql.setLength(0);
			sql.append("select\n");
			sql.append("   840 as TERR_CD\n"); 
			sql.append("  ,case LEN(cast(Store as varchar(12))) when 1 then '0000'\n");
			sql.append("                                        when 2 then '000'\n");
			sql.append("                                        when 3 then '00'\n");
			sql.append("                                        when 4 then '0'\n");
			sql.append("                                        else '' end + cast(Store as varchar(12)) as LGCY_LCL_RFR_DEF_CD\n");
			sql.append("from " + database + ".DM_ProjectStoreList with (NOLOCK)\n");
			
			rset = sqlServer.resultSet(sql.toString());

			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(locationList,true)));

			while ( rset.next() ) {
				bw.write(rset.getString("TERR_CD") + "\t" + rset.getString("LGCY_LCL_RFR_DEF_CD"));
				bw.write("\n");
				rowCnt++;
			}

			rset.close();
			
			bw.close();
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateOffersReport.createListDistCache:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}
		
		return(rowCnt);
		
	}
	
	private ArrayList<String> createHistListDistCache(Path locationList
                                                     ,String database) { 
		
		ArrayList<String> returnTerrDate = new ArrayList<String>();
		HashMap<String,String> uniqueDates = new HashMap<String,String>();
		Calendar caldt = Calendar.getInstance();
		SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");

		try {
			StringBuffer sql = new StringBuffer();
			ResultSet rset;
			
			sql.setLength(0);
			sql.append("select\n"); 
			sql.append("   '840' as TERR_CD\n");
			sql.append("  ,case LEN(cast(Store as varchar(12))) when 1 then '0000'\n");
			sql.append("                                        when 2 then '000'\n");
			sql.append("                                        when 3 then '00'\n");
			sql.append("                                        when 4 then '0'\n");
			sql.append("                                        else '' end + cast(Store as varchar(12)) as LGCY_LCL_RFR_DEF_CD\n");
			sql.append("  ,cast(year(StartDate) as int) as BEG_YYYY\n");
			sql.append("  ,cast(month(StartDate) as int)-1 as BEG_MM\n");
			sql.append("  ,cast(day(StartDate) as int) as BEG_DD\n");
			sql.append("  ,cast(DATEDIFF(day,StartDate,EndDate) as INT) as DATE_DIFF_QT\n"); 
			sql.append("  ,substring(cast(StartDate as varchar(10)),1,4) + substring(cast(StartDate as varchar(10)),6,2) + substring(cast(StartDate as varchar(10)),9,2) as BEG_DT_TEXT\n");
			sql.append("  ,substring(cast(EndDate as varchar(10)),1,4) + substring(cast(EndDate as varchar(10)),6,2) + substring(cast(EndDate as varchar(10)),9,2) as END_DT_TEXT\n");
			sql.append("from " + database + ".DM_HistoryStoreList with (NOLOCK)\n");
			sql.append("where StartDate <= EndDate\n");
			
			rset = sqlServer.resultSet(sql.toString());

			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(locationList,true)));

			while ( rset.next() ) {
				
				caldt.set(rset.getInt("BEG_YYYY"), rset.getInt("BEG_MM"), rset.getInt("BEG_DD"));
				
				for (int idx=0; idx <= rset.getInt("DATE_DIFF_QT"); idx++ ) {
					if ( !uniqueDates.containsKey(fmt.format(caldt.getTime())) ) {
						uniqueDates.put(fmt.format(caldt.getTime()), fmt.format(caldt.getTime()));
					}
					caldt.add(Calendar.DAY_OF_MONTH, 1);
				}

				bw.write(rset.getString("TERR_CD") + "\t" + rset.getString("LGCY_LCL_RFR_DEF_CD") + "\t" + rset.getString("BEG_DT_TEXT") + "\t" + rset.getString("END_DT_TEXT"));
				bw.write("\n");
			}

			rset.close();
			
			bw.close();
			
			int cnt = 0;
			String terrDate = "";
			
			for (Map.Entry<String, String> entry : uniqueDates.entrySet()) {
				cnt++;
				if ( cnt > 7 ) {
					returnTerrDate.add(terrDate);
					cnt = 1;
					terrDate = "";
				}

				if ( terrDate.length() > 0 ) {
					terrDate += ",";
				}
				terrDate += "840:" + entry.getValue();
			}


			if ( terrDate.length() > 0 ) {
				returnTerrDate.add(terrDate);
			}

				
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateOffersReport.createHistListDistCache:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}

		
		return(returnTerrDate);
	}
}

package com.mcd.gdw.daas.abac;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.RDBMS;
import com.mcd.gdw.daas.abac.ABaCUtility;

public class ABaCRegister {

	public class FileFilterByIsoAndDate implements FileFilter {
	
		private String prefix;
		private String iso_code;
		private int iso_code_pos;
		private int date_pos;
		
		private String[] dates;
		
		public FileFilterByIsoAndDate(String prefix
				                     ,String iso_code
				                     ,int iso_code_pos
				                     ,String date
				                     ,int date_pos) {
			
			this.prefix = prefix;
			this.iso_code = iso_code;
			this.iso_code_pos = iso_code_pos;
			this.dates = date.split(",");
			this.date_pos = date_pos;
			
		}
		
	    @Override
		public boolean accept(File f) {
			boolean pass = false;
			
			if ( f.isFile() ) {
				if ( !f.getName().toLowerCase().endsWith(".part") ) {
					if ( f.getName().toLowerCase().equals("~stop") ) {
						pass= true;
					} else {
						if ( f.getName().startsWith(prefix) ) {
							String[] parts = f.getName().split("\\.");
							String[] subParts = parts[0].split("\\_");
							
							if ( subParts.length >= iso_code_pos && subParts.length >= date_pos ) {
								
								if ( this.dates[0].equals("*") ) {
									if ( (subParts[iso_code_pos-1].equals(this.iso_code) || this.iso_code.equals("*") ) )  {
										pass = true;
									}
								} else {
									for (int idx=0; idx < dates.length; idx++ ) {
										
										if ( (subParts[iso_code_pos-1].equals(this.iso_code) || this.iso_code.equals("*") )  &&  this.dates[idx].indexOf(subParts[date_pos-1]) >= 0) {
											pass = true;
										}
									}
								}
							}
						}
					}
				}
			}
			
			return(pass);
		}
 
	}
	
	public class RegistryThread implements Runnable {

		private final BigDecimal ZERO = new BigDecimal("0");
		
		@SuppressWarnings("unused")
		private final String STATUS_UNKNOWN = "UNKNOWN";
		@SuppressWarnings("unused") 
		private final String STATUS_ARRIVED = "ARRIVED";
		private final String STATUS_VALIDATED = "VALIDATED";
		@SuppressWarnings("unused")
		private final String STATUS_READY = "READY";
		@SuppressWarnings("unused")
		private final String STATUS_PROCESSING = "PROCESSING";
		@SuppressWarnings("unused")
		private final String STATUS_SUCCESSFUL = "SUCCESSFUL";
		private final String STATUS_REJECTED = "REJECTED";
		@SuppressWarnings("unused")
		private final String STATUS_FAILED = "FAILED";
		@SuppressWarnings("unused")
		private final String STATUS_REPROCESS = "REPROCESS";
		@SuppressWarnings("unused")
		private final String STATUS_HOLD_VALIDATED = "HOLD - VALIDATED";

		@SuppressWarnings("unused")
		private final String REASON_BUS_TS_CTNT_FN_DATA_MISMATCH = "BUS_TS_CTNT_FN_DATA_MISMATCH";
		@SuppressWarnings("unused")
		private final String REASON_CTRY_CD_FN_DATA_MISMATCH = "CTRY_CD_FN_DATA_MISMATCH";
		private final String REASON_CTRY_ISO_NOT_FOUND = "CTRY_ISO_NOT_FOUND";
		private final String REASON_CTRY_STR_NOT_FOUND = "CTRY_STR_NOT_FOUND";
		private final String REASON_DUPLICATE_FILE = "DUPLICATE_FILE";
		private final String REASON_INVALID_APP = "INVALID_APP";
		@SuppressWarnings("unused")
		private final String REASON_MULT_BUS_TS = "MULT_BUS_TS";
		@SuppressWarnings("unused")
		private final String REASON_MULT_CTRY_ISO = "MULT_CTRY_ISO";
		@SuppressWarnings("unused")
		private final String REASON_MULT_STR_ID = "MULT_STR_ID";
		@SuppressWarnings("unused")
		private final String REASON_MULT_UPD_TS = "MULT_UPD_TS";
		private final String REASON_NO_BUS_TS = "NO_BUS_TS";
		@SuppressWarnings("unused")
		private final String REASON_NO_CTRY_ISO = "NO_CTRY_ISO";
		private final String REASON_NO_STR_ID = "NO_STR_ID";
		private final String REASON_NO_UPD_TS = "NO_UPD_TS";
		@SuppressWarnings("unused")
		private final String REASON_STR_CTNT_FN_DATA_MISMATCH = "STR_CTNT_FN_DATA_MISMATCH";
		@SuppressWarnings("unused")
		private final String REASON_UNKNOWN_FILE_TYPE = "UNKNOWN_FILE_TYPE";
		@SuppressWarnings("unused")
		private final String REASON_ETL_REJ = "ETL_REJ";
		@SuppressWarnings("unused")
		private final String REASON_FILE_BUS_DT_FUTURE_DT = "FILE_BUS_DT_FUTURE_DT";
		@SuppressWarnings("unused")
		private final String REASON_NON_MCOPCO = "NON-MCOPCO";
		@SuppressWarnings("unused")
		private final String REASON_INTERBATCH_DUPLICATE = "INTERBATCH DUPLICATE";
		private final String REASON_CORRUPTED_COMPRESSED_FORMAT = "CORRUPTED COMPRESSED FORMAT";
		@SuppressWarnings("unused")
		private final String REASON_MALFORMED_XML = "MALFORMED XML";
		private final String REASON_MISSING = "MISSING";
		@SuppressWarnings("unused")
		private final String REASON_LATE_ARRIVAL = "LATE ARRIVAL";
		
		private final CountDownLatch startSignal;
		private final CountDownLatch doneSignal;
		private String desc = null;
		private Thread registryThread;
		private ABaCFiles files;
		private String fileProcessedPath;
		private RDBMS db;
		private FileSystem hdfsFileSystem;
		private DaaSConfig daasConfig;
		private String abacRootDir;
		private Map<String,Integer> countryCodes;
		private Map<String,BigDecimal> lcat;
		private Map<String,Short> stus;
		private Map<String,Short> resn;
		private int successCount;
		private int rejectCount;
		private int errorCount;
		//AWS START
		private int hdfsFileFailureCnt;
		private int hdfsTotalRetryCnt;
		//AWS END 
		private double totalFileSize;
		
		private StringBuffer sql = new StringBuffer();
		
		public RegistryThread(String desc
				             ,CountDownLatch startSignal
                             ,CountDownLatch doneSignal
                             ,ABaCFiles files
                             ,String fileProcessedPath
                             ,RDBMS db
                             ,FileSystem hdfsFileSystem
                             ,DaaSConfig daasConfig
                             ,Map<String,Integer> countryCodes
                             ,Map<String,BigDecimal> lcat
                             ,Map<String,Short> stus
                             ,Map<String,Short> resn) throws Exception {
			
			this.desc = desc;
			this.startSignal = startSignal;
			this.doneSignal = doneSignal;
			this.files = files;
			this.fileProcessedPath = fileProcessedPath;
			this.db = db;
			this.hdfsFileSystem = hdfsFileSystem;
			this.daasConfig = daasConfig;
			this.countryCodes = countryCodes;
			this.lcat = lcat;
			this.stus = stus;
			this.resn = resn;
			this.successCount = 0;
			this.rejectCount = 0;
			this.errorCount = 0;
			//ASW START
			this.hdfsFileFailureCnt = 0;
			this.hdfsTotalRetryCnt = 0;
			//ASW END
			this.totalFileSize = 0;
			
			this.abacRootDir = this.daasConfig.hdfsRoot() + Path.SEPARATOR + this.daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + this.daasConfig.hdfsLandingZoneArrivalSubDir();
			
			this.db.setAutoCommit(false);

			RDBMS.MultiPreparedStatementSql dwFile;
			RDBMS.MultiPreparedStatementSql dwRjctResn;

			sql.setLength(0);
			sql.append("insert into " + this.daasConfig.abacSqlServerDb() + ".dw_file\n");
			sql.append("  (DW_FILE_NA\n");
			sql.append("  ,FILE_INCM_OUTG_CD\n");
			sql.append("  ,FILE_MKT_OGIN_TS\n");
			sql.append("  ,FILE_DW_ARRV_TS\n");
			sql.append("  ,MCD_GBAL_LCAT_ID_NU\n");
			sql.append("  ,TERR_CD\n");
			sql.append("  ,LGCY_LCL_RFR_DEF_CD\n");
			sql.append("  ,FILE_SIZE_NU\n");
			sql.append("  ,FILE_PATH_DS\n");
			sql.append("  ,CAL_DT\n");
			sql.append("  ,DW_DATA_TYP_ID\n");
			sql.append("  ,DW_AUDT_STUS_TYP_ID)\n");
			sql.append("values\n");
			sql.append("  (?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?\n");
			sql.append("  ,?)\n;");

			//db.setPreparedStatement(sql.toString());
			
			dwFile = db.new MultiPreparedStatementSql("dwfile", sql);

			sql.setLength(0);
			sql.append("insert into " + this.daasConfig.abacSqlServerDb() + ".dw_file_rjct_resn_assc\n");
			sql.append("  (DW_FILE_ID\n");
			sql.append("  ,DW_AUDT_RJCT_RESN_ID)\n");
			sql.append("values\n");
			sql.append("  (?\n");
			sql.append("  ,?)\n");
			
			dwRjctResn = db.new MultiPreparedStatementSql("dwRjctResn", sql);
			
			db.multiSetPreparedStatement(dwFile,dwRjctResn);
			
			//db.multiSetPreparedStatement(sql);
			
			registryThread = new Thread(this, desc);
			registryThread.start();
			
		}

		public String getDesc() {
			return(this.desc);
		}
		
		public int getSuccessCount() {
			return(this.successCount);
		}
		
		public int getRejectCount() {
			return(this.rejectCount);
		}
		
		public int getErrorCount() {
			return(this.errorCount);
		}
		
		//AWS START
		public int getHdfsFileFailureCount() {
			return(this.hdfsFileFailureCnt);
		}

		public int getHdfsTotalRetryCount() {
			return(this.hdfsTotalRetryCnt);
		}
		//AWS END
		
		public double getTotalFileSize() {
			return(this.totalFileSize);
		}
		
		public void run() {

			try {
				startSignal.await();

				processFiles();
					
				doneSignal.countDown();
				db.dispose();
			} catch (Exception ex) {
				ex.printStackTrace(System.err);
			}

		}
		
		private void processFiles() throws Exception {
			
			String countryIsoAbbr2Code = "";
			int countryIsoCode = 0;
			BigDecimal glin = ZERO;
			String localId;
			String lcatKey;
			ResultSet rset;
			int id;
			short fileStatus;
			ArrayList<Short> fileRejectReasons = new ArrayList<Short>();
			File fileSource;
			File fileSourceDest;
			boolean fileSourceExists;
			Path destPath = null;
			FsPermission setNewPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_EXECUTE);
			String fileName;
			short dataType = -1;
			java.sql.Timestamp mktTs;
			java.sql.Date calDt;
			Calendar dt = Calendar.getInstance();
			int currSuccessCount=0;
			int currRejectCount=0;
			boolean continueFl;
			
			for (ABaCFile file : this.files ) {
				
				fileStatus = stus.get(STATUS_VALIDATED);
				fileRejectReasons.clear();

				if ( file.getDataType() == null ) {
					fileStatus = stus.get(STATUS_REJECTED);
					fileRejectReasons.add(resn.get(REASON_INVALID_APP));
					dataType = 0;
				} else {
					dataType = (short)file.getDataType().getDataTypeId();
				}
				
				try {
					try {
						countryIsoAbbr2Code = file.getCountryIsoAbbr2Code();
						
						if ( this.countryCodes.containsKey(countryIsoAbbr2Code) ) {
							countryIsoCode = this.countryCodes.get(countryIsoAbbr2Code);
						} else {
							countryIsoCode = 0;
							fileStatus = stus.get(STATUS_REJECTED);
							fileRejectReasons.add(resn.get(REASON_CTRY_ISO_NOT_FOUND));
						}
					} catch (Exception ex) {
						countryIsoCode = 0;
						fileStatus = stus.get(STATUS_REJECTED);
						fileRejectReasons.add(resn.get(REASON_CTRY_ISO_NOT_FOUND));
					}
					
					localId = "";
					
					try {
						localId = file.getRestaurantLocalIdentifer();
					} catch (Exception ex) {
						localId = "";
						fileStatus = stus.get(STATUS_REJECTED);
						fileRejectReasons.add(resn.get(REASON_NO_STR_ID));
					}
					
					glin = ZERO;
					
					if ( countryIsoCode > 0 && localId.length() > 0 ) {
						try {
							localId = file.getRestaurantLocalIdentifer();
							
							if ( countryIsoCode == 840 && localId.length() < 5 ) {
								localId = ("0000".substring(0,5-localId.length())) + localId; 
							}
							
							lcatKey = String.valueOf(countryIsoCode) + "_" + localId;
							
							if ( lcat.containsKey(lcatKey) ) {
								glin = lcat.get(lcatKey);
							} else {
								
							}
							
						} catch (Exception ex) {
							fileStatus = stus.get(STATUS_REJECTED);
							fileRejectReasons.add(resn.get(REASON_CTRY_STR_NOT_FOUND));
						}
					}
					
					try {
						calDt = file.getBusinessDateAsDate();
					} catch (Exception ex) {
						dt.set(1955, 03, 15);
						calDt = new java.sql.Date(dt.getTimeInMillis());
						fileStatus = stus.get(STATUS_REJECTED);
						fileRejectReasons.add(resn.get(REASON_NO_BUS_TS));
					}
					
					fileSourceExists = false;
					fileSource = new File(file.getFilePath() + File.separator + file.getFileName());
					fileSourceDest = new File(this.fileProcessedPath + File.separator + file.getFileName());
					if ( fileSource.exists() ) {
						fileSourceExists = true;
						if ( file.getFileName().toLowerCase().endsWith("zip") ) {
							if ( !validateZip(fileSource) ) {
								fileStatus = stus.get(STATUS_REJECTED);
								fileRejectReasons.add(resn.get(REASON_CORRUPTED_COMPRESSED_FORMAT));
							}
						}
					} else {
						fileStatus = stus.get(STATUS_REJECTED);
						fileRejectReasons.add(resn.get(REASON_MISSING));
					}

					try {
						mktTs = file.getMarketTimestamp();
					} catch (Exception ex) {
						mktTs = file.getFileTimestamp();
						fileStatus = stus.get(STATUS_REJECTED);
						fileRejectReasons.add(resn.get(REASON_NO_UPD_TS));
					}
					
	    			fileName = file.getFileName();
	    			continueFl = true;
	    			
					try {
						db.multiExecute("dwfile"
						  		       ,fileName
						               ,"I"
						               ,mktTs
						               ,file.getFileTimestamp()
						               ,glin
				                       ,(short)countryIsoCode
				                       ,localId
				                       ,(float)file.getFileLength()
				                       ,file.getFilePath()
				                       ,calDt
				                       ,dataType
				                       ,fileStatus);
					} catch (Exception ex) {
						if (ex.toString().toLowerCase().indexOf("cannot insert duplicate key row") >=0 ) {
							String uuid = UUID.randomUUID().toString();
							fileName = file.getFileName() + "(" + uuid + ")";
							//System.out.println("NEW NAME="+fileName);
	
							fileStatus = stus.get(STATUS_REJECTED);
							fileRejectReasons.add(resn.get(REASON_DUPLICATE_FILE));
							
							if ( fileName.length() > 80 ) {
								fileName = fileName.substring(0, 80);
							}
							
							try {
								db.multiExecute("dwfile"
							  		       ,fileName
							               ,"I"
							               ,mktTs
							               ,file.getFileTimestamp()
							               ,glin
					                       ,(short)countryIsoCode
					                       ,localId
					                       ,(float)file.getFileLength()
					                       ,file.getFilePath()
					                       ,calDt
					                       ,dataType
					                       ,fileStatus);
								
							} catch (Exception ex1) {
								continueFl = false;
								System.err.println(file.getFileName() + " SQL FAIL");
								ex.printStackTrace(System.err);
								this.errorCount++;
							}
						} else {
							continueFl = false;
							System.err.println(file.getFileName() + " SQL FAIL");
							ex.printStackTrace(System.err);
							this.errorCount++;
						}
					}

					if ( continueFl ) {
						rset = db.resultSet("SELECT @@IDENTITY as DW_FILE_ID");
		    			rset.next();
		    			id = rset.getInt("DW_FILE_ID");  
		    			rset.close();
						
		    			if ( fileRejectReasons.size() > 0 ) {
		    				for (short fileRejectReason : fileRejectReasons ) {
				    			db.multiExecute("dwRjctResn"
				    					   ,id
				    					   ,fileRejectReason);
		    				}
		    			}
		    			
						try {
							if ( fileSourceExists ) {

								if ( fileRejectReasons.size() == 0 ) {
									destPath = new Path(this.abacRootDir + Path.SEPARATOR + file.getFileName());
									currSuccessCount=1;
									currRejectCount=0;
								} else {
									destPath = new Path(this.abacRootDir + Path.SEPARATOR + "reject" + Path.SEPARATOR + fileName);
									//System.out.println("DEST PATH="+destPath.toString());
									currSuccessCount=0;
									currRejectCount=1;
								}
								
								//AWS START
								boolean moveSuccess = false;
								int failCnt = 0;
								
								while ( !moveSuccess && failCnt < 4) {
									try {
										hdfsFileSystem.copyFromLocalFile(false, true, new Path(fileSource.getAbsolutePath()), destPath);
										moveSuccess = true;
									} catch (Exception ex1) {
										if ( failCnt == 0 ) {
											this.hdfsFileFailureCnt++;
										}
										this.hdfsTotalRetryCnt++;
										failCnt++;
										Thread.sleep(300);
									}
								}
								if ( !moveSuccess ) {
									hdfsFileSystem.copyFromLocalFile(false, true, new Path(fileSource.getAbsolutePath()), destPath);
								}
								//AWS END 
								hdfsFileSystem.setPermission(destPath, setNewPermission);
								
								if ( fileSource.renameTo(fileSourceDest) ) {
									db.commit();
									this.successCount+=currSuccessCount;
									this.rejectCount+=currRejectCount;
									this.totalFileSize+=file.getFileLength();
								} else {
									System.err.println(file.getFileName() + " UNIX FILESYSTEM DELETE FAIL");
									db.rollback();
									this.errorCount++;
								}
							}
						} catch (Exception ex) {
							db.rollback();
							System.err.println(file.getFileName() + " HDFS FAIL");
							System.err.println(ex.getMessage());
							ex.printStackTrace(System.err);
							this.errorCount++;
						}
					}
				} catch (Exception ex) {
					System.err.println(file.getFileName() + " SQL FAIL");
					ex.printStackTrace(System.err);
					this.errorCount++;
				}

			}
		}
		
		private boolean validateZip(File zipFile) {
			
			boolean valid = false; 
			
			try {
				ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
				ZipEntry ze = zis.getNextEntry();

				while(ze!=null) {

	    			zis.closeEntry();
					
		    		ze = zis.getNextEntry();
				}

				zis.closeEntry();
				zis.close();
				
				valid = true; 
			} catch (Exception ex) {
			}

			return(valid);
		}

	}
	
	private static final String CORE_SITE_XML_FILE = "core-site.xml";
	private static final String HDFS_SITE_XML_FILE = "hdfs-site.xml";
	private static final String MAPRED_SITE_XML_FILE = "mapred-site.xml";

	private ABaCDataTypes dataTypes = null;
	private Map<String,Integer> countryCodes = null;
	private Map<String,BigDecimal> lcat = null;
	private Map<String,Short> stus = null;
	private Map<String,Short> resn = null;
	private RDBMS db = null;
	private StringBuffer sql = new StringBuffer();
	
	private long startTime;
	private long endTime;

	private long lastInitStartTime;
	private long lastInitEndTime;
	
	private Configuration hdfsConfig = null;
	   
    private FileSystem hdfsFileSystem = null;

    private Logger logger = Logger.getLogger(ABaCRegister.class.getName());
	private FileHandler loggerFile;
	private FileHandler loggerFile2;
	
	private long maxMemory;
	private long minMemory;

	//This custom formatter formats parts of a log record to a single line
	class MyHtmlFormatter extends Formatter {
	  // This method is called for every log records
	  public String format(LogRecord rec) {
	    StringBuffer buf = new StringBuffer(1000);
	    // Bold any levels >= WARNING
	    buf.append("<tr>");
	    buf.append("<td>");

	    if (rec.getLevel().intValue() >= Level.WARNING.intValue()) {
	      buf.append("<b>");
	      buf.append(rec.getLevel());
	      buf.append("</b>");
	    } else {
	      buf.append(rec.getLevel());
	    }
	    buf.append("</td>");
	    buf.append("<td>");
	    buf.append(calcDate(rec.getMillis()));
	    buf.append("</td><td>");
	    buf.append(formatMessage(rec));
	    buf.append("</td>\n");
	    buf.append("<td>");
	    buf.append("</tr>\n");
	    return buf.toString();
	  }

	  private String calcDate(long millisecs) {
	    SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    java.util.Date resultdate = new java.util.Date(millisecs);
	    return date_format.format(resultdate);
	  }

	  // This method is called just after the handler using this
	  // formatter is created
	  public String getHead(Handler h) {
	    return "<HTML>\n<HEAD>\n" + (new java.util.Date()) 
	        + "\n</HEAD>\n<BODY>\n<PRE>\n"
	        + "<table width=\"100%\" border=\"1\">\n  "
	        + "<tr><th>Level</th>" +
	        "<th>Time</th>" +
	        "<th>Log Message</th>" +
	        "</tr>\n";
	  }

	  // This method is called just after the handler using this
	  // formatter is closed
	  public String getTail(Handler h) {
	    return "</table>\n  </PRE></BODY>\n</HTML>\n";
	  }
	} 

	//This custom formatter formats parts of a log record to a single line
	class MySingleLineFormatter extends Formatter {
	  // This method is called for every log records
	  public String format(LogRecord rec) {
		  StringBuffer buf = new StringBuffer(1000);
		  buf.append(rec.getLevel());
	      buf.append("\t");
	      buf.append(calcDate(rec.getMillis()));
	      buf.append("\t");
	      buf.append(formatMessage(rec));
	      buf.append("\n");
	      return buf.toString();
	  }

	  private String calcDate(long millisecs) {
		  SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		  java.util.Date resultdate = new java.util.Date(millisecs);
		  return date_format.format(resultdate);
	  }

	  // This method is called just after the handler using this
	  // formatter is created
	  public String getHead(Handler h) {
		  return("Level\tTime\tLog Message\n");
	  }

	  // This method is called just after the handler using this
	  // formatter is closed
	  public String getTail(Handler h) {
		  return("****   END OF LOG   ****\n");
	  }
	} 

	public static void main(String[] args) {

		int threadCount = 0;
		
		ABaCRegister instance = new ABaCRegister();
		
		if ( args.length < 4 ) {
			System.err.println("Missing parameters");
			System.exit(8);
		}
		
		if ( !new File(args[0]).isFile() ) {
			System.err.println("Not able to find config XML file " + args[0]);
			System.exit(8);
		}
		
		if ( !new File(args[1]).isDirectory() ) {
			System.err.println("Not able to find source file directory " + args[1]);
			System.exit(8);
		}
		
		if ( !new File(args[2]).isDirectory() ) {
			System.err.println("Not able to find source file directory " + args[2]);
			System.exit(8);
		}
		
		try {
			threadCount = Integer.parseInt(args[3]);
			
			if ( threadCount < 1 || threadCount > 100 ) {
				System.err.println("Invalid thread count " + threadCount + " Must be an integer between 1 and 100");
				System.exit(8);
			}
		} catch (Exception ex) {
			System.err.println("Invalid thread count " + args[3]);
			System.exit(8);
		}
		
		String dateFilter = "*";
		
		if ( args.length >=5 ) {
			dateFilter = args[4];
		}
		
		String isoCode = "*";
		if ( args.length >= 6 ) {
			isoCode = args[5];
		}
		
		instance.run(args[0],args[1],args[2],threadCount,dateFilter,isoCode);
		
	}

	private void run(String configFile
			        ,String path
			        ,String processedPath
			        ,int threadCount
			        ,String pattern
			        ,String isoCode) {
		
		DaaSConfig daasConfig = new DaaSConfig(configFile);
		
		if ( !daasConfig.configValid() ) {
			System.err.println("Invalid XML config file " + configFile);
			System.exit(8);
		}
		
		if ( daasConfig.displayMsgs() ) {
			System.out.println(daasConfig.toString());
		}
		
		maxMemory = getAvailableMemory();
		minMemory = getAvailableMemory();
		
		CountDownLatch startSignal;
		CountDownLatch doneSignal;
		int threadIdx;
		boolean more = true;
		File dir = new File(path);
		File[] fileList;
		int idx;
		int useThreadCount = 0;
		long processStartTime = System.currentTimeMillis();
		int totalFilesProcessed = 0;
		int totalSuccessCount=0;
		int totalRejectCount=0;
		int totalErrorCount=0;
		int totalExecuteCount=0;
		//AWS START
		int totalHdfsFileFailureCount=0;
		int totalHdfsRetryCount=0;
		boolean checkFlag;
		int minFileCount = 30;
		//AWS END 
		double totalExecuteTime=0;
		double totalFileSize=0;
		long currMemory=0;
		
		try {
	        loggerFile = new FileHandler("log.htm");  
	        logger.addHandler(loggerFile);
//	        SimpleFormatter formatter = new SimpleFormatter();
	        MyHtmlFormatter formatter = new MyHtmlFormatter();
	        loggerFile.setFormatter(formatter);

	        loggerFile2 = new FileHandler("log.txt");  
	        logger.addHandler(loggerFile2);
	        MySingleLineFormatter formatter2 = new MySingleLineFormatter();
	        loggerFile2.setFormatter(formatter2);
	        
	        ConsoleHandler consoleHandler = new ConsoleHandler();
	        MySingleLineFormatter formatter3 = new MySingleLineFormatter();
	        consoleHandler.setFormatter(formatter3);
	        logger.addHandler(consoleHandler);

	        
	        logger.setUseParentHandlers(false);
	        
		} catch (Exception ex) {
			System.err.println("Logger setup failed");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
		logger.log(Level.INFO, "Process started");
		
		RegistryThread[] registryThread = new RegistryThread[threadCount]; 

		startTime = System.currentTimeMillis();

		initHDFS(daasConfig);
		
		logger.log(Level.INFO, "HDFS initialize complete");

		lastInitStartTime = System.currentTimeMillis();

		ABaCUtility.UpdateABaCReference(daasConfig);
		logger.log(Level.INFO, "Updated ABaC Reference Data from Source");

		initReference(daasConfig);
		
		logger.log(Level.INFO, "Reference initialize complete");
		
		endTime = System.currentTimeMillis();

		logger.log(Level.INFO, "Total initialize time = " + (double)(endTime-startTime)/1000d+" seconds");
		
		
		while ( more ) {

			lastInitEndTime = System.currentTimeMillis();
		
			if ( ((double)(lastInitEndTime-lastInitStartTime)/1000d/60d) > 180d ) {

				logger.log(Level.INFO, "Reinitializing reference data");

				lastInitStartTime = System.currentTimeMillis();

				ABaCUtility.UpdateABaCReference(daasConfig);
				logger.log(Level.INFO, "Updated ABaC Reference Data from Source");

				initReference(daasConfig);
				
				logger.log(Level.INFO, "Reinitializing reference data complete");
			}
			
			/*
			fileList = dir.listFiles(new FileFilter() {
			    @Override
			    public boolean accept(File pathname) {
			    	return(pathname.isFile());
			    }
			});
			*/
			
			
			fileList = dir.listFiles(new FileFilterByIsoAndDate("POS_XML",isoCode,3,pattern,5));

			System.out.println("Filtered " + fileList.length + " files");
			
			idx=0;
			while ( idx < fileList.length && more ) {
				if ( fileList[idx].getName().toLowerCase().equals("~stop") ) {
					more = false;
					if ( !fileList[idx].delete() ) {
						System.err.println("Unable to delete ~STOP signal file, stopping");
		        		logger.log(Level.SEVERE, "Unable to delete ~STOP signal file.  Aborting process.");
						loggerFile.flush();
						loggerFile.close();
						System.exit(8);
					}
				}
				idx++;
			}

			if ( more && fileList.length > 0 ) {
				startTime = System.currentTimeMillis();
				
		        try {
		    		
		    		useThreadCount = 0; 
		    		
		    		if ( fileList.length > 0 ) {
		    			totalFilesProcessed+= fileList.length;
		    			
		    			//AWS START
		    			checkFlag = true;
		    			useThreadCount = threadCount; 
		    			while ( checkFlag ) {
		    				if ( useThreadCount == 1 || ((float)fileList.length / (float)useThreadCount) >= minFileCount ) {
		    					checkFlag = false;
		    				} else {
		    					useThreadCount--;
		    				}
		    			}
		    			
		    			/*
		        		if ( (((float)fileList.length) / ((float)threadCount)) < 30 ) {
		        			useThreadCount = 1;
		        		} else {
		        			useThreadCount = threadCount;
		        		}
		        		*/
		    			//AWS END 
		    			
		        		totalExecuteCount++;
		        		
		        		//System.out.println("Use thread Count = " + useThreadCount + " for " + fileList.length + " file(s)");
		        		
		            	startSignal = new CountDownLatch(1);
		    	    	doneSignal = new CountDownLatch(useThreadCount);

		    	    	for ( threadIdx=0; threadIdx < useThreadCount; threadIdx++ ) {
		    	    		RDBMS db = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(), daasConfig.abacSqlServerUserId(), daasConfig.abacSqlServerPassword()); 
		    	        	registryThread[threadIdx] = new RegistryThread("RT"+(threadIdx+1), startSignal, doneSignal, new ABaCFiles(fileList, (threadIdx+1),useThreadCount,dataTypes), processedPath, db, hdfsFileSystem, daasConfig, countryCodes, lcat, stus, resn);
		    	        }
		    	    	
		    	    	startSignal.countDown();
		    			doneSignal.await();
		        		
		    	    	for ( threadIdx=0; threadIdx < useThreadCount; threadIdx++ ) {
		    	    		totalSuccessCount+=registryThread[threadIdx].getSuccessCount();
		    	    		totalRejectCount+=registryThread[threadIdx].getRejectCount();
		    	    		totalErrorCount+=registryThread[threadIdx].getErrorCount();
		    	    		totalFileSize+=registryThread[threadIdx].getTotalFileSize();
		    	    		//AWS START
		    	    		totalHdfsFileFailureCount+=registryThread[threadIdx].getHdfsFileFailureCount();
		    	    		totalHdfsRetryCount+=registryThread[threadIdx].getHdfsTotalRetryCount();
		    	    		//AWS END
		    	    	}
		    			
		    	    	if ( totalErrorCount > 0 ) {
		    	    		more=false;
		    	    	}
		    	    	
		    			endTime = System.currentTimeMillis();
		    		} else {
		    			System.out.println("Empty List");
		    		}
		    		
					//System.out.println("Time = "+(double)(endTime-startTime)/1000d+" seconds");

	        		logger.log(Level.INFO, "Executed using thread count = " + useThreadCount + " for " + fileList.length + " file(s). Time = "+(double)(endTime-startTime)/1000d+" seconds");
	        		loggerFile.flush();
	        		totalExecuteTime+=(double)(endTime-startTime);
	        		
		        } catch (Exception ex) {
		        	ex.printStackTrace(System.err);
		        	logger.log(Level.SEVERE, "Exception occurred: "+ ex.toString());
					loggerFile.flush();
					loggerFile.close();
		        	System.exit(8);
		        }
			}
			
			currMemory = getAvailableMemory();
			
			if ( currMemory > maxMemory ) {
				maxMemory = currMemory;
			}
			
			if ( currMemory < minMemory ) {
				minMemory = currMemory;
			}
			
			System.gc();

			//AWS START
			//more = false;
			//AWS END
			
			if ( more ) {
				try {
					Thread.sleep(5000);
				} catch (Exception ex) {
					ex.printStackTrace(System.err);
		        	logger.log(Level.SEVERE, "Exception occurred: "+ ex.toString());
					loggerFile.flush();
					loggerFile.close();
					System.exit(8);
				}
			}
		}
		
		double totalProcessTime = System.currentTimeMillis() - processStartTime;
		double totalRunSeconds = Math.rint(totalProcessTime / 1000d);
		int runDays = (int)Math.floor(totalRunSeconds / (24d * 60d * 60d));
		int runHours = (int)Math.floor((totalRunSeconds - ((double)runDays * 24d * 60d * 60d)) / (60d * 60d)); 
		int runMinutes = (int)Math.floor((totalRunSeconds - ((double)runDays * 24d * 60d * 60d) - ((double)runHours * 60d * 60d)) / 60d);
		int runSeconds = (int)(totalRunSeconds - ((double)runDays * 24d * 60d * 60d) - ((double)runHours * 60d * 60d) - ((double)runMinutes * 60d));
		
		logger.log(Level.INFO, "Process ended:");
		logger.log(Level.INFO, "--Total Execute Count = " + totalExecuteCount + " time(s)");
		logger.log(Level.INFO, "--Total Files Processed = " + totalFilesProcessed);
		logger.log(Level.INFO, "--Success File Count = " + totalSuccessCount);
		logger.log(Level.INFO, "--Reject File Count = " + totalRejectCount);
		logger.log(Level.INFO, "--Error Count = " + totalErrorCount);
		logger.log(Level.INFO, "--Total HDFS Put File With Exception(s) Count = " + totalHdfsFileFailureCount);
		logger.log(Level.INFO, "--Total HDFS Put File With Exception Retry Count = " + totalHdfsRetryCount);
		logger.log(Level.INFO, "--Total File Size = " + String.format("%,15.0f", totalFileSize) + " (" + String.format("%,10.2f",(totalFileSize/(1024d*1024d))) + " MB)");
		logger.log(Level.INFO, "--Run Time = " + runDays + " day(s) " + runHours + " hour(s) " + runMinutes + " minute(s) " + runSeconds + " second(s)");
		logger.log(Level.INFO, "--Total Execute Time = " + (totalExecuteTime/1000d) + " seconds");
		logger.log(Level.INFO, "--Max Available Memory During Execution = " + String.format("%,10.2f",((double)maxMemory/(1024d * 1024d))) + " MB");
		logger.log(Level.INFO, "--Min Available Memory During Execution = " + String.format("%,10.2f",((double)minMemory/(1024d * 1024d))) + " MB");
		loggerFile.flush();
		loggerFile.close();
	}
	
	public void initHDFS(DaaSConfig daasConfig) {
	
		try {

	    	boolean hdfsConfigXmlFound=true;
	    	String hdfsConfigXmlFile = null;
	    	
	    	hdfsConfig = new Configuration();

	    	hdfsConfigXmlFile = daasConfig.hdfsConfigDir() + File.separator + CORE_SITE_XML_FILE;
	    	if ( new File(hdfsConfigXmlFile).isFile() ) {
	    		hdfsConfig.addResource(new Path(hdfsConfigXmlFile));
	    		System.out.println("ADDED: " + hdfsConfigXmlFile);
	    	} else {
				logger.log(Level.SEVERE, "Hadoop Config File (XML) " + hdfsConfigXmlFile + " not found.");
	    		hdfsConfigXmlFound=false;
	    	}
	    	
	    	hdfsConfigXmlFile = daasConfig.hdfsConfigDir() + File.separator + HDFS_SITE_XML_FILE;
	    	if ( new File(hdfsConfigXmlFile).isFile() ) {
	    		hdfsConfig.addResource(new Path(hdfsConfigXmlFile));
	    		System.out.println("ADDED: " + hdfsConfigXmlFile);
	    	} else {
				logger.log(Level.SEVERE, "Hadoop Config File (XML) " + hdfsConfigXmlFile + " not found.");
	    		hdfsConfigXmlFound=false;
	    	}
	    	
	    	hdfsConfigXmlFile = daasConfig.hdfsConfigDir() + File.separator + MAPRED_SITE_XML_FILE;
	    	if ( new File(hdfsConfigXmlFile).isFile() ) {
	    		hdfsConfig.addResource(new Path(hdfsConfigXmlFile));
	    		System.out.println("ADDED: " + hdfsConfigXmlFile);
	    	} else {
				logger.log(Level.SEVERE, "Hadoop Config File (XML) " + hdfsConfigXmlFile + " not found.");
	    		hdfsConfigXmlFound=false;
	    	}
	    	
	    	hdfsConfigXmlFile = daasConfig.hdfsConfigDir() + File.separator + "yarn-site.xml";
	    	if ( new File(hdfsConfigXmlFile).isFile() ) {
	    		hdfsConfig.addResource(new Path(hdfsConfigXmlFile));
	    		System.out.println("ADDED -> " + hdfsConfigXmlFile);
	    	} else {
				logger.log(Level.SEVERE, "Hadoop Config File (XML) " + hdfsConfigXmlFile + " not found.");
	    		hdfsConfigXmlFound=false;
	    	}
	    	
	    	if ( !hdfsConfigXmlFound ) {
	    		System.err.println("Missing one or more Hadoop Config files (XML), Stopping");
				logger.log(Level.SEVERE, "Missing one or more Hadoop Config files (XML), Stopping");
				loggerFile.flush();
				loggerFile.close();
	    		System.exit(8);
	    	}
	    	
		    hdfsConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
		    
		    System.out.println(hdfsConfig.get("fs.s3.impl"));
		    
		    try {
		    	//AWS START
		    	//hdfsFileSystem = FileSystem.get(hdfsConfig);
		    	hdfsFileSystem = HDFSUtil.getFileSystem(daasConfig,hdfsConfig);
		    	// CYZE
		    	//   hdfsFileSystem = FileSystem.get(new URI("s3://us-emr-poc1-staging-data"), hdfsConfig);
		    	//AWS END
		    } catch (Exception ex) {
		    	System.err.println("Error creating HDFS FileSystem, Stopping");
	        	logger.log(Level.SEVERE, "Exception occurred: "+ ex.toString());
				loggerFile.flush();
				loggerFile.close();
		    	System.exit(8);
		    }

		} catch (Exception ex) {
			System.err.println("Exception occured in initHDFS()");
			ex.printStackTrace(System.err);
        	logger.log(Level.SEVERE, "Exception occurred: "+ ex.toString());
			loggerFile.flush();
			loggerFile.close();
			System.exit(8);
		}
		
	}
	
	public void initReference(DaaSConfig daasConfig) {
		
		try {
			db = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());
			
			sql.setLength(0);
			sql.append("select\n");
			sql.append("   DW_DATA_TYP_DS\n");
			sql.append("  ,DW_DATA_TYP_ID\n");
			sql.append("  ,DW_LOAD_TBLE_NA\n");
			sql.append("  ,DW_VLD_TYP_CD\n");
			sql.append("  ,DW_FILE_CPNT_CNT_QT\n");
			sql.append("  ,DW_APLC_TYP_CNT_QT\n");
			sql.append("  ,REST_CPNT_PSTN_NU\n");
			sql.append("  ,CTRY_CPNT_PSTN_NU\n");
			sql.append("  ,BUSN_DT_CPNT_PSTN_NU\n");
			sql.append("from " + daasConfig.abacSqlServerDb() + ".dw_data_typ with (NOLOCK)\n");

			ResultSet rset = db.resultSet(sql.toString());

			dataTypes = new ABaCDataTypes();

			while (rset.next()) {
				dataTypes.addDataType(new ABaCDataType(rset.getString("DW_DATA_TYP_DS")
						                              ,rset.getInt("DW_DATA_TYP_ID")
						                              ,rset.getString("DW_LOAD_TBLE_NA")
						                              ,rset.getInt("DW_VLD_TYP_CD")
						                              ,rset.getInt("DW_FILE_CPNT_CNT_QT")
						                              ,rset.getInt("DW_APLC_TYP_CNT_QT")
						                              ,rset.getInt("REST_CPNT_PSTN_NU")+1
						                              ,rset.getInt("CTRY_CPNT_PSTN_NU")+1
						                              ,rset.getInt("BUSN_DT_CPNT_PSTN_NU")+1));
			}
			
			rset.close();

			countryCodes = new HashMap<String,Integer>();
			
			sql.setLength(0);
			sql.append("select\n");
			sql.append("   CTRY_ISO2_ABBR_CD\n");
			sql.append("  ,CTRY_ISO_NU\n");
			sql.append("from " + daasConfig.abacSqlServerDb() + ".ctry with (NOLOCK);\n");
			
			rset = db.resultSet(sql.toString());

			while (rset.next()) {
				countryCodes.put(rset.getString("CTRY_ISO2_ABBR_CD"), rset.getInt("CTRY_ISO_NU"));
			}
			
			rset.close();
			
			lcat = new HashMap<String,BigDecimal>();
			
			sql.setLength(0);
			sql.append("select\n");
			sql.append("   CTRY_ISO_NU\n");
			sql.append("  ,LGCY_LCL_RFR_DEF_CD\n");
			sql.append("  ,MCD_GBAL_LCAT_ID_NU\n");
			sql.append("from " + daasConfig.abacSqlServerDb() + ".gbal_lcat with (NOLOCK);\n");
			
			rset = db.resultSet(sql.toString());

			while (rset.next()) {
				lcat.put(String.valueOf(rset.getInt("CTRY_ISO_NU")) + "_" + rset.getString("LGCY_LCL_RFR_DEF_CD"), rset.getBigDecimal("MCD_GBAL_LCAT_ID_NU"));
			}
			
			rset.close();
			
			stus = new HashMap<String,Short>();
			
			sql.setLength(0);
			sql.append("select\n");
			sql.append("   DW_AUDT_STUS_TYP_ID\n");
			sql.append("  ,DW_AUDT_STUS_TYP_DS\n");
			sql.append("from " + daasConfig.abacSqlServerDb() + ".dw_audt_stus_typ with (NOLOCK);\n");
			
			rset = db.resultSet(sql.toString());

			int stusId;
			String stusDs;

			while (rset.next()) {
				stusId = rset.getInt("DW_AUDT_STUS_TYP_ID");
				stusDs = rset.getString("DW_AUDT_STUS_TYP_DS");
				stus.put(stusDs, Short.parseShort(Integer.toString(stusId)));
			}
			
			rset.close();
			
			resn = new HashMap<String,Short>();
			
			sql.setLength(0);
			sql.append("select\n");
			sql.append("   DW_AUDT_RJCT_RESN_ID\n");
			sql.append("  ,DW_AUDT_RJCT_RESN_DS\n");
			sql.append("from " + daasConfig.abacSqlServerDb() + ".dw_audt_rjct_resn with (NOLOCK);\n");
			
			rset = db.resultSet(sql.toString());

			int resnId;
			String resnDs;
			
			while (rset.next()) {
				resnId = rset.getInt("DW_AUDT_RJCT_RESN_ID");
				resnDs = rset.getString("DW_AUDT_RJCT_RESN_DS");
				resn.put(resnDs,Short.parseShort(Integer.toString(resnId)));
			}
			
			rset.close();
			
			db.dispose();
			
		} catch (Exception ex) {
			System.err.println("Exception occured in initReference()");
        	logger.log(Level.SEVERE, "Exception occurred: "+ ex.toString());
			loggerFile.flush();
			loggerFile.close();
			ex.printStackTrace(System.err);
	    	System.exit(8);
		}
	}
	
	private long getAvailableMemory() {
		
		Runtime runtime = Runtime.getRuntime();
		long totalMemory = runtime.totalMemory(); // current heap allocated to the VM process
		long freeMemory = runtime.freeMemory(); // out of the current heap, how much is free
		long maxMemory = runtime.maxMemory(); // Max heap VM can use e.g. Xmx setting
		long usedMemory = totalMemory - freeMemory; // how much of the current heap the VM is using
		long availableMemory = maxMemory - usedMemory; // available memory i.e. Maximum heap size minus the current amount used
		return(availableMemory);
	}
}

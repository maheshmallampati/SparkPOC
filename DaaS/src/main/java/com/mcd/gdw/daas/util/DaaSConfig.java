package com.mcd.gdw.daas.util;

import java.util.Locale;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.util.SimpleEncryptAndDecrypt;

public class DaaSConfig {

	public enum VerboseLevelType { None, Minimum, Maximum}
	
	public VerboseLevelType verboseLevel = VerboseLevelType.None;
	
	public final static int MIN_NUM_SESSIONS = 1;
	public final static int MAX_NUM_SESSIONS = 15;
	public final static int MIN_BATCH_SIZE = 100;
	public final static int MAX_BATCH_SIZE = 10000;

	private String _configFileName;
	
	private String _gblTpid = "";
	private String _gblUserId = "";
	private String _gblPassword = "";
	private String _gblViewDb = "";
	private int _gblNumSessions = 0;
	private String _gblAddlConnectionSettings = "";
	private String _genLocale = "en_US";
	
	
	private String _tranpolConnectionString = "";
	private String _tranpolUserId = "";
	private String _tranpolPassword = "";
	public String tranpolConnectionString() {
		return _tranpolConnectionString;
	}

	public String tranpolUserId() {
		return _tranpolUserId;
	}

	public String tranpolPassword() {
		return _tranpolPassword;
	}

	
	
	
	/*
	private String _abacTpid = "";
	private String _abacUserId = "";
	private String _abacPassword = "";
	private String _abacViewDb = "";
	private String _abacUtilityDb = "";
	private int _abacNumSessions = 0;
	private int _abacBatchSize = 0;
	private String _abacAddlConnectionSettings = "";
	private String _abacTablePrefix = "";
	private String _abacCacheFileName = "";
	*/

	private String _abacSqlServerServerName = "";
	private String _abacSqlServerUserId = "";
	private String _abacSqlServerPassword = "";
	private String _abacSqlServerDb = "";
	private int _abacSqlServerBatchSize = 0;
	private String _abacSqlServerAddlConnectionSettings = "";
	private String _abacSqlServerCacheFileName = "";
	private int _abacSqlServerNumFilesLimit = 100000;
	
	private String _abacSqlServerStatusValidated  = "VALIDATED";
	private String _abacSqlServerStatusReady      = "READY";
	private String _abacSqlServerStatusProcessing = "PROCESSING";
	private String _abacSqlServerStatusSuccessful = "SUCCESSFUL";
	private String _abacSqlServerStatusRejected   = "REJECTED";
	private String _abacSqlServerStatusFailed     = "FAILED";
	
	private String _abacSqlServerStatusValidatedFile  = "";
	private String _abacSqlServerStatusReadyFile      = "";
	private String _abacSqlServerStatusProcessingFile = "";
	private String _abacSqlServerStatusSuccessfulFile = "";
	private String _abacSqlServerStatusRejectedFile   = "";
	private String _abacSqlServerStatusFailedFile     = "";
	
	private String _abacSqlServeReasonNonMcOpCo                 = "NON-MCOPCO";                 
	private String _abacSqlServeReasonInterbatchDuplicate       = "INTERBATCH DUPLICATE";
	private String _abacSqlServeReasonCorruptedCompressedFormat = "CORRUPTED COMPRESSED FORMAT";
	private String _abacSqlServeReasonMalformedXml              = "MALFORMED XML";
	private String _abacSqlServerReasonMissing                  = "MISSING";
	private String _abacSqlServerReasonLateArrival              = "LATE ARRIVAL";

	private String _abacSqlServeReasonNonMcOpCoFile                 = "";                 
	private String _abacSqlServeReasonInterbatchDuplicateFile       = "";
	private String _abacSqlServeReasonCorruptedCompressedFormatFile = "";
	private String _abacSqlServeReasonMalformedXmlFile              = "";
	private String _abacSqlServerReasonMissingFile                  = "";
	private String _abacSqlServerReasonLateArrivalFile              = "";
	
	private String _abacSqlServerStatusArrived     = "ARRIVED";
	private String _abacSqlServerStatusUpdated     = "UPDATED";
	
	private String _abacSqlServerStatusArrivedFile     = "";
	private String _abacSqlServerStatusUpdatedFile     = "";

	private boolean _asterEntry = false;
	private String _asterServerName = "";
	private String _asterUserId = "";
	private String _asterPassword = "";
	
	private String _genAltInputPath = "";
	private String _genVerboseLevel = "";
	private int _genDefaultABaCDateRangeInDays = 0;
	
	private String _hdfsConfigDir = "";
	private String _hdfsRoot = "";
	private String _hdfsLandingZoneSubDir = "";
	private String _hdfsWorkSubDir = "";
	private String _hdfsHiveSubDir = "";
	private String _hdfsOutputSubDir = "";
	private String _hdfsFinalSubDir = "";
	private String _hdfsLandingZoneArrivalSubDir = "";
	private boolean _hdfsKeepTempDirs = false;
	
	private String _fileType = "";
	private String _fileSubDir = "";
	//@mc41946- New Attribute for processing file types
	private boolean _nonmergedZipFile = false;
	private String _fileDesc = "";
	private boolean _fileInputIsZipFormat = false;
	private boolean _fileCompressOutput = false;
	private int _fileMaxFileSizeInMb = 0;
	private String _fileApplyCompanyOwnedFilterTerrList = "";
	private String _fileEncryptNonCompanyOwnedTerrList = "";
	private String _fileEncryptAllTerrList = "";
	private String _fileMaskNonCompanyOwnedTerrList = "";
	private String _fileMaskAllTerrList = "";
	private int _fileDefaultABaCDateRangeInDays = 0;
	private String _fileRestLimitList = "";
	private int _fileMapReduceJavaHeapSize = 0;
	private char _fileFileSeparatorCharacter = '\t';
	private int _fileTerrCdFieldPosition = 0;
	private int _fileLgcyLclRfrDefCdFieldPosition = 0;
	private int _fileBusinessDateFieldPosition = 0;
	
	private int _numberofReducers = 0;
	private String _filePattern = "";
	//private String _heapDumpPath = "";
	private String _skipFilesonSize = "true";
	private String _maxFileSize = "";
	
	private boolean _configValid = true;
	private String _errText = "";

	private boolean emptyFileType = false;
	
	private String _smtpHost;
	private String _smtpPort;
	
	
	public DaaSConfig(String configXmlFile
			         ,String fileType) {
		
		processDaaSConfig(configXmlFile,fileType,false);
		
	}
	
	public DaaSConfig(String configXmlFile) {

		processDaaSConfig(configXmlFile,"",true);
		
	}

	public String gblTpid() {
		return (_gblTpid);
	}
	
	public String gblUserId() {
		return (_gblUserId);
	}
	
	public String gblPassword() {
		return (_gblPassword);
	}
	
	public String gblViewDb() {
		return(_gblViewDb);
	}
	
	public int gblNumSessions() {
		return(_gblNumSessions);
	}
	
	public String gblAddlConnectionSettings() {
		return(_gblAddlConnectionSettings);
	}

	public String abacSqlServerServerName() {
		return (_abacSqlServerServerName);
	}
	
	public String abacSqlServerUserId() {
		return (_abacSqlServerUserId);
	}
	
	public String abacSqlServerPassword() {
		return (_abacSqlServerPassword);
	}
	
	public String abacSqlServerDb() {
		return(_abacSqlServerDb);
	}
	
	public int abacSqlServerBatchSize() {
		return(_abacSqlServerBatchSize);
	}
	
	public int abacSqlServerNumFilesLimit() {
		return(_abacSqlServerNumFilesLimit);
	}
	
	public String abacSqlServerAddlConnectionSettings() {
		return(_abacSqlServerAddlConnectionSettings);
	}
	
	public String abacSqlServerCacheFileName() {
		return(_abacSqlServerCacheFileName);
	}

	public String abacSqlServerStatusValidated() {
		return(_abacSqlServerStatusValidated);
	}
	
	public String abacSqlServerStatusArrived() {
		return(_abacSqlServerStatusArrived);
	}
	
	public String abacSqlServerStatusUpdated() {
		return(_abacSqlServerStatusUpdated);
	}
	
	public String abacSqlServerStatusReady() {
		return(_abacSqlServerStatusReady);
	}
	
	public String abacSqlServerStatusProcessing() {
		return(_abacSqlServerStatusProcessing);
	}
	
	public String abacSqlServerStatusSuccessful() {
		return(_abacSqlServerStatusSuccessful);
	}
	
	public String abacSqlServerStatusRejected() {
		return(_abacSqlServerStatusRejected);
	}
	
	public String abacSqlServerStatusFailed() {
		return(_abacSqlServerStatusFailed);
	}

	public String abacSqlServerReasonNonMcOpCo() {
		return(_abacSqlServeReasonNonMcOpCo);
	}
	
	public String abacSqlServerReasonInterbatchDuplicate() {
		return(_abacSqlServeReasonInterbatchDuplicate);
	}
	
	public String abacSqlServerReasonCorruptedCompressedFormat() {
		return(_abacSqlServeReasonCorruptedCompressedFormat);
	}
	
	public String abacSqlServerReasonMalformedXml() {
		return(_abacSqlServeReasonMalformedXml);
	}
	
	public String abacSqlServerReasonMissing() {
		return(_abacSqlServerReasonMissing);
	}
	
	public String abacSqlServerReasonLateArrival() {
		return(_abacSqlServerReasonLateArrival);
	}

	public boolean asterEntry() {
		return(_asterEntry);
	}
	
	public String asterServerName() {
		return (_asterServerName);
	}
	
	public String asterUserId() {
		return (_asterUserId);
	}
	
	public String asterPassword() {
		return (_asterPassword);
	}

	/*
	public String abacTpid() {
		return (_abacTpid);
	}
	
	public String abacUserId() {
		return (_abacUserId);
	}
	
	public String abacPassword() {
		return (_abacPassword);
	}
	
	public String abacViewDb() {
		return(_abacViewDb);
	}
	
	public String abacUtilityDb() {
		return(_abacUtilityDb);
	}
	
	public int abacNumSessions() {
		return(_abacNumSessions);
	}
	
	public int abacBatchSize() {
		return(_abacBatchSize);
	}
	
	public String abacAddlConnectionSettings() {
		return(_abacAddlConnectionSettings);
	}
	
	public String abacTablePrefix() {
		return(_abacTablePrefix);
	}
	
	public String abacCacheFileName() {
		return(_abacCacheFileName);
	}
*/
	public String genAltInputPath() {
		return(_genAltInputPath);
	}

	public String genVerboseLevel() {
		return(_genVerboseLevel);
	}
	
	public int genDefaultABaCDateRangeInDays() {
		return(_genDefaultABaCDateRangeInDays);
	}

	public String genLocale() {
		return(_genLocale);
	}
	
	public Locale getLocale() {
	
		Locale retLocale = null;
		
		retLocale = new Locale(_genLocale.substring(0, 2).toLowerCase(),_genLocale.substring(3, 5).toUpperCase());
		
		return(retLocale);
	}
	
	public String hdfsConfigDir() {
		return(_hdfsConfigDir);
	}
	
	public String hdfsRoot() {
		return(_hdfsRoot);
	}
	
	public String hdfsLandingZoneSubDir() {
		return(_hdfsLandingZoneSubDir);
	}
	
	public String hdfsLandingZoneArrivalSubDir() {
		return(_hdfsLandingZoneArrivalSubDir);
	}
	
	public String hdfsWorkSubDir() {
		return(_hdfsWorkSubDir);
	}
	
	public String hdfsHiveSubDir() {
		return(_hdfsHiveSubDir);
	}
	
	public String hdfsOutputSubDir() {
		return(_hdfsOutputSubDir);
	}
	
	public String hdfsFinalSubDir() {
		return(_hdfsFinalSubDir);
	}
	
	public boolean hdfsKeepTempDirs() {
		return(_hdfsKeepTempDirs);
	}
	
	public String fileType() {
		return(_fileType);
	}
	
	public boolean nonmergedZipFile() {
		return(_nonmergedZipFile);
	}
	
	public String fileDesc() {
		return(_fileDesc);
	}
	
	public String fileSubDir() {
		return(_fileSubDir);
	}

	public String fileApplyCompanyOwnedFilterTerrList() {
		return(_fileApplyCompanyOwnedFilterTerrList);
	}
	
	public String fileEncryptNonCompanyOwnedTerrList() {
		return(_fileEncryptNonCompanyOwnedTerrList);
	}
	
	public String fileEncryptAllTerrList() {
		return(_fileEncryptAllTerrList);
	}
	
	public String fileMaskNonCompanyOwnedTerrList() {
		return(_fileMaskNonCompanyOwnedTerrList);
	}

	public String fileMaskAllTerrList() {
		return(_fileMaskAllTerrList);
	}

	public boolean fileInputIsZipFormat() {
		return(_fileInputIsZipFormat);
	}
	
	public boolean fileCompressOutput() {
		return(_fileCompressOutput);
	}
	
	public int fileMaxFileSizeInMb() {
		return(_fileMaxFileSizeInMb);
	}
	
	public int fileTerrCdFieldPosition() {
		return(_fileTerrCdFieldPosition);
	}
	
	public int fileLgcyLclRfrDefCdFieldPosition() {
		return(_fileLgcyLclRfrDefCdFieldPosition);
	}
	
	public int fileBusinessDateFieldPosition() {
		return(_fileBusinessDateFieldPosition);
	}
	
	public int fileDefaultABaCDateRangeInDays() {
		return(_fileDefaultABaCDateRangeInDays);
	}

	public String fileRestLimitList() {
		return (_fileRestLimitList);
	}
	
	public String fileMapReduceJavaHeapSizeParm() {
	
		if ( _fileMapReduceJavaHeapSize > 0 ) {
			return("-Xmx" + _fileMapReduceJavaHeapSize + "m");
		} else {
			return("-Xmx1024m");
		}
	}
	
	public char fileFileSeparatorCharacter() {
		return(_fileFileSeparatorCharacter);
	}
	
	public String fileFileSeparatorRegxSplit() {
	
		int charVal = _fileFileSeparatorCharacter;

		String retVal = Integer.toHexString(charVal);
    	
    	if ( retVal.length() == 1 ) {
    		retVal = "\\x0" + retVal;
    	} else {
    		retVal = "\\x" + retVal;
    	}
		
		return(retVal);
		
	}
	
	public int numberofReducers() {
		return (_numberofReducers);
	}
	
	public String filePattern() {
		return (_filePattern);
	}
	
	public String skipFilesonSize() {
		return (_skipFilesonSize);
	}
	
	public String maxFileSize() {
		return (_maxFileSize);
	}
	
	
	public boolean displayMsgs() {
		
		boolean retValue = false;
		
		if ( verboseLevel != VerboseLevelType.None ) {
			retValue = true;
		}
		
		return(retValue);
	}
	
	public boolean configValid() {
		return(_configValid);
	}
	
	public String errText() {
		return(_errText);
	}
	
	public String smtpHost(){
		return _smtpHost;
	}
	
	public String smtpPort(){
		return _smtpPort;
	}

	public String toString() {
		
		String retValue;

		retValue = "XML Config File:" + _configFileName;

		if ( _configValid ) {
			retValue += ":\nGBL Connection:\n";
		    retValue += "    TPID                           = " + _gblTpid + "\n";
		    retValue += "    User ID                        = " + _gblUserId + "\n";
		    retValue += "    Password                       = " + "*******************************************************************".substring(0,_gblPassword.length()) + "\n"; 
		    retValue += "    View DB                        = " + _gblViewDb + "\n";
		    retValue += "    Number of Sessions             = " + _gblNumSessions + " (" + MIN_NUM_SESSIONS + " - " + MAX_NUM_SESSIONS + ")\n";
		    retValue += "    Additional Connection Settings = ";
		    if ( _gblAddlConnectionSettings.length() > 0 ) {
		    	retValue += _gblAddlConnectionSettings + "\n";
		    } else {
		    	retValue += "None\n";
		    }

		    /*
			retValue += "\nABaC Connection:\n";
		    retValue += "    TPID                           = " + _abacTpid + "\n";
		    retValue += "    User ID                        = " + _abacUserId + "\n";
		    retValue += "    Password                       = " + "*******************************************************************".substring(0,_abacPassword.length()) + "\n"; 
		    retValue += "    View DB                        = " + _abacViewDb + "\n";
		    retValue += "    Utility DB                     = " + _abacUtilityDb + "\n";
		    retValue += "    Table Prefix                   = " + _abacTablePrefix + "\n";
		    retValue += "    Cache File Name                = " + _abacCacheFileName + "\n";
		    retValue += "    Number of Sessions             = " + _abacNumSessions + " (" + MIN_NUM_SESSIONS + " - " + MAX_NUM_SESSIONS + ")\n";
		    retValue += "    Batch Size                     = " + _abacBatchSize + " (" + MIN_BATCH_SIZE + " - " + MAX_BATCH_SIZE + ")\n";
		    retValue += "    Additional Connection Settings = ";
		    if ( _abacAddlConnectionSettings.length() > 0 ) {
		    	retValue += _abacAddlConnectionSettings + "\n";
		    } else {
		    	retValue += "None\n";
		    }
            */
		    
			retValue += "\nABaC Sql Server Connection:\n";
		    retValue += "    Server Name                    = " + _abacSqlServerServerName + "\n";
		    retValue += "    User ID                        = " + _abacSqlServerUserId + "\n";
		    retValue += "    Password                       = " + "*******************************************************************".substring(0,_abacSqlServerPassword.length()) + "\n"; 
		    retValue += "    DB                             = " + _abacSqlServerDb + "\n";
		    retValue += "    Cache File Name                = " + _abacSqlServerCacheFileName + "\n";
		    retValue += "    Batch Size                     = " + _abacSqlServerBatchSize + " (" + MIN_BATCH_SIZE + " - " + MAX_BATCH_SIZE + ")\n";
		    retValue += "    Number of Files Limit          = " + _abacSqlServerNumFilesLimit + "\n";
		    retValue += "    Additional Connection Settings = ";
		    if ( _abacSqlServerAddlConnectionSettings.length() > 0 ) {
		    	retValue += _abacSqlServerAddlConnectionSettings + "\n";
		    } else {
		    	retValue += "None\n";
		    }

		    retValue += "    Status Codes:\n";
		    retValue += "      Validated                    = " + _abacSqlServerStatusValidated; 
		    if ( _abacSqlServerStatusValidatedFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Ready                        = " + _abacSqlServerStatusReady; 
		    if ( _abacSqlServerStatusReadyFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Processing                   = " + _abacSqlServerStatusProcessing; 
		    if ( _abacSqlServerStatusProcessingFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Successful                   = " + _abacSqlServerStatusSuccessful; 
		    if ( _abacSqlServerStatusSuccessfulFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Rejected                     = " + _abacSqlServerStatusRejected; 
		    if ( _abacSqlServerStatusRejectedFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Failed                       = " + _abacSqlServerStatusFailed; 
		    if ( _abacSqlServerStatusFailedFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "    Reject Reason Codes:\n";
		    retValue += "      Non-McOpCo                   = " + _abacSqlServeReasonNonMcOpCo; 
		    if ( _abacSqlServeReasonNonMcOpCoFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Inter Batch Dup File         = " + _abacSqlServeReasonInterbatchDuplicate; 
		    if ( _abacSqlServeReasonInterbatchDuplicateFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Corrupted Compressed File    = " + _abacSqlServeReasonCorruptedCompressedFormat; 
		    if ( _abacSqlServeReasonCorruptedCompressedFormatFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Malformed XML File           = " + _abacSqlServeReasonMalformedXml; 
		    if ( _abacSqlServeReasonMalformedXmlFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Missing File                 = " + _abacSqlServerReasonMissing; 
		    if ( _abacSqlServerReasonMissingFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Late Arrival                 = " + _abacSqlServerReasonLateArrival; 
		    if ( _abacSqlServerReasonLateArrivalFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";		    
		    retValue += "    File List Codes:\n";
		    retValue += "      Arrived                   = " + _abacSqlServerStatusArrived; 
		    if ( _abacSqlServerStatusArrivedFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";
		    retValue += "      Updated         = " + _abacSqlServerStatusUpdated; 
		    if ( _abacSqlServerStatusUpdatedFile.length() == 0 ) {
		    	retValue += " (Default)";
		    }
		    retValue += "\n";

		    if ( _asterEntry ) {
				retValue += "\nAster Server Connection:\n";
			    retValue += "    Server Name                    = " + _asterServerName + "\n";
			    retValue += "    User ID                        = " + _asterUserId + "\n";
			    retValue += "    Password                       = " + "*******************************************************************".substring(0,_asterPassword.length()) + "\n"; 
		    }

			retValue += "\nGeneral:\n";
		    retValue += "    Alternate Input Path           = ";
		    if ( _genAltInputPath.length() > 0 ) {
		    	retValue += _genAltInputPath + "\n";
		    } else {
		    	retValue += "None\n";
		    }
		    retValue += "    ABaC Default Date Range        = " + _genDefaultABaCDateRangeInDays + " (days)\n";
		    
		    retValue += "    Verbose Level                  = " + _genVerboseLevel + "\n";
		    retValue += "    Report Locale                  = " + _genLocale + "\n";
		    
		    retValue += "\nHDFS:\n";
		    retValue += "    Configuration Directory        = " + _hdfsConfigDir + "\n";
		    retValue += "    HDFS Root Directory            = " + _hdfsRoot + "\n";
		    retValue += "    HDFS Landing Zone Subdirectory = " + _hdfsLandingZoneSubDir + "\n";
		    retValue += "    HDFS Landing Zone Arrival Dir  = " + _hdfsLandingZoneArrivalSubDir + "\n";
 		    retValue += "    HDFS Work Subdirectory         = " + _hdfsWorkSubDir + "\n";
 		    retValue += "    HDFS Hive Subdirectory         = " + _hdfsHiveSubDir + "\n";
		    retValue += "    HDFS Output Subdirectory       = " + _hdfsOutputSubDir + "\n";

		    if ( _fileInputIsZipFormat ) {
		    	retValue += "    HDFS Keep Temp Directories     = True\n";
		    } else {
		    	retValue += "    HDFS Keep Temp Directories     = False\n";
		    }
		    
		    if ( this.emptyFileType ) {
			    retValue += "\nFile:\n";
			    retValue += "    File Type                      = EMPTY\n";
		    } else {
			    retValue += "\nFile:\n";
			    retValue += "    File Type                      = " + _fileType + "\n";
			    retValue += "    File Description               = " + _fileDesc + "\n";
			    retValue += "    File Sub Directory             = " + _fileSubDir + "\n";

			    if ( _fileInputIsZipFormat ) {
			    	retValue += "    Input File Format Is Zip       = True\n";
			    } else {
			    	retValue += "    Input File Format Is Zip       = False\n";
			    }

			    if ( _fileCompressOutput ) {
			    	retValue += "    Compress Output                = True\n";
			    } else {
			    	retValue += "    Compress Output                = False\n";
			    }
			    
			    if ( _fileApplyCompanyOwnedFilterTerrList.length() > 0 ) {
			    	retValue += "    Apply Company Owned Filter     = " + _fileApplyCompanyOwnedFilterTerrList + " territories only\n";
			    } else {
			    	retValue += "    Apply Company Owned Filter     = no territories\n";
			    }
			    
			    if ( _fileEncryptNonCompanyOwnedTerrList.length() > 0 ) {
			    	retValue += "    Encrypt Names for Non Company  = " + _fileEncryptNonCompanyOwnedTerrList + " territories only\n";
			    } else {
			    	retValue += "    Encrypt Names for Non Company  = no territories\n";
			    }
			    
			    if ( _fileEncryptAllTerrList.length() > 0 ) {
			    	retValue += "    Encrypt Names for All Locations= " + _fileEncryptAllTerrList + " territories only\n";
			    } else {
			    	retValue += "    Encrypt Names for All Locations= no territories\n";
			    }
			    
			    if ( _fileMaskNonCompanyOwnedTerrList.length() > 0 ) {
			    	retValue += "    Mask Names for Non Company     = " + _fileMaskNonCompanyOwnedTerrList + " territories only\n";
			    } else {
			    	retValue += "    Mask Names for Non Company     = no territories\n";
			    }
			    
			    if ( _fileMaskAllTerrList.length() > 0 ) {
			    	retValue += "    Mask Names for All Locations   = " + _fileMaskAllTerrList + " territories only\n";
			    } else {
			    	retValue += "    Mask Names for All Locations   = no territories\n";
			    }

			    if ( _fileMaxFileSizeInMb > 0 ) {
				    retValue += "    Maximum File Size (MB)         = " + _fileMaxFileSizeInMb + "\n";
			    }

			    if ( _fileFileSeparatorCharacter == '\t' ) {
			    	retValue += "    File Separator Character       = TAB\n";
			    } else {
			    	retValue += "    File Separator Character       = '" + _fileFileSeparatorCharacter + "'\n";
			    }
			    
			    retValue += "    Territory Code Field Position  = " + _fileTerrCdFieldPosition + "\n";
			    retValue += "    LgcyLclRfrDefCd Field Position = " + _fileLgcyLclRfrDefCdFieldPosition + "\n";
			    retValue += "    Business Date   Field Position = " + _fileBusinessDateFieldPosition + "\n";
			    
			    if ( _fileDefaultABaCDateRangeInDays > 0 ) {
				    retValue += "    ABaC Default Date Range        = " + _fileDefaultABaCDateRangeInDays + " (days overrides General days value)\n";
			    }

			    if ( _fileMapReduceJavaHeapSize > 0 ) {
				    retValue += "    Java MapReduce Heap Size (MB)  = " + _fileMapReduceJavaHeapSize + "\n";
			    }


			    if ( _numberofReducers > 0 ) {
				    retValue += "    Number of Reducers             = " + _numberofReducers + "\n";
			    }
			    
			    if ( _skipFilesonSize != null &&_skipFilesonSize.trim().length() > 0 ) {
				    retValue += "    Skip Files on Size             = " + _skipFilesonSize + "\n";
			    }
			    
			    if ( _maxFileSize != null && _maxFileSize.trim().length() > 0 ) {
				    retValue += "    Max File Size to process       = " + _maxFileSize + "\n";
			    }
			    
			    retValue += "    Restaurant Limit List          = ";

			    if ( _fileRestLimitList.length() > 0 ) {
			    	retValue += _fileRestLimitList + "\n";
			    } else {
			    	retValue += "NONE\n";
			    }
			    
			    retValue += "\nEMAIL:\n";
			    retValue += "    SMTP Host                      = "+_smtpHost +"\n";
			    retValue += "    SMTP Port                      = "+_smtpPort +"\n";
		    }

		} else {
			retValue += " invalid\n" + _errText;
		}
		
		return(retValue);
	}
	
	private void processDaaSConfig(String configXmlFile
	                              ,String fileType
	                              ,boolean emptyFileType) {

		SimpleEncryptAndDecrypt decrypt = new SimpleEncryptAndDecrypt();
		
		this.emptyFileType = emptyFileType; 
		_fileType = fileType;
		_configFileName = configXmlFile;
		
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			InputSource xmlSource = new InputSource(configXmlFile);
			Document doc = docBuilder.parse(xmlSource);

			Element eleRoot = (Element)doc.getFirstChild();
			NodeList parmsNodeList = eleRoot.getChildNodes();
			
			if ( parmsNodeList != null && parmsNodeList.getLength() > 0 ) {
				for ( int idx=0; idx < parmsNodeList.getLength(); idx++ ) {
					
					if ( parmsNodeList.item(idx).getNodeType() == Node.ELEMENT_NODE ) {
						Element parmSection = (Element)parmsNodeList.item(idx);
						
						if ( parmSection.getNodeName().equals("GBL_Teradata_Connection") ) {
							_gblTpid = parmSection.getAttribute("tpid");
							_gblUserId = parmSection.getAttribute("userId");
							_gblPassword = decrypt.decryptFromHexString(parmSection.getAttribute("password"));
							_gblViewDb = parmSection.getAttribute("viewDb");
							
							try {
								_gblNumSessions = Integer.parseInt(parmSection.getAttribute("numSessions"));
							} catch (Exception ex) {
								_configValid = false;
								if ( _errText.length() > 0 ) {
									_errText += "\n";
								}
								_errText += "GBL_Teradata_Connection:numSessions invalid integer";
							}
							
							_gblAddlConnectionSettings = parmSection.getAttribute("additionalConnectionSettings");
						}

						/*
						if ( parmSection.getNodeName().equals("ABaC_Teradata_Connection") ) {
							_abacTpid = parmSection.getAttribute("tpid");
							_abacUserId = parmSection.getAttribute("userId");
							_abacPassword = decrypt.decryptFromHexString(parmSection.getAttribute("password"));
							_abacViewDb = parmSection.getAttribute("viewDb");
							_abacUtilityDb = parmSection.getAttribute("utilityDb");
							_abacTablePrefix = parmSection.getAttribute("tablePrefix");
							_abacCacheFileName = parmSection.getAttribute("cacheFileName");
						
							try {
								_abacNumSessions = Integer.parseInt(parmSection.getAttribute("numSessions"));
							} catch (Exception ex) {
								_configValid = false;
								if ( _errText.length() > 0 ) {
									_errText += "\n";
								}
								_errText += "ABaC_Teradata_Connection:numSessions invalid integer";
							}
						
							try {
								_abacBatchSize = Integer.parseInt(parmSection.getAttribute("batchSize"));
							} catch (Exception ex) {
								_configValid = false;
								if ( _errText.length() > 0 ) {
									_errText += "\n";
								}
								_errText += "ABaC_Teradata_Connection:batchSize invalid integer";
							}
							
							_abacAddlConnectionSettings = parmSection.getAttribute("additionalConnectionSettings");
						}
						*/

						if ( parmSection.getNodeName().equals("ABaC_SqlServer_Connection") ) {
							_abacSqlServerServerName = parmSection.getAttribute("server");
							_abacSqlServerUserId = parmSection.getAttribute("userId");
							_abacSqlServerPassword = decrypt.decryptFromHexString(parmSection.getAttribute("password"));
							_abacSqlServerDb = parmSection.getAttribute("db");
							_abacSqlServerCacheFileName = parmSection.getAttribute("cacheFileName");
						
							try {
								_abacSqlServerBatchSize = Integer.parseInt(parmSection.getAttribute("batchSize"));
							} catch (Exception ex) {
								_configValid = false;
								if ( _errText.length() > 0 ) {
									_errText += "\n";
								}
								_errText += "ABaC_SqlServer_Connection:batchSize invalid integer";
							}
							
							if (parmSection.getAttribute("numFilesLimit").length() > 0) {
								try {
									_abacSqlServerNumFilesLimit = Integer.parseInt(parmSection.getAttribute("numFilesLimit"));
								} catch (Exception ex) {
									_configValid = false;
									if ( _errText.length() > 0 ) {
										_errText += "\n";
									}
									_errText += "ABaC_SqlServer_Connection:numFilesLimit invalid integer";
								}
							}
							
							_abacSqlServerAddlConnectionSettings = parmSection.getAttribute("additionalConnectionSettings");
							
							_abacSqlServerStatusValidatedFile  = parmSection.getAttribute("statusValidated") + "";
							_abacSqlServerStatusReadyFile      = parmSection.getAttribute("statusReady") + "";
							_abacSqlServerStatusProcessingFile = parmSection.getAttribute("statusProcessing") + "";
							_abacSqlServerStatusSuccessfulFile = parmSection.getAttribute("statusSuccessful") + "";
							_abacSqlServerStatusRejectedFile   = parmSection.getAttribute("statusRejected") + "";
							_abacSqlServerStatusFailedFile     = parmSection.getAttribute("statusFailed") + "";
							_abacSqlServeReasonNonMcOpCoFile                 = parmSection.getAttribute("reasonNonMcOpCo") + "";                 
							_abacSqlServeReasonInterbatchDuplicateFile       = parmSection.getAttribute("reasonInterbatchDuplicate") + "";
							_abacSqlServeReasonCorruptedCompressedFormatFile = parmSection.getAttribute("reasonCorruptedCompressedFormat") + "";
							_abacSqlServeReasonMalformedXmlFile              = parmSection.getAttribute("reasonMalformedXml") + "";
							_abacSqlServerReasonMissingFile                  = parmSection.getAttribute("reasonMissing") + "";
							_abacSqlServerReasonLateArrivalFile              = parmSection.getAttribute("reasonLateArrival") + "";
							_abacSqlServerStatusArrivedFile= parmSection.getAttribute("statusArrived") + "";
							_abacSqlServerStatusUpdatedFile= parmSection.getAttribute("statusUpdated") + "";

							if ( _abacSqlServerStatusValidatedFile.length() > 0 ) {
								_abacSqlServerStatusValidated = _abacSqlServerStatusValidatedFile;
							}
							
							if ( _abacSqlServerStatusReadyFile.length() > 0 ) {
								_abacSqlServerStatusReady = _abacSqlServerStatusReadyFile;
							}
							if ( _abacSqlServerStatusProcessingFile.length() > 0 ) {
								_abacSqlServerStatusProcessing = _abacSqlServerStatusProcessingFile;
							}
							if ( _abacSqlServerStatusSuccessfulFile.length() > 0 ) {
								_abacSqlServerStatusSuccessful = _abacSqlServerStatusSuccessfulFile;
							}
							
							if ( _abacSqlServerStatusRejectedFile.length() > 0 ) {
								_abacSqlServerStatusRejected = _abacSqlServerStatusRejectedFile;
							}
							
							if ( _abacSqlServerStatusFailedFile.length() > 0 ) {
								_abacSqlServerStatusFailed = _abacSqlServerStatusFailedFile;
							}
							
							if ( _abacSqlServeReasonNonMcOpCoFile.length() > 0 ) {
								_abacSqlServeReasonNonMcOpCo = _abacSqlServeReasonNonMcOpCoFile;
							}
							
							if ( _abacSqlServeReasonInterbatchDuplicateFile.length() > 0 ) {
								_abacSqlServeReasonInterbatchDuplicate = _abacSqlServeReasonInterbatchDuplicateFile;
							}
							
							if ( _abacSqlServeReasonCorruptedCompressedFormatFile.length() > 0 ) {
								_abacSqlServeReasonCorruptedCompressedFormat = _abacSqlServeReasonCorruptedCompressedFormatFile;
							}
							
							if ( _abacSqlServeReasonMalformedXmlFile.length() > 0 ) {
								_abacSqlServeReasonMalformedXml = _abacSqlServeReasonMalformedXmlFile;
							}
							
							if ( _abacSqlServerReasonMissingFile.length() > 0 ) {
								_abacSqlServerReasonMissing = _abacSqlServerReasonMissingFile;
							}
							
							if ( _abacSqlServerReasonLateArrivalFile.length() > 0 ) {
								_abacSqlServerReasonLateArrival = _abacSqlServerReasonLateArrivalFile;
							}
							if ( _abacSqlServerStatusUpdatedFile.length() > 0 ) {
								_abacSqlServerStatusUpdated = _abacSqlServerStatusUpdated;
							}
							if ( _abacSqlServerStatusArrivedFile.length() > 0 ) {
								_abacSqlServerStatusArrived = _abacSqlServerStatusArrived;
							}

						}

						if ( parmSection.getNodeName().equals("Aster_Connection") ) {
							_asterServerName = parmSection.getAttribute("server");
							_asterUserId = parmSection.getAttribute("userId");
							_asterPassword = decrypt.decryptFromHexString(parmSection.getAttribute("password"));
						}

						if ( parmSection.getNodeName().equals("General") ) {
							_genAltInputPath = parmSection.getAttribute("altInputPath");
							_genVerboseLevel = parmSection.getAttribute("verboseLevel") + "";
							
							if ( _genVerboseLevel.equals("1") ) {
								verboseLevel = VerboseLevelType.Minimum;
							}
							
							if ( _genVerboseLevel.equals("2") ) {
								verboseLevel = VerboseLevelType.Maximum;
							}
							
							String localeText = parmSection.getAttribute("locale") + "";
							
							if ( localeText.length() == 5 && localeText.substring(2, 3).equals("_") ) {
								_genLocale = localeText;
							}

							try {
								_genDefaultABaCDateRangeInDays = Integer.parseInt(parmSection.getAttribute("defaultABaCDateRangeInDays"));
							} catch (Exception ex) {
								_configValid = false;
								if ( _errText.length() > 0 ) {
									_errText += "\n";
								}
								_errText += "General:defaultABaCDateRangeInDays invalid integer";
							}
						}
						
						if ( parmSection.getNodeName().equals("HDFS") ) {
							_hdfsConfigDir = parmSection.getAttribute("configDir");
							_hdfsRoot = parmSection.getAttribute("hdfsRoot");
							_hdfsLandingZoneSubDir = parmSection.getAttribute("landingZoneSubDir");
							_hdfsLandingZoneArrivalSubDir = parmSection.getAttribute("landingZoneArrivalSubDir");
							_hdfsWorkSubDir = parmSection.getAttribute("workSubDir");
							_hdfsHiveSubDir = parmSection.getAttribute("hiveSubDir");
							_hdfsOutputSubDir = parmSection.getAttribute("outputSubDir");
							_hdfsFinalSubDir = parmSection.getAttribute("finalSubDir");
							
							if ( (parmSection.getAttribute("keepTempDirs") + "").toUpperCase().equals("Y") ||
								 (parmSection.getAttribute("keepTempDirs") + "").toUpperCase().equals("YES") ||
								 (parmSection.getAttribute("keepTempDirs") + "").toUpperCase().equals("T")   ||
								 (parmSection.getAttribute("keepTempDirs") + "").toUpperCase().equals("TRUE") ) {
								_hdfsKeepTempDirs = true;
							}
						}
						
						if ( !this.emptyFileType && parmSection.getNodeName().equals("File") ) {
							if ( (parmSection.getAttribute("fileType") + "").equals(_fileType) ) {
								_fileSubDir = parmSection.getAttribute("fileSubDir");
								_fileDesc = parmSection.getAttribute("fileDesc");
								_fileRestLimitList = parmSection.getAttribute("restLimitList") + "";
								_fileApplyCompanyOwnedFilterTerrList = parmSection.getAttribute("applyCompanyOwnedFilterTerrList") + "";
								_fileEncryptNonCompanyOwnedTerrList = parmSection.getAttribute("encryptNonCompanyOwnedTerrList") + "";
								_fileEncryptAllTerrList = parmSection.getAttribute("encryptAllTerrList") + "";
								_fileMaskNonCompanyOwnedTerrList = parmSection.getAttribute("maskNonCompanyOwnedTerrList") + "";
								_fileMaskAllTerrList = parmSection.getAttribute("maskAllTerrList") + "";
								
								if ((parmSection
										.getAttribute("nonmergedZipFile") + "")
										.toUpperCase().equals("Y")
										|| (parmSection
												.getAttribute("nonmergedZipFile") + "")
												.toUpperCase().equals("YES")
										|| (parmSection
												.getAttribute("nonmergedZipFile") + "")
												.toUpperCase().equals("T")
										|| (parmSection
												.getAttribute("nonmergedZipFile") + "")
												.toUpperCase().equals("TRUE")) {
									_nonmergedZipFile = true;
								}
																
								if ( (parmSection.getAttribute("fileSeparatorCharacter") +"").length() > 0 ) {
									if ( parmSection.getAttribute("fileSeparatorCharacter").toUpperCase().equals("TAB") ||
										 parmSection.getAttribute("fileSeparatorCharacter").toUpperCase().equals("\\T")	) {
										_fileFileSeparatorCharacter = '\t';
									} else {
										_fileFileSeparatorCharacter = parmSection.getAttribute("fileSeparatorCharacter").charAt(0);
									}
								}
								
								if ( (parmSection.getAttribute("inputIsZipFormat") + "").toUpperCase().equals("Y") ||
									 (parmSection.getAttribute("inputIsZipFormat") + "").toUpperCase().equals("YES") ||
									 (parmSection.getAttribute("inputIsZipFormat") + "").toUpperCase().equals("T")   ||
									 (parmSection.getAttribute("inputIsZipFormat") + "").toUpperCase().equals("TRUE") ) {
									_fileInputIsZipFormat = true;
								}

								if ( (parmSection.getAttribute("compressOutput") + "").toUpperCase().equals("Y") ||
									 (parmSection.getAttribute("compressOutput") + "").toUpperCase().equals("YES") ||
									 (parmSection.getAttribute("compressOutput") + "").toUpperCase().equals("T")   ||
									 (parmSection.getAttribute("compressOutput") + "").toUpperCase().equals("TRUE") ) {
									_fileCompressOutput = true;
								}

								try {
									_fileTerrCdFieldPosition = Integer.parseInt("0" + parmSection.getAttribute("terrCdFieldPosition"));
								} catch (Exception ex) {
									_configValid = false;
									if ( _errText.length() > 0 ) {
										_errText += "\n";
									}
									_errText += "File:terrCdFieldPosition invalid integer";
								}

								try {
									_fileLgcyLclRfrDefCdFieldPosition = Integer.parseInt("0" + parmSection.getAttribute("lgcyLclRfrDefCdFieldPosition"));
								} catch (Exception ex) {
									_configValid = false;
									if ( _errText.length() > 0 ) {
										_errText += "\n";
									}
									_errText += "File:lgcyLclRfrDefCdFieldPosition invalid integer";
								}

								try {
									_fileBusinessDateFieldPosition = Integer.parseInt("0" + parmSection.getAttribute("businessDateFieldPosition"));
								} catch (Exception ex) {
									_configValid = false;
									if ( _errText.length() > 0 ) {
										_errText += "\n";
									}
									_errText += "File:businessDateFieldPosition invalid integer";
								}
								
								try {
									_fileMaxFileSizeInMb = Integer.parseInt("0" + parmSection.getAttribute("maxFileSizeInMb"));
								} catch (Exception ex) {
									_configValid = false;
									if ( _errText.length() > 0 ) {
										_errText += "\n";
									}
									_errText += "File:maxFileSizeInMb invalid integer";
								}

								try {
									_fileDefaultABaCDateRangeInDays = Integer.parseInt("0" + parmSection.getAttribute("defaultABaCDateRangeInDays"));
								} catch (Exception ex) {
									_configValid = false;
									if ( _errText.length() > 0 ) {
										_errText += "\n";
									}
									_errText += "File:defaultABaCDateRangeInDays invalid integer";
								}
								
								try {
									_fileMapReduceJavaHeapSize = Integer.parseInt("0" + parmSection.getAttribute("mapReduceJavaHeapSize"));
								} catch (Exception ex) {
									_configValid = false;
									if ( _errText.length() > 0 ) {
										_errText += "\n";
									}
									_errText += "File:mapReduceJavaHeapSize invalid integer";
								}

								_numberofReducers = Integer.parseInt("0" + parmSection.getAttribute("numberofReducers"));
															
								_filePattern = parmSection.getAttribute("filePattern");
								
								//_heapDumpPath = parmSection.getAttribute("heapDumpPath");
								
								_skipFilesonSize = parmSection.getAttribute("skipFilesonSize");
								_maxFileSize     = parmSection.getAttribute("maxFileSize");								
							}
						}
						
						if ( parmSection.getNodeName().equals("EMAIL") ) {
							_smtpHost = parmSection.getAttribute("smtpHost");
							_smtpPort = parmSection.getAttribute("smtpPort");
						}
						if ( parmSection.getNodeName().equals("TRANPOL_Connection") ) {
							_tranpolConnectionString = parmSection.getAttribute("jdbcConnectionString");
							_tranpolUserId = parmSection.getAttribute("userId");
							_tranpolPassword = decrypt.decryptFromHexString(parmSection.getAttribute("password"));
						}

					}
				}
			}
		} catch (Exception ex ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "General Error occured:" + ex.getMessage();
		}

		if (_gblTpid.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "GBL_Teradata_Connection:gblTpid missing";
		}

		if (_gblUserId.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "GBL_Teradata_Connection:gblUserId missing";
		}

		if (_gblPassword.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "GBL_Teradata_Connection:gblPassword missing";
		}

		if (_gblViewDb.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "GBL_Teradata_Connection:gblViewDb missing";
		}

		if (_gblNumSessions < MIN_NUM_SESSIONS || _gblNumSessions > MAX_NUM_SESSIONS ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "GBL_Teradata_Connection:gblNumSessions missing or must be valid integer between " + MIN_NUM_SESSIONS + " and " + MAX_NUM_SESSIONS;
		}

		/*
		if (_abacTpid.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacTpid missing";
		}

		if (_abacUserId.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacUserId missing";
		}

		if (_abacPassword.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacPassword missing";
		}

		if (_abacViewDb.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacViewDb missing";
		}

		if (_abacUtilityDb.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacUtilityDb missing";
		}

		if (_abacNumSessions < MIN_NUM_SESSIONS || _abacNumSessions > MAX_NUM_SESSIONS ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacNumSessions missing or must be valid integer between " + MIN_NUM_SESSIONS + " and " + MAX_NUM_SESSIONS;
		}

		if (_abacBatchSize < MIN_BATCH_SIZE || _abacBatchSize > MAX_BATCH_SIZE ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacBatchSize missing or must be valid integer between " + MIN_BATCH_SIZE + " and " + MAX_BATCH_SIZE;
		}
		
		if (_abacCacheFileName.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacCacheFileName missing";
		}
		
		if (_abacTablePrefix.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_Connection:abacTablePrefix missing";
		}
		*/

		if (_abacSqlServerServerName.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_SqlServer_Connection:abacSqlServerServer missing";
		}

		if (_abacSqlServerUserId.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_SqlServer_Connection:abacUserId missing";
		}

		if (_abacSqlServerPassword.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_SqlServer_Connection:abacPassword missing";
		}

		if (_abacSqlServerDb.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_SqlServer_Connection:abacViewDb missing";
		}

		if (_abacSqlServerBatchSize < MIN_BATCH_SIZE || _abacSqlServerBatchSize > MAX_BATCH_SIZE ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_SqlServer_Connection:abacBatchSize missing or must be valid integer between " + MIN_BATCH_SIZE + " and " + MAX_BATCH_SIZE;
		}
		
		if (_abacSqlServerCacheFileName.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "ABaC_SqlServer_Connection:abacCacheFileName missing";
		}
		
		if (_hdfsConfigDir.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "HDFS:configDir missing";
		}

		if (_hdfsRoot.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "HDFS:hdfsRoot missing";
		}

		if (_hdfsLandingZoneSubDir.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "HDFS:landingZoneSubDir missing";
		}

		if (_hdfsLandingZoneArrivalSubDir.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "HDFS:landingZoneArrivalSubDir missing";
		}
		
		if (_hdfsWorkSubDir.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "HDFS:workSubDir missing";
		}
		
		if (_hdfsHiveSubDir.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "HDFS:hiveSubDir missing";
		}

		if (_hdfsOutputSubDir.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "HDFS:outputSubDir missing";
		}

		if (_hdfsFinalSubDir.length() == 0 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "HDFS:finalSubDir missing";
		}

		if (_genDefaultABaCDateRangeInDays < 1 ) {
			_configValid = false;
			if ( _errText.length() > 0 ) {
				_errText += "\n";
			}
			_errText += "General:defaultABaCDateRangeInDays missing or must be valid integer greater than zero.";
		}

		if ( !this.emptyFileType ) {
			if (_fileSubDir.length() == 0 ) {
				_configValid = false;
				if ( _errText.length() > 0 ) {
					_errText += "\n";
				}
				_errText += "File:fileSubDir missing for fileType=\"" + _fileType + "\"";
			}

			if (_fileDesc.length() == 0 ) {
				_configValid = false;
				if ( _errText.length() > 0 ) {
					_errText += "\n";
				}
				_errText += "File:fileDesc missing for fileType=\"" + _fileType + "\"";
			}

			if (_fileTerrCdFieldPosition < 0 ) {
				_configValid = false;
				if ( _errText.length() > 0 ) {
					_errText += "\n";
				}
				_errText += "File:terrCdFieldPosition must be greater or equal to than zero";
			}

			if (_fileLgcyLclRfrDefCdFieldPosition < 0 ) {
				_configValid = false;
				if ( _errText.length() > 0 ) {
					_errText += "\n";
				}
				_errText += "File:lgcyLclRfrDefCdFieldPosition must be greater or equal to than zero";
			}

			if (_fileBusinessDateFieldPosition < 0 ) {
				_configValid = false;
				if ( _errText.length() > 0 ) {
					_errText += "\n";
				}
				_errText += "File:businessDateFieldPosition must be greater or equal to than zero";
			}

			if (_fileDefaultABaCDateRangeInDays < 0 ) {
				_configValid = false;
				if ( _errText.length() > 0 ) {
					_errText += "\n";
				}
				_errText += "File:defaultABaCDateRangeInDays is suppled must be positive valid integer.";
			}
		}
	}

}
package com.mcd.gdw.daas.abac;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.ResultSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.RDBMS;

public class ABaCUtility {

	private static final int UTF8_BOM = 65279;
	private static final String CORE_SITE_XML_FILE = "core-site.xml";
	private static final String HDFS_SITE_XML_FILE = "hdfs-site.xml";
	private static final String MAPRED_SITE_XML_FILE = "mapred-site.xml";

	private RDBMS abac;
	private RDBMS gdw;
	
	private Configuration hdfsConfig = null;
	private FileSystem hdfsFileSystem = null;

	public static void main(String[] args) {

		ABaCUtility instance = new ABaCUtility();

		instance.run(args);
		
	}

	public void run(String[] args) {

		String configXmlFile = "";
		String ctryParm = "";
		String lcatParm = "";
		String owshParm = "";
		String owshTypeParm = "";
		String aowOutParm = "";
		//AWS START
		String listTsParm = "";
		String listTerrCdParm = "";
		//AWS END
		boolean helpRequest = false;
		DaaSConfig daasConfig;
		File ctryFile = null;
		File lcatFile = null;
		File owshFile = null;
		File owshTypeFile = null;
		File aowFile = null;
		boolean includeGdwFl = false;
		boolean includeABaCFl = false;
		
		boolean updABaCRequest = false;
		boolean extAowRequest = false;
		boolean abacPurgeRequest = false;
		//AWS START
		boolean chgListRequest = false;
		//AWS END 
		
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equalsIgnoreCase("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equalsIgnoreCase("-ctry") ) {
				updABaCRequest = true;
				if ( (idx+1) < args.length ) {
					ctryParm = args[idx+1];
				}
			}

			if ( args[idx].equalsIgnoreCase("-lcat") ) { 
				updABaCRequest = true;
				if ( (idx+1) < args.length ) {
					lcatParm = args[idx+1];
				}
			}

			if ( args[idx].equalsIgnoreCase("-owsh") ) {
				updABaCRequest = true;
				if ( (idx+1) < args.length ) {
					owshParm = args[idx+1];
				}
			}
			
			if ( args[idx].equalsIgnoreCase("-owshtype") ) {
				updABaCRequest = true;
				if ( (idx+1) < args.length ) {
					owshTypeParm = args[idx+1];
				}
			}

			if ( args[idx].equalsIgnoreCase("-aowout") ) {
				extAowRequest = true;
				if ( (idx+1) < args.length ) {
					aowOutParm = args[idx+1];
				}
			}
			
			//AWS START
			if ( args[idx].equalsIgnoreCase("-listts") ) {
				chgListRequest = true;
				if ( (idx+1) < args.length ) {
					listTsParm = args[idx+1];
				}
			}

			if ( args[idx].equalsIgnoreCase("-listterrcd") ) {
				chgListRequest = true;
				if ( (idx+1) < args.length ) {
					listTerrCdParm = args[idx+1];
				}
			}
			//AWS END
			
			if ( args[idx].equalsIgnoreCase("-h") || args[idx].equalsIgnoreCase("-help")  ) {
				helpRequest = true;
			}
			
			if ( args[idx].equalsIgnoreCase("-abacpurge") ) {
				abacPurgeRequest = true;
			}
		}
		
		if ( helpRequest ) {
			System.out.println("Usage: ABaCUtility -c config.xml -ctry gdw|ctryFileName -lcat gdw|lcatFileName -owsh gdw|owshFileName -owshtype gdw|owshtypeFileName -aowout aowOutFileName");
			System.out.println("or     ABaCUtility -c config.xml -ctry gdw|ctryFileName -lcat gdw|lcatFileName -owsh gdw|owshFileName -owshtype gdw|owshtypeFileName");
			System.out.println("or     ABaCUtility -c config.xml  -aowout aowOutFileName");
			System.exit(0);
		}

		if ( configXmlFile.length() == 0 ) {
			System.err.println("Missing config.xml (-c) parameter");
			System.exit(8);
		}
		
		if ( updABaCRequest && ( ctryParm.length() == 0 || lcatParm.length() == 0 || owshParm.length() == 0 || owshTypeParm.length() == 0 ) ) {
			System.err.println("Missing gdw|ctryFileName (ctry), gdw|lcatFileName (lcat), gdw|owshFileName (-owsh) and/or gdw|owshTypeFileName (owshType) parameter(s)");
			System.exit(8);
		}
		
		if ( extAowRequest && aowOutParm.length() == 0 ) {
			System.err.println("Missing aowOutFileName (aowout) parameter");
			System.exit(8);
		}

		//AWS START
		if ( chgListRequest && ( listTsParm.length() == 0 ) ) {
			System.err.println("Missing listts parameter");
			System.exit(8);
		}
		//AWS END 
		daasConfig = new DaaSConfig(configXmlFile);
		
		if ( daasConfig.configValid() ) {
			
			if ( daasConfig.displayMsgs()  ) {
				System.out.println(daasConfig.toString());
			}

			if ( abacPurgeRequest ) {
				includeABaCFl = true;
			}
			
			if ( updABaCRequest ) {
				includeABaCFl = true;
				ctryFile = validateFile(ctryParm,"Country");
				lcatFile = validateFile(lcatParm,"Location");
				owshFile = validateFile(owshParm,"Ownership");
				owshTypeFile = validateFile(owshTypeParm,"Ownership Type");
				
				if ( ctryParm.equalsIgnoreCase("gdw") || lcatParm.equalsIgnoreCase("gdw") || owshParm.equalsIgnoreCase("gdw") || owshTypeParm.equalsIgnoreCase("gdw") ) {
					includeGdwFl = true;
				}
			}

			if ( extAowRequest ) {
				includeGdwFl = true;
			}
			
		    connectSQL(daasConfig,includeGdwFl,includeABaCFl);

			if ( updABaCRequest ) {
				updateCtry(daasConfig,ctryFile);
				updateLcat(daasConfig,lcatFile);
				updateOwsh(daasConfig,owshFile);
				updateOwshType(daasConfig,owshTypeFile);
			}
			
			if ( extAowRequest ) {
				aowFile = new File(aowOutParm);
				extractAow(daasConfig,aowFile);
			}

			if ( abacPurgeRequest ) {
				initHDFS(daasConfig);
				abacPurge(daasConfig);
			}
			
			//AWS START
			if ( chgListRequest ) {
				//initHDFS(daasConfig);
				getList(daasConfig, listTsParm, listTerrCdParm);
			}
			//AWS END 

		} else {
			System.err.println("Invalid config.xml");
			System.err.println("Config File    = " + configXmlFile);
			System.err.println(daasConfig.errText());
			System.exit(8);
		}

		try {
		    gdw.dispose();
		    abac.dispose();
		} catch (Exception ex) {
		}
		
	}

	//AWS START
	public static boolean UpdateABaCReference(DaaSConfig daasConfig) {
		ABaCUtility instance = new ABaCUtility();
		
		return(instance.UpdABaCReference(daasConfig));
		
	}
	
	private boolean UpdABaCReference(DaaSConfig daasConfig) {
		
		boolean successFl = false;
		
		try {
		    connectSQL(daasConfig,true,true);

		    updateCtry(daasConfig,null);
			updateLcat(daasConfig,null);
			updateOwsh(daasConfig,null);
			updateOwshType(daasConfig,null);
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			successFl = false;
		}

		try {
		    gdw.dispose();
		    abac.dispose();
		} catch (Exception ex) {
		}

		return(successFl);
	}
	//AWS END
	
	private File validateFile(String fileParm
			                 ,String msgText) {
	
		File retFile = null;
		
		if ( !fileParm.equalsIgnoreCase("gdw") ) {
			retFile = new File(fileParm);
			if ( !retFile.isFile() ) {
				System.err.println("Invalid " + msgText + " File: " + fileParm);
				System.exit(8);
			}
		}
		
		return(retFile);
		
	}
	
	private void connectSQL(DaaSConfig daasConfig
			               ,boolean includeGdwFl
			               ,boolean includeABaCFl) {

		try {
			if ( includeGdwFl) {
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.print("Connecting to GDW ... ");
			    }
			    
				gdw = new RDBMS(RDBMS.ConnectionType.Teradata,daasConfig.gblTpid(),daasConfig.gblUserId(),daasConfig.gblPassword(),daasConfig.gblNumSessions());
			    
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.println("done");
			    }
			}

			if ( includeABaCFl ) {
				if ( daasConfig.displayMsgs() ) {
			    	System.out.print("Connecting to ABaC ... ");
			    }
			    
				abac = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());
				abac.setBatchSize(daasConfig.abacSqlServerBatchSize());
			    abac.setAutoCommit(false);
			    
			    if ( daasConfig.displayMsgs() ) {
			    	System.out.println("done");
			    }
			}
			
		} catch (RDBMS.DBException ex ) {
			System.err.println("RDBMS Exception occured connecting to database:");
			ex.printStackTrace(System.err);
			System.exit(8);
			
		} catch (Exception ex ) {
			System.err.println("Exception occured connecting to database:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	private void updateCtry(DaaSConfig daasConfig
			               ,File ctryFile) {
		
		ResultSet rset=null;
		BufferedReader reader=null;
		StringBuffer sqlSource = new StringBuffer();
		StringBuffer sqlTarget = new StringBuffer();
	    int totUpdCount = 0;
	    int updCount = 0;
	    boolean moreFl = true;
	    boolean skipLine = false;
	    
	    String lineIn=null;
	    String[] parts;
	    
	    short ctryIsoNu=0;
	    String ctryIso2AbbrCd="";
	    String ctryNa="";
	    String ctryShrtNa="";

        boolean firstLine = true;

	    if ( daasConfig.displayMsgs() ) {
	    	if ( ctryFile == null ) {
				System.out.println("\nUpdate ABaC Country from GDW ... ");
	    	} else {
				System.out.println("\nUpdate ABaC Country from " + ctryFile.getAbsolutePath() + " ...");
	    	}
		}
		
		try {
			abac.executeUpdate("delete from " + daasConfig.abacSqlServerDb() + ".ctry;");
		
			if ( ctryFile == null ) {
				sqlSource.setLength(0);
				sqlSource.append("select\n");
				sqlSource.append("   CTRY_ISO_NU\n");
				sqlSource.append("  ,CTRY_ISO2_ABBR_CD\n");
				sqlSource.append("  ,CTRY_NA\n");
				sqlSource.append("  ,CTRY_SHRT_NA\n");
				sqlSource.append("from {VDB}.V1CTRY\n");
				
			    rset = gdw.resultSet(sqlSource.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));
			    moreFl = rset.next();
			} else {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(ctryFile),"UTF8"));
				if ( (lineIn = reader.readLine()) == null) {
					moreFl=false;
				}
			}

		    sqlTarget.setLength(0);
		    sqlTarget.append("insert into " + daasConfig.abacSqlServerDb() + ".ctry\n");
		    sqlTarget.append("   (\n");
		    sqlTarget.append("    CTRY_ISO_NU\n");
		    sqlTarget.append("   ,CTRY_ISO2_ABBR_CD\n");
		    sqlTarget.append("   ,CTRY_NA\n");
		    sqlTarget.append("   ,CTRY_SHRT_NA)\n");
		    sqlTarget.append("  values (?,?,?,?);\n");

		    abac.setPreparedStatement(sqlTarget.toString());
		    totUpdCount = 0;

		    while ( moreFl ) {

		    	if ( ctryFile == null ) {
			    	ctryIsoNu = rset.getShort("CTRY_ISO_NU");
			    	ctryIso2AbbrCd = rset.getString("CTRY_ISO2_ABBR_CD"); 
		            ctryNa = rset.getString("CTRY_NA");
		            ctryShrtNa = rset.getString("CTRY_SHRT_NA");
		    	} else {
		    		if ( firstLine ) {
		    			firstLine = false;
		    			if ( (int)lineIn.charAt(0) == UTF8_BOM ) {
		    				lineIn = lineIn.substring(1);
		    			}
		    		}
		    		
		    		parts = lineIn.split("\\t");
		    		
		    		if ( parts.length >= 4 ) {
		    			try {
			    			ctryIsoNu = Short.parseShort(parts[0]);
		    			} catch (Exception ex) {
		    				skipLine = true;
		    			}
		    			ctryIso2AbbrCd = parts[1]; 
		    			ctryNa = parts[2];
		    			ctryShrtNa = parts[3];
		    		} else {
		    			skipLine = true;
		    		}
		    	}
		    	
		    	if ( skipLine ) {
		    		skipLine = false;
		    		System.err.println("  ==>>Skipping invalid input line: '" + lineIn + "'<<==");
		    	} else {
		    		updCount = abac.addBatch(ctryIsoNu
		    		    	                ,ctryIso2AbbrCd
		    			                    ,ctryNa
		    			                    ,ctryShrtNa);
		    	
		    		if ( updCount > 0 ) {
		    			totUpdCount += updCount;
		    			if ( daasConfig.displayMsgs() ) {
		    				System.out.println("  inserted " + totUpdCount + " rows");
		    			}
		    		}
		    	}
		    	
		    	if ( ctryFile == null ) {
		    		moreFl = rset.next();
		    	} else {
					if ( (lineIn = reader.readLine()) == null) {
						moreFl=false;
					}
		    	}
		    }
		    
		    updCount = abac.finalizeBatch();
			if ( daasConfig.displayMsgs() ) {
				System.out.println("  inserted " + updCount + " total rows");
			}
		    abac.commit();

		    if ( ctryFile == null ) {
		    	rset.close();
		    } else {
		    	reader.close();
		    }

		    if ( daasConfig.displayMsgs() ) {
		    	System.out.println("Update ABaC Country done");
		    }
			
		} catch (RDBMS.DBException ex ) {
			System.err.println("RDBMS Exception occured in update ABaC Country data:");
			ex.printStackTrace(System.err);
			System.exit(8);
			
		} catch (Exception ex ) {
			System.err.println("Exception occured in update ABaC Country data:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void updateLcat(DaaSConfig daasConfig
			               ,File lcatFile) {
		
		ResultSet rset=null;
		BufferedReader reader=null;
		StringBuffer sqlSource = new StringBuffer();
		StringBuffer sqlTarget = new StringBuffer();
	    int totUpdCount = 0;
	    int updCount = 0;
	    boolean moreFl = true;
	    boolean skipLine = false;
	    
	    String lineIn=null;
	    String[] parts;
	    
	    short ctryIsoNu=0;
        String lcat = ""; 
        String lcatNa = "";
        BigDecimal gblNu = null;
        String openDt = "";
        String clseDt = "";
        java.sql.Date strOpenDt;
        java.sql.Date strClseDt;

        boolean firstLine = true;

	    if ( daasConfig.displayMsgs() ) {
	    	if ( lcatFile == null ) {
				System.out.println("\nUpdate ABaC Location from GDW ... ");
	    	} else {
				System.out.println("\nUpdate ABaC Location from " + lcatFile.getAbsolutePath() + " ...");
	    	}
		}
		
		try {
			abac.executeUpdate("delete from " + daasConfig.abacSqlServerDb() + ".gbal_lcat;");
		
			if ( lcatFile == null ) {
				sqlSource.setLength(0);
			    sqlSource.append("select\n");
			    sqlSource.append("   a.CTRY_ISO_NU\n");
			    sqlSource.append("  ,a.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("  ,a.MCD_GBAL_LCAT_ID_NU\n");
			    sqlSource.append("  ,a.MCD_GBAL_BUSN_LCAT_NA\n");
			    sqlSource.append("  ,case when c.STR_OPEN_DT is null then cast('' as varchar(10)) else cast(c.STR_OPEN_DT as varchar(10)) end as STR_OPEN_DT\n");
			    sqlSource.append("  ,case when d.STR_CLSE_DT is null then cast('' as varchar(10)) else cast(d.STR_CLSE_DT as varchar(10)) end as STR_CLSE_DT\n");
			    sqlSource.append("from {VDB}.V1MCD_GBAL_BUSN_LCAT a\n");
			    sqlSource.append("inner join {VDB}.V1MCD_GBAL_BUSN_LCAT_TYP b\n");
			    sqlSource.append("  on (b.MCD_GBAL_BUSN_LCAT_TYP_ID = a.MCD_GBAL_BUSN_LCAT_TYP_ID)\n");
			    sqlSource.append("left outer join (\n");
			    sqlSource.append("     select\n");
			    sqlSource.append("        a.MCD_GBAL_LCAT_ID_NU\n");
			    sqlSource.append("       ,max(a.REST_LFCY_EVNT_DT) as STR_OPEN_DT\n");
			    sqlSource.append("     from {VDB}.V1REST_LFCY_EVNT a\n");
			    sqlSource.append("     inner join {VDB}.V1REST_LFCY_EVNT_TYP b\n");
			    sqlSource.append("       on (b.REST_LFCY_EVNT_TYP_ID = a.REST_LFCY_EVNT_TYP_ID)\n");
			    sqlSource.append("     where a.REST_LFCY_EVNT_PLAN_ACT_CD = 'A'\n");
			    sqlSource.append("     and   b.REST_LFCY_EVNT_TYP_SHRT_DS = 'OP'\n");
			    sqlSource.append("     group by\n");
			    sqlSource.append("        a.MCD_GBAL_LCAT_ID_NU) c\n");
			    sqlSource.append("  on (c.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU)\n");
			    sqlSource.append("left outer join (\n");  
			    sqlSource.append("     select\n");
			    sqlSource.append("        a.MCD_GBAL_LCAT_ID_NU\n");
			    sqlSource.append("       ,min(a.REST_LFCY_EVNT_DT) as STR_CLSE_DT\n");
			    sqlSource.append("     from {VDB}.V1REST_LFCY_EVNT a\n");
			    sqlSource.append("     inner join {VDB}.V1REST_LFCY_EVNT_TYP b\n");
			    sqlSource.append("       on (b.REST_LFCY_EVNT_TYP_ID = a.REST_LFCY_EVNT_TYP_ID)\n");
			    sqlSource.append("     where a.REST_LFCY_EVNT_PLAN_ACT_CD = 'A'\n");
			    sqlSource.append("     and   b.REST_LFCY_EVNT_TYP_SHRT_DS = 'CL'\n");
			    sqlSource.append("     group by\n");
			    sqlSource.append("        a.MCD_GBAL_LCAT_ID_NU) d\n");
			    sqlSource.append("  on (d.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU)\n");
			    sqlSource.append("left outer join (\n");  
			    sqlSource.append("     select\n");
			    sqlSource.append("	    a.CTRY_ISO_NU\n");
			    sqlSource.append("       ,a.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("     from {VDB}.V1MCD_GBAL_BUSN_LCAT a\n");
			    sqlSource.append("     inner join {VDB}.V1MCD_GBAL_BUSN_LCAT_TYP b\n");
			    sqlSource.append("       on (b.MCD_GBAL_BUSN_LCAT_TYP_ID = a.MCD_GBAL_BUSN_LCAT_TYP_ID)\n");
			    sqlSource.append("     where substring(b.MCD_GBAL_BUSN_LCAT_TYP_SHRT_DS from 1 for 1) = 'R'\n");
			    sqlSource.append("     group by\n"); 
			    sqlSource.append("        a.CTRY_ISO_NU\n");
			    sqlSource.append("       ,a.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("     having count(*) > 1) e\n");		    
			    sqlSource.append("  on (e.CTRY_ISO_NU = a.CTRY_ISO_NU\n");
			    sqlSource.append("      and e.LGCY_LCL_RFR_DEF_CD = a.LGCY_LCL_RFR_DEF_CD)\n");
			    sqlSource.append("where substring(b.MCD_GBAL_BUSN_LCAT_TYP_SHRT_DS from 1 for 1) = 'R'\n");
			    sqlSource.append("and   e.CTRY_ISO_NU is null\n");
			    sqlSource.append("order by\n");
			    sqlSource.append("   a.CTRY_ISO_NU\n");
			    sqlSource.append("  ,a.LGCY_LCL_RFR_DEF_CD\n");
				
			    rset = gdw.resultSet(sqlSource.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));
			    moreFl = rset.next();
			} else {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(lcatFile),"UTF8"));
				if ( (lineIn = reader.readLine()) == null) {
					moreFl=false;
				}
			}

		    sqlTarget.setLength(0);
		    sqlTarget.append("insert into " + daasConfig.abacSqlServerDb() + ".gbal_lcat\n");
		    sqlTarget.append("   (\n");
		    sqlTarget.append("    CTRY_ISO_NU\n");
		    sqlTarget.append("   ,LGCY_LCL_RFR_DEF_CD\n");
		    sqlTarget.append("   ,MCD_GBAL_BUSN_LCAT_NA\n");
		    sqlTarget.append("   ,MCD_GBAL_LCAT_ID_NU\n");
		    sqlTarget.append("   ,STR_OPEN_DT\n");
		    sqlTarget.append("   ,STR_CLSE_DT)\n");
		    sqlTarget.append("  values (?,?,?,?,?,?);\n");

		    abac.setPreparedStatement(sqlTarget.toString());
		    totUpdCount = 0;

		    while ( moreFl ) {

		    	if ( lcatFile == null ) {
			    	ctryIsoNu = rset.getShort("CTRY_ISO_NU");
		            lcat = rset.getString("LGCY_LCL_RFR_DEF_CD"); 
		            gblNu = rset.getBigDecimal("MCD_GBAL_LCAT_ID_NU");
		            lcatNa = rset.getString("MCD_GBAL_BUSN_LCAT_NA");
		            openDt = rset.getString("STR_OPEN_DT");
		            clseDt = rset.getString("STR_CLSE_DT");
		    	} else {
		    		if ( firstLine ) {
		    			firstLine = false;
		    			if ( (int)lineIn.charAt(0) == UTF8_BOM ) {
		    				lineIn = lineIn.substring(1);
		    			}
		    		}
		    		
		    		parts = lineIn.split("\\t");
		    		
		    		if ( parts.length >= 4 ) {
		    			try {
			    			ctryIsoNu = Short.parseShort(parts[0]);
		    			} catch (Exception ex) {
		    				skipLine = true;
		    			}
		    			lcat = parts[1]; 
		    			try {
		    				gblNu = new BigDecimal(parts[2]);
		    			} catch (Exception ex) {
		    				skipLine = true;
		    			}
		    			lcatNa = parts[3];
		    			
		    			if ( parts.length >= 5 ) {
			    			openDt = parts[4];
		    			} else {
		    				openDt = "";
		    			}
		    			
		    			if ( parts.length >= 6 ) {
			    			clseDt = parts[5];
		    			} else {
		    				clseDt = "";
		    			}
		    		} else {
		    			skipLine = true;
		    		}
		    	}

		    	if ( openDt.length() < 10 ) {
		    		strOpenDt = java.sql.Date.valueOf("1955-04-15");
		    	} else {
		    		strOpenDt = java.sql.Date.valueOf(openDt);
		    	}

		    	if ( clseDt.length() < 10 ) {
		    		strClseDt = java.sql.Date.valueOf("9999-12-31");
		    	} else {
		    		strClseDt = java.sql.Date.valueOf(clseDt);
		    	}
		    	
		    	if ( skipLine ) {
		    		skipLine = false;
		    		System.err.println("  ==>>Skipping invalid input line: '" + lineIn + "'<<==");
		    	} else {
		    		updCount = abac.addBatch(ctryIsoNu
		    		    	                ,lcat
		    			                    ,lcatNa
		    			                    ,gblNu
		    			                    ,strOpenDt
		    			                    ,strClseDt);
		    	
		    		if ( updCount > 0 ) {
		    			totUpdCount += updCount;
		    			if ( daasConfig.displayMsgs() ) {
		    				System.out.println("  inserted " + totUpdCount + " rows");
		    			}
		    		}
		    	}
		    	
		    	if ( lcatFile == null ) {
		    		moreFl = rset.next();
		    	} else {
					if ( (lineIn = reader.readLine()) == null) {
						moreFl=false;
					}
		    	}
		    }
		    
		    updCount = abac.finalizeBatch();
			if ( daasConfig.displayMsgs() ) {
				System.out.println("  inserted " + updCount + " total rows");
			}
		    abac.commit();

		    if ( lcatFile == null ) {
		    	rset.close();
		    } else {
		    	reader.close();
		    }

		    if ( daasConfig.displayMsgs() ) {
		    	System.out.println("Update ABaC Location done");
		    }
			
		} catch (RDBMS.DBException ex ) {
			System.err.println("RDBMS Exception occured in update ABaC Location data:");
			ex.printStackTrace(System.err);
			System.exit(8);
			
		} catch (Exception ex ) {
			System.err.println("Exception occured in update ABaC Location data:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void updateOwsh(DaaSConfig daasConfig
			               ,File owshFile) {
		
		ResultSet rset=null;
		BufferedReader reader=null;
		StringBuffer sqlSource = new StringBuffer();
		StringBuffer sqlTarget = new StringBuffer();
	    int totUpdCount = 0;
	    int updCount = 0;
	    boolean moreFl = true;
	    boolean skipLine = false;
	    
	    String lineIn=null;
	    String[] parts;
	    
	    short ctryIsoNu=0;
        String lcat = ""; 
        String owshTypeCd = "";
        String owshSubTypeCd = "";
        String effDt = "";
        String endDt = "";
        java.sql.Date owshEffDt;
        java.sql.Date owshEndDt;

        boolean firstLine = true;

	    if ( daasConfig.displayMsgs() ) {
	    	if ( owshFile == null ) {
				System.out.println("\nUpdate ABaC Ownership from GDW ... ");
	    	} else {
				System.out.println("\nUpdate ABaC Ownership from " + owshFile.getAbsolutePath() + " ...");
	    	}
		}
		
		try {
			abac.executeUpdate("delete from " + daasConfig.abacSqlServerDb() + ".rest_owsh;");
		
			if ( owshFile == null ) {
			    sqlSource.setLength(0);
			    sqlSource.append("select\n");
			    sqlSource.append("    a.CTRY_ISO_NU\n");
			    sqlSource.append("   ,a.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("   ,a.REST_OWSH_TYP_SHRT_DS\n");
			    sqlSource.append("   ,a.REST_OWSH_SUB_TYP_SHRT_DS\n");
			    sqlSource.append("   ,cast(cast(extract(year from a.REST_OWSH_EFF_DT) as char(4)) || case when extract(month from a.REST_OWSH_EFF_DT) < 10 then '-0' else '-' end || cast(extract(month from a.REST_OWSH_EFF_DT) as varchar(2)) || case when extract(day from a.REST_OWSH_EFF_DT) < 10 then '-0' else '-' end || cast(extract(day from a.REST_OWSH_EFF_DT) as varchar(2)) as varchar(10)) as REST_OWSH_EFF_DT\n");
			    sqlSource.append("   ,case when a.NEW_REST_OWSH_END_DT = cast('9999-12-31' as date) then cast('' as varchar(10)) else cast(cast(extract(year from a.NEW_REST_OWSH_END_DT) as char(4)) || case when extract(month from a.NEW_REST_OWSH_END_DT) < 10 then '-0' else '-' end || cast(extract(month from a.NEW_REST_OWSH_END_DT) as varchar(2)) || case when extract(day from a.NEW_REST_OWSH_END_DT) < 10 then '-0' else '-' end || cast(extract(day from a.NEW_REST_OWSH_END_DT) as varchar(2)) as varchar(10)) end as REST_OWSH_END_DT\n");
			    sqlSource.append("from (\n");
			    sqlSource.append("     select\n"); 
			    sqlSource.append("         a.CTRY_ISO_NU\n");
			    sqlSource.append("        ,a.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("        ,a.REST_OWSH_TYP_SHRT_DS\n");
			    sqlSource.append("        ,a.REST_OWSH_SUB_TYP_SHRT_DS\n");
			    sqlSource.append("        ,a.REST_OWSH_EFF_DT\n");
			    sqlSource.append("        ,a.REST_OWSH_END_DT\n");
			    sqlSource.append("        ,a.NEXT_REST_OWSH_EFF_DT\n");
			    sqlSource.append("        ,cast(case when a.NEXT_REST_OWSH_EFF_DT = a.REST_OWSH_EFF_DT then a.REST_OWSH_END_DT else \n");
			    sqlSource.append("           case when cast((a.NEXT_REST_OWSH_EFF_DT - 1) as date) < a.REST_OWSH_END_DT then cast((a.NEXT_REST_OWSH_EFF_DT - 1) as date) else a.REST_OWSH_END_DT end end as date) as NEW_REST_OWSH_END_DT\n");
			    sqlSource.append("     from (\n");
			    sqlSource.append("          select\n");
			    sqlSource.append("              a.CTRY_ISO_NU\n");
			    sqlSource.append("             ,a.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("             ,a.REST_OWSH_TYP_SHRT_DS\n");
			    sqlSource.append("             ,a.REST_OWSH_SUB_TYP_SHRT_DS\n");
			    sqlSource.append("             ,a.REST_OWSH_EFF_DT\n");
			    sqlSource.append("             ,a.REST_OWSH_END_DT\n");
			    sqlSource.append("             ,b.REST_OWSH_EFF_DT as NEXT_REST_OWSH_EFF_DT\n");
			    sqlSource.append("             ,row_number() over(partition by a.CTRY_ISO_NU,a.LGCY_LCL_RFR_DEF_CD,a.REST_OWSH_EFF_DT,a.REST_OWSH_END_DT order by b.REST_OWSH_EFF_DT desc) as ROW_NUM\n");
			    sqlSource.append("          from (\n");
			    sqlSource.append("               select\n"); 
			    sqlSource.append("                   a.CTRY_ISO_NU\n");
			    sqlSource.append("                  ,a.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("                  ,a.REST_OWSH_TYP_SHRT_DS\n");
			    sqlSource.append("                  ,a.REST_OWSH_SUB_TYP_SHRT_DS\n");
			    sqlSource.append("                  ,a.REST_OWSH_EFF_DT\n");
			    sqlSource.append("                  ,a.REST_OWSH_END_DT\n");
			    sqlSource.append("               from (\n");
			    sqlSource.append("                    select\n"); 
			    sqlSource.append("                        d.CTRY_ISO_NU\n");
			    sqlSource.append("                       ,d.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("                       ,b.REST_OWSH_TYP_SHRT_DS\n");
			    sqlSource.append("                       ,c.REST_OWSH_SUB_TYP_SHRT_DS\n");
			    sqlSource.append("                       ,a.REST_OWSH_EFF_DT\n");
			    sqlSource.append("                       ,case when a.REST_OWSH_END_DT is null then cast('9999-12-31' as date) else a.REST_OWSH_END_DT end as REST_OWSH_END_DT\n");
			    sqlSource.append("                       ,row_number() over(partition by d.CTRY_ISO_NU,d.LGCY_LCL_RFR_DEF_CD,a.REST_OWSH_EFF_DT order by a.REST_OWSH_END_DT desc) as ROW_NUM\n");
			    sqlSource.append("                    from {VDB}.V1REST_OWSH a\n");
			    sqlSource.append("                    inner join {VDB}.V1REST_OWSH_TYP b\n");
			    sqlSource.append("                      on (b.REST_OWSH_TYP_ID = a.REST_OWSH_TYP_ID)\n");
			    sqlSource.append("                    inner join {VDB}.V1REST_OWSH_SUB_TYP c\n");
			    sqlSource.append("                      on (c.REST_OWSH_TYP_ID = a.REST_OWSH_TYP_ID\n");
			    sqlSource.append("                          and c.REST_OWSH_SUB_TYP_ID = a.REST_OWSH_SUB_TYP_ID)\n");
			    sqlSource.append("                    inner join {VDB}.V1MCD_GBAL_BUSN_LCAT d\n");
			    sqlSource.append("                      on (d.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU)) a\n");
			    sqlSource.append("               where a.ROW_NUM = 1) a\n");
			    sqlSource.append("          inner join (\n");
			    sqlSource.append("               select \n");
			    sqlSource.append("                   a.CTRY_ISO_NU\n");
			    sqlSource.append("                  ,a.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("                  ,a.REST_OWSH_EFF_DT\n");
			    sqlSource.append("                  ,a.REST_OWSH_END_DT\n");
			    sqlSource.append("               from (\n");
			    sqlSource.append("                    select\n"); 
			    sqlSource.append("                        d.CTRY_ISO_NU\n");
			    sqlSource.append("                       ,d.LGCY_LCL_RFR_DEF_CD\n");
			    sqlSource.append("                       ,a.REST_OWSH_EFF_DT\n");
			    sqlSource.append("                       ,case when a.REST_OWSH_END_DT is null then cast('9999-12-31' as date) else a.REST_OWSH_END_DT end as REST_OWSH_END_DT\n");
			    sqlSource.append("                       ,row_number() over(partition by d.CTRY_ISO_NU,d.LGCY_LCL_RFR_DEF_CD,a.REST_OWSH_EFF_DT order by a.REST_OWSH_END_DT desc) as ROW_NUM\n");
			    sqlSource.append("                    from {VDB}.V1REST_OWSH a\n");
			    sqlSource.append("                    inner join {VDB}.V1MCD_GBAL_BUSN_LCAT d\n");
			    sqlSource.append("                      on (d.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU)) a\n");
			    sqlSource.append("               where a.ROW_NUM = 1) b\n");
			    sqlSource.append("            on (b.CTRY_ISO_NU = a.CTRY_ISO_NU\n");
			    sqlSource.append("                and b.LGCY_LCL_RFR_DEF_CD = a.LGCY_LCL_RFR_DEF_CD)) a\n");
			    sqlSource.append("     where a.ROW_NUM = 1) a\n");
//			    sqlSource.append("select\n");
//			    sqlSource.append("    a.CTRY_ISO_NU\n");
//			    sqlSource.append("   ,a.LGCY_LCL_RFR_DEF_CD\n");
//			    sqlSource.append("   ,a.REST_OWSH_TYP_SHRT_DS\n");
//			    sqlSource.append("   ,a.REST_OWSH_SUB_TYP_SHRT_DS\n");
//			    sqlSource.append("   ,a.REST_OWSH_EFF_DT\n");
//			    sqlSource.append("   ,a.REST_OWSH_END_DT\n");
//			    sqlSource.append("from (\n");
//			    sqlSource.append("     select\n"); 
//			    sqlSource.append("         d.CTRY_ISO_NU\n");
//			    sqlSource.append("        ,d.LGCY_LCL_RFR_DEF_CD\n");
//			    sqlSource.append("        ,b.REST_OWSH_TYP_SHRT_DS\n");
//			    sqlSource.append("        ,c.REST_OWSH_SUB_TYP_SHRT_DS\n");
//			    sqlSource.append("        ,cast(a.REST_OWSH_EFF_DT as varchar(10)) as REST_OWSH_EFF_DT\n");
//			    sqlSource.append("        ,case when a.REST_OWSH_END_DT is null then cast('' as varchar(10)) else cast(a.REST_OWSH_END_DT as varchar(10)) end as REST_OWSH_END_DT\n");
//			    sqlSource.append("        ,row_number() over(partition by d.CTRY_ISO_NU,d.LGCY_LCL_RFR_DEF_CD,a.REST_OWSH_EFF_DT order by a.REST_OWSH_END_DT desc) as ROW_NUM\n");
//			    sqlSource.append("     from {VDB}.V1REST_OWSH a\n");
//			    sqlSource.append("     inner join {VDB}.V1REST_OWSH_TYP b\n");
//			    sqlSource.append("       on (b.REST_OWSH_TYP_ID = a.REST_OWSH_TYP_ID)\n");
//			    sqlSource.append("     inner join {VDB}.V1REST_OWSH_SUB_TYP c\n");
//			    sqlSource.append("       on (c.REST_OWSH_TYP_ID = a.REST_OWSH_TYP_ID\n");
//			    sqlSource.append("           and c.REST_OWSH_SUB_TYP_ID = a.REST_OWSH_SUB_TYP_ID)\n");
//			    sqlSource.append("     inner join {VDB}.V1MCD_GBAL_BUSN_LCAT d\n");
//			    sqlSource.append("       on (d.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU)) a\n");
//			    sqlSource.append("where a.ROW_NUM = 1\n");		    
				
			    rset = gdw.resultSet(sqlSource.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));
			    moreFl = rset.next();
			} else {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(owshFile),"UTF8"));
				if ( (lineIn = reader.readLine()) == null) {
					moreFl=false;
				}
			}

		    sqlTarget.setLength(0);
		    sqlTarget.append("insert into " + daasConfig.abacSqlServerDb() + ".rest_owsh\n");
		    sqlTarget.append("   (\n");
		    sqlTarget.append("    CTRY_ISO_NU\n");
		    sqlTarget.append("   ,LGCY_LCL_RFR_DEF_CD\n");
		    sqlTarget.append("   ,REST_OWSH_TYP_SHRT_DS\n");
		    sqlTarget.append("   ,REST_OWSH_SUB_TYP_SHRT_DS\n");
		    sqlTarget.append("   ,REST_OWSH_EFF_DT\n");
		    sqlTarget.append("   ,REST_OWSH_END_DT)\n");
		    sqlTarget.append("  values (?,?,?,?,?,?);\n");

		    abac.setPreparedStatement(sqlTarget.toString());
		    totUpdCount = 0;

		    while ( moreFl ) {

		    	if ( owshFile == null ) {
			    	ctryIsoNu = rset.getShort("CTRY_ISO_NU");
		            lcat = rset.getString("LGCY_LCL_RFR_DEF_CD"); 
		            owshTypeCd = rset.getString("REST_OWSH_TYP_SHRT_DS").trim();
		            owshSubTypeCd = rset.getString("REST_OWSH_SUB_TYP_SHRT_DS").trim();
		            effDt = rset.getString("REST_OWSH_EFF_DT");
		            endDt = rset.getString("REST_OWSH_END_DT");
		    	} else {
		    		if ( firstLine ) {
		    			firstLine = false;
		    			if ( (int)lineIn.charAt(0) == UTF8_BOM ) {
		    				lineIn = lineIn.substring(1);
		    			}
		    		}
		    		
		    		parts = lineIn.split("\\t");
		    		
		    		if ( parts.length >= 4 ) {
		    			try {
			    			ctryIsoNu = Short.parseShort(parts[0]);
		    			} catch (Exception ex) {
		    				skipLine = true;
		    			}
		    			lcat = parts[1]; 
		    			owshTypeCd = parts[2];
		    			owshSubTypeCd = parts[3];
		    			
		    			if ( parts.length >= 5 ) {
			    			effDt = parts[4];
		    			} else {
		    				effDt = "";
		    			}
		    			
		    			if ( parts.length >= 6 ) {
			    			endDt = parts[5];
		    			} else {
		    				endDt = "";
		    			}
		    		} else {
		    			skipLine = true;
		    		}
		    	}

		    	if ( effDt.length() < 10 ) {
		    		owshEffDt = java.sql.Date.valueOf("1955-04-15");
		    	} else {
		    		owshEffDt = java.sql.Date.valueOf(effDt);
		    	}

		    	if ( endDt.length() < 10 ) {
		    		owshEndDt = java.sql.Date.valueOf("9999-12-31");
		    	} else {
		    		owshEndDt = java.sql.Date.valueOf(endDt);
		    	}
		    	
		    	if ( skipLine ) {
		    		skipLine = false;
		    		System.err.println("  ==>>Skipping invalid input line: '" + lineIn + "'<<==");
		    	} else {
		    		updCount = abac.addBatch(ctryIsoNu
		    		    	                ,lcat
		    			                    ,owshTypeCd
		    			                    ,owshSubTypeCd
		    			                    ,owshEffDt
		    			                    ,owshEndDt);
		    	
		    		if ( updCount > 0 ) {
		    			totUpdCount += updCount;
		    			if ( daasConfig.displayMsgs() ) {
		    				System.out.println("  inserted " + totUpdCount + " rows");
		    			}
		    		}
		    	}
		    	
		    	if ( owshFile == null ) {
		    		moreFl = rset.next();
		    	} else {
					if ( (lineIn = reader.readLine()) == null) {
						moreFl=false;
					}
		    	}
		    }
		    
		    updCount = abac.finalizeBatch();
			if ( daasConfig.displayMsgs() ) {
				System.out.println("  inserted " + updCount + " total rows");
			}
		    abac.commit();

		    if ( owshFile == null ) {
		    	rset.close();
		    } else {
		    	reader.close();
		    }

		    if ( daasConfig.displayMsgs() ) {
		    	System.out.println("Update ABaC Ownership done");
		    }
			
		} catch (RDBMS.DBException ex ) {
			System.err.println("RDBMS Exception occured in update ABaC Ownership data:");
			ex.printStackTrace(System.err);
			System.exit(8);
			
		} catch (Exception ex ) {
			System.err.println("Exception occured in update ABaC Ownership data:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void updateOwshType(DaaSConfig daasConfig
			                   ,File owshTypeFile) {
		
		ResultSet rset=null;
		BufferedReader reader=null;
		StringBuffer sqlSource = new StringBuffer();
		StringBuffer sqlTarget = new StringBuffer();
	    int totUpdCount = 0;
	    int updCount = 0;
	    boolean moreFl = true;
	    boolean skipLine = false;
	    
	    String lineIn=null;
	    String[] parts;
	    
        String owshTypeCd = "";
        String owshTypeDs = "";
        String owshSubTypeCd = "";
        String owshSubTypeDs = "";

        boolean firstLine = true;

	    if ( daasConfig.displayMsgs() ) {
	    	if ( owshTypeFile == null ) {
				System.out.println("\nUpdate ABaC Ownership Type from GDW ... ");
	    	} else {
				System.out.println("\nUpdate ABaC Ownership Type from " + owshTypeFile.getAbsolutePath() + " ...");
	    	}
		}
		
		try {
			abac.executeUpdate("delete from " + daasConfig.abacSqlServerDb() + ".rest_owsh_typ;");
		
			if ( owshTypeFile == null ) {
			    sqlSource.setLength(0);
			    sqlSource.append("select\n");
			    sqlSource.append("   b.REST_OWSH_TYP_SHRT_DS\n");
			    sqlSource.append("  ,b.REST_OWSH_TYP_DS\n");
			    sqlSource.append("  ,a.REST_OWSH_SUB_TYP_SHRT_DS\n");
			    sqlSource.append("  ,a.REST_OWSH_SUB_TYP_DS\n");
			    sqlSource.append("from {VDB}.V1REST_OWSH_SUB_TYP a\n");
			    sqlSource.append("inner join {VDB}.V1REST_OWSH_TYP b\n");
			    sqlSource.append("on (b.REST_OWSH_TYP_ID = a.REST_OWSH_TYP_ID)\n");
				
			    rset = gdw.resultSet(sqlSource.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));
			    moreFl = rset.next();
			} else {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(owshTypeFile),"UTF8"));
				if ( (lineIn = reader.readLine()) == null) {
					moreFl=false;
				}
			}

		    sqlTarget.setLength(0);
		    sqlTarget.append("insert into " + daasConfig.abacSqlServerDb() + ".rest_owsh_typ\n");
		    sqlTarget.append("   (\n");
		    sqlTarget.append("    REST_OWSH_TYP_SHRT_DS\n");
		    sqlTarget.append("   ,REST_OWSH_TYP_DS\n");
		    sqlTarget.append("   ,REST_OWSH_SUB_TYP_SHRT_DS\n");
		    sqlTarget.append("   ,REST_OWSH_SUB_TYP_DS)\n");
		    sqlTarget.append("  values (?,?,?,?);\n");

		    abac.setPreparedStatement(sqlTarget.toString());
		    totUpdCount = 0;

		    while ( moreFl ) {

		    	if ( owshTypeFile == null ) {
		            owshTypeCd = rset.getString("REST_OWSH_TYP_SHRT_DS").trim();
		            owshTypeDs = rset.getString("REST_OWSH_TYP_DS");
		            owshSubTypeCd = rset.getString("REST_OWSH_SUB_TYP_SHRT_DS").trim();
		            owshSubTypeDs = rset.getString("REST_OWSH_SUB_TYP_DS");
		    	} else {
		    		if ( firstLine ) {
		    			firstLine = false;
		    			if ( (int)lineIn.charAt(0) == UTF8_BOM ) {
		    				lineIn = lineIn.substring(1);
		    			}
		    		}
		    		
		    		parts = lineIn.split("\\t");
		    		
		    		if ( parts.length >= 4 ) {
		    			owshTypeCd = parts[0];
		    			owshTypeDs = parts[1];
		    			owshSubTypeCd = parts[2];
		    			owshSubTypeDs = parts[3];
		    		} else {
		    			skipLine = true;
		    		}
		    	}
		    	
		    	if ( skipLine ) {
		    		skipLine = false;
		    		System.err.println("  ==>>Skipping invalid input line: '" + lineIn + "'<<==");
		    	} else {
		    		updCount = abac.addBatch(owshTypeCd
		    				                ,owshTypeDs
		    			                    ,owshSubTypeCd
		    			                    ,owshSubTypeDs);
		    	
		    		if ( updCount > 0 ) {
		    			totUpdCount += updCount;
		    			if ( daasConfig.displayMsgs() ) {
		    				System.out.println("  inserted " + totUpdCount + " rows");
		    			}
		    		}
		    	}
		    	
		    	if ( owshTypeFile == null ) {
		    		moreFl = rset.next();
		    	} else {
					if ( (lineIn = reader.readLine()) == null) {
						moreFl=false;
					}
		    	}
		    }
		    
		    updCount = abac.finalizeBatch();
			if ( daasConfig.displayMsgs() ) {
				System.out.println("  inserted " + updCount + " total rows");
			}
		    abac.commit();

		    if ( owshTypeFile == null ) {
		    	rset.close();
		    } else {
		    	reader.close();
		    }

		    if ( daasConfig.displayMsgs() ) {
		    	System.out.println("Update ABaC Ownership Type done");
		    }
			
		} catch (RDBMS.DBException ex ) {
			System.err.println("RDBMS Exception occured in update ABaC Ownership Type data:");
			ex.printStackTrace(System.err);
			System.exit(8);
			
		} catch (Exception ex ) {
			System.err.println("Exception occured in update ABaC Ownership Type data:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void extractAow(DaaSConfig daasConfig
			               ,File aowFile) {

		ResultSet rset=null;
		StringBuffer sqlSource = new StringBuffer();

		sqlSource.setLength(0);
		sqlSource.append("select\n");
		sqlSource.append("   a.TERR_CD\n");
		sqlSource.append("  ,a.SCMA_NA\n");
		sqlSource.append("from (\n");
		sqlSource.append("	select\n"); 
		sqlSource.append("	   lvl2.GBAL_HRCY_NODE_NA as AOW_NA\n");
		sqlSource.append("	  ,case lvl2.GBAL_HRCY_NODE_NA when 'APMEA' then 'ap'\n");
		//AWS Migration
		sqlSource.append("	                               when 'Europe' then  case when ctry.CTRY_ISO_NU=616  then  'poland' else  'eu' end\n");
		sqlSource.append("									when 'North America' then case when ctry.CTRY_ISO_NU = 124 then 'canada' when ctry.CTRY_ISO_NU = 840 then 'us' else 'xx' end\n");//Added to load data into both us and cananda schema
		//sqlSource.append("	                               when 'North America' then case when ctry.CTRY_ISO_NU = 840 then 'us' else 'xx' end\n"); 
		sqlSource.append("	                               else 'xx' end as SCMA_NA\n");
		sqlSource.append("	  ,ctry.CTRY_ISO_NU as TERR_CD\n");
		sqlSource.append("	  ,ctry.CTRY_SHRT_NA\n"); 
		sqlSource.append("	from (\n");
		sqlSource.append("		select\n");
		sqlSource.append("		   a.GBAL_HRCY_NODE_ID\n");
		sqlSource.append("		  ,a.GBAL_HRCY_NODE_NA\n");
		sqlSource.append("		  ,c.HRCY_LVL_NU\n");
		sqlSource.append("		  ,a.GBL_APLC_LCL_NODE_RFR_CD\n");
		sqlSource.append("		  ,d.PREN_GBAL_HRCY_NODE_ID\n");
		sqlSource.append("		from {VDB}.V1GBAL_HRCY_NODE a\n");
		sqlSource.append("		inner join {VDB}.V1GBAL_HRCY_TYP b\n");
		sqlSource.append("		  on (b.GBAL_HRCY_TYP_ID = a.GBAL_HRCY_TYP_ID)\n");
		sqlSource.append("		inner join {VDB}.V1HRCY_LVL_NODE_ASGN c\n");
		sqlSource.append("		  on (c.GBAL_HRCY_NODE_ID = a.GBAL_HRCY_NODE_ID)\n");
		sqlSource.append("		left outer join (select * from {VDB}.V1GBAL_HRCY_NODE_ASGN where current_date between GBAL_HRCY_NODE_ASGN_EFF_DT and coalesce(GBAL_HRCY_NODE_ASGN_END_DT,cast('9999-12-31' as date))) d\n");
		sqlSource.append("		  on (d.GBAL_HRCY_NODE_ID = a.GBAL_HRCY_NODE_ID)\n");
		sqlSource.append("		where current_date between a.GBAL_HRCY_NODE_EFF_DT and coalesce(a.GBAL_HRCY_NODE_END_DT,cast('9999-12-31' as date))\n");
		sqlSource.append("		and   b.GBAL_HRCY_TYP_NA = 'Area of the World Hierarchy'\n");
		sqlSource.append("		and   current_date between c.LVL_NODE_ASGN_EFF_DT and coalesce(c.LVL_NODE_ASGN_END_DT,cast('9999-12-31' as date))) lvl4\n");
		sqlSource.append("	inner join (\n");
		sqlSource.append("		select\n"); 
		sqlSource.append("		   a.GBAL_HRCY_NODE_ID\n");
		sqlSource.append("		  ,a.GBAL_HRCY_NODE_NA\n");
		sqlSource.append("		  ,c.HRCY_LVL_NU\n");
		sqlSource.append("		  ,a.GBL_APLC_LCL_NODE_RFR_CD\n");
		sqlSource.append("		  ,d.PREN_GBAL_HRCY_NODE_ID\n");
		sqlSource.append("		from {VDB}.V1GBAL_HRCY_NODE a\n");
		sqlSource.append("		inner join {VDB}.V1GBAL_HRCY_TYP b\n");
		sqlSource.append("		  on (b.GBAL_HRCY_TYP_ID = a.GBAL_HRCY_TYP_ID)\n");
		sqlSource.append("		inner join {VDB}.V1HRCY_LVL_NODE_ASGN c\n");
		sqlSource.append("		  on (c.GBAL_HRCY_NODE_ID = a.GBAL_HRCY_NODE_ID)\n");
		sqlSource.append("		left outer join (select * from {VDB}.V1GBAL_HRCY_NODE_ASGN where current_date between GBAL_HRCY_NODE_ASGN_EFF_DT and coalesce(GBAL_HRCY_NODE_ASGN_END_DT,cast('9999-12-31' as date))) d\n");
		sqlSource.append("		  on (d.GBAL_HRCY_NODE_ID = a.GBAL_HRCY_NODE_ID)\n");
		sqlSource.append("		where current_date between a.GBAL_HRCY_NODE_EFF_DT and coalesce(a.GBAL_HRCY_NODE_END_DT,cast('9999-12-31' as date))\n");
		sqlSource.append("		and   b.GBAL_HRCY_TYP_NA = 'Area of the World Hierarchy'\n");
		sqlSource.append("		and   current_date between c.LVL_NODE_ASGN_EFF_DT and coalesce(c.LVL_NODE_ASGN_END_DT,cast('9999-12-31' as date))) lvl3\n");
		sqlSource.append("	  on (lvl3.GBAL_HRCY_NODE_ID = lvl4.PREN_GBAL_HRCY_NODE_ID)\n");
		sqlSource.append("	inner join (\n");
		sqlSource.append("		select\n"); 
		sqlSource.append("		   a.GBAL_HRCY_NODE_ID\n");
		sqlSource.append("		  ,a.GBAL_HRCY_NODE_NA\n");
		sqlSource.append("		  ,c.HRCY_LVL_NU\n");
		sqlSource.append("		  ,a.GBL_APLC_LCL_NODE_RFR_CD\n");
		sqlSource.append("		  ,d.PREN_GBAL_HRCY_NODE_ID\n");
		sqlSource.append("		from {VDB}.V1GBAL_HRCY_NODE a\n");
		sqlSource.append("		inner join {VDB}.V1GBAL_HRCY_TYP b\n");
		sqlSource.append("		  on (b.GBAL_HRCY_TYP_ID = a.GBAL_HRCY_TYP_ID)\n");
		sqlSource.append("		inner join {VDB}.V1HRCY_LVL_NODE_ASGN c\n");
		sqlSource.append("		  on (c.GBAL_HRCY_NODE_ID = a.GBAL_HRCY_NODE_ID)\n");
		sqlSource.append("		left outer join (select * from {VDB}.V1GBAL_HRCY_NODE_ASGN where current_date between GBAL_HRCY_NODE_ASGN_EFF_DT and coalesce(GBAL_HRCY_NODE_ASGN_END_DT,cast('9999-12-31' as date))) d\n");
		sqlSource.append("		  on (d.GBAL_HRCY_NODE_ID = a.GBAL_HRCY_NODE_ID)\n");
		sqlSource.append("		where current_date between a.GBAL_HRCY_NODE_EFF_DT and coalesce(a.GBAL_HRCY_NODE_END_DT,cast('9999-12-31' as date))\n");
		sqlSource.append("		and   b.GBAL_HRCY_TYP_NA = 'Area of the World Hierarchy'\n");
		sqlSource.append("		and   current_date between c.LVL_NODE_ASGN_EFF_DT and coalesce(c.LVL_NODE_ASGN_END_DT,cast('9999-12-31' as date))) lvl2\n");
		sqlSource.append("	  on (lvl2.GBAL_HRCY_NODE_ID = lvl3.PREN_GBAL_HRCY_NODE_ID)\n");
		sqlSource.append("	inner join {VDB}.V1CTRY ctry\n");
		sqlSource.append("	  on (ctry.CTRY_ISO2_ABBR_CD = cast(lvl4.GBL_APLC_LCL_NODE_RFR_CD as char(2)))\n");
		sqlSource.append("	where lvl4.HRCY_LVL_NU = 4\n");
		sqlSource.append("	and   lvl3.HRCY_LVL_NU = 3\n");
		sqlSource.append("	and   lvl2.HRCY_LVL_NU = 2) a\n");
		sqlSource.append("where a.SCMA_NA <> 'xx';\n");
		
		try {
			System.out.println("\nExtract AOW / Country from GDW ... ");

			
			if ( aowFile.exists() ) {
				aowFile.delete();
			}
			aowFile.createNewFile();
			
			int rowCount = 0;
			BufferedWriter bw = new BufferedWriter(new FileWriter(aowFile.getAbsoluteFile()));
			
			//System.out.println(sqlSource.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));
			
		    rset = gdw.resultSet(sqlSource.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));
		    while ( rset.next() ) {
		    	bw.write(rset.getInt("TERR_CD") + "\t" + rset.getString("SCMA_NA") + "\n");
		    	rowCount++;
		    }
		    rset.close();
		    bw.close();
		    
		    System.out.println("  extracted " + rowCount + " row(s)");
		    System.out.println("Extract AOW / Country from GDW done");
			
		} catch (RDBMS.DBException ex ) {
			System.err.println("RDBMS Exception occured in extracting AOW data:");
			ex.printStackTrace(System.err);
			System.exit(8);
			
		} catch (Exception ex ) {
			System.err.println("Exception occured in extracting AOW data:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void abacPurge(DaaSConfig daasConfig) {
		
		try {
			ABaC abacApi = new ABaC(daasConfig);

			abacApi.purgeExpiredLandingZoneFiles(hdfsFileSystem, daasConfig, hdfsConfig, 14);
			
			abacApi.dispose();
			
		} catch (Exception ex) {
			System.err.println("The following exception occurred in abacPurge():");
			ex.printStackTrace();
			System.exit(8);
		}
		
		
	}
	
	public void initHDFS(DaaSConfig daasConfig) {
		
		try {

	    	boolean hdfsConfigXmlFound=true;
	    	String hdfsConfigXmlFile = null;
	    	
	    	hdfsConfig = new Configuration();

	    	hdfsConfigXmlFile = daasConfig.hdfsConfigDir() + File.separator + CORE_SITE_XML_FILE;
	    	if ( new File(hdfsConfigXmlFile).isFile() ) {
	    		hdfsConfig.addResource(new Path(hdfsConfigXmlFile));
	    	} else {
				System.err.println("Hadoop Config File (XML) " + hdfsConfigXmlFile + " not found.");
	    		hdfsConfigXmlFound=false;
	    	}
	    	
	    	hdfsConfigXmlFile = daasConfig.hdfsConfigDir() + File.separator + HDFS_SITE_XML_FILE;
	    	if ( new File(hdfsConfigXmlFile).isFile() ) {
	    		hdfsConfig.addResource(new Path(hdfsConfigXmlFile));
	    	} else {
	    		System.err.println("Hadoop Config File (XML) " + hdfsConfigXmlFile + " not found.");
	    		hdfsConfigXmlFound=false;
	    	}
	    	
	    	hdfsConfigXmlFile = daasConfig.hdfsConfigDir() + File.separator + MAPRED_SITE_XML_FILE;
	    	if ( new File(hdfsConfigXmlFile).isFile() ) {
	    		hdfsConfig.addResource(new Path(hdfsConfigXmlFile));
	    	} else {
	    		System.err.println("Hadoop Config File (XML) " + hdfsConfigXmlFile + " not found.");
	    		hdfsConfigXmlFound=false;
	    	}
	    	
	    	if ( !hdfsConfigXmlFound ) {
	    		System.err.println("Missing one or more Hadoop Config files (XML), Stopping");
	    		System.exit(8);
	    	}
		    
		    try {
		    	hdfsFileSystem = FileSystem.get(hdfsConfig);
		    } catch (Exception ex) {
		    	System.err.println("Error creating HDFS FileSystem, Stopping");
		    	System.exit(8);
		    }

		} catch (Exception ex) {
			System.err.println("Exception occured in initHDFS()");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	//AWS START
	private void getList(DaaSConfig daasConfig
			            ,String listTsParm
			            ,String listTerrCdParm) {

		try {
			ABaC abacApi = new ABaC(daasConfig);
			
			System.out.println(abacApi.getChangedTerrBusinessDatesSinceTs(listTsParm, listTerrCdParm).toString());
			System.out.println(abacApi.getChangedDatahubPathsSinceLastTs(listTsParm, listTerrCdParm).toString());
			
			abacApi.dispose();
			
		} catch (Exception ex ) {
			System.err.println("Exception occured in getList data:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	//AWS END
}

package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.mapreduce.AsterFormatMapper;
import com.mcd.gdw.daas.mapreduce.AsterFormatReducer;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.RDBMS;

public class GenerateAsterFormat extends Configured implements Tool {

	private String selectSubTypes = "";
	private ArrayList<String> workTerrCodeList = new ArrayList<String>();
	
	public static void main(String[] args) throws Exception {
	
		int retval = ToolRunner.run(new Configuration(),new GenerateAsterFormat(), args);

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
			
			runMrAsterExtract(daasConfig,fileType,getConf(),terrDate,terrDateFile,owshFltr);
			//postDataGenerationProcessing(daasConfig);
			
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
                                  ,String owshFltr) {

		Job job;
		ArrayList<Path> requestedPaths = null;
		SimpleDateFormat sdf;
		Calendar dt;
		String subDir = "";
		ArrayList<String> subTypeList;
		ABaC abac = null;
		

		try {
			//AWS START
			hdfsConfig.set(DaaSConstants.HDFS_ROOT_CONFIG, daasConfig.hdfsRoot());
			//AWS END
			
			
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
			
			String jobTitle = "";
			
			if ( terrDateFile.length() > 0 ) {
				jobTitle = "Processing MapReduce Aster History Extract";
			} else {
				jobTitle = "Processing MapReduce Aster Extract";
			}
			job = new Job(hdfsConfig, jobTitle);
			
			System.out.println("\nCreate Aster Loader File Format\n");

			//AWS START
			//FileSystem fileSystem = FileSystem.get(hdfsConfig);
			FileSystem fileSystem = HDFSUtil.getFileSystem(daasConfig, hdfsConfig);
			//AWS END
			
			sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			dt = Calendar.getInstance();
			subDir = sdf.format(dt.getTime());
			
			Path outPath=null;

			if ( terrDateFile.length() > 0 ) {
				outPath= new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "asterextract_hist_extract");
			} else {
				outPath= new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "asterextract" + Path.SEPARATOR + subDir);
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
				Path cacheFile = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + "aster_load_list.txt");
				
				if ( fileSystem.exists(cacheFile)) {
					fileSystem.delete(cacheFile, false);
				}
				
				fileSystem.copyFromLocalFile(new Path(terrDateFile), cacheFile);
				
				DistributedCache.addCacheFile(cacheFile.toUri(),job.getConfiguration());
			} else {
				DistributedCache.addCacheFile(new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + "cache" + Path.SEPARATOR + "aster_include_list.txt").toUri(),job.getConfiguration());
			}

			job.setJarByClass(GenerateAsterFormat.class);
			job.setMapperClass(AsterFormatMapper.class);
			job.setReducerClass(AsterFormatReducer.class);
			job.setMapOutputKeyClass(Text.class);
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
	
	private void postDataGenerationProcessing(DaaSConfig daasConfig) throws Exception {
		
		StringBuilder sql = new StringBuilder();
		StringBuilder sql1 = new StringBuilder();
		StringBuilder sql2 = new StringBuilder();
		StringBuilder sqlSource = new StringBuilder();
		RDBMS gdw;
		RDBMS ast;
		ResultSet rset;
		
		gdw = new RDBMS(RDBMS.ConnectionType.Teradata,daasConfig.gblTpid(),daasConfig.gblUserId(),daasConfig.gblPassword(),daasConfig.gblNumSessions());
		ast = new RDBMS(RDBMS.ConnectionType.Aster,daasConfig.asterServerName(),daasConfig.asterUserId(),daasConfig.asterPassword());

		sql.setLength(0);
		sql.append("drop table staging.STG_POS_XML\n");
		
		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sql.toString());
		}
		
		try {
			ast.executeUpdate(sql.toString());
		} catch(Exception ex) {
			if ( !(ex.getMessage().contains("relation") && ex.getMessage().contains("does not exist")) ) {
				throw ex;
			}
		}

		sql.setLength(0);
		sql.append("drop table staging.STG_AOW\n");

		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sql.toString());
		}
	
		try {
			ast.executeUpdate(sql.toString());
		} catch(Exception ex) {
			if ( !(ex.getMessage().contains("relation") && ex.getMessage().contains("does not exist")) ) {
				throw ex;
			}
		}

		sql.setLength(0);
		sql.append("create fact table staging.STG_POS_XML (\n");
		sql.append("   DATA_TYP_CD varchar(20) not null\n");
		sql.append("  ,MCD_GBAL_LCAT_ID_NU decimal(13,0) not null\n");
		sql.append("  ,TERR_CD smallint not null\n");
		sql.append("  ,LGCY_LCL_RFR_DEF_CD varchar(12) not null\n");
		sql.append("  ,POS_BUSN_DT date not null\n");
		sql.append("  ,REST_OWSH_TYP_SHRT_DS char(1) not null\n");
		sql.append("  ,SEQ smallint not null\n");
		sql.append("  ,xmltext varchar not null)\n");
		sql.append("distribute by hash(MCD_GBAL_LCAT_ID_NU);\n");

		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sql.toString());
		}
		
		ast.executeUpdate(sql.toString());

		sql.setLength(0);
		sql.append("create dimension table staging.STG_AOW (\n");
		sql.append("   TERR_CD smallint not null\n");
		sql.append("  ,SCMA_NA varchar(10) not null)\n");
		sql.append("distribute by replication;\n");

		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sql.toString());
		}
		
		ast.executeUpdate(sql.toString());

		sqlSource.setLength(0);
		sqlSource.append("select\n");
		sqlSource.append("   a.TERR_CD\n");
		sqlSource.append("  ,a.SCMA_NA\n");
		sqlSource.append("from (\n");
		sqlSource.append("	select\n"); 
		sqlSource.append("	   lvl2.GBAL_HRCY_NODE_NA as AOW_NA\n");
		sqlSource.append("	  ,case lvl2.GBAL_HRCY_NODE_NA when 'APMEA' then 'ap'\n");
		//@mc41946 --> AWS Migration
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
		
		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sqlSource.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));
		}
		
		rset = gdw.resultSet(sqlSource.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));
		
		sql.setLength(0);
		sql.append("insert into staging.stg_aow (TERR_CD, SCMA_NA) values (?,?);\n");
		
		ast.setPreparedStatement(sql.toString());
		
		while ( rset.next() ) {
			ast.addBatch(rset.getString("TERR_CD"), rset.getString("SCMA_NA"));
		}
		ast.finalizeBatch();
		rset.close();
		
		sql.setLength(0);
		sql.append("insert into staging.STG_POS_XML (\n");
		sql.append("   DATA_TYP_CD\n");
		sql.append("  ,MCD_GBAL_LCAT_ID_NU\n");
		sql.append("  ,TERR_CD\n");
		sql.append("  ,LGCY_LCL_RFR_DEF_CD\n");
		sql.append("  ,POS_BUSN_DT\n");
		sql.append("  ,REST_OWSH_TYP_SHRT_DS\n");
		sql.append("  ,SEQ\n");
		sql.append("  ,xmltext)\n");
		sql.append("  select\n");
		sql.append("     a.DATA_TYP_CD\n");
		sql.append("    ,a.MCD_GBAL_LCAT_ID_NU\n");
		sql.append("    ,a.TERR_CD\n");
		sql.append("    ,a.LGCY_LCL_RFR_DEF_CD\n");
		sql.append("    ,a.POS_BUSN_DT\n");
		sql.append("    ,a.REST_OWSH_TYP_SHRT_DS\n");
		sql.append("    ,a.SEQ\n");
		sql.append("    ,a.xmltext\n");
		sql.append("  from (\n");
		sql.append("    SELECT\n");
		sql.append("       DATA_TYP_CD\n");
		sql.append("      ,cast(MCD_GBAL_LCAT_ID_NU as decimal(13,0)) as MCD_GBAL_LCAT_ID_NU\n");
		sql.append("      ,TERR_CD\n");
		sql.append("      ,LGCY_LCL_RFR_DEF_CD\n");
		sql.append("      ,cast(POS_BUSN_DT as date) as POS_BUSN_DT\n");
		sql.append("      ,REST_OWSH_TYP_SHRT_DS\n");
		sql.append("      ,SEQ\n");
		sql.append("      ,xmltext\n");
		sql.append("    FROM load_from_hcatalog (\n");
		sql.append("      ON public.empty\n");
		sql.append("      SERVER ('142.11.156.25')\n");
		sql.append("      USERNAME ('hdfs')\n");
		sql.append("      DBNAME ('default')\n");
		sql.append("      TABLENAME ('ast_stg2')\n");
		sql.append("    )) a;\n");

		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sql.toString());
		}
		
		ast.executeUpdate(sql.toString());

		System.out.println("STOP");
		System.exit(8);
		
		sql.setLength(0);
		sql.append("select\n");
		sql.append("   b.scma_na\n");
		sql.append("  ,a.data_typ_cd\n");
		sql.append("from staging.stg_pos_xml a\n");
		sql.append("inner join staging.stg_aow b\n");
		sql.append("  on (b.terr_cd = a.terr_cd)\n");
		sql.append("group by\n");
		sql.append("   b.scma_na\n");
		sql.append("  ,a.data_typ_cd\n");

		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sql.toString());
		}

		ArrayList<String> lst = new ArrayList<String>();
		
		rset = ast.resultSet(sql.toString());
		
		while( rset.next() ) {
			lst.add(rset.getString("scma_na") + "|" + rset.getString("data_typ_cd").replaceAll("-","_") + "|" + rset.getString("data_typ_cd"));
		}
		rset.close();

		ast.setAutoCommit(false);
		
		String[] parts;
		int rowCount;
		
		for ( String itm : lst ) {
			parts = itm.split("\\|");
			
			sql1.setLength(0);
			sql1.append("delete\n");
			sql1.append("from only {SCMANA}.pos_{TBL}\n");
			sql1.append("using (select\n");
			sql1.append("          a.terr_cd\n");
			sql1.append("         ,a.lgcy_lcl_rfr_def_cd\n");
			sql1.append("         ,a.pos_busn_dt\n");
			sql1.append("       from staging.stg_pos_xml a\n");
			sql1.append("       inner join staging.stg_aow b\n");
			sql1.append("         on (b.terr_cd = a.terr_cd)\n");
			sql1.append("       where a.data_typ_cd = '{DATATYP}'\n");
			sql1.append("       and b.scma_na = '{SCMANA}') b\n");
			sql1.append("where b.terr_cd             = pos_{TBL}.terr_cd\n");
			sql1.append("and   b.lgcy_lcl_rfr_def_cd = pos_{TBL}.lgcy_lcl_rfr_def_cd\n");
			sql1.append("and   b.pos_busn_dt         = pos_{TBL}.pos_busn_dt\n");

			if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
				System.out.println(sql1.toString().replaceAll("\\{SCMANA\\}", parts[0]).replaceAll("\\{TBL\\}", parts[1]).replaceAll("\\{DATATYP\\}", parts[2]));
			}
			
			rowCount = ast.executeUpdate(sql1.toString().replaceAll("\\{SCMANA\\}", parts[0]).replaceAll("\\{TBL\\}", parts[1]).replaceAll("\\{DATATYP\\}", parts[2]));

			if ( daasConfig.displayMsgs() ) {
				System.out.println("Deleted " + rowCount + " rows");
			}

			sql2.setLength(0);
			sql2.append("insert into {SCMANA}.pos_{TBL} (\n");
			sql2.append("   mcd_gbal_lcat_id_nu\n");
			sql2.append("  ,terr_cd\n");
			sql2.append("  ,lgcy_lcl_rfr_def_cd\n");
			sql2.append("  ,pos_busn_dt\n");
			sql2.append("  ,rest_owsh_typ_shrt_ds\n");
			sql2.append("  ,xmltext)\n");
			sql2.append("  select\n");
			sql2.append("     a.mcd_gbal_lcat_id_nu\n");
			sql2.append("    ,a.terr_cd\n");
			sql2.append("    ,a.lgcy_lcl_rfr_def_cd\n");
			sql2.append("    ,a.pos_busn_dt\n");
			sql2.append("    ,a.rest_owsh_typ_shrt_ds\n");
			sql2.append("    ,a.xmltext\n");
			sql2.append("  from staging.stg_pos_xml a\n");
			sql2.append("  inner join staging.stg_aow b\n");
			sql2.append("    on (b.terr_cd = a.terr_cd)\n");
			sql2.append("  where a.data_typ_cd = '{DATATYP}'\n");
			sql2.append("  and   b.scma_na = '{SCMANA}';\n");

			if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
				System.out.println(sql2.toString().replaceAll("\\{SCMANA\\}", parts[0]).replaceAll("\\{TBL\\}", parts[1]).replaceAll("\\{DATATYP\\}", parts[2]));
			}
			
			rowCount = ast.executeUpdate(sql2.toString().replaceAll("\\{SCMANA\\}", parts[0]).replaceAll("\\{TBL\\}", parts[1]).replaceAll("\\{DATATYP\\}", parts[2]));

			if ( daasConfig.displayMsgs() ) {
				System.out.println("Inserted " + rowCount + " rows");
			}
		}
		
		ast.commit();
		ast.setAutoCommit(true);
		
		sql.setLength(0);
		sql.append("drop table staging.STG_POS_XML\n");
		
		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sql.toString());
		}
		
		ast.executeUpdate(sql.toString());

		sql.setLength(0);
		sql.append("drop table staging.STG_AOW\n");

		if ( daasConfig.verboseLevel == DaaSConfig.VerboseLevelType.Maximum ) {
			System.out.println(sql.toString());
		}
		
		ast.executeUpdate(sql.toString());
		
		ast.dispose();
		
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
				
				//if ( filePath.startsWith("STLD") || filePath.startsWith("DetailedSOS") || filePath.startsWith("MenuItem") || filePath.startsWith("SecurityData") || filePath.startsWith("store-db") || filePath.startsWith("product-db") ) {
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

			//Path[] requestPaths = HDFSUtil.requestedArgsPaths(fileSystem, daasConfig, requestedTerrDateParms, "STLD", "DetailedSOS", "MenuItem", "SecurityData","store-db","product-db");
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
	
}

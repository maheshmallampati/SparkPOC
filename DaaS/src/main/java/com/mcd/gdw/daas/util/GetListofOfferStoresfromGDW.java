package com.mcd.gdw.daas.util;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.driver.CreateUSStoreListDriver;

public class GetListofOfferStoresfromGDW extends Configured implements Tool{
	
	static FileSystem fileSystem;
	String[] args;
	String configXmlFile = "";
	
	public static final String CORE_SITE_XML_FILE = "core-site.xml";
	public static final String HDFS_SITE_XML_FILE = "hdfs-site.xml";
	public static final String MAPRED_SITE_XML_FILE = "mapred-site.xml";
	public static final String YARN_SITE_XML_FILE = "yarn-site.xml";
	
	
	public static void main(String[] args) throws Exception {

	
			int retval = ToolRunner.run(new Configuration(),
					new GetListofOfferStoresfromGDW(), args);

			System.out.println(" return value : " + retval);
			
			
	}
	
	@Override
	public int run(String[] argsall) throws Exception {

		GenericOptionsParser gop = new GenericOptionsParser(argsall);

		args = gop.getRemainingArgs();

		DaaSConfig daasConfig = new DaaSConfig(args[0]);
		
		Configuration conf = new Configuration();
		
		//AWS START
		//fileSystem = FileSystem.get(conf);
		fileSystem = HDFSUtil.getFileSystem(daasConfig, conf);
		//AWS END
		
		//AWS START
		Path fileDestPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsHiveSubDir() + Path.SEPARATOR + args[1]);
		//AWS END 
		
		
		GetListofOfferStoresfromGDW glosGDW = new GetListofOfferStoresfromGDW();
		
		
		glosGDW.createListDistCache(  daasConfig);
		return (0);

	}

	
	
	

	private void createListDistCache(DaaSConfig daasConfig) {
		
		ResultSet rset = null;
		BufferedWriter bw = null;
		Path locationList;
		try {
			StringBuffer sql = new StringBuffer();
			

			if ( daasConfig.displayMsgs() ) {
		    	System.out.print("Connecting to GDW ... ");
		    }
		    
			locationList= new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsHiveSubDir()+Path.SEPARATOR+ 
								"datamart"+ Path.SEPARATOR+ "offers_include_list"+ Path.SEPARATOR+ "offers_include_list.txt");
			
			RDBMS gdw = new RDBMS(RDBMS.ConnectionType.Teradata,daasConfig.gblTpid(),daasConfig.gblUserId(),daasConfig.gblPassword(),daasConfig.gblNumSessions());
		    
		    if ( daasConfig.displayMsgs() ) {
		    	System.out.println("done");
		    }

			sql.setLength(0);
			sql.append("select\n");
			sql.append("   d.CTRY_ISO_NU as TERR_CD\n"); 
			sql.append("  ,d.LGCY_LCL_RFR_DEF_CD\n");
			sql.append("from (select * from {VDB}.V1REST_CHAR_VAL where current_date between REST_CHAR_EFF_DT and coalesce(REST_CHAR_END_DT,cast('9999-12-31' as date))) a\n");
			sql.append("inner join {VDB}.V1GBAL_REST_CHAR b\n");
			sql.append("  on (b.GBAL_REST_CHAR_ID = a.GBAL_REST_CHAR_ID)\n");
			sql.append("inner join (select * from {VDB}.V1REST_CHAR_VLD_LIST where current_date between CHAR_VLD_VAL_EFF_DT and coalesce(CHAR_VLD_VAL_END_DT,cast('9999-12-31' as date))) c\n");
			sql.append("  on (c.REST_CHAR_VLD_LIST_ID = a.REST_CHAR_VLD_LIST_ID)\n");
			sql.append("inner join {VDB}.V1MCD_GBAL_BUSN_LCAT d\n");
			sql.append("  on (d.MCD_GBAL_LCAT_ID_NU = a.MCD_GBAL_LCAT_ID_NU)\n");
			sql.append("where b.GBAL_REST_CHAR_NA = 'XML Provided For'\n");  
			sql.append("and   c.CHAR_VLD_LIST_DS = 'Mobile Offers'\n");
			
			rset = gdw.resultSet(sql.toString().replaceAll("\\{VDB\\}", daasConfig.gblViewDb()));

			bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(locationList,true)));

			while ( rset.next() ) {
				bw.write(rset.getString("TERR_CD") + "\t" + rset.getString("LGCY_LCL_RFR_DEF_CD") + "\t1955-04-15\t9999-12-31");
				bw.write("\n");
			}
			bw.close();
			rset.close();
			
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenerateOffersReport.createListDistCache:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}finally{
				try{
					if(rset!=null)
						rset.close();
					if(bw != null)
						bw.close();
				}catch(Exception ex){
					ex.printStackTrace();
				}
			
		}
		
		
	}

}

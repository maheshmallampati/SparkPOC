package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.RDBMS;
import com.mcd.gdw.daas.util.RDBMS.MultiPreparedStatementSql;
import com.mcd.gdw.test.daas.driver.NextGenUnzipFile.ResultListFilter;
import com.mcd.gdw.test.daas.mapreduce.FixGoldPart1Mapper;
import com.mcd.gdw.test.daas.mapreduce.FixGoldPart2Mapper;
import com.mcd.gdw.test.daas.mapreduce.GenericMoveMapper;

public class FixGold extends Configured implements Tool {

	public class ResultListFilter implements PathFilter {
		
		private String prefix = "";
		
		public ResultListFilter() {
			
		}
		
		public ResultListFilter(String prefix) {
		
			this.prefix = prefix;
			
		}
		
	    public boolean accept(Path path) {
	    	
	    	boolean retFlag = false; 
	    	
	    	if ( this.prefix.length() == 0 ) {
		    	retFlag = !path.getName().startsWith("_");
	    	} else {
	    		retFlag = path.getName().startsWith(prefix);
	    	}
	    	
	    	return(retFlag);
	    }
	}
	
	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
	private Path cachePath = null;
	private String[] xmlTypes;
	private String root;
	private Path move1Path;
	private Path move2Path;
	
	public static void main(String[] args) throws Exception {
		
		Configuration hdfsConfig = new Configuration();
		
		int retval = ToolRunner.run(hdfsConfig,new FixGold(), args);

		System.out.println(" return value : " + retval);
	}
	
	public int run(String[] args) throws Exception {

		String configXmlFile = "";
		String fileType = "";
		String dtParm = "";
		String terrParm = "";
		
		xmlTypes = "STLD\tDetailedSOS\tMenuItem\tSecurityData\tStore-Db\tProduct-Db".split("\t");
		
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-t") && (idx+1) < args.length ) {
				fileType = args[idx+1];
			}

			if ( args[idx].equals("-dt") && (idx+1) < args.length ) {
				dtParm = args[idx+1];
			}

			if ( args[idx].equals("-terr") && (idx+1) < args.length ) {
				terrParm = args[idx+1];
			}
		}

		if ( configXmlFile.length() == 0 || fileType.length() == 0 || dtParm.length() == 0 || terrParm.length() == 0 ) {
			System.err.println("one or more parameters missing");
			System.exit(8);
		}

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		
		if ( daasConfig.configValid() ) {
			
			if ( daasConfig.displayMsgs()  ) {
				System.out.println(daasConfig.toString());
			}

			hdfsConfig = getConf();
			fileSystem = FileSystem.get(hdfsConfig);

			//root = daasConfig.hdfsRoot();
			root = "/daas";
			
			runJob(daasConfig,fileType,terrParm,dtParm);
			runJob2(daasConfig,fileType,terrParm,dtParm);
			runMoveJob(daasConfig,move1Path,"Existing to Backup");
			runMoveJob(daasConfig,move2Path,"Fixed to Gold");
			
		} else {
			System.err.println("Invalid Config XML file, stopping");
			System.err.println(daasConfig.errText());
			System.exit(8);
		}
	
		return(0);
	}
	
	private void runJob(DaaSConfig daasConfig
                       ,String fileType
                       ,String terrParm
                       ,String dtParm) {

		Job job;
		Path addPath;
		
		try {
			cachePath = new Path(root + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "FixGold" + Path.SEPARATOR + "Cache");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, cachePath,daasConfig.displayMsgs());
			
			baseOutputPath = new Path(root + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "FixGold" + Path.SEPARATOR + "Out");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");
			}					
			
			setOutputCompression(false);
			
			job = Job.getInstance(hdfsConfig, "Fix Gold File List");	
			
			for ( int idx=0; idx < xmlTypes.length; idx++ ) {
				addPath = new Path(root + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + xmlTypes[idx] + Path.SEPARATOR + terrParm + Path.SEPARATOR + dtParm);
				//addPath = new Path(root + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + xmlTypes[idx] + Path.SEPARATOR + terrParm + Path.SEPARATOR + dtParm);

				System.out.println(addPath.toString());
				
				if ( fileSystem.isDirectory(addPath) ) {
					FileInputFormat.addInputPath(job, addPath);
				}
			}
			
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			job.setJarByClass(FixGold.class); 
			job.setMapperClass(FixGoldPart1Mapper.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);			
			job.setOutputKeyClass(Text.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}
			
			FileStatus[] status = fileSystem.listStatus(baseOutputPath, new ResultListFilter());
			String[] parts;
			BufferedReader br;
			String line;
			String fileName;
			String terrCd;
			String lgcyLclRfrDefCd;
			String posBusnDt;
			String fileId;
			String rowNum;
			String xmlType;
			RDBMS abac;
			int cnt;
			int tot=0;
			StringBuffer sql = new StringBuffer();
			java.sql.Date calDt;
			Calendar dt = Calendar.getInstance();
			
			abac = new RDBMS(RDBMS.ConnectionType.SQLServer, daasConfig.abacSqlServerServerName(), daasConfig.abacSqlServerUserId(), daasConfig.abacSqlServerPassword());
			abac.setBatchSize(daasConfig.abacSqlServerBatchSize());
			abac.setAutoCommit(false);
			
			sql.setLength(0);
			sql.append("CREATE TABLE #DUP_LIST(\n");
			sql.append("   FILE_PATH varchar(300) NOT NULL\n");
			sql.append("  ,ROW_NUM int NOT NULL\n");
			sql.append("  ,FILE_TYPE varchar(30) NOT NULL\n");
			sql.append("  ,TERR_CD int NOT NULL\n");
			sql.append("  ,LGCY_LCL_RFR_DEF_CD varchar(12) NOT NULL\n");
			sql.append("  ,CAL_DT date NOT NULL\n");
			sql.append("  ,DW_FILE_ID int NOT NULL\n");
			sql.append("  ,PRIMARY KEY (\n");
			sql.append("     FILE_PATH\n");
			sql.append("    ,ROW_NUM\n))\n");

			//sql.setLength(0);
			//sql.append("delete from [ABaCProd].[dbo].[TMP_DUP_LIST]");
			
			abac.executeUpdate(sql.toString());

			//sql.setLength(0);
			//sql.append("insert into [ABaCProd].[dbo].[TMP_DUP_LIST] ([FILE_PATH],[ROW_NUM],[FILE_TYPE],[TERR_CD],[LGCY_LCL_RFR_DEF_CD],[CAL_DT],[DW_FILE_ID]) values (?,?,?,?,?,?,?);");

			sql.setLength(0);
			sql.append("insert into #DUP_LIST ([FILE_PATH],[ROW_NUM],[FILE_TYPE],[TERR_CD],[LGCY_LCL_RFR_DEF_CD],[CAL_DT],[DW_FILE_ID]) values (?,?,?,?,?,?,?);");
			
			abac.setPreparedStatement(sql.toString());
			
			for ( int idx = 0; idx < status.length; idx++ ) {
				br = new BufferedReader(new InputStreamReader(new DataInputStream(fileSystem.open(status[idx].getPath()))));

				//System.out.println(status[idx].getPath().getName());
				
				while ( (line=br.readLine()) != null) {
					parts = line.split("\t");
					xmlType = parts[0];
					terrCd = parts[1];
					lgcyLclRfrDefCd = parts[2];
					posBusnDt = parts[3];
					fileId = parts[4];
					fileName = parts[5];
					rowNum = parts[6];
					
					dt.set(Integer.parseInt(posBusnDt.substring(0,4)), Integer.parseInt(posBusnDt.substring(4,6))-1, Integer.parseInt(posBusnDt.substring(6,8)));
					calDt = new java.sql.Date(dt.getTimeInMillis());

					cnt = abac.addBatch(fileName
							           ,Integer.parseInt(rowNum)
							           ,xmlType
							           ,Integer.parseInt(terrCd)
							           ,lgcyLclRfrDefCd
							           ,calDt
							           ,Integer.parseInt(fileId)
							           );
					
					if ( cnt > 0 ) {
						tot += cnt;
					
						System.out.println(tot);
					}
				}
				
				br.close();
			}
			
			tot = abac.finalizeBatch();
			
			System.out.println(tot);
			
			abac.commit();
			
			sql.setLength(0);
			sql.append("select distinct\n");
			sql.append("   a.FILE_PATH\n");
			sql.append("  ,a.ROW_NUM\n");
			sql.append("from (\n");
			sql.append("     select\n"); 
			sql.append("        FILE_PATH\n");
			sql.append("       ,ROW_NUM\n");
			sql.append("       ,FILE_TYPE\n");
			sql.append("       ,TERR_CD\n");
			sql.append("       ,LGCY_LCL_RFR_DEF_CD\n");
			sql.append("       ,CAL_DT\n");
			sql.append("       ,DW_FILE_ID\n");
			sql.append("       ,ROW_NUMBER() over(partition by FILE_TYPE,TERR_CD,LGCY_LCL_RFR_DEF_CD,CAL_DT,DW_FILE_ID order by FILE_PATH, ROW_NUM) as ID\n");
			sql.append("     from #DUP_LIST) a\n");
			//sql.append("     from [ABaCProd].[dbo].[TMP_DUP_LIST]) a\n");
			sql.append("where a.ID > 1\n");
			sql.append("union all\n");
			sql.append("select distinct\n");
			sql.append("   a.FILE_PATH\n");
			sql.append("  ,a.ROW_NUM\n");
			sql.append("from (\n");
			sql.append("     select\n");
			sql.append("        a.FILE_PATH\n");
			sql.append("       ,a.ROW_NUM\n");
			sql.append("       ,a.FILE_TYPE\n");
			sql.append("       ,a.TERR_CD\n");
			sql.append("       ,a.LGCY_LCL_RFR_DEF_CD\n");
			sql.append("       ,a.CAL_DT\n");
			sql.append("       ,a.DW_FILE_ID as GOLD_FILE_ID\n");
			sql.append("       ,coalesce(b.DW_FILE_ID,-1) as ABAC_FILE_ID\n");
			sql.append("     from (\n");
			sql.append("          select\n"); 
			sql.append("             a.FILE_PATH\n");
			sql.append("            ,a.ROW_NUM\n");
			sql.append("            ,a.FILE_TYPE\n");
			sql.append("            ,a.TERR_CD\n");
			sql.append("            ,a.LGCY_LCL_RFR_DEF_CD\n");
			sql.append("            ,a.CAL_DT\n");
			sql.append("            ,a.DW_FILE_ID\n");
			sql.append("          from (\n");
			sql.append("               select\n");
			sql.append("                  FILE_PATH\n");
			sql.append("                 ,ROW_NUM\n");
			sql.append("                 ,FILE_TYPE\n");
			sql.append("                 ,TERR_CD\n");
			sql.append("                 ,LGCY_LCL_RFR_DEF_CD\n");
			sql.append("                 ,CAL_DT\n");
			sql.append("                 ,DW_FILE_ID\n");
			sql.append("                 ,ROW_NUMBER() over(partition by FILE_TYPE,TERR_CD,LGCY_LCL_RFR_DEF_CD,CAL_DT,DW_FILE_ID order by FILE_PATH, ROW_NUM) as ID\n");
			sql.append("               from #DUP_LIST) a\n");
			//sql.append("               from [ABaCProd].[dbo].[TMP_DUP_LIST]) a\n");		
			sql.append("          where a.ID = 1) a\n");
			sql.append("     left outer join (\n");
			sql.append("          select\n");
			sql.append("            a.TERR_CD\n");
			sql.append("           ,a.LGCY_LCL_RFR_DEF_CD\n");
			sql.append("           ,a.CAL_DT\n");
			sql.append("           ,a.DW_FILE_ID\n");
			sql.append("          from (\n");
			sql.append("               select\n"); 
			sql.append("                  TERR_CD\n");
			sql.append("                 ,case len(LGCY_LCL_RFR_DEF_CD) when 4 then '0'\n");
			sql.append("                                                        when 3 then '00'\n");
			sql.append("                                                        when 2 then '000'\n");
			sql.append("                                                        when 1 then '0000'\n");
			sql.append("                                                        else '' end + LGCY_LCL_RFR_DEF_CD as LGCY_LCL_RFR_DEF_CD\n");
			sql.append("                 ,CAL_DT\n");
			sql.append("                 ,DW_FILE_ID\n");
			sql.append("                 ,FILE_MKT_OGIN_TS\n");
			sql.append("                 ,FILE_DW_ARRV_TS\n");
			sql.append("                 ,ROW_NUMBER() over(partition by TERR_CD,case len(LGCY_LCL_RFR_DEF_CD) when 4 then '0' when 3 then '00' when 2 then '000' when 1 then '0000' else '' end + LGCY_LCL_RFR_DEF_CD,CAL_DT order by FILE_MKT_OGIN_TS desc, FILE_DW_ARRV_TS desc) as ID\n");
			sql.append("               from ABaCProd.dbo.Vdw_file\n");
			sql.append("               where TERR_CD = " + terrParm + "\n");
			sql.append("               and   CAL_DT in (select distinct CAL_DT from #DUP_LIST)\n");
			//sql.append("               and   CAL_DT in (select distinct CAL_DT from [ABaCProd].[dbo].[TMP_DUP_LIST])\n");
			sql.append("               and   DW_AUDT_STUS_TYP_ID = 5) a\n");
			sql.append("               where a.ID = 1) b\n");
			sql.append("       on (b.TERR_CD = a.TERR_CD\n");
			sql.append("            and b.LGCY_LCL_RFR_DEF_CD = a.LGCY_LCL_RFR_DEF_CD\n");
			sql.append("            and b.CAL_DT = a.CAL_DT)\n");
			sql.append("where a.DW_FILE_ID <> coalesce(b.DW_FILE_ID,-1)");
			sql.append("            ) a\n");
			
			ResultSet rset;
			Path tmpPath;
			Path cacheFile = new Path(cachePath.toString() + Path.SEPARATOR + "filelist.txt");
			
			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(cacheFile,true)));
			
			rset = abac.resultSet(sql.toString());
			while ( rset.next() ) {
				tmpPath = new Path(rset.getString("FILE_PATH"));
				
				bw.write(tmpPath.getName() + "\t" + rset.getString("FILE_PATH") + "\t" + rset.getString("ROW_NUM") + "\n");
			}

			bw.close();
			rset.close();
			
			abac.dispose();
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	
	private void runJob2(DaaSConfig daasConfig
                        ,String fileType
                        ,String terrParm
                        ,String dtParm) {

		Job job;
		Path addPath;
		Path createPath;
		ArrayList<Path> inPaths = new ArrayList<Path>();
		
		try {
			cachePath = new Path(root + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "FixGold" + Path.SEPARATOR + "Cache" + Path.SEPARATOR + "filelist.txt");
			baseOutputPath = new Path(root + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "FixGold" + Path.SEPARATOR + "Out2");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,daasConfig.displayMsgs());

			if ( daasConfig.displayMsgs() ) {
				System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");
			}					
			
			setOutputCompression(true);
			
			job = Job.getInstance(hdfsConfig, "Fix Gold File Step2");	

			job.addCacheFile(new URI(cachePath.toString() + "#" + cachePath.getName()));
			
			
			for ( int idx=0; idx < xmlTypes.length; idx++ ) {
				addPath = new Path(root + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + xmlTypes[idx] + Path.SEPARATOR + terrParm + Path.SEPARATOR + dtParm);
				//addPath = new Path(root + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + xmlTypes[idx] + Path.SEPARATOR + terrParm + Path.SEPARATOR + dtParm);

				inPaths.add(addPath);
				System.out.println(addPath.toString());
				
				if ( fileSystem.isDirectory(addPath) ) {
					createPath = new Path(root + Path.SEPARATOR + "gold-bk" + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + xmlTypes[idx] + Path.SEPARATOR + terrParm + Path.SEPARATOR + dtParm);
					if ( fileSystem.isDirectory(createPath)) {
						System.err.println(createPath.toString() + " already exists");
						System.exit(8);
					}
					HDFSUtil.createHdfsSubDirIfNecessary(fileSystem,createPath, true);
					
					FileInputFormat.addInputPath(job, addPath);
				}
			}
			
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			job.setJarByClass(FixGold.class); 
			job.setMapperClass(FixGoldPart2Mapper.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);			
			job.setOutputKeyClass(Text.class);
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}
			
			move1Path = new Path(root + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "FixGold" + Path.SEPARATOR + "move1");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, move1Path,daasConfig.displayMsgs());	
			
			Path outFile; 
			Path toPath;
			String fileName;
			String parts[];
			String parts2[];
			FileStatus[] status;
			BufferedWriter bw = null;
			int batch = 0;
			int maxCnt = 20;
			int cnt = maxCnt+1;
			int fileId = 0;
			
			for ( Path inPath : inPaths ) {
				status = fileSystem.listStatus(inPath, new ResultListFilter());
				
				for ( int idx = 0; idx < status.length; idx++ ) {
					
					cnt++;
					
					if ( cnt > maxCnt ) {
						if ( batch > 0 ) {
							bw.close();
						}
	
						batch++;
						
						outFile = new Path(move1Path.toString() + Path.SEPARATOR + "move" + String.format("%04d", batch) + ".txt");
						bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(outFile,true)));
						cnt = 1;
					}

					fileName = status[idx].getPath().getName();
					parts = fileName.split("~");
					
					toPath = new Path(root + Path.SEPARATOR + "gold-bk" + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + parts[0] + Path.SEPARATOR + parts[1] + Path.SEPARATOR + parts[2] + Path.SEPARATOR + status[idx].getPath().getName());
					
					bw.write(status[idx].getPath().toString() + "\t" + toPath.toString() + "\n");
					
				}
			}
			
			if ( batch > 0 ) {
				bw.close();
			}

			move2Path = new Path(root + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "FixGold" + Path.SEPARATOR + "move2");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, move2Path,daasConfig.displayMsgs());	

			batch = 0;
			cnt = maxCnt+1;

			status = fileSystem.listStatus(baseOutputPath, new ResultListFilter());

			for ( int idx = 0; idx < status.length; idx++ ) {
				
				cnt++;
				
				if ( cnt > maxCnt ) {
					if ( batch > 0 ) {
						bw.close();
					}

					batch++;
					
					outFile = new Path(move2Path.toString() + Path.SEPARATOR + "move" + String.format("%04d", batch) + ".txt");
					bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(outFile,true)));
					cnt = 1;
				}

				fileName = status[idx].getPath().getName();
				parts2 = fileName.split("-m-|-r-");
				parts = parts2[0].split("~");
				
				fileName = parts[0] + "~" + parts[1] + "~" + parts[2] + "~" + String.format("%07d", fileId) + ".gz";
				fileId++;
				
				toPath = new Path(root + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir() + Path.SEPARATOR + parts[0] + Path.SEPARATOR + parts[1] + Path.SEPARATOR + parts[2] + Path.SEPARATOR + fileName);

				bw.write(status[idx].getPath().toString() + "\t" + toPath.toString() + "\n");
				
			}
			
			if ( batch > 0 ) {
				bw.close();
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void runMoveJob(DaaSConfig daasConfig
			               ,Path inPath
			               ,String msg) {

		Job job;

		try {
			baseOutputPath = new Path(root + Path.SEPARATOR + daasConfig.hdfsWorkSubDir() + Path.SEPARATOR + "FixGold" + Path.SEPARATOR + "Out3");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,true);
			
			System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");

			setOutputCompression(false);

			job = Job.getInstance(hdfsConfig, "Fix Gold File - " + msg);
			
			FileInputFormat.addInputPath(job, inPath);

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			job.setJarByClass(NextGenUnzipFile.class);
			job.setMapperClass(GenericMoveMapper.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( !job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void setOutputCompression(boolean compress) {

		if ( compress ) {
			hdfsConfig.set("mapreduce.map.output.compress", "true");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
			hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			
			System.out.println("Set output compression on");
		} else {
			hdfsConfig.set("mapreduce.map.output.compress", "false");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "false");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
			hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");			
			
			System.out.println("Set output compression off");
		}

	}
}

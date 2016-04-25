package com.mcd.gdw.daas.driver;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.mapreduce.NpSosXmlMapper;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.PathFilterByFileName;
/**
 * 
 * @author Sateesh Pula
 * Driver class to extract SOS from DetailedSOS files
 * hadoop jar /home/mc32445/scripts/daasmapreduce.jar com.mcd.gdw.daas.driver.GenerateTdaFormatSos -Dmapred.job.queue.name=default \
 * -r /user/mc32445/poc40000files/work/np_xml/step1/DetailedSOS* \
 * -o /user/mc32445/poc/sosextract38tfiles \
 * -c /user/mc32445/tdaextract/distcache/TDA_CODE_VALUES.psv
 */
public class GenerateTdaFormatSos extends Configured implements Tool {

	ABaC abac = null;
	DaaSConfig daasConfig;
	int prevJobGroupId = -1;
	int prevjobSeqNbr = -1;
	String owshFltr = "*";
	String generatFieldsForNielsen = "false";
	private Set<Integer> validTerrCdSet = new HashSet<Integer>();
	private String createJobDetails = "true";
	String filterOnStoreId = "FALSE";
		
	@Override
	public int run(String[] argsall) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		String[] args = gop.getRemainingArgs();
		
		int idx;
		String inputRootDir = "";
	
		String outputDir = "";
		String cacheFile = "";	
		

		String configXmlFile = "";
		String fileType = "";
		String vldTerrCdsStr = "";
		
		

		try {
			for (idx = 0; idx < args.length; idx++) {
				if ((idx % 2) != 0) {
					if (args[idx - 1].equals("-r")) {
						inputRootDir = args[idx];
					}
					
					if (args[idx - 1].equals("-o")) {
						outputDir = args[idx];
					}
					if (args[idx - 1].equals("-dc")) {
						cacheFile = args[idx];
					}
					
					if ( args[idx-1].equals("-c")) {
						configXmlFile = args[idx];
					}

					if ( args[idx-1].equals("-t")) {
						fileType = args[idx];
					}
					if ( args[idx-1].equals("-prevJobGroupId")  ) {
						prevJobGroupId = Integer.parseInt(args[idx]);
					}
					if (args[idx-1].equals("-seqNbr")) {
						prevjobSeqNbr = Integer.parseInt(args[idx]);
					}
					if ( args[idx-1].equals("-owshfltr")  ) {
						owshFltr = args[idx];
					}
					if ( args[idx-1].equals("-generatFieldsForNielsen") ) {
						generatFieldsForNielsen = args[idx];
					}
					if ( args[idx-1].equals("-generatFieldsForNielsen") ) {
						generatFieldsForNielsen = args[idx];
					}
					if ( args[idx-1].equalsIgnoreCase("-vldTerrCodes") ) {
						vldTerrCdsStr = args[idx];
						
						String[] terrCds = vldTerrCdsStr.split(",");
						for(String terrCd:terrCds){
							validTerrCdSet.add(Integer.parseInt(terrCd));
						}
					}
					if ( args[idx-1].equalsIgnoreCase("-filterOnStoreId") ) {
						filterOnStoreId = args[idx];
						if( !filterOnStoreId.equalsIgnoreCase("TRUE")){
							filterOnStoreId = "FALSE";
						}
					}
					if ( args[idx-1].equals("-createJobDetails") ) {
						createJobDetails = args[idx];
					}
					if(StringUtils.isBlank(createJobDetails)){
						createJobDetails = "TRUE";
					}
					
					
				}
			}
			daasConfig = new DaaSConfig(configXmlFile,fileType);
			
			if("TRUE".equalsIgnoreCase(createJobDetails)){
				abac = new ABaC(daasConfig);
				
				if(prevJobGroupId == -1){
					prevJobGroupId= abac.getOpenJobGroupId(DaaSConstants.TDA_EXTRACT_JOBGROUP_NAME);
					System.out.println( " ABAC query returned " + prevJobGroupId);
					if(prevJobGroupId == -1)
							prevJobGroupId = abac.createJobGroup(DaaSConstants.TDA_EXTRACT_JOBGROUP_NAME);
				}
				
				if(prevjobSeqNbr == -1){
					prevjobSeqNbr = 1;
				}
			}
			
			if (inputRootDir.length() > 0
					&& outputDir.length() > 0) {
				return runJob(inputRootDir, outputDir,cacheFile,getConf());
			} else {
				Logger.getLogger(GenerateTdaFormatSos.class.getName())
						.log(Level.SEVERE,
								"Missing input root directory or output directory or cache file arguments");
				Logger.getLogger(GenerateTdaFormatSos.class.getName())
						.log(Level.INFO,
								"Usage "
										+ GenerateTdaFormatSos.class
												.getName()
										+ " -r rootdirectory -o outputdirectory -c cachefile");
				System.exit(8);
			}
		} catch (Exception ex) {
			Logger.getLogger(GenerateTdaFormatSos.class.getName()).log(
					Level.SEVERE, null, ex);
			throw ex;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int retvalue = ToolRunner.run(new Configuration(),new GenerateTdaFormatSos(), args);
		System.out.println(" retvalue : " + retvalue);
	}

	public int runJob(String inputRootDir, 
			String outputDir,String cacheFile,Configuration conf) throws Exception {

		
		Job job;
		FileSystem hdfsFileSystem;
		Path hdfsOutputPath;
		ArrayList<String> inputDirList = new ArrayList<String>();
		ArrayList<Path> inputDirPathList = new ArrayList<Path>();
//		PathFilterByFileName detailedsosPathfiler = new PathFilterByFileName("DetailedSOS");
		int jobId = 0;
	
		
		
		
		try {
//			conf = new Configuration();
			//AWS START
			//hdfsFileSystem = FileSystem.get(conf);
			hdfsFileSystem = HDFSUtil.getFileSystem(daasConfig, conf);
			//AWS END
			
			
	
			conf.set(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER, owshFltr);
			
			job = new Job(conf, "Generate TDA Format DetailedSOS");
			
			if("TRUE".equalsIgnoreCase(createJobDetails)){
				jobId = abac.createJob(prevJobGroupId, ++prevjobSeqNbr, job.getJobName());
			}
//			FileInputFormat.addInputPath(job, new Path(inputRootDir + File.separator+ "DetailedSOS*"));
//			FileInputFormat.addInputPath(job, new Path(inputRootDir+"/DetailedSOS*"));
//			FileInputFormat.addInputPath(job, new Path(inputRootDir));
//			FileInputFormat.addInputPaths(job, inputRootDir);
//			FileInputFormat.setInputPathFilter(job, new SOSPathFilter().getClass());
			
			job.setJarByClass(GenerateTdaFormatSos.class);
			job.setMapperClass(NpSosXmlMapper.class);
			
			job.setNumReduceTasks(0);
			// job.setInputFormatClass(XmlInputFormat.class);
//			job.setOutputKeyClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

//			hdfsOutputPath = new Path(outputDir +  File.separator + "sos");
			hdfsOutputPath = new Path(outputDir );	

			// hdfsOutputPath = new Path(outputDir);

			FileOutputFormat.setOutputPath(job, hdfsOutputPath);
			//AWS START
			//hdfsFileSystem = FileSystem.get(hdfsOutputPath.toUri(), conf);
			//AWS END

			//AWS START
			String[] cp = cacheFile.split(",");
			
			for (String cpi:cp) {
				Path ncp = new Path(cpi);
				DistributedCache.addCacheFile(ncp.toUri(),job.getConfiguration());
			}

			//Path cachePath = new Path(cacheFile);
			//DistributedCache.addCacheFile(cachePath.toUri(),
			//		job.getConfiguration());
			
			
		//	Path cachePath = new Path(cacheFile);
		//	DistributedCache.addCacheFile(cachePath.toUri(),
		//				job.getConfiguration());
		//AWS END
//			System.out.println("adding " +cachePath.toUri()+"#"+cachePath.getName() + " to dist cache");
//			job.addCacheFile(new URI(cachePath.toUri()+"#"+cachePath.getName()));
			

			if (hdfsFileSystem.exists(hdfsOutputPath)) {
				hdfsFileSystem.delete(hdfsOutputPath, true);
				Logger.getLogger(GenerateTdaFormatSos.class.getName()).log(
						Level.INFO,
						"Removed existing output path = " + outputDir);
			}

			MultipleOutputs.addNamedOutput(job, "NEWTDACODEVALUES", TextOutputFormat.class,
					Text.class, Text.class);
			
			FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;
			String[] inputpathstrs = inputRootDir.split(",");
			
			
			for(String cachepathstr:inputpathstrs){
				
				fstatustmp = hdfsFileSystem.globStatus(new Path(cachepathstr+"/DetailedSOS*"));
				fstatus = (FileStatus[])ArrayUtils.addAll(fstatus, fstatustmp);
	
			}
			System.out.println(" fstat length " + fstatus.length);
			
			String filepath;
			String datepart;
			
			String terrCdDatepart;
			HashSet<String> terrCdDtset = new HashSet<String>();
			String[] fileNameParts;
			
			for(FileStatus fstat:fstatus){
				
				String fileName = fstat.getPath().getName().toUpperCase();
				
				String fileNamePartsDelimiter = "~";
				
				if(fileName.indexOf("RXD126") > 0){
					fileNamePartsDelimiter = "RXD126";
				}
				fileNameParts = fileName.split(fileNamePartsDelimiter);
				String terrCdfrmFileName = fileNameParts[1];
				
				
				
//				if(fileName.startsWith("DETAILEDSOSRXD126840RXD126") || fileName.startsWith("DETAILEDSOS~840~") ||
//						fileName.startsWith("DETAILEDSOSRXD126702RXD126") || fileName.startsWith("DETAILEDSOS~702~")){
				if(validTerrCdSet != null && validTerrCdSet.contains(Integer.parseInt(terrCdfrmFileName))){
					FileInputFormat.addInputPath(job, fstat.getPath());
					
//					filepath = fstat.getPath().toString().toUpperCase();
					
					datepart = fileNameParts[2].substring(0,8);
					terrCdDtset.add(terrCdfrmFileName+DaaSConstants.SPLCHARTILDE_DELIMITER+datepart);
//					
//					int lastindx = filepath.lastIndexOf("~");
//					if(lastindx == -1)
//						lastindx = filepath.lastIndexOf("-R-");
//					datepart = filepath.substring(lastindx-8,lastindx);
//					dtset.add(datepart);
				}
			}
			Iterator<String> it = terrCdDtset.iterator();
			
			while(it.hasNext()){
				terrCdDatepart = it.next();
				MultipleOutputs.addNamedOutput(job,"SOS"+DaaSConstants.SPLCHARTILDE_DELIMITER+terrCdDatepart,TextOutputFormat.class, Text.class, Text.class);
				
			}
			
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			int retCode = job.waitForCompletion(true) ? 0 : 1;
			
			
			if("TRUE".equalsIgnoreCase(createJobDetails)){
				abac.closeJob(jobId, DaaSConstants.JOB_SUCCESSFUL_ID, DaaSConstants.JOB_SUCCESSFUL_CD);
			}
			
//			String ooziePropFile = System.getProperty(DaaSConstants.OOZIE_ACTION_OUTPUT_PROPERTIES);
//			if(ooziePropFile != null){
//				File propFile = new File(ooziePropFile);
//				
//				Properties props = new Properties();
//				props.setProperty(DaaSConstants.LAST_JOB_GROUP_ID, ""+prevJobGroupId);
//				props.setProperty(DaaSConstants.LAST_JOB_SEQ_NBR, ""+(jobSeqNbr));
//				
//				OutputStream outputStream = new FileOutputStream(propFile);
//				props.store(outputStream, "custom props");
//				outputStream.close();
//				
//			}
//			FsPermission newFilePremission = new FsPermission(FsAction.READ_WRITE,FsAction.READ,FsAction.READ);
//			hdfsFileSystem.setPermission(hdfsOutputPath,newFilePremission);
			return retCode;

		} catch (InterruptedException ex) {
			Logger.getLogger(GenerateTdaFormatSos.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(GenerateTdaFormatSos.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (Exception ex) {
			Logger.getLogger(GenerateTdaFormatSos.class.getName()).log(
					Level.SEVERE, null, ex);
			throw ex;
		}finally{
			if(abac != null)
				abac.dispose();
		}

		return 0;
	}

}

package com.mcd.gdw.test.daas.driver;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.MergeToFinal;
import com.mcd.gdw.daas.util.CopyMoveNFileMapper;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;


public class TestCopyMoveNFileDriver extends Configured implements Tool{

	private int nextFileNum = 1;
	DaaSConfig daasConfig;
	
	public static void main(String[] args){
		try{
			
			
			
		ToolRunner.run(new Configuration(), new TestCopyMoveNFileDriver(), args);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	@Override
	public int run(String[] argsAll) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(argsAll);
		String[] args = gop.getRemainingArgs();
		
		
		String configXmlFile = null;
		String fileType = null;

		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-t") && (idx+1) < args.length ) {
				fileType = args[idx+1];
			}
		}
		
		if ( configXmlFile.length() == 0 || fileType.length() == 0 ) {
			System.err.println("Missing config.xml and/or filetype");
			System.err.println("Usage: MergeToFinal -c config.xml -t filetype");
			System.exit(8);
		}

		daasConfig = new DaaSConfig(configXmlFile,fileType);
		
		Path step3OutputPath = new Path("/daas/work/np_xml/step3");
		Path baseFinalPath = new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsFinalSubDir() + Path.SEPARATOR + daasConfig.fileSubDir());
		String fileSuffix = "gz";
		
		 moveFiles(step3OutputPath,baseFinalPath,fileSuffix,true,true,getConf(),99999,daasConfig);
		
		return 0;
	}
	private int moveFiles(Path sourcePath, Path targetPath, String fileSuffix,
			boolean separateDir, boolean keepCopy, Configuration hdfsConfig,
			Integer fileListJobId,DaaSConfig daasConfig) {

		
		FileStatus[] status = null;
		String[] fileParts = null;

		String targetPathText = targetPath.toString();
		String fullTargetPathText = null;
		String targetName = null;

		String fullFileSuffix = "";

		if (fileSuffix.length() > 0) {
			fullFileSuffix = "." + fileSuffix;
		} else {
			fullFileSuffix = "";
		}

		int retCnt = 0;

		HashMap<String, String> sourcePathDestPathMap = new HashMap<String, String>();
		BufferedWriter bwForListofFilestoCopy = null;
		try {
			
			

			
			FileSystem fileSystem = FileSystem.get(hdfsConfig);
			status = fileSystem.listStatus(sourcePath);
			fileParts = null;

			String fileType = "";
			String terrCd = "";
			String businessDt = "";

			Path newName = null;
			FsPermission newFilePremission = new FsPermission(FsAction.ALL,
					FsAction.ALL, FsAction.READ_EXECUTE);

			String nameFromPath = "";

			if (status != null) {
				for (int idx = 0; idx < status.length; idx++) {

					if (status[idx].getLen() == 0)
						continue;

					nameFromPath = HDFSUtil
							.restoreMultiOutSpecialChars(status[idx].getPath()
									.getName());

					if (nameFromPath.contains(HDFSUtil.FILE_PART_SEPARATOR)) {

						fileParts = nameFromPath
								.split(HDFSUtil.FILE_PART_SEPARATOR);

						// System.out.println(fileParts.length);
						// for (int iii=0; iii < fileParts.length; iii++ ) {
						// System.out.println(iii + ")" + fileParts[iii]);
						// }

						if (!Character.isDigit(fileParts[0].charAt(0))) {
							fileType = fileParts[0];
							terrCd = fileParts[1];
							businessDt = fileParts[2].substring(0, 8);
						} else {
							fileType = "";
							terrCd = fileParts[0];
							businessDt = fileParts[1].substring(0, 8);
						}

						if (separateDir) {
							if (fileType.length() > 0) {
								fullTargetPathText = targetPathText
										+ Path.SEPARATOR + fileType
										+ Path.SEPARATOR + terrCd
										+ Path.SEPARATOR + businessDt;
							} else {
								fullTargetPathText = targetPathText
										+ Path.SEPARATOR + terrCd
										+ Path.SEPARATOR + businessDt;
							}
						} else {
							fullTargetPathText = targetPathText;
						}

						if (fileType.length() > 0) {
							targetName = fileType
									+ HDFSUtil.FILE_PART_SEPARATOR + terrCd
									+ HDFSUtil.FILE_PART_SEPARATOR + businessDt
									+ HDFSUtil.FILE_PART_SEPARATOR
									+ String.format("%07d", nextFileNum)
									+ fullFileSuffix;
						} else {
							targetName = terrCd + HDFSUtil.FILE_PART_SEPARATOR
									+ businessDt + HDFSUtil.FILE_PART_SEPARATOR
									+ String.format("%07d", nextFileNum)
									+ fullFileSuffix;
						}

						newName = new Path(fullTargetPathText + Path.SEPARATOR
								+ targetName);

						if (keepCopy) {
							System.out.print("MOVE:" + status[idx].getPath()
									+ " TO:" + newName.getName() + "...");
							// FileUtil.copy(fileSystem, status[idx].getPath(),
							// fileSystem, newName, false, hdfsConfig);
							sourcePathDestPathMap.put(status[idx].getPath()
									.toString(), newName.toString());

							System.out.println("done");
						} else {
							fileSystem.rename(status[idx].getPath(), newName);
							fileSystem
									.setPermission(newName, newFilePremission);
						}

						retCnt++;
						nextFileNum++;
					}
				}

				// call mapreduce to copy files
				if (keepCopy) {
					Path destPathRoot = new Path(daasConfig.hdfsRoot()+ Path.SEPARATOR+ daasConfig.hdfsLandingZoneSubDir()+ Path.SEPARATOR + fileListJobId);
					Path destPathCache = new Path(destPathRoot.toString()+ Path.SEPARATOR + "cache");
					String sourcePathLastDir = sourcePath.getName().substring(sourcePath.getName().lastIndexOf("/") + 1);
					String outputPath = destPathRoot.toString()+ Path.SEPARATOR + "CopyMoveFileList" +Path.SEPARATOR+sourcePathLastDir+ Path.SEPARATOR+ "filelist_tocopymoveto_goldlayer.txt";

					bwForListofFilestoCopy = new BufferedWriter(
							new OutputStreamWriter(fileSystem.create(new Path(
									outputPath), true)));

					Iterator<String> keyIt = sourcePathDestPathMap.keySet()
							.iterator();
					StringBuffer sbf = new StringBuffer();
					int i = 0;
					while (keyIt.hasNext()) {
						String key = keyIt.next();

						sbf.setLength(0);
						if (i > 0)
							sbf.append("\n");
						sbf.append(key).append("\t")
								.append(sourcePathDestPathMap.get(key));

						bwForListofFilestoCopy.write(sbf.toString());
						i++;
					}

					bwForListofFilestoCopy.close();

					FileStatus stat = fileSystem.getFileStatus(new Path(
							outputPath));
					if (stat.getLen() > 0) {

						hdfsConfig
								.set(DaaSConstants.JOB_CONFIG_PARM_COPY_OR_MOVE_FILES,
										"COPY");
						Job moveFileJob = new Job(hdfsConfig, "Copy/Move "
								+ sourcePathLastDir + " files to Gold Layer");

						moveFileJob.setJarByClass(MergeToFinal.class);

						moveFileJob.setMapperClass(CopyMoveNFileMapper.class);
						moveFileJob.setNumReduceTasks(0);

						moveFileJob.setInputFormatClass(NLineInputFormat.class);
						// moveFileJob.setInputFormatClass(TextInputFormat.class);
						moveFileJob
								.setOutputFormatClass(TextOutputFormat.class);

						moveFileJob.setMapOutputKeyClass(NullWritable.class);
						moveFileJob.setMapOutputValueClass(Text.class);

						moveFileJob.setOutputKeyClass(NullWritable.class);
						moveFileJob.setOutputValueClass(Text.class);

						FileInputFormat.addInputPath(moveFileJob, new Path(
								outputPath));
						NLineInputFormat.setNumLinesPerSplit(moveFileJob, 10);
						FileOutputFormat.setOutputPath(moveFileJob, new Path(destPathRoot.toString() + Path.SEPARATOR+"CopyMoveFileStatus"+Path.SEPARATOR+ sourcePathLastDir));

						MultipleOutputs.addNamedOutput(moveFileJob,"CopyMoveFileStatus", TextOutputFormat.class,NullWritable.class, Text.class);

						LazyOutputFormat.setOutputFormatClass(moveFileJob, TextOutputFormat.class);
						
						moveFileJob.waitForCompletion(true);
					}

				}
			}

		} catch (Exception ex) {
			System.err.println("Error occured in move files:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		} finally {
			try {
				if (bwForListofFilestoCopy != null)
					bwForListofFilestoCopy.close();
			} catch (Exception ex) {

			}
		}

		return (retCnt);
	}
	public int run1(String[] argAll) throws Exception {
		
		Job moveFileJob = new Job(getConf(),"MoveFiles to Gold Layer");
		
		moveFileJob.setJarByClass(MergeToFinal.class);
		
		moveFileJob.setMapperClass(CopyMoveNFileMapper.class);
		moveFileJob.setNumReduceTasks(0);
		
//		moveFileJob.setInputFormatClass(NLineInputFormat.class);
		moveFileJob.setInputFormatClass(TextInputFormat.class);
		moveFileJob.setOutputFormatClass(TextOutputFormat.class);
		
		moveFileJob.setMapOutputKeyClass(NullWritable.class);
		moveFileJob.setMapOutputValueClass(Text.class);
		
		moveFileJob.setOutputKeyClass(NullWritable.class);
		moveFileJob.setOutputValueClass(Text.class);
		
	
		FileInputFormat.addInputPath(moveFileJob, new Path("/daastest/lz/4935/cache/filelist_tocopyto_goldlayer.txt"));
//		NLineInputFormat.setNumLinesPerSplit(moveFileJob, 1);
		FileOutputFormat.setOutputPath(moveFileJob, new Path("/daastest/lz/4935/CopyMoveFileStatus"));
		
		MultipleOutputs.addNamedOutput(moveFileJob, "CopyMoveFileStatus", TextOutputFormat.class, NullWritable.class, Text.class);
		
		moveFileJob.waitForCompletion(true);
		
		return 0;
	}

	
}


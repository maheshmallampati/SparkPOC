package com.mcd.gdw.daas.driver;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.mapreduce.UnZipCompositeKeyComparator;
import com.mcd.gdw.daas.mapreduce.UnZipGroupComparator;
import com.mcd.gdw.daas.mapreduce.UnZipPartitioner;
import com.mcd.gdw.daas.mapreduce.UnzipInputFileMapper;
import com.mcd.gdw.daas.mapreduce.UnzipInputFileReducer;
import com.mcd.gdw.daas.mapreduce.ZipFileInputFormat;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

public class UnzipInputFileDriver extends Configured implements Tool {

	public static void main(String[] argsAll) throws Exception {
		ToolRunner
				.run(new Configuration(), new UnzipInputFileDriver(), argsAll);
	}

	@Override
	public int run(String[] argsAll) throws Exception {

		GenericOptionsParser gop = new GenericOptionsParser(argsAll);

		String[] args = gop.getRemainingArgs();
		String configXmlFile = args[0];
		String fileType 	 = args[1];
		String inputPath 	 = args[2];
		String outputPath 	 = args[3];
		String cachePath 	 = args[4];
		String terrCd 		 = args[5];

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);

		runMrUnzip(daasConfig, fileType, getConf(), inputPath, outputPath,
				cachePath,terrCd);
		// TODO Auto-generated method stub
		return 0;
	}

	private void runMrUnzip(DaaSConfig daasConfig, String fileType,
			Configuration hdfsConfig, String inputPath, String outputPath,
			String cachePath,String terrCd) throws Exception {

		String[] fileSubTypes = new String[] { "STLD ", "SecurityData",
				"MenuItem", "DetailedSOS", "product-db", "store-db" };
		
		FileSystem hdfsFileSystem = FileSystem.get(hdfsConfig);
		
		HashMap<String, Integer> terrDateMap = new HashMap<String, Integer>();
		
		FileStatus[] fstat = hdfsFileSystem.listStatus(new Path(inputPath));
		
		for(FileStatus fileStatus:fstat){
			String fileName = fileStatus.getPath().getName();
			int indx = fileName.indexOf(".");
			String datePart = fileName.substring(indx-8,indx);
			
			terrDateMap.put(terrCd+"|"+datePart, 1);
		}
//		terrDateMap.put("840|20140304", 1);
//		terrDateMap.put("840|20140305", 1);
//		terrDateMap.put("840|20140306", 1);
//		terrDateMap.put("840|20140307", 1);
//		terrDateMap.put("840|20140310", 1);
//		terrDateMap.put("840|20140308", 1);
//		terrDateMap.put("840|20140309", 1);

		Job job;

		hdfsConfig.set("mapred.compress.map.output", "true");
		hdfsConfig.set("mapred.output.compress", "true");
		// hdfsConfig.set("mapred.output.compression.type", "BLOCK");
		hdfsConfig.set("mapred.output.compression.type", "RECORD");
		hdfsConfig.set("mapred.map.output.compression.codec",
				"org.apache.hadoop.io.compress.SnappyCodec");
		// hdfsConfig.set("mapred.map.output.compression.codec",
		// "org.apache.hadoop.io.compress.GzipCodec");
		hdfsConfig.set("mapred.output.compression.codec",
				"org.apache.hadoop.io.compress.GzipCodec");

		try {
			System.out.println("\nUnzipping files\n");

			hdfsConfig.set("skipFilesonSize", daasConfig.skipFilesonSize());
			hdfsConfig.set("MAX_FILE_SIZE", daasConfig.maxFileSize());

			job = new Job(hdfsConfig, "Processing - " + fileType);

			ZipFileInputFormat.setLenient(true);
			// ZipFileInputFormat.setInputPaths(job, new
			// Path(daasConfig.hdfsRoot() + Path.SEPARATOR +
			// daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR +
			// daasConfig.fileSubDir() + Path.SEPARATOR + "*.zip"));
			// ZipFileInputFormat.setInputPaths(job, new
			// Path(daasConfig.hdfsRoot() + Path.SEPARATOR +
			// daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR +
			// daasConfig.fileSubDir() + Path.SEPARATOR +
			// daasConfig.filePattern()));
			// ZipFileInputFormat.setInputPaths(job, new
			// Path(daasConfig.hdfsRoot() + Path.SEPARATOR +
			// daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR +
			// fileListJobId + Path.SEPARATOR + "source" ));
			ZipFileInputFormat.setInputPaths(job, new Path(inputPath));
			job.setJarByClass(UnzipInputFileDriver.class);
			job.setMapperClass(UnzipInputFileMapper.class);
			job.setReducerClass(UnzipInputFileReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			// job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(daasConfig.numberofReducers());
			job.setInputFormatClass(ZipFileInputFormat.class);
			// job.setOutputFormatClass(TextOutputFormat.class);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			job.setPartitionerClass(UnZipPartitioner.class);
			job.setGroupingComparatorClass(UnZipGroupComparator.class);
			job.setSortComparatorClass(UnZipCompositeKeyComparator.class);

			if (fileSubTypes != null) {

				String[] keyValueParts = { "", "" };

				for (Map.Entry<String, Integer> entry : terrDateMap.entrySet()) {

					try {
						keyValueParts = (entry.getKey()).split("\\|");

						for (int idx = 0; idx < fileSubTypes.length; idx++) {
							MultipleOutputs
									.addNamedOutput(
											job,
											HDFSUtil.replaceMultiOutSpecialChars(fileSubTypes[idx]
													+ HDFSUtil.FILE_PART_SEPARATOR
													+ keyValueParts[0]
													+ HDFSUtil.FILE_PART_SEPARATOR
													+ keyValueParts[1]),
											TextOutputFormat.class, Text.class,
											Text.class);

							System.out.println("Added Multiple Output for "
									+ fileSubTypes[idx] + " | "
									+ keyValueParts[0] + " | "
									+ keyValueParts[1]);
						}
					} catch (Exception ex) {
						System.err
								.println("Error occured in building MultipleOutputs File Name");
						System.err.println(ex.toString());
						System.exit(8);
					}
				}
			}

			MultipleOutputs.addNamedOutput(job, "FileStatus",
					TextOutputFormat.class, Text.class, Text.class);
			System.out.println("Added Multiple Output for FileStatus\n");

			TextOutputFormat.setOutputPath(job, new Path(outputPath));

			DistributedCache.addCacheFile(new Path(cachePath + Path.SEPARATOR
					+ daasConfig.abacSqlServerCacheFileName()).toUri(),
					job.getConfiguration());
			
			job.waitForCompletion(true);

		} catch (Exception ex) {
			System.err.println("Error occured in MapReduce process:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(8);
		}
	}

}

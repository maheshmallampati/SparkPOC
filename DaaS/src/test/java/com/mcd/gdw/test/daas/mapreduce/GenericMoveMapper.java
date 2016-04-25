package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenericMoveMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static FileSystem hdfs = null;
	private static String[] parts = null;
	private static Path fromPath = null;
	private static Path toPath = null;
	
	private Text valueOut = new Text();
	
	@Override
	public void setup(Context context) {

		try {
			Configuration conf = context.getConfiguration();
			hdfs = FileSystem.get(conf);
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenericMoveMapper:");
			ex.printStackTrace(System.err);
		}
		
	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		try {
			parts = value.toString().split("\\t");

			fromPath = new Path(parts[0]);
			toPath = new Path(parts[1]);
			
			if ( hdfs.exists(fromPath) ) { 
				if ( hdfs.exists(toPath.getParent()) ) {
					if  ( !hdfs.exists(toPath) ) {
						try {
							hdfs.rename(fromPath, toPath);

							context.getCounter("DaaS Counters", "Missing file").increment(0);
							context.getCounter("DaaS Counters", "Missing Parent Directory").increment(0);
							context.getCounter("DaaS Counters", "Target File Already Exists").increment(0);
							context.getCounter("DaaS Counters", "Error Moving File").increment(0);
							context.getCounter("DaaS Counters", "Moved File").increment(1);
							
							valueOut.clear();
							valueOut.set(fromPath.getName());
							
							context.write(NullWritable.get(), valueOut);

						} catch (Exception ex) {
							context.getCounter("DaaS Counters", "Missing file").increment(0);
							context.getCounter("DaaS Counters", "Missing Parent Directory").increment(0);
							context.getCounter("DaaS Counters", "Target File Already Exists").increment(0);
							context.getCounter("DaaS Counters", "Error Moving File").increment(1);
							context.getCounter("DaaS Counters", "Moved File").increment(0);
						}
					} else {
						context.getCounter("DaaS Counters", "Missing file").increment(0);
						context.getCounter("DaaS Counters", "Missing Parent Directory").increment(0);
						context.getCounter("DaaS Counters", "Target File Already Exists").increment(1);
						context.getCounter("DaaS Counters", "Error Moving File").increment(0);
						context.getCounter("DaaS Counters", "Moved File").increment(0);
					}
				} else {
					context.getCounter("DaaS Counters", "Missing file").increment(0);
					context.getCounter("DaaS Counters", "Missing Parent Directory").increment(1);
					context.getCounter("DaaS Counters", "Target File Already Exists").increment(0);
					context.getCounter("DaaS Counters", "Error Moving File").increment(0);
					context.getCounter("DaaS Counters", "Moved File").increment(0);
				}
			} else {
				context.getCounter("DaaS Counters", "Missing file").increment(1);
				context.getCounter("DaaS Counters", "Missing Parent Directory").increment(0);
				context.getCounter("DaaS Counters", "Target File Already Exists").increment(0);
				context.getCounter("DaaS Counters", "Error Moving File").increment(0);
				context.getCounter("DaaS Counters", "Moved File").increment(0);
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in GenericMoveMapper:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
}

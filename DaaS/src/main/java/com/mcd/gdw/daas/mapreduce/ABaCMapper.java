package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC.CodeType;
import com.mcd.gdw.daas.driver.MergeToFinal;
import com.mcd.gdw.daas.util.HDFSUtil;
//import com.mcd.gdw.daas.abac.ABaCListItem;

public class ABaCMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static String[] parts = null;
	private static String[] subParts = null;
	private static String[] subItemParts = null;
	private static HashMap<CodeType,Integer> typeCodes = new HashMap<CodeType,Integer>();
	private static FileSystem hdfs = null;
	private static Path fromPath = null;
	private static Path toPath = null;
	private String fromPathRoot = null;
	private String toPathRoot = null;
	private String toRejectPathRoot = null;
	private int fileStatus;
	private static Text currentValue = new Text();
	private static boolean rejectFl = false;
	
	@Override
	public void setup(Context context) {
		
		try {
			Configuration conf = context.getConfiguration();
			//AWS START
			//hdfs = FileSystem.get(conf);
			hdfs = HDFSUtil.getFileSystem(conf.get(DaaSConstants.HDFS_ROOT_CONFIG), conf);
			//AWS END
			
			fromPathRoot = conf.get(DaaSConstants.JOB_CONFIG_PARM_ABAC_FROM_PATH);
			toPathRoot = conf.get(DaaSConstants.JOB_CONFIG_PARM_ABAC_TO_PATH);
			toRejectPathRoot = conf.get(DaaSConstants.JOB_CONFIG_PARM_ABAC_TO_REJECT_PATH);
			
			if ( fromPathRoot == null || toPathRoot == null || toRejectPathRoot == null ) {
				System.err.println("Missing 1 or more of the folling required parameters:" + DaaSConstants.JOB_CONFIG_PARM_ABAC_FROM_PATH + ", " + 
			                                                                                 DaaSConstants.JOB_CONFIG_PARM_ABAC_TO_PATH + ", " +
			                                                                                 DaaSConstants.JOB_CONFIG_PARM_ABAC_TO_REJECT_PATH);
				System.exit(8);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in ABaCMapper:");
			ex.printStackTrace(System.err);
		}
		
	}

	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		try {
			context.getCounter("DaaS Counters", "INPUT FILE COUNT").increment(1);

			parts = value.toString().split("\t");

			if ( typeCodes.size() == 0 ) {
				subParts = parts[10].split(",");
				for ( int idx=0; idx < subParts.length; idx++ ) {
					subItemParts = subParts[idx].split("\\|");
					
					typeCodes.put(CodeType.values()[Integer.parseInt(subItemParts[0])], Integer.parseInt(subItemParts[1]));
				}
			}
			
			fromPath = new Path(fromPathRoot + Path.SEPARATOR + parts[1]);
			
			fileStatus = Integer.parseInt(parts[7]);
			
			if ( fileStatus == typeCodes.get(CodeType.REJECTED) ) {
				toPath = new Path(toRejectPathRoot + Path.SEPARATOR + parts[1]);
				context.getCounter("DaaS Counters", "SOURCE FILE COUNT").increment(0);
				context.getCounter("DaaS Counters", "REJECT FILE COUNT").increment(1);
				rejectFl = true;
			} else {
				toPath = new Path(toPathRoot + Path.SEPARATOR + parts[1]);
				context.getCounter("DaaS Counters", "SOURCE FILE COUNT").increment(1);
				context.getCounter("DaaS Counters", "REJECT FILE COUNT").increment(0);
				rejectFl = false;
			}

			boolean moveFailure=false;
			
			if ( hdfs.exists(fromPath) ) {
                // retry N number of if move fails
				for (int i=0; i<=6;i++) {
					try {
						if (!hdfs.exists(toPath)) {
                            if (hdfs.rename(fromPath, toPath)) {
                                // if ( !hdfs.exists(toPath) && hdfs.renameWithRetry(fromPath, toPath) ) {
                                context.getCounter("DaaS Counters", "MOVE FILE FROM LZ SUCCESS").increment(1);
                                context.getCounter("DaaS Counters", "MOVE FILE FROM LZ FAILURE").increment(0);
                                context.getCounter("DaaS Counters", "MOVE FILE FROM LZ MISSING").increment(0);
                                if (rejectFl) {
                                    currentValue.clear();
                                    currentValue.set("REJECT|" + parts[0]);
                                    context.write(NullWritable.get(), currentValue);
                                } else {
                                    currentValue.clear();
                                    currentValue.set("SUCCESS|" + parts[0]);
                                    context.write(NullWritable.get(), currentValue);
                                }
                                moveFailure = false;
                                break;//mc41946
                            }
                            // else if (hdfs.exists(toPath)) {
                            // 	// TODO refactor - make OR
                            //     context.getCounter("DaaS Counters", "MOVE FILE FROM LZ SUCCESS").increment(1);
                            //     context.getCounter("DaaS Counters", "MOVE FILE FROM LZ FAILURE").increment(0);
                            //     context.getCounter("DaaS Counters", "MOVE FILE FROM LZ MISSING").increment(0);
                            //     if ( rejectFl ) {
                            //     	currentValue.clear();
                            //     	currentValue.set("REJECT|" + parts[0]);
                            //     	context.write(NullWritable.get(),currentValue);
                            //     } else {
                            //     	currentValue.clear();
                            //     	currentValue.set("SUCCESS|" + parts[0]);
                            //     	context.write(NullWritable.get(),currentValue);
                            //     }
                            //     moveFailure=false;
                            //     break;//mc41946
                            // }
                            else {
                                moveFailure = true;
                            }
                        }
                        else {
                            context.getCounter("DaaS Counters", "MOVE FILE FROM LZ SUCCESS").increment(1);
                            context.getCounter("DaaS Counters", "MOVE FILE FROM LZ FAILURE").increment(0);
                            context.getCounter("DaaS Counters", "MOVE FILE FROM LZ MISSING").increment(0);
                            if (rejectFl) {
                                currentValue.clear();
                                currentValue.set("REJECT|" + parts[0]);
                                context.write(NullWritable.get(), currentValue);
                            } else {
                                currentValue.clear();
                                currentValue.set("SUCCESS|" + parts[0]);
                                context.write(NullWritable.get(), currentValue);
                            }
                            moveFailure = false;
                            break;//mc41946
                        }
						/*else {
							//mc41946
							if ( hdfs.exists(toPath) ) {
								context.getCounter("DaaS Counters", "MOVE FILE FROM LZ SUCCESS").increment(1);
							    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ FAILURE").increment(0);
							    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ MISSING").increment(0);
							    break;
							}
							else {
								System.err.println("renameWithRetry retry " + fromPath.toString() + " " + toPath.toString());
							}
							//
							
							// mc41946
						}*/
						
					} catch (Exception ex1) {
						//AWS START
                        System.err.println("renameWithRetry failed on " + i + " retry attempt for " + fromPath.toString() + " to " + toPath.toString());
						ex1.printStackTrace(System.err);
						//AWS END

					   /* context.getCounter("DaaS Counters", "MOVE FILE FROM LZ SUCCESS").increment(0);
					    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ FAILURE").increment(1);
					    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ MISSING").increment(0);
				    	currentValue.clear();
				    	currentValue.set("FAIL|" + parts[0]);
				    	context.write(NullWritable.get(),currentValue);*/
				    	moveFailure=true;
					}
					
					Random ran = new Random();
					int x = ran.nextInt(10) + 5;
					System.err.println("renameWithRetry retry (wait " + x + " sec) " + fromPath.toString() + " to " + toPath.toString());
					Thread.sleep(x*1000);
				} //for loop
			}//if block
			else {
				//AWS START
				System.err.println("from path missing " + fromPath.toString());
				//AWS END

			    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ SUCCESS").increment(0);
			    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ FAILURE").increment(0);
			    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ MISSING").increment(1);
		    	currentValue.clear();
		    	currentValue.set("MISSING|" + parts[0]);
		    	context.write(NullWritable.get(),currentValue);
			}
					
			//mc41946
			if ( moveFailure ) {
				System.err.println("renameWithRetry retry " + fromPath.toString() + " " + toPath.toString());
				context.getCounter("DaaS Counters", "MOVE FILE FROM LZ SUCCESS").increment(0);
			    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ FAILURE").increment(1);
			    context.getCounter("DaaS Counters", "MOVE FILE FROM LZ MISSING").increment(0);
		    	currentValue.clear();
		    	currentValue.set("FAIL|" + parts[0]);
		    	context.write(NullWritable.get(),currentValue);
			}	
		} catch (Exception ex) {
			System.err.println("Error occured in ABaCMapper:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
}

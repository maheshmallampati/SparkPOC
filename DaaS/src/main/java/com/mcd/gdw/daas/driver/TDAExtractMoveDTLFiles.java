package com.mcd.gdw.daas.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;



import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.util.HDFSUtil;

/**
 * 
 * @author Sateesh Pula
 * This class moves the DTL-m* files from the STLD Extract to its own directory 'DTL'.
 * This is to create a hive table on top for analysis. This move is not required if there is no 
 * need for a hive table.
 */
public class TDAExtractMoveDTLFiles extends Configured implements Tool{

	
	@Override
	public int run(String[] args) throws Exception {
		try{
				if(args != null && args.length >= 2){
				FileSystem fs = HDFSUtil.getHdfsFileSystem();
				
				
				Configuration conf = getConf();//new Configuration();
				
			
				
				Path srcpath = new Path(args[0]);
				Path destpath = new Path(args[1]);
				String jobname = conf.get("mapred.job.name");
				if(jobname != null && !jobname.isEmpty()){
				jobname = jobname.substring(jobname.lastIndexOf("=")+1);
				
				System.out.println(" workflow id  "+jobname);
				}else{
					System.out.println("mapredjobname is null");
				}
				System.out.println(" job id :  " + System.getProperty("oozie.launcher.job.id"));
				
				
				PathFilter pf = new PathFilter() {
					
					@Override
					public boolean accept(Path fname) {
						
						if(fname.toString().contains("DTL-m")){
							System.out.println("filter checking passed"  + fname.toString());
							return true;
						}{
							System.out.println("filter checking failed "  + fname.toString());
						}
								
						return false;
					}
				};
				
				FileStatus[] fstsarr = fs.listStatus(srcpath,pf);
				
				System.out.println(" fs length " + fstsarr.length);
				
				fs.mkdirs(new Path(args[1]));
				
				for(FileStatus fstat:fstsarr){
					System.out.println(" looping thru file " + fstat.getPath().toString());
					if(!fstat.isDir()){
						FileUtil.copy(fs, fstat.getPath(),fs, destpath,true,conf);
						
						System.out.println(" copying file " + fstat.getPath().toString());
					}else{
						System.out.println(" not a file " + fstat.getPath().toString());
					}
				}
			
			}else{
				System.out.println(" Incorrect parameters. Expecting SourcePath and Destination Path");
			}
			
		
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(),new TDAExtractMoveDTLFiles(), args);
		
	}
}

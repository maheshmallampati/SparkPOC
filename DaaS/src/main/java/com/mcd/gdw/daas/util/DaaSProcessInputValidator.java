package com.mcd.gdw.daas.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class DaaSProcessInputValidator {

	public static void main(String[] args) throws Exception{
		
		String configXML = args[0];
		
		DaaSConfig daasConfig = new DaaSConfig(configXML);
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		Path sourcePath =new Path(daasConfig.hdfsRoot() + Path.SEPARATOR + daasConfig.hdfsLandingZoneSubDir() + Path.SEPARATOR + daasConfig.hdfsLandingZoneArrivalSubDir());
		
		FileStatus[] fstats = fs.listStatus(sourcePath, new PathFilter() {
			
			@Override
			public boolean accept(Path path) {
				if(path.getName().toUpperCase().contains("ZIP"))
					return true;
				
				return false;
			}
		});
		
		if(fstats == null || fstats.length <= 0){
			throw new Exception(" The input path " +sourcePath.toUri() + " does not contain any zip files for processing.");
		}
	}
	
	
	

}

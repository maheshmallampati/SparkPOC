package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

public class CatHDFSFile {

	public static void main(String[] args) {

		CatHDFSFile instance = new CatHDFSFile();
		ArrayList<String> hdfsPaths = new ArrayList<String>();
		String configXmlFile = "";
		
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-s") && (idx+1) < args.length ) {
				String[] paths = args[idx+1].split(",");
				for ( int pathIdx=0; pathIdx < paths.length; pathIdx++ ) {
					hdfsPaths.add(paths[pathIdx]);
				}
			}
		}
		
		instance.run(configXmlFile, hdfsPaths);
	}
	
	private void run(String configXmlFile
			        ,ArrayList<String> hdfsPaths) {

		try {
	 		DaaSConfig daasConfig = new DaaSConfig(configXmlFile);
			
			if ( daasConfig.configValid() ) {
				
				HDFSUtil.setHdfsSetUpDir(daasConfig.hdfsConfigDir());
				Configuration conf = HDFSUtil.getHdfsConfiguration();

				FileSystem fileSystem = FileSystem.get(conf);

				DataInputStream fileStatusStream = null;
				InputStreamReader isr = null;
				BufferedReader br = null;
				
				for ( String hdfsPath : hdfsPaths ) {
					//System.out.println("FILE = " + hdfsPath);
					Path src = new Path(hdfsPath);
					
					if ( fileSystem.exists(src) ) {
						//System.out.println("Process");

						if ( hdfsPath.endsWith(".gz") ) {
							isr=new InputStreamReader(new GZIPInputStream(fileSystem.open(src)));
							br=new BufferedReader(isr);
						} else {
							fileStatusStream = new DataInputStream(fileSystem.open(src));
							br = new BufferedReader(new InputStreamReader(fileStatusStream));
						}

						
						//DataInputStream fileStatusStream = new DataInputStream(fileSystem.open(src));
						//BufferedReader br = new BufferedReader(new InputStreamReader(fileStatusStream));

						String line=br.readLine();
						while (line != null) {
							System.out.println(line);
							line=br.readLine();
						}

						br.close();
						
						//System.out.println("DONE");
					}
				}
				
			} else {
				System.err.println(configXmlFile + " is not valid");
			}
			
		} catch(Exception ex) {
			ex.printStackTrace(System.err);
		}
		
	}

}

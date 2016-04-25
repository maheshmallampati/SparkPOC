package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;

import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;

public class GetData {

	private final String BACK = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";

	public static void main(String[] args) throws Exception {

		String configXmlFile = "";
		String hdfsPath = "";
		String outputPath = "";
		String overwrite = "N";
		String remove = "N";
		boolean overwriteFl = false;
		boolean removeFl = false;
		
		GetData instance = new GetData();
		
		if ( args.length == 0 || (args.length > 0 && args[0].toUpperCase().endsWith("-H")) ) {
			System.out.println("Usage GetData -c configXmlFile -s sourceHdfsPath -d localSystemOutputPath [-o overwrite] [-r remove]");
			System.out.println("where configXmlFile = Valid DaaS XML config File");
			System.out.println("      sourceHdfsPath = Valid HDFS source directory or Valid HDFS source file");
			System.out.println("      localSystemOutputPath = Valid local system output directory or valid local system output file");
			System.out.println("      overwrite = optional Y or N value that indicates if local files are overritten. Default N.");
			System.out.println("      overwrite = optional Y or N value that indicates if HDFS file is removed after copy to local system.  Default N.");
			System.exit(0);
		}
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}

			if ( args[idx].equals("-s") && (idx+1) < args.length ) {
				hdfsPath = args[idx+1];
			}

			if ( args[idx].equals("-d") && (idx+1) < args.length ) {
				outputPath = args[idx+1];
			}

			if ( args[idx].equals("-o") && (idx+1) < args.length ) {
				overwrite = args[idx+1];
			}

			if ( args[idx].equals("-r") && (idx+1) < args.length ) {
				remove = args[idx+1];
			}
		}
	
		if ( configXmlFile.length() > 0 && hdfsPath.length() > 0 && outputPath.length() > 0 ) {
			if ( overwrite.toUpperCase().startsWith("Y") ) {
				overwriteFl = true;
			}
			if ( remove.toUpperCase().startsWith("Y") ) {
				removeFl = true;
			}
			instance.run(configXmlFile,hdfsPath,outputPath,overwriteFl,removeFl);
		} else {
			System.err.println("Missing one or more required parameters");
			System.err.println("Usage GetData -c configXmlFile -s sourceHdfsPath -d localSystemOutputPath [-o overwrite] [-r remove]");
			System.err.println("where configXmlFile = Valid DaaS XML config File");
			System.err.println("      sourceHdfsPath = Valid HDFS source directory or Valid HDFS source file");
			System.err.println("      localSystemOutputPath = Valid local system output directory or valid local system output file");
			System.err.println("      overwrite = optional Y or N value that indicates if local files are overritten. Default N.");
			System.err.println("      overwrite = optional Y or N value that indicates if HDFS file is removed after copy to local system.  Default N.");
		}
	}

	private void run(String configXmlFile
			        ,String hdfsPath
			        ,String outputPath
			        ,boolean overwrite
			        ,boolean remove) throws Exception {
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile);
		ArrayList<Path> removeHdfsFiles = new ArrayList<Path>();
		
		if ( daasConfig.configValid() ) {
			if ( daasConfig.displayMsgs() ) {
				System.out.println(daasConfig.toString());
			}
			
			HDFSUtil.setHdfsSetUpDir(daasConfig.hdfsConfigDir());
			Configuration conf = HDFSUtil.getHdfsConfiguration();

			FileSystem fileSystem = FileSystem.get(conf);
			
			Path src = new Path(hdfsPath);
			
			if ( !fileSystem.exists(src) ) {
				System.err.println("Error: " + src.toString() + " does not exist");
				System.exit(8);
			}
			
			System.out.println("=================================");
			System.out.println("Processing HDFS Get File Request:");
			System.out.println("  Overwrite = " + overwrite);
			System.out.println("  Remove    = " + remove);
			System.out.println("=================================\n");

			File outFile = null;
			
			if  ( fileSystem.getFileStatus(src).isDir() ) {
				System.out.println("=================================");
				System.out.println("Processing directory request");
				System.out.println("=================================\n");
				FileStatus[] status = fileSystem.listStatus(src);
				File outDir = new File(outputPath);
				
				if ( status.length == 0 ) {
					System.err.println("HDFS Directory empty");
					System.exit(8);
				}
				
				if ( outDir.exists() ) {
					if ( outDir.isDirectory() ) {
						for (int i=0; i < status.length; i++ ) {
							outFile = new File(outputPath + File.separator + status[i].getPath().getName());
							if ( outFile.exists() && !overwrite ) {
								System.err.println(outFile.getCanonicalPath() + " aready exists, remove or use overwrite");
								System.exit(8);
							}
							getHdfsFile(fileSystem,status[i].getPath(),outFile);
							if ( remove ) {
								removeHdfsFiles.add(status[i].getPath());
							}
						}
					} else {
						System.err.println("Output directory " + outputPath + " is not a valid directory");
						System.exit(8);
					}
				} else {
					System.err.println("Output directory " + outputPath + " does not exist");
					System.exit(8);
				}
			} else {
				System.out.println("=================================");
				System.out.println("Processing single file request");
				System.out.println("=================================\n");
				outFile = new File(outputPath);
				if ( outFile.exists() && outFile.isDirectory() ) {
					outFile = new File(outputPath + File.separator + src.getName());
					if ( outFile.exists() && !overwrite ) {
						System.err.println(outFile.getCanonicalPath() + " aready exists, remove or use overwrite");
						System.exit(8);
					}
				}
				getHdfsFile(fileSystem,src,outFile);
				if ( remove ) {
					removeHdfsFiles.add(src);
				}
			}

			if ( removeHdfsFiles.size() > 0 ) {
				
				Trash tt = new Trash(fileSystem,conf);

				System.out.println("\n===============================");
				System.out.println("Removing HDFS files (move to Trash)");
				System.out.println("===============================\n");
				for ( Path trashItem : removeHdfsFiles) {
					System.out.println(trashItem.toString() + " sent to trash");
					tt.moveToTrash(trashItem);
				}
			}
			
		} else {
			System.err.println("DaaS Config File (" + configXmlFile + ") not valid");
			System.exit(8);
		}
		
		System.out.println("\n===============================");
		System.out.println("DONE");
	}

	private void getHdfsFile(FileSystem fileSystem
			                ,Path src
			                ,File dest) throws Exception {

		DataInputStream fileStatusStream = null;
		BufferedReader br = null;
		
		long fileSize;
		String msg;
		double pct;
		String pct_txt = "";
		String msg2;
		long hdfsSize;

		fileStatusStream = new DataInputStream(fileSystem.open(src));
		br = new BufferedReader(new InputStreamReader(fileStatusStream));

		FileWriter fw = new FileWriter(dest);
		BufferedWriter bw = new BufferedWriter(fw);

		hdfsSize = fileSystem.getFileStatus(src).getLen();
		pct = 0;
		msg = "Getting " + src.toString() + " to " + dest.getCanonicalPath() + " ... ";
		msg2 = String.format("%.1f", pct) + "%";
				
		System.out.print(msg);
        System.out.print(msg2);	
		
		fileSize=0;
		String line;
		line=br.readLine();
		while (line != null) {
			fileSize+=line.length()+1;
			pct = (double)fileSize/(double)hdfsSize*100;
			pct_txt = String.format("%.1f", pct) + "%";
			if ( !msg2.equals(pct_txt) ) {
				System.out.print(BACK.substring(0, msg2.length()));
				msg2 = pct_txt;
				System.out.print(msg2);
			}
			
			bw.write(line + "\n");
			line=br.readLine();
		}
		
		br.close();
		bw.close();

		System.out.print(BACK.substring(0, msg2.length()));
		
		System.out.print("done (");
		
		if ( fileSize < 1048576 ) {
			System.out.println(fileSize + " bytes)");
		} else {
			if ( fileSize < 1073741824 ) {
				System.out.println(String.format("%.1f",(double)fileSize/(1024*1024)) + " MB)");
			} else {
				System.out.println(String.format("%.1f",(double)fileSize/(1024*1024*1024)) + " GB)");
			}
		}
		
	}
}

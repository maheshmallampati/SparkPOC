/**
 * 
 */
package com.mcd.gdw.daas.driver;

/**
 * @author mc41946
 *
 */
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.util.DaaSConfig;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.SendMail;
// mc42833
// import com.mcd.gdw.test.daas.mapreduce.GenericMoveMapper;

public class MoveFilesInHDFSDriver extends Configured implements Tool {

	String selectSubTypes = "";
	String fileType = "";
	String configXmlFile = "";
	String inputPath = "";
	String[] args;
	String outputPath = "";
	FileSystem fileSystem;
	SendMail sendMail;
	
	String fromAddress = "";
	String toAddress = "";	
	String subject = "";
	String emailText    = "";

	public static void main(String[] args) throws Exception {

		int retval = ToolRunner.run(new Configuration(),
				new MoveFilesInHDFSDriver(), args);

		System.out.println(" return value : " + retval);
	}

	@Override
	public int run(String[] argsall) throws Exception {

		GenericOptionsParser gop = new GenericOptionsParser(argsall);

		args = gop.getRemainingArgs();

		for (int idx = 0; idx < args.length; idx++) {
			if (args[idx].equals("-c") && (idx + 1) < args.length) {
				configXmlFile = args[idx + 1];
			}

			if (args[idx].equals("-i") && (idx + 1) < args.length) {
				inputPath = args[idx + 1];
			}
			if (args[idx].equals("-o") && (idx + 1) < args.length) {
				outputPath = args[idx + 1];
			}
			if (args[idx].equals("-t") && (idx + 1) < args.length) {
				fileType = args[idx + 1];
			}
			if (args[idx].equals("-selectsubtypes") && (idx + 1) < args.length) {
				selectSubTypes = args[idx + 1];
			}
			if (args[idx].equals("-fromAddress") && (idx + 1) < args.length) {
				fromAddress = args[idx + 1];
			}
			if (args[idx].equals("-toAddress") && (idx + 1) < args.length) {
				toAddress = args[idx + 1];
			}
			if (args[idx].equals("-subject") && (idx + 1) < args.length) {
				subject = args[idx + 1];
			}
			if (args[idx].equals("-emailText") && (idx + 1) < args.length) {
				emailText = args[idx + 1];
			}
		}

		if (configXmlFile.length() == 0 || fileType.length() == 0
				|| inputPath.length() == 0 || outputPath.length() == 0 || fromAddress.length()==0 || toAddress.length()==0 || subject.length()==0 || emailText.length()==0) {
			System.err.println("Invalid parameters");
			System.err
					.println("Usage: MoveFilesToWorkDriver -c config.xml -i inputPath -o outputPath -t fileType -fromAddress fromAddress -toAddress toAddress -subject subject -emailText emailText");
			System.exit(8);
		}

		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		//AWS START
		//fileSystem = FileSystem.get(getConf());
		//AWS END

		if (daasConfig.configValid()) {
			if (daasConfig.displayMsgs()) {
				System.out.println(daasConfig.toString());
			}

			//AWS START
			fileSystem = HDFSUtil.getFileSystem(daasConfig, getConf());
			//AWS END
			
			sendMail = new SendMail(daasConfig);
			addValidSubTypeCodes(daasConfig, getConf(), inputPath, outputPath, fileSystem);
		} else {
			System.err.println("Invalid config.xml and/or filetype");
			System.err.println("Config File = " + configXmlFile);
			System.exit(8);
		}

		return (0);

	}

	private void addValidSubTypeCodes(DaaSConfig daasConfig, Configuration hdfsConfig,
			String step1CurrentOutputPath, String step1OutputPath,
			FileSystem hdfsFileSystem) {
		ArrayList<String> subTypeList;
		ABaC abac = null;
			try {
			if (selectSubTypes.length() > 0) {
				subTypeList = new ArrayList<String>();
				System.out.println("Select Sub Types:");
				String[] parts = selectSubTypes.split(",");
				for (String addSubType : parts) {
					System.out.println("   " + addSubType);
					subTypeList.add(addSubType);
				}
				subTypeList.add("FileStatus");
			} else {
				abac = new ABaC(daasConfig);
				subTypeList = abac.getSubFileTypeCodes();
				subTypeList.add("FileStatus");
				abac.dispose();
			}

			 //FileStatus[] fstus = hdfsFileSystem.listStatus(new Path(step1CurrentOutputPath));
			/*for (int idx=0; idx < fstus.length; idx++ ) {
				filePath = fstus[idx].getPath().getName();*/			
				
				for (int idxCode=0; idxCode < subTypeList.size(); idxCode++) {
					String subTypeCode=subTypeList.get(idxCode);
					moveFilesinHDFS(daasConfig, step1CurrentOutputPath, step1OutputPath, hdfsFileSystem, subTypeCode);
				}
			//}
		}
		 catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.err.println("Error occured in Moving Files, stopping");
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			System.exit(8);
		}
	}

	public void moveFilesinHDFS(DaaSConfig daasConfig,String step1CurrentOutputPath,
			String step1OutputPath, FileSystem hdfsFileSystem,
			final String subTypeCode) throws InterruptedException {
		
		try {
			//FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;
			HDFSUtil.createHdfsSubDirIfNecessary(hdfsFileSystem, new Path(
					step1OutputPath), daasConfig.displayMsgs());
			
			//Checking the files in Current Path
			fstatustmp = hdfsFileSystem.listStatus(new Path(
					step1CurrentOutputPath), new PathFilter() {
				@Override
				public boolean accept(Path pathname) {
					//AWS START
					//if (pathname.getName().contains(subTypeCode))
					if (pathname.getName().contains(subTypeCode) || pathname.getName().contains(HDFSUtil.replaceMultiOutSpecialChars(subTypeCode)))
					//AWS END	
						return true;
					return false;
				}
			});
			System.out.println(" num of output files for "+subTypeCode+" at "
					+ step1CurrentOutputPath.toString() + " "
					+ fstatustmp.length);
			String fileName;
			FsPermission fspermission = new FsPermission(FsAction.ALL,
					FsAction.ALL, FsAction.ALL);
			
			//Checking the files in existing Step1 Path
			FileStatus[] fstus = hdfsFileSystem.listStatus(new Path(step1OutputPath), new PathFilter() {
				@Override
				public boolean accept(Path pathname) {
					//AWS START
					//if (pathname.getName().contains(subTypeCode))
					if (pathname.getName().contains(subTypeCode) || pathname.getName().contains(HDFSUtil.replaceMultiOutSpecialChars(subTypeCode)))
					//AWS END	
						return true;
					return false;
				}
			});
			int subTypeFileCount=fstus.length;
				for (FileStatus fstat : fstatustmp) {
					fileName = fstat.getPath().getName();
					System.out.println(" fileName  " + fileName);
					
					String filename=fileName.replace(".gz", "");
					String[] fileContent=filename.split("r-");
					String newfileName=fileContent[0]+"r-"+String.format("%05d",(++subTypeFileCount))+".gz";					

					if (!hdfsFileSystem.rename(
							new Path(fstat.getPath().toString()), new Path(
									step1OutputPath + Path.SEPARATOR + newfileName))) {
						System.out.println("could not rename "
								+ fstat.getPath().toString() + " to "
								+ step1OutputPath + Path.SEPARATOR + newfileName);
					} else {
						hdfsFileSystem.setPermission(new Path(step1OutputPath
								+ Path.SEPARATOR + newfileName), fspermission);
						System.out.println(" renamed " + fstat.getPath().toString()
								+ " to " + step1OutputPath + Path.SEPARATOR
								+ newfileName);
					}
					
				}
		
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			sendEmailToSupport(fromAddress, toAddress, subject, emailText);
			ex.printStackTrace();
			throw new InterruptedException("Error occured in Moving Files:"+ex.toString());
		}
	}
	public void sendEmailToSupport(String fromAddress,String toAddress,String subject,String body)
	{
		sendMail.SendEmail(fromAddress, toAddress, subject, body);
	}
}

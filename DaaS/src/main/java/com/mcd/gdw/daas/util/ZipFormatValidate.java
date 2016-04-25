package com.mcd.gdw.daas.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

public class ZipFormatValidate {

	public enum ZipFormatStatus { FileMissing, ZipFormatInvalid, ZipFileOk }

	public class ZipFormatValidationResult {
		public String zipFileAndPath = "";
		public String zipFilePath = "";
		public ZipFormatStatus zipFileStatus = ZipFormatStatus.ZipFormatInvalid; 
	}
	
	public ZipFormatValidationResult validate(String filePath
			                                 ,String altFilePath
			                                 ,String fileName) {
		
		File zipSourceFile;
		ZipInputStream zipInStream;
		ZipEntry zipEntry;
		long subFileSize;
		long totalFileSize = 0;
		
		ZipFormatValidationResult returnResults = new ZipFormatValidationResult();
		
		try {
			returnResults.zipFileAndPath = filePath + File.separator + fileName;
			returnResults.zipFilePath = filePath;
			zipSourceFile = new File(returnResults.zipFileAndPath);
			
			if ( !zipSourceFile.isFile() ) {
				returnResults.zipFileAndPath = altFilePath + File.separator + fileName;
				returnResults.zipFilePath = altFilePath;
				zipSourceFile = new File(returnResults.zipFileAndPath);

				if ( !zipSourceFile.isFile() ) {
					returnResults.zipFileStatus = ZipFormatStatus.FileMissing;
				} else {
					returnResults.zipFileStatus = ZipFormatStatus.ZipFileOk;
				}
			} else {
				returnResults.zipFileStatus = ZipFormatStatus.ZipFileOk;
			}
							
			if ( returnResults.zipFileStatus == ZipFormatStatus.ZipFileOk  ) {
				zipInStream = new ZipInputStream(new FileInputStream(zipSourceFile));
				zipEntry = zipInStream.getNextEntry();		
				
				while( zipEntry != null ) {
			        subFileSize = zipEntry.getSize();
			        totalFileSize += subFileSize;
			        zipEntry = zipInStream.getNextEntry();
				}
				
				zipInStream.close();
				
				if ( totalFileSize > 0 ) {
					returnResults.zipFileStatus = ZipFormatStatus.ZipFileOk;
				} else {
					returnResults.zipFileStatus = ZipFormatStatus.ZipFormatInvalid;
				}
			}
			
		} catch (ZipException ex) {
			returnResults.zipFileStatus = ZipFormatStatus.ZipFormatInvalid;
		} catch (SecurityException ex) {
			returnResults.zipFileStatus = ZipFormatStatus.ZipFormatInvalid;
		} catch (IOException ex) {
			returnResults.zipFileStatus = ZipFormatStatus.ZipFormatInvalid;
		} catch (Exception ex) {
			returnResults.zipFileStatus = ZipFormatStatus.ZipFormatInvalid;
		}

		return(returnResults);
	}	
}
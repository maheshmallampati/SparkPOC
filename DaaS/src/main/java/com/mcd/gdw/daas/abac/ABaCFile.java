package com.mcd.gdw.daas.abac;

import java.io.File;
import java.util.Calendar;

public class ABaCFile {

	private String fileName;
	private String filePath;
	private Calendar fileTimestamp;
	private long fileLength;
	private String[] fileNameParts;
	private ABaCDataType dataType = null;
	
	public ABaCFile(File file
			       ,ABaCDataTypes dataTypes) {
		
		Calendar ts = Calendar.getInstance();
		ts.setTimeInMillis(file.lastModified());
		
		this.fileName = file.getName();
		this.filePath = file.getParent();
		this.fileTimestamp = ts;
		this.fileLength = file.length();
		this.fileNameParts = this.fileName.split("_|\\.");
		this.dataType = dataTypes.findDataType(this.fileNameParts);
		
	}
	
	public ABaCDataType getDataType() {
		
		return(dataType);
		
	}
	
	public String getFileName() {
		
		return(this.fileName);
		
	}
	
	public String getFilePath() {
		
		return(this.filePath);
		
	}
	
	public long getFileLength() {
		
		return(this.fileLength);
		
	}
	
	public java.sql.Timestamp getFileTimestamp() {

		java.sql.Timestamp retTimestamp = new java.sql.Timestamp(this.fileTimestamp.getTimeInMillis());
		
		return(retTimestamp);
		
	}
	
	public java.sql.Timestamp getMarketTimestamp() throws Exception {

		String marketTimestampText = getFileNamePart(this.numComponents()-1,"Market Timestamp");
		Calendar ts = Calendar.getInstance();

		//System.out.println("TS="+marketTimestampText);
		
		ts.set(Integer.parseInt(marketTimestampText.substring(0, 4))
			  ,Integer.parseInt(marketTimestampText.substring(4, 6)) - 1
			  ,Integer.parseInt(marketTimestampText.substring(6, 8))
			  ,Integer.parseInt(marketTimestampText.substring(8, 10))
			  ,Integer.parseInt(marketTimestampText.substring(10, 12))
			  ,Integer.parseInt(marketTimestampText.substring(12, 14)));
		
		java.sql.Timestamp tt = new java.sql.Timestamp(ts.getTimeInMillis());
		
		return(tt);
		
	}
	
	public int numComponents() {
		
		return(this.fileNameParts.length);
		
	}
	
	public String getApplicationType() throws Exception {
		
		return(this.getApplicationType(this.dataType.getApplicationTypeComponentCount()));
		
	}
	
	public String getApplicationType(int numComponents) throws Exception {

		StringBuffer applicationTypeText = new StringBuffer();
		
		if ( numComponents > 0 && numComponents <= this.fileNameParts.length ) {
			for ( int idx = 0; idx < numComponents; idx++ ) {
				if ( idx > 0 ) {
					applicationTypeText.append("_");
				}
				applicationTypeText.append(fileNameParts[idx]);
			}
		} else {
			throw new Exception("Application Type component length (" + numComponents + ") is invalid");
		}
		
		return(applicationTypeText.toString());
		
	}
	
	public String getRestaurantLocalIdentifer() throws Exception {
		
		return(getFileNamePart(this.dataType.getRestaurantComponentPosition(),"Restaurant Local"));
		
	}
	
	public String getCountryIsoAbbr2Code() throws Exception {
		
		return(getFileNamePart(this.dataType.getCountryCodeComponentPosition(),"County Code"));
		
	}
	
	public String getBusinessDate() throws Exception {
		
		return(getFileNamePart(this.dataType.getBusinessDateComponentPosition(),"Business Date"));
		
	}
	
	public java.sql.Date getBusinessDateAsDate() throws Exception {
		
		String dtText = getFileNamePart(this.dataType.getBusinessDateComponentPosition(),"Business Date");
		Calendar dt = Calendar.getInstance();
		dt.set(Integer.parseInt(dtText.substring(0, 4)), Integer.parseInt(dtText.substring(4, 6))-1, Integer.parseInt(dtText.substring(6, 8)));

		return(new java.sql.Date(dt.getTimeInMillis()));
		
	}
	
	private String getFileNamePart(int componentPosition
			                      ,String fieldName) throws Exception {
		
		if ( (componentPosition-1) >= 0 && (componentPosition-1) < this.fileNameParts.length ) {
			
		} else {
			throw new Exception(fieldName + " Position (" + componentPosition + ") is invalid");
		}
		
		return(this.fileNameParts[componentPosition-1]);
		
	}
}
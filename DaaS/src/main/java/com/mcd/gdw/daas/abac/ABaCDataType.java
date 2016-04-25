package com.mcd.gdw.daas.abac;

public class ABaCDataType {

	private String dataTypeDesc;
	private int dataTypeId;
	private String loadTableName;
	private int validateTypeCode;
	private int fileComponentCount;
	private int applicationTypeComponentCount;
	private int restaurantComponentPosition;
	private int countryCodeComponentPosition;
	private int businessDateComponentPosition;
	
	public ABaCDataType(String dataTypeDesc
	                   ,int dataTypeId
	                   ,String loadTableName
	                   ,int validateTypeCode
	                   ,int fileComponentCount
	                   ,int applicationTypeComponentCount
	                   ,int restaurantComponentPosition
	                   ,int countryCodeComponentPosition
	                   ,int businessDateComponentPosition) {
		
		this.dataTypeDesc = dataTypeDesc;
		this.dataTypeId = dataTypeId;
		this.loadTableName = loadTableName;
		this.validateTypeCode = validateTypeCode; 
		this.fileComponentCount = fileComponentCount;
		this.applicationTypeComponentCount = applicationTypeComponentCount;
		this.restaurantComponentPosition = restaurantComponentPosition;
		this.countryCodeComponentPosition = countryCodeComponentPosition;
		this.businessDateComponentPosition = businessDateComponentPosition;
		
	}

	public String getDataTypeDesc() {
		
		return(this.dataTypeDesc);
	}
	
	public int getDataTypeId() { 
		
		return(this.dataTypeId);
	
	}

	public String getLoadTableName() {
		
		return(loadTableName);
		
	}
	
	public int getValidateTypeCode() {
		
		return(validateTypeCode);
		
	}
	
	public int getFileComponentCount() {
	
		return(fileComponentCount);
		
	}
	
	public int getApplicationTypeComponentCount() {
		
		return(applicationTypeComponentCount);
		
	}
	
	public int getRestaurantComponentPosition() {
		
		return(restaurantComponentPosition);
		
	}
	
	public int getCountryCodeComponentPosition() {
		
		return(countryCodeComponentPosition);
		
	}
	
	public int getBusinessDateComponentPosition() { 
		
		return(businessDateComponentPosition);
		
	}
	
	public boolean fileIsDataType(ABaCFile file) {
		
		boolean isMatch = false;
		
		try {
			if ( file.numComponents() == this.fileComponentCount && file.numComponents() >= this.applicationTypeComponentCount ) {
				if ( this.dataTypeDesc.equals(file.getApplicationType(this.applicationTypeComponentCount)) ) {
					isMatch = true;
				}
			}
		} catch (Exception ex) {
			isMatch = false;
		}
		
		return(isMatch);
	}
	
	public boolean fileIsDataType(String[] fileComponents) {
		
		boolean isMatch = false;
		StringBuffer applicationType = new StringBuffer();
		
		try {
			if ( fileComponents.length == this.fileComponentCount && fileComponents.length >= this.applicationTypeComponentCount ) {
				applicationType.setLength(0);
				for (int idx=0; idx < this.applicationTypeComponentCount; idx++ ) {
					if (idx > 0 ) {
						applicationType.append("_");
					}
					applicationType.append(fileComponents[idx]);
				}
				
				if ( this.dataTypeDesc.equals(applicationType.toString()) ) {
					isMatch = true;
				}
			}
		} catch (Exception ex) {
			isMatch = false;
		}
		
		return(isMatch);
	}
}
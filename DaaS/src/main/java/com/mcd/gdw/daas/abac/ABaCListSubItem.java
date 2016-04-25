package com.mcd.gdw.daas.abac;

import java.io.Serializable;
import java.util.HashMap;

import com.mcd.gdw.daas.abac.ABaC.CodeType;

public class ABaCListSubItem implements Serializable {

	private static final long serialVersionUID = -4342413695812848788L;

	private int subFileDataTypeId;
	private String subFileDataTypeCd;
	private int statusTypeId;
	private int rejectReasonId;
	private String subFileName;
	private float fileSizeNum;
	private String xmlErrorMsg="";
    
    private HashMap<CodeType,Integer> typeCodes;
	
	public ABaCListSubItem(int subFileDataTypeId
			               ,String subFileDataTypeCd
			               ,int statusTypeId
			               ,String subFileName
			               ,float fileSizeNum
			               ,int rejectReasonId
			               ,HashMap<CodeType,Integer> typeCodes) {
		
		this.subFileDataTypeId = subFileDataTypeId;
		this.subFileDataTypeCd = subFileDataTypeCd; 
		this.statusTypeId = statusTypeId;
		this.subFileName = subFileName;
		this.fileSizeNum = fileSizeNum;
		this.rejectReasonId = rejectReasonId;
		this.typeCodes = typeCodes;
		
	}

	public ABaCListSubItem(int subFileDataTypeId
                           ,String subFileDataTypeCd
                           ,int statusTypeId
                           ,String subFileName
                           ,float fileSizeNum
                           ,HashMap<CodeType,Integer> typeCodes) {

		this(subFileDataTypeId,subFileDataTypeCd,statusTypeId,subFileName,fileSizeNum,0,typeCodes);
		
	}

	public ABaCListSubItem(int subFileDataTypeId
                           ,String subFileDataTypeCd
                           ,int statusTypeId
                           ,String subFileName
                           ,HashMap<CodeType,Integer> typeCodes) {

		this(subFileDataTypeId,subFileDataTypeCd,statusTypeId,subFileName,0,0,typeCodes);
		
	}

	public ABaCListSubItem(int subFileDataTypeId
                           ,String subFileDataTypeCd
                           ,int statusTypeId
                           ,HashMap<CodeType,Integer> typeCodes) {

		this(subFileDataTypeId,subFileDataTypeCd,statusTypeId,"",0,0,typeCodes);
		
	}

	public int getSubFileDataTypeId() {
		
		return(this.subFileDataTypeId);
	}

	public String getSubFileDataTypeCd() {
		
		return(this.subFileDataTypeCd);
		
	}

	public int getStatusTypeId() {
		
		return(this.statusTypeId);
		
	}

    public void setStatusTypeId(CodeType typeCode) throws Exception {
    	
    	if ( typeCodes.containsKey(typeCode) ) {
    		statusTypeId = typeCodes.get(typeCode);
    	} else {
    		throw new Exception("Type Code: " + typeCode + " not found");
    	}
    	
    }

	public String getSubFileName() {
	
		return(this.subFileName);
		
	}

	public void setSubFileName(String subFileName) {
	
		this.subFileName = subFileName;
		
	}

	public float getFileSizeNum() {
		
		 return this.fileSizeNum;
		
	}
	
	public void setFileSizeNum(float fileSizeNum) {
		
		 this.fileSizeNum  = fileSizeNum;
		
	}
	
    public int getRejectReasonId() {
    
    	return(this.rejectReasonId);
    	
    }

    public void setRejectReasonTypeId(CodeType typeCode) throws Exception {
    	
    	if ( typeCodes.containsKey(typeCode) ) {
    		rejectReasonId = typeCodes.get(typeCode);
    	} else {
    		throw new Exception("Type Code: " + typeCode + " not found");
    	}
    	
    }
    
    public void setXmlErrorMsg(String xmlErrorMsg) {
    
    	this.xmlErrorMsg = xmlErrorMsg;
    	
    }
    
    public String getXmlErrorMsg() {
    	
    	if ( this.xmlErrorMsg.length() > 200 ) {
    		return(this.xmlErrorMsg.substring(0,199));
    	} else {
    		return(this.xmlErrorMsg);
    	}
    	
    }

}

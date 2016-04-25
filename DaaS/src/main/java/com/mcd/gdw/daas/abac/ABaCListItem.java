package com.mcd.gdw.daas.abac;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC.CodeType;

public class ABaCListItem implements Iterable<Entry<String, ABaCListSubItem>>,Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3820363203940131152L;

    private int fileId;
    private String fileName;
    private String filePath;
    private String busnDt;
    private int terrCd;
    private String lgcyLclRfrDefCd;
    private String mcdGbalBusnLcat;
    private int statusTypeId; 
    private int rejectReasonId;
    private int fileSeparatorCharacter;
    private String obfuscateTypeCd;
    private String currentOwnershipType;
    private Timestamp fileProcessStartTimestamp;
    
    private HashMap<CodeType,Integer> typeCodes;

    private HashMap<String,ABaCListSubItem> subItems = new HashMap<String,ABaCListSubItem>();

    public ABaCListItem(int fileId
                        ,String fileName
                        ,String filePath
                        ,String busnDt
                        ,int terrCd
                        ,String lgcyLclRfrDefCd
                        ,BigDecimal mcdGbalBusnLcat
                        ,int statusTypeId
                        ,int rejectReasonId
                        ,HashMap<CodeType,Integer> typeCodes
                        ,char fileSeparatorCharacter
                        ,String obfuscateTypeCd
                        ,String currentOwnershipType) {

    	this.fileId = fileId;
    	this.fileName = fileName;
    	this.filePath = filePath;
    	this.busnDt = busnDt;
    	this.terrCd = terrCd;
    	this.lgcyLclRfrDefCd = lgcyLclRfrDefCd;
    	this.mcdGbalBusnLcat = mcdGbalBusnLcat.toString();
    	this.statusTypeId = statusTypeId;
    	this.rejectReasonId = rejectReasonId;
    	this.fileSeparatorCharacter = fileSeparatorCharacter;
    	this.typeCodes = typeCodes;
    	this.obfuscateTypeCd = obfuscateTypeCd;
    	this.currentOwnershipType = currentOwnershipType;

    }

    public int getFileId() {
    	
    	return(this.fileId);
    	
    }
    
    public String getFileName() {
    	
    	return(this.fileName);
    	
    }
    
    public String getFilePath() { 
    	
    	return(this.filePath);
    	
    }
    
    public String getBusnDt() {
    	
    	return(this.busnDt);
    	
    }
    
    public int getTerrCd() {
    	
    	return(this.terrCd);
    	
    }
    
    public String getLgcyLclRfrDefCd() {
    	
    	return(this.lgcyLclRfrDefCd);
    	
    }
    
    public BigDecimal getMcdGbalBusnLcat() {
    	
    	return(new BigDecimal(this.mcdGbalBusnLcat));
    	
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
    
    public int getRejectReasonId() {
    	
    	return(this.rejectReasonId);
    	
    }
    
    public void setRejectReasonId(CodeType reasonCode) throws Exception {
    	
    	if ( typeCodes.containsKey(reasonCode) ) {
    		this.rejectReasonId = typeCodes.get(reasonCode);
    	} else {
    		throw new Exception("Type Code: " + reasonCode + " not found");
    	}
    	
    }
	
    public String getFileSeparatorCharacter() {
    
    	return(String.valueOf((char)this.fileSeparatorCharacter));
    	
    }
    
    public String getObfuscateTypeCd() {
    	
    	return(this.obfuscateTypeCd);
    	
    }
    
    public String getCurrentOwnershipType() {
    	
    	return(this.currentOwnershipType);
    	
    }
    
    public String getFileNameHashKey() {
    	
    	return(getHashKey(this.fileName));
    	
    }
    
    public static String generateFileNameHashKey(String seedText) {
    	
    	return(getHashKey(seedText));
    	
    }
    
    private static String getHashKey(String seedText) {
    	
    	int pos = 0;
    
    	pos = (seedText+".").indexOf('.');
    	
    	return((seedText+".").substring(0, pos));
    	
    }
    
    public ABaCListSubItem addSubItem(int subFileDataTypeId
    		                          ,String subFileDataTypeCd
                                      ,int auditStatusTypeId
                                      ,String subFileName) {
    	
    	ABaCListSubItem subItem = new ABaCListSubItem(subFileDataTypeId, subFileDataTypeCd, auditStatusTypeId, subFileName,typeCodes);
    	subItems.put(subFileDataTypeCd, subItem);
    	return(subItem);
    	
    }
    
    public ABaCListSubItem addSubItem(int subFileDataTypeId
    		                          ,String subFileDataTypeCd
                                      ,int auditStatusTypeId) {
    	
    	ABaCListSubItem subItem = new ABaCListSubItem(subFileDataTypeId, subFileDataTypeCd, auditStatusTypeId, typeCodes);
    	subItems.put(subFileDataTypeCd, subItem);
    	return(subItem);
    	
    }
    
    public Iterator<Entry<String, ABaCListSubItem>> iterator() {
    	
    	Iterator<Entry<String, ABaCListSubItem>> itrSubItem = subItems.entrySet().iterator();
    	return(itrSubItem);
    	
    }
    
    public ABaCListSubItem getSubItem(String subFileDataTypeCd) throws Exception {
    	
    	if ( subItems.containsKey(subFileDataTypeCd) ) {
    		return(subItems.get(subFileDataTypeCd));	
    	} else {
    		throw new Exception("Sub File Data Type Code " + subFileDataTypeCd + " not found");
    	}
    	
    }

	public Timestamp getFileProcessStartTimestamp() {
		
		return fileProcessStartTimestamp;
		
//		try{
//			if(fileProcessStartTimestamp != null)
////				return new Timestamp(DaaSConstants.SDF_yyyyMMddHHmmssSSS.parse(fileProcessStartTimestamp).getTime());
//			else
//				return null;
////		return fileProcessStartTimestamp;
//		}catch(Exception ex){
//			ex.printStackTrace();
//		}
//		return null;
	}

	public void setFileProcessStartTimestamp(Timestamp fileProcessStartTimestamp) {
		this.fileProcessStartTimestamp = fileProcessStartTimestamp;
	}
    
    //public Iterator<ABaC2ListSubItem> iterator() {
    	
    //    Iterator<ABaC2ListSubItem> itrSubItem = subItemList.iterator();
    //    return(itrSubItem);
        
    //}
    

}

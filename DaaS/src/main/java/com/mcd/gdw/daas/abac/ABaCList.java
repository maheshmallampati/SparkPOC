package com.mcd.gdw.daas.abac;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.mcd.gdw.daas.abac.ABaC.CodeType;

public class ABaCList implements Iterable<Entry<String, ABaCListItem>>,Serializable {

	private static final long serialVersionUID = -2519848403646401364L;

	private Map<String,ABaCListItem> itemList = new HashMap<String,ABaCListItem>();
 
	private char fileSeparatorCharacter;
	
	public ABaCList(char fileSeparatorCharacter) {
	
		this.fileSeparatorCharacter = fileSeparatorCharacter;
		
	}
	
	public ABaCListItem addItem(ABaCListItem itm) {
		
		itemList.put(itm.getFileNameHashKey(), itm);
		return(itm);
		
	}
	
	public ABaCListItem addItem(int fileId
                                ,String fileName
                                ,String filePath
                                ,String busnDt
                                ,int terrCd
                                ,String lgcyLclRfrDefCd
                                ,BigDecimal mcdGbalBusnLcat
                                ,int statusTypeId
                                ,int reasonCode
                                ,String obfuscateTypeCd
                                ,String currentOwnershipType
                                ,HashMap<CodeType,Integer> typeCodes) {
		
		ABaCListItem itm = new ABaCListItem(fileId
                ,fileName
                ,filePath
                ,busnDt
                ,terrCd
                ,lgcyLclRfrDefCd
                ,mcdGbalBusnLcat
                ,statusTypeId
                ,reasonCode
                ,typeCodes
                ,fileSeparatorCharacter
                ,obfuscateTypeCd
                ,currentOwnershipType);
		
		itemList.put(itm.getFileNameHashKey(), itm);
		
		return(itm);
		
	}
	
    public Iterator<Entry<String, ABaCListItem>> iterator() {
    	
    	Iterator<Entry<String, ABaCListItem>> itrItem = itemList.entrySet().iterator();
    	return(itrItem);
    	
    }    
    
    public void clear() {
    	itemList.clear();
    }
    
    public ABaCListItem getItem(String fileHashKey) throws Exception {
    	
    	if ( itemList.containsKey(fileHashKey) ) {
    		return(itemList.get(fileHashKey));	
    	} else {
    		throw new Exception("File Hash Key: " + fileHashKey + " Not found");
    	}
    	
    }	
}

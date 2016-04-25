package com.mcd.gdw.daas.abac;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class ABaCDataTypes implements Iterable<Entry<String, ABaCDataType>> {

	private Map<String,ABaCDataType> dataTypes = new HashMap<String,ABaCDataType>();

	public ABaCDataType addDataType(ABaCDataType ABaCDataType) {
		
		dataTypes.put(ABaCDataType.getDataTypeDesc(), ABaCDataType);
		
		return(ABaCDataType);
	}
	
	public ABaCDataType getDataType(String dataTypeDesc) {
		
		return(dataTypes.get(dataTypeDesc));
		
	}
	
	@Override
	public Iterator<Entry<String, ABaCDataType>> iterator() {

		return(dataTypes.entrySet().iterator());
		
	}
	
	public void clear() {
		
		dataTypes.clear();
		
	}
	
	public ABaCDataType findDataType(ABaCFile fileItem) {
		
		ABaCDataType retType = null;

		Iterator<Entry<String, ABaCDataType>> iterator = dataTypes.entrySet().iterator();

		while ( iterator.hasNext() && retType == null ) {
			Map.Entry<String,ABaCDataType> mapEntry = (Map.Entry<String,ABaCDataType>) iterator.next();
			if( mapEntry.getValue().fileIsDataType(fileItem) ){
				retType = (ABaCDataType)mapEntry.getValue();
			}
		}
		
		return(retType);
	}
	
	public ABaCDataType findDataType(String[] fileComponents) {
		
		ABaCDataType retType = null;

		Iterator<Entry<String, ABaCDataType>> iterator = dataTypes.entrySet().iterator();

		while ( iterator.hasNext() && retType == null ) {
			Map.Entry<String,ABaCDataType> mapEntry = (Map.Entry<String,ABaCDataType>) iterator.next();
			if( mapEntry.getValue().fileIsDataType(fileComponents) ){
				retType = (ABaCDataType)mapEntry.getValue();
			}
		}
		
		return(retType);
	}
}
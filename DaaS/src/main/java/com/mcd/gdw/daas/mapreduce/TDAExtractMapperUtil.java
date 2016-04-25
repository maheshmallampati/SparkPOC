package com.mcd.gdw.daas.mapreduce;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TDAExtractMapperUtil {
	
	 Integer MAX_POS_TYPECODE_VALUE    = -1;
	 Integer MAX_PYMT_TYPECODE_VALUE   = -1;
	 Integer MAX_DLVR_TYPECODE_VALUE   = -1;
	 Integer MAX_SPLTRX_TYPECODE_VALUE = -1;

	 public String handleMissingCode(Context thiscontext,HashMap<String, String>  typeCdHashMap,
			 String mapKeyPart,String codeKey,String gdwTerrCd,
			 MultipleOutputs<Text, Text> mos,
			 String newcodesFileName){
		  
		  
		  String mapKey    = gdwTerrCd+"-"+mapKeyPart.trim().toUpperCase()+"-"+codeKey.trim().toUpperCase();
		  
		  String codeValue  = "Missing-"+mapKey;
		  String counterKey = "AddedNew"+mapKeyPart+"TypeCd";
		  Integer MAX_VALUE = -1;
		 
			  
		  try{
		  
			  long counterValue = thiscontext.getCounter("Count",counterKey).getValue();
		
//			  if(MAX_POS_TYPECODE_VALUE.intValue() == -1) //lookup MAX Value only once per mapper
//				  MAX_POS_TYPECODE_VALUE = new Integer(getMaxPOSTypeCd(tdacodesLookupMap,gdwTerrCd+"-"+mapKeyPart+"-"));

			   if(counterValue <= 0){
		  		
				    MAX_VALUE = new Integer(getMaxCdVal(typeCdHashMap,gdwTerrCd+"-"+mapKeyPart+"-"));
		  			codeValue = ""+(MAX_VALUE.intValue()+counterValue+1);
			  		
		  		  if("POS".equalsIgnoreCase(mapKeyPart)){
		  			MAX_POS_TYPECODE_VALUE = new Integer(MAX_VALUE);
		  		  }else  if("DLVR".equalsIgnoreCase(mapKeyPart)){
		  			MAX_DLVR_TYPECODE_VALUE = new Integer(MAX_VALUE);
		  		  }else  if("PYMT".equalsIgnoreCase(mapKeyPart)){
		  			MAX_PYMT_TYPECODE_VALUE = new Integer(MAX_VALUE);
		  		  }else if("SPLTRX".equalsIgnoreCase(mapKeyPart)) {
		  			MAX_SPLTRX_TYPECODE_VALUE = new Integer(MAX_VALUE);
		  		  }
		  			
		  			
		  			thiscontext.getCounter("Count",counterKey).increment(1);
			  		
			  		mos.write(newcodesFileName, NullWritable.get(), new Text(mapKey+"|"+codeValue));
		  		}else{
		  			 if("POS".equalsIgnoreCase(mapKeyPart)){
		  				  MAX_VALUE = MAX_POS_TYPECODE_VALUE;
		  			  }else  if("DLVR".equalsIgnoreCase(mapKeyPart)){
		  				  MAX_VALUE = MAX_DLVR_TYPECODE_VALUE;
		  			  }else  if("PYMT".equalsIgnoreCase(mapKeyPart)){
		  				  MAX_VALUE = MAX_PYMT_TYPECODE_VALUE;
		  			  }else if("SPLTRX".equalsIgnoreCase(mapKeyPart)) {
		  				  MAX_VALUE = MAX_SPLTRX_TYPECODE_VALUE;
		  			  }
		  			 
//		  			codeValue = ""+(MAX_VALUE.intValue()+counterValue);
		  			codeValue = ""+(MAX_VALUE.intValue()+1);
		  		}
		  	
		  		thiscontext.getCounter("Count","Missing-"+mapKey).increment(1);
		  
		  }catch(Exception ex){
			  ex.printStackTrace();
		  }
		  return codeValue;
	  }
	  
	  public int getMaxCdVal( HashMap<String, String>  typeCdHashMap,String keyPattern){
		  
		  Set<String> typeCdHashMapKeySet = typeCdHashMap.keySet();
		  Iterator<String> it = typeCdHashMapKeySet.iterator();
		  
		  String key;
		 int MAX_VAL = -1;
		  int VAL = -1;
		  while(it.hasNext()){
			  key = it.next();
			  if(key.startsWith(keyPattern) && !key.equalsIgnoreCase(keyPattern+"NULL")){
				  VAL = Integer.parseInt(typeCdHashMap.get(key));
//				  System.out.println( " val  " +  VAL + " MAX_VAL " + MAX_VAL);
				  if(VAL > MAX_VAL)
					  MAX_VAL = VAL;
				  
			  }
		  }
//		  System.out.println( " returning MAX_VAL "+ MAX_VAL);
		  return MAX_VAL;
	  }
}

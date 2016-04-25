package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USTLDReformatReducer extends Reducer<Text, Text, NullWritable, Text> {

	private final static String SEPARATOR_CHARACTER            = "\t";
	private final static String COMPONENT_SPEARATOR_CHARACTER  = ":";
	private final static String COMPONENTS_SPEARATOR_CHARACTER = "~";
	@SuppressWarnings("unused")
	private final static String REC_TRN                        = "TRN";
	private final static String REC_PRD                        = "PRD";

	private HashMap<String,String> componentMap = new HashMap<String,String>();
	private List<String> valueList = new ArrayList<String>();
	
	private HashMap<String,String> compontentSubMap = new HashMap<String,String>();
	
	private String keyText;
	private String valueText;
	private String[] parts;
	private String[] parts2;
	private String[] parts3;
	private String[] partsHold;
	private int partsHoldQty = 0;
	private int seqId = 0;
	
	private String posOrdKey;
	
	private String parentSldMnuItm;
			
	private StringBuffer valueOut = new StringBuffer();
	private Text valueTextOut = new Text();
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		keyText = key.toString();
			
		componentMap.clear();
		valueList.clear();
		
		for ( Text value : values ) {
			valueText = value.toString();
			if ( valueText.startsWith(REC_PRD) ) {
				parts = valueText.split(SEPARATOR_CHARACTER);

				componentMap.put(parts[1], parts[2]);

			} else {
				valueList.add(valueText);
			}
		}
		
		Collections.sort(valueList);

		posOrdKey = "@";
		seqId = 0; 
		parentSldMnuItm = "";
		compontentSubMap.clear();
		
		for ( int idx=0; idx < valueList.size(); idx++ ) {
			parts = valueList.get(idx).split(SEPARATOR_CHARACTER);

			if ( parts[11].equals("0") && compontentSubMap.size() > 0 ) {
				for (Map.Entry<String, String> compontentSubMapEntry : compontentSubMap.entrySet())  {
					seqId++;
					partsHold[2] = String.valueOf(seqId);
					partsHold[12] = compontentSubMapEntry.getKey();
					partsHold[16] = String.valueOf(partsHoldQty * Integer.parseInt(compontentSubMapEntry.getValue()));
					
					writeLine(partsHold,context);
				}
			}

			if ( !parts[1].equals(posOrdKey) ) {
				posOrdKey = parts[1];
				seqId = 0;
			}

			if ( parts[14].equals("VALUE_MEAL") ) { 
				
				partsHold = valueList.get(idx).split(SEPARATOR_CHARACTER);
				partsHold[11] = "1";
				partsHold[13] = partsHold[12];
				partsHold[14] = "RECIPE";
				partsHold[15] = "AUTO";
				partsHold[19] = "0.00";
				partsHold[20] = "0.00";
				partsHold[21] = "0.00";
				partsHold[22] = "0.00";
				partsHold[23] = "0.00";
				partsHold[24] = "0.00";
				partsHoldQty = Integer.parseInt(partsHold[16]);
				
				compontentSubMap.clear();
				parentSldMnuItm = parts[12];

				if ( componentMap.containsKey(parts[12]) ) {
					parts2 = componentMap.get(parts[12]).split(COMPONENTS_SPEARATOR_CHARACTER);
					
					for ( int componentIdx=0; componentIdx < parts2.length; componentIdx++ ) {
						parts3 = parts2[componentIdx].split(COMPONENT_SPEARATOR_CHARACTER);
						
						compontentSubMap.put(parts3[0], parts3[1]);
					}
				}
			}
			
			if ( parts[11].equals("0") && !parts[14].equals("VALUE_MEAL") ) {
				compontentSubMap.clear();
				parentSldMnuItm = "";
			}
			
			if ( compontentSubMap.size() > 0 && parts[13].equals(parentSldMnuItm) ) {
				if ( compontentSubMap.containsKey(parts[12]) ) {
					compontentSubMap.remove(parts[12]);
				}
			}

			seqId++;
			parts[2] = String.valueOf(seqId);
			writeLine(parts,context);
		}
		
		if ( compontentSubMap.size() > 0 ) {
			for (Map.Entry<String, String> compontentSubMapEntry : compontentSubMap.entrySet())  {
				seqId++;
				partsHold[2] = String.valueOf(seqId);
				partsHold[2] = "END";
				partsHold[12] = compontentSubMapEntry.getKey();
				partsHold[16] = String.valueOf(partsHoldQty * Integer.parseInt(compontentSubMapEntry.getValue()));
				
				writeLine(partsHold,context);
			}
		}
	}
	
	private void writeLine(String[] lineParts
			              ,Context context) throws IOException, InterruptedException {
		
		valueOut.setLength(0);
		valueOut.append(keyText.substring(0, 3));
		valueOut.append(SEPARATOR_CHARACTER);
		valueOut.append(keyText.substring(3, 13));
		valueOut.append(SEPARATOR_CHARACTER);
		valueOut.append(keyText.substring(13));
			
		for ( int itm=1; itm < lineParts.length; itm++ ) {
			valueOut.append(SEPARATOR_CHARACTER);
			valueOut.append(lineParts[itm]);
		}

		valueTextOut.clear();
		valueTextOut.set(valueOut.toString());
		
		context.write(NullWritable.get(), valueTextOut);
	}
}

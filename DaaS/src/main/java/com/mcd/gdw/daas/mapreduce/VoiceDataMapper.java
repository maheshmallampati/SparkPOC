package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.GenerateVoiceData;

public class VoiceDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private final static String PIPE_DELIMITER  	= "|";
	private final static String REC_HEADER 			= "HDR";
	private final static String REC_DETAIL 			= "DTL";
	
	private String[] parts;
	private Text mapKey = new Text();
	private Text mapValue = new Text();
	
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	
	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputValue = new StringBuffer();
	
	private HashMap<String,String> questionMap = new HashMap<String,String>();
	private HashMap<String,String> questionTextMap = new HashMap<String,String>();
	
	private String calDt;
	private String voiceTerrCd;
	private String voiceAmountSpent;
	
	private String voiceHdrResponseId;
	private String voiceHdrAmountSpent;
	private String voiceHdrStoreId;
	private String voiceHdrVisitDateTime;
	private String voiceHdrVisitDate;
	private String voiceHdrVisitTime;
	private String voiceHdrVisitAmPm;
	
	private String voiceDtlResponseId;
	private String voiceDtlQuestionId;
	private String voiceDtlAnswerId;
	private String voiceDtlComment;
	
	private String splitFileName;
	private String findKey;
	private boolean skipQuestion;
	private boolean includeFile;
	private boolean isStandardQuestion;
	private boolean isFreeTextQuestion;
	
	@Override
	public void setup(Context context) {

		Path path;
		
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;
		
		try {
	    	path = ((FileSplit) context.getInputSplit()).getPath();
	    	splitFileName = path.getName();
	    	
	    	parts = splitFileName.split("~");
	    		voiceTerrCd = parts[1];
	    		calDt 		= parts[2];
	    		
	    	docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
				
			distPaths = context.getCacheFiles();
			    
			if (distPaths == null){
			   	System.err.println("distpath is null");
			   	System.exit(8);
			}
			      
			if ( distPaths != null && distPaths.length > 0 )  {
			    	  
			   	System.out.println(" number of distcache files : " + distPaths.length);
			    	  
			   	for ( int i=0; i<distPaths.length; i++ ) {
			   		
			   		System.out.println("distpaths:" + distPaths[i].toString());
				    	  
			    	distPathParts = distPaths[i].toString().split("#");
				    	  
			    	if ( distPaths[i].toString().contains("VOICE_DATA_QUESTION.txt") ) {
				    		  		      	  
			    		br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	addQuestionsKeyValuestoMap(br);
				      	System.out.println("Loaded Voice Data - Questions Values Map");
				      	
				    } else if ( distPaths[i].toString().contains("VOICE_DATA_QUESTION_TEXT.txt") ) {
				    	
				    	br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	addQuestionsTextKeyValuestoMap(br);
				      	System.out.println("Loaded Voice Data - Free Form Text Questions Values Map");
			        }
			   	}
			}
	    	
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void addQuestionsKeyValuestoMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String questionKey;
		String terrCd;
		String questionId;
		
		questionMap.clear();
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\|", -1);

					terrCd      = parts[0];
					questionId	= parts[1];
					
					questionKey = terrCd + PIPE_DELIMITER + questionId;
					questionMap.put(questionKey, questionId);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(8);
		} finally {
			try {
				if (br != null)
					br.close();
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}
	
	private void addQuestionsTextKeyValuestoMap(BufferedReader br) {
		
		String line = null;
		String[] parts;
		
		String questionTextKey;
		String terrCd;
		String questionId;
		
		questionTextMap.clear();
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\|", -1);

					terrCd      = parts[0];
					questionId	= parts[1];
					
					questionTextKey = terrCd + PIPE_DELIMITER + questionId;
					questionTextMap.put(questionTextKey, questionId);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(8);
		} finally {
			try {
				if (br != null)
					br.close();
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		parts = value.toString().split("\\|", -1);
		
		if ( splitFileName.toUpperCase().contains("HEADER") && parts.length >= 35 ) {
			getVoiceHeaderData(context);
		} else if ( splitFileName.toUpperCase().contains("DETAIL") && parts.length >= 3 ) {
			getVoiceDetailData(context);
		}
	}
	
	private void getVoiceHeaderData(Context context) {
		
		try {
			if ( parts.length >=35 ) {
			
				voiceHdrResponseId = parts[0];
				voiceHdrAmountSpent = parts[1];
				voiceHdrStoreId = parts[2];
				voiceHdrVisitDate = timestampFromSMGFormat(parts[34]);
				
				try {
					String[] tmpParts = ("0" + voiceHdrAmountSpent.trim() + ".00").split("\\.");
					String dollars = String.valueOf(Integer.parseInt(tmpParts[0]));
					String cents = String.format("%02d",(Integer.parseInt(tmpParts[1])));
	
					voiceAmountSpent = dollars + "." + cents;
				} catch (Exception ex) {
					voiceAmountSpent = "0.00";
				}
					
				outputKey.setLength(0);
				outputKey.append(voiceHdrResponseId);
					
				mapKey.clear();
				mapKey.set(outputKey.toString());
					
				outputValue.setLength(0);
				outputValue.append(REC_HEADER);
				outputValue.append(PIPE_DELIMITER);
				outputValue.append(voiceTerrCd);
				outputValue.append(PIPE_DELIMITER);
				outputValue.append(voiceHdrStoreId);
				outputValue.append(PIPE_DELIMITER);
				outputValue.append(voiceHdrResponseId);
				outputValue.append(PIPE_DELIMITER);
				outputValue.append(voiceHdrVisitDate);
				outputValue.append(PIPE_DELIMITER);
				outputValue.append(voiceHdrVisitDateTime);
				outputValue.append(PIPE_DELIMITER);
				outputValue.append("POS");
				outputValue.append(PIPE_DELIMITER);
				outputValue.append("SMG");
				outputValue.append(PIPE_DELIMITER);
				outputValue.append(voiceAmountSpent);
		
				mapValue.clear();
				mapValue.set(outputValue.toString());
		
				context.write(mapKey, mapValue);	
				context.getCounter("DaaS","VOICE HEADER").increment(1);
			} else {
				context.getCounter("DaaS","VOICE HEADER SKIPPED").increment(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void getVoiceDetailData(Context context) {
		
		try {
		
			if ( parts.length >=3 ) {
				
				voiceDtlResponseId = parts[0];
				voiceDtlQuestionId = parts[1];
				voiceDtlAnswerId = parts[2];
				
				skipQuestion = false;
				includeFile = false;
				isStandardQuestion = false;
				isFreeTextQuestion = false;
		    		
	    		if (voiceDtlAnswerId != null && !voiceDtlAnswerId.isEmpty()) {
	    			skipQuestion = false;
				} else {
					skipQuestion = true;
				}
    		
	    		if (!skipQuestion) {
	    			
	    			findKey = voiceTerrCd + PIPE_DELIMITER + voiceDtlQuestionId;
	    			
	    			if ( questionMap.containsKey(findKey) ) {
	    				
	    				includeFile = true;
	    				isStandardQuestion = true;
	    			}
	    			
	    			if ( questionTextMap.containsKey(findKey) ) {
	    				System.out.println("Free Text Question: " + voiceDtlQuestionId);
	    				includeFile = true;
	    				isFreeTextQuestion = true;
	    			}
	    			
	    			if (includeFile && isStandardQuestion) {
		    				
	    				voiceDtlComment = "";
				
						outputKey.setLength(0);
						outputKey.append(voiceDtlResponseId);
							
						mapKey.clear();
						mapKey.set(outputKey.toString());
												
						outputValue.setLength(0);
						outputValue.append(REC_DETAIL);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceTerrCd);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceDtlResponseId);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceDtlQuestionId);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceDtlAnswerId);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceDtlComment);
			
						mapValue.clear();
						mapValue.set(outputValue.toString());
			
						context.write(mapKey, mapValue);	
						context.getCounter("DaaS","VOICE DETAIL").increment(1);
	    			}
	    			
	    			if (includeFile && isFreeTextQuestion) {
		    			
	    				voiceDtlComment = voiceDtlAnswerId;
		    			voiceDtlAnswerId = "9";

						outputKey.setLength(0);
						outputKey.append(voiceDtlResponseId);
							
						mapKey.clear();
						mapKey.set(outputKey.toString());
												
						outputValue.setLength(0);
						outputValue.append(REC_DETAIL);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceTerrCd);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceDtlResponseId);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceDtlQuestionId);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceDtlAnswerId);
						outputValue.append(PIPE_DELIMITER);
						outputValue.append(voiceDtlComment);
			
						mapValue.clear();
						mapValue.set(outputValue.toString());
			
						context.write(mapKey, mapValue);	
						context.getCounter("DaaS","VOICE DETAIL").increment(1);
		    		}
				}
			} else {
				context.getCounter("DaaS","VOICE DETAIL SKIPPED").increment(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private String timestampFromSMGFormat(String timestampIn) {

		String retVisitDate = "";
		String tempTotalTime = "";
		
		voiceHdrVisitDate = "";
		voiceHdrVisitTime = "";
		voiceHdrVisitAmPm = "";
		voiceHdrVisitDateTime = "";
		
		try {
			parts = timestampIn.split(" ", -1);
			
			if ( parts.length >= 3 ) {
				voiceHdrVisitDate = parts[0];
				voiceHdrVisitTime = parts[1];
				voiceHdrVisitAmPm = parts[2];
				
				voiceHdrVisitDate = formatDateFromSMGFormat(voiceHdrVisitDate);
				
				tempTotalTime = voiceHdrVisitTime + " " + voiceHdrVisitAmPm;
				voiceHdrVisitDateTime = formatTimeFromSMGFormat(tempTotalTime);
			}
			
			retVisitDate = voiceHdrVisitDate;
			
		} catch (Exception ex ) {
			retVisitDate = "04151955";
		}
		
		return(retVisitDate);
	}
	
	private String formatDateFromSMGFormat(String dateIn) {

		String retDate = "";
		String month = "";
		String day = "";
		String year = "";
		
		try {
		
			parts = dateIn.split("/", -1);
			month = parts[0];
			day = parts[1];
			year = parts[2];
				
			int numMonth = Integer.parseInt(month);
			int numday = Integer.parseInt(day);
				
			if (numday < 10) {
				day = "0" + numday;
			} else {
				day = String.valueOf(numday);
			}
			
			if (numMonth < 10) {
				month = "0" + numMonth;
			} else {
				month = String.valueOf(numMonth);
			}
			
			retDate = year + month + day;
		
		} catch (Exception ex ) {
			retDate = "04151955";
		}

		return(retDate);
	}
	
	private String formatTimeFromSMGFormat(String timeIn) {
	    
		DateFormat f1 = new SimpleDateFormat("hh:mm:ss a");
	    Date d = null;
	    
	    try {
	        d = f1.parse(timeIn);
	    } catch (ParseException e) {
	        e.printStackTrace();
	    }
	    
	    DateFormat f2 = new SimpleDateFormat("HHmm");
	    String x = f2.format(d);

	    return x;
	}
}

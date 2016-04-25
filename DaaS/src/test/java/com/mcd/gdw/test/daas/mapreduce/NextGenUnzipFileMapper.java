package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import com.mcd.gdw.test.daas.driver.NextGenUnzipFile;

public class NextGenUnzipFileMapper extends Mapper<Text, BytesWritable, Text, Text> {

	private boolean isValid;

	private String terrCd;
	private String lgcyLclRfrDefCd;
	private String posBusnDt;
	private String mcdGbalLcatIdNu;
	private String owshTypCd;
	private String fileId;
	private String partId;

	private String fileType;
	
	private String ctryIso2ToTerrCd;
	private String validTypes;
	
    private boolean fileEntryFound;

	private String[] parts;
	
	private String zipFileName;
	
	private String xmlFileName;
	private StringBuffer valueText = new StringBuffer(5242880);
	private StringBuffer newFields = new StringBuffer(150);
	
	private int charIdx;

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	@SuppressWarnings("unused")
	private Document doc = null;
	private StringReader strReader = null;
	
	private Text outKey = new Text();
	private Text outValue = new Text();

	@Override
	public void setup(Context context) {
		
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;
	    String line;
	    Path path;

	    ctryIso2ToTerrCd = context.getConfiguration().get(NextGenUnzipFile.CONFIG_SETTING_CTRY_ISO2_LOOKUP);
	    //ctryIso2ToTerrCd = "AF4  AL8  AQ10 DZ12 AS16 AD20 AO24 AG28 AZ31 AR32 AU36 AT40 BS44 BH48 BD50 AM51 BB52 BE56 BM60 BT64 BO68 BA70 BW72 BV74 BR76 BZ84 IO86 SB90 VG92 BN96 BG100MM104BI108BY112KH116CM120CA124CV132KY136CF140LK144TD148CL152CN156TW158CX162CC166CO170KM174YT175CG178CD180CK184CR188HR191CU192CY196CZ203BJ204DK208DM212DO214EC218SV222GQ226ET231ER232EE233FO234FK238GS239FJ242FI246FR250GF254PF258TF260DJ262GA266GE268GM270PS275DE276GH288GI292KI296GR300GL304GD308GP312GU316GT320GN324GY328HT332HM334VA336HN340HK344HU348IS352IN356ID360IR364IQ368IE372IL376IT380CI384JM388JP392KZ398JO400KE404KP408KR410KW414KG417LA418LB422LS426LV428LR430LY434LI438LT440LU442MO446MG450MW454MY458MV462ML466MT470MQ474MR478MU480MX484MC492MN496MD498MS500MA504MZ508OM512NA516NR520NP524NL528AN530CW531AW533NC540VU548NZ554NI558NE562NG566NU570NF574NO578MP580UM581FM583MH584PW585PK586PA591PG598PY600PE604PH608PN612PL616PT620GW624TL626PR630QA634RE638RO642RU643RW646SH654KN659AI660LC662MF663PM666VC670SM674ST678SA682SN686RS688SC690SL694SG702SK703VN704SI705SO706ZA710ZW716ES724EH732SD736SR740SJ744SZ748SE752CH756SY760TJ762TH764TG768TK772TO776TT780AE784TN788TR792TM795TC796TV798UG800UA804MK807EG818GB826TZ834US840VI850BF854UY858UZ860VE862WF876WS882YE887CS891ZM894";
	    validTypes = context.getConfiguration().get(NextGenUnzipFile.CONFIG_SETTING_VALID_TYPES);
	    //validTypes = "STLD|DetailedSOS|MenuItem|SecurityData|Store-Db|Product-Db";
	    
	    try {
	    	
	    	path = ((FileSplit) context.getInputSplit()).getPath();
	    	zipFileName = path.getName();
	    	
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
		    	  
	    			if( distPaths[i].toString().contains("abac.txt") ) {
	    				
	    				br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
	    				
	    				fileEntryFound = false;

	    				terrCd = "";
						lgcyLclRfrDefCd = "";
						posBusnDt = "";
						mcdGbalLcatIdNu = "";
						owshTypCd = "";
						fileId = "";
						partId = "0";
	    				
	    				while ( (line=br.readLine()) != null && !fileEntryFound ) {
	    					//System.out.println(zipFileName + "\n*************\n" + line + "\n***************");
	    					
	    					parts = line.split("\t");

	    					//System.out.println("parts[0]   ='" + parts[0] + "' " + parts[0].length());
	    					//System.out.println("zipFileName='" + zipFileName + "' " + zipFileName.length());
	    					
	    					//if ( parts[0].startsWith(zipFileName) ) {
	    					//	System.out.println("parts[0]   = zipFileName"); 
	    					//} else {
	    					//	System.out.println("parts[0]   != zipFileName");
	    					//}
	    					
	    					if ( parts[0].equals(zipFileName) ) {
	    						fileEntryFound = true;
	    						terrCd = parts[1];
	    						lgcyLclRfrDefCd = parts[2];
	    						posBusnDt = parts[3].substring(0, 8);
	    						mcdGbalLcatIdNu = parts[4];
	    						owshTypCd = parts[5];
	    						fileId = parts[6];
	    						partId = parts[7];
	    						
	    						//System.err.println("parts[7]=" + partId);
	    					}
	    				}
	    				
	    				br.close();
	    			}
	    		}
	    	}
		
	    } catch (Exception ex) {
	    	ex.printStackTrace(System.err);
	    	System.exit(8);
	    }

	}
	
	
	@Override
	public void map(Text key
			       ,BytesWritable value
			       ,Context context) throws IOException, InterruptedException {

		isValid = false;
				
		try {
			xmlFileName = key.toString();
			
			valueText.setLength(0);
			valueText.append(new String(value.getBytes(),"UTF-8"));
			
			processFile(xmlFileName,context);
			
		} catch (Exception ex1) {
			xmlFileName = "unknown";
			
			outKey.clear();
			outKey.set("INVALID");
			
			outValue.clear();
			outValue.set(zipFileName + "|" + xmlFileName);
			
			context.write(outKey, outValue);
		}


	}
	
	private void processFile(String xmlFileName
			                ,Context context) {
		
		try {
			parts = (xmlFileName+"_@@@").split("_");

			fileType = parts[1];
			
			if ( validTypes.contains(fileType) && parts.length >= 5 && parts[4].length() >= 8 ) {
				if ( !fileEntryFound ) {
	
					if ( ctryIso2ToTerrCd.indexOf(parts[1]) > 0 ) {
						terrCd = ctryIso2ToTerrCd.substring(ctryIso2ToTerrCd.indexOf(parts[2])+2, ctryIso2ToTerrCd.indexOf(parts[2])+5);
					}
					
					lgcyLclRfrDefCd = parts[3];
					posBusnDt = parts[4].substring(0, 8);
					partId = "0";
					
				}
				
				if ( valueText.length() > 5 && valueText.charAt(valueText.length()-1) == '#' ) {
					valueText.setLength(valueText.length()-5);
				}
		
				if ( valueText.length() > 0 && valueText.charAt(0) != '<' ) {
					valueText.deleteCharAt(0);
					while ( valueText.length() > 0 && valueText.charAt(0) != '<' ) {
						valueText.deleteCharAt(0);
					}
				}
		
				try {
					strReader  = new StringReader(valueText.toString());
					xmlSource = new InputSource(strReader);
					doc = docBuilder.parse(xmlSource);
					isValid = true;
					
				} catch (Exception ex) {
					try {
						strReader  = new StringReader(valueText.toString().replaceAll("&#x1F" , "_"));
						xmlSource = new InputSource(strReader);
						doc = docBuilder.parse(xmlSource);
						
						isValid = true;
					} catch (Exception ex1) {
						
					}
				}
				
				if ( isValid ) {
					
					newFields.setLength(0);
					newFields.append(" gdwFileName=\"");
					newFields.append(xmlFileName);
					newFields.append("\"");
					newFields.append(" gdwBusinessDate=\"");
					newFields.append(posBusnDt);
					newFields.append("\"");
					newFields.append(" gdwFileId=\"");
					newFields.append(fileId);
					newFields.append("\"");
					newFields.append(" gdwMcdGbalLcatIdNu=\"");
					newFields.append(mcdGbalLcatIdNu);
					newFields.append("\"");
					newFields.append(" gdwTerrCd=\"");
					newFields.append(terrCd);
					newFields.append("\"");
					newFields.append(" gdwLgcyLclRfrDefCd=\"");
					newFields.append(lgcyLclRfrDefCd);
					newFields.append("\"");
					
					charIdx = valueText.indexOf(">");
					if ( valueText.charAt(charIdx-1) == '?' ) {
						charIdx = valueText.indexOf(">",charIdx+1);
					}
					
					valueText.insert(charIdx,newFields);
					
					newFields.setLength(0);
					newFields.append(fileType);
					newFields.append("`!");
					newFields.append(posBusnDt);
					newFields.append("`!");
					newFields.append(fileId);
					newFields.append("`!");
					newFields.append(mcdGbalLcatIdNu);
					newFields.append("`!");
					newFields.append(terrCd);
					newFields.append("`!");
					newFields.append(lgcyLclRfrDefCd);
					newFields.append("`!");
					newFields.append(owshTypCd);
					newFields.append("`!");
					
					valueText.insert(0,newFields);
					
					newFields.setLength(0);
					newFields.append(fileType);
					newFields.append("\t");
					newFields.append(terrCd);
					newFields.append("\t");
					newFields.append(posBusnDt);
					newFields.append("\t");
					newFields.append(partId);
					//if ( lgcyLclRfrDefCd.length() < 2 ) {
					//	newFields.append("00");
					//} else {
					//	newFields.append(lgcyLclRfrDefCd.substring(lgcyLclRfrDefCd.length()-2, lgcyLclRfrDefCd.length()));
					//}
					//newFields.append("\t");
					
					//System.err.println("partId=" + partId + " -- " + newFields.toString());
					
					outKey.clear();
					outKey.set(newFields.toString());
					
					outValue.clear();
					outValue.set(valueText.toString().replaceAll("[\n\r\t]", " ").replaceAll("`!", "\t"));
					
					context.write(outKey, outValue);
				} else {
					outKey.clear();
					outKey.set("INVALID");
					
					outValue.clear();
					outValue.set(zipFileName + "|" + xmlFileName);
					
					context.write(outKey, outValue);
					
				}
			}

		} catch (Exception ex) {
	    	ex.printStackTrace(System.err);
	    	System.exit(8);
		}
		
	}
}

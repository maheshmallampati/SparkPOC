package com.mcd.gdw.daas.mapreduce;

//import java.io.BufferedReader;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
//import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
//import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.dom.DOMSource;

//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
//import org.xml.sax.XMLReader;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.abac.ABaC;
//import com.mcd.gdw.daas.abac.ABaC2List;
import com.mcd.gdw.daas.abac.ABaCListItem;
import com.mcd.gdw.daas.abac.ABaCListSubItem;
import com.mcd.gdw.daas.abac.ABaC.CodeType;
import com.mcd.gdw.daas.driver.MergeToFinal;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.SimpleEncryptAndDecrypt;
//import com.mcd.gdw.daas.abac.ABaC2.CodeType;

/**
 * 
 * @author Sateesh Pula
 * The unzip mapper uses custom ZipFileRecordReader to read contents of the zip file. It receives the contents of each file within
 * the zip file as a value. The xml content of the value is modified to include few extra fields like the fileName, fileId etc and
 * passed on to the reducer. The partition is done by the File Type and the Last Digit of the LegacyRef cd for a given TerrCd
 *
 */
public class UnzipInputFileMapper extends Mapper<Text, BytesWritable, Text, Text> {
	private MultipleOutputs<Text, Text> mos;
	String fileType="";
	boolean nonmergedZipFile=false;
		

	public class SaxErr implements ErrorHandler {

		public ArrayList<String> errorList = new ArrayList<String>();
		public ArrayList<String> warningList = new ArrayList<String>();
		public ArrayList<String> fatialList = new ArrayList<String>();
		
		@Override
		public void error(SAXParseException ex) throws SAXException {

			errorList.add(ex.toString());
			
		}

		@Override
		public void fatalError(SAXParseException ex) throws SAXException {

			warningList.add(ex.toString());
			
		}

		@Override
		public void warning(SAXParseException ex) throws SAXException {

			fatialList.add(ex.toString());
			
		}
	}
	
	private HashMap<String,Integer> processedFiles = null;
	
	private static ABaCListItem currentXmlFileListItem = null;
	private static String validFileTypes = null;
	private static Text currentText = new Text();
	private static Text currentTextKey = new Text();
	private SimpleEncryptAndDecrypt encypt = new SimpleEncryptAndDecrypt();

	String[] lineparts =null;
	String[] fileNameParts = null;
	String fileName = null;
	SAXParserFactory factory = SAXParserFactory.newInstance();
	
	Path path = null;
	
	String xmlMsg = "";
	
	@Override
	public void setup(Context context) {
		
		mos = new MultipleOutputs<Text, Text>(context);
		fileType=context.getConfiguration().get(MergeToFinal.FILE_TYPE);
		nonmergedZipFile=Boolean.parseBoolean(context.getConfiguration().get(MergeToFinal.NONMERGE_FILE_TYPE));
		
		ABaCListSubItem abac2ListSubItem;
		BufferedReader br =null;
		
		processedFiles = new HashMap<String,Integer>();
		
		path = ((FileSplit)context.getInputSplit()).getPath();
		fileName = path.getName();
		try {
			URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
			Path path = new Path(uris[0]);

			//AWS START
			//FileSystem fs = FileSystem.get(context.getConfiguration());
			FileSystem fs = HDFSUtil.getFileSystem(context.getConfiguration().get(DaaSConstants.HDFS_ROOT_CONFIG), context.getConfiguration());
			//AWS END

			String line;
			boolean found = false;
			String[] parts;
			String[] partsListItem;
			String[] partsSubItem;
			String fileNameHashKey = ABaCListItem.generateFileNameHashKey(fileName);
			
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			
			while ( (line=br.readLine()) != null && !found ) {
				parts = line.split("\t");
			
				if ( ABaCListItem.generateFileNameHashKey(parts[1]).equals(fileNameHashKey) ) {
					found = true;
					
					HashMap<CodeType,Integer> typeCodes = new HashMap<CodeType,Integer>();
					
					partsListItem = parts[10].split(",");
					
					for ( int idx=0; idx < partsListItem.length; idx++ ) {
						partsSubItem = partsListItem[idx].split("\\|");
						typeCodes.put(CodeType.values()[Integer.parseInt(partsSubItem[0])], Integer.parseInt(partsSubItem[1]));
					}
					
					int statusCode = Integer.parseInt(parts[7]);
					
					currentXmlFileListItem = new ABaCListItem(Integer.parseInt(parts[0])
							                                 ,parts[1]
							                                 ,parts[2]
							                                 ,parts[3]
							                                 ,Integer.parseInt(parts[4])
							                                 ,parts[5]
							                                 ,new BigDecimal(parts[6])
					                                         ,statusCode
					                                         ,0
					                                         ,typeCodes
					                                         ,' '
					                                         ,parts[8]
					                                         ,parts[9]);
					
					partsListItem = parts[11].split(",");
					
					for ( int idx=0; idx < partsListItem.length; idx++ ) {
						partsSubItem = partsListItem[idx].split("\\|");
						currentXmlFileListItem.addSubItem(Integer.parseInt(partsSubItem[0]), partsSubItem[1], statusCode);
					}
				}
			}
			
			//currentXmlFileListItem = ABaC.readList(fs, new Path(context.getConfiguration().get("path.to.cache")), ABaCListItem.generateFileNameHashKey(fileName));
			
			String fileTypes = "";
			
			Iterator<Entry<String, ABaCListSubItem>>  subItemIterator = currentXmlFileListItem.iterator();
			while ( subItemIterator.hasNext() ) {
				abac2ListSubItem = subItemIterator.next().getValue();

				abac2ListSubItem.setStatusTypeId(ABaC.CodeType.REJECTED);
				abac2ListSubItem.setRejectReasonTypeId(ABaC.CodeType.MISSING);
				
				if ( fileTypes.length() > 0 ) {
					fileTypes += "|";
				}
				
				fileTypes += abac2ListSubItem.getSubFileDataTypeCd();
				
			}

			validFileTypes = fileTypes;
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		} finally {
			if ( br != null ) {
				try {
					br.close();
				} catch (Exception ex1 ) {
					
				}
			}
		}
	}

	StringBuffer xmlBuffer = new StringBuffer();
	StringBuffer newtext = new StringBuffer();
	StringBuffer currentKeyBuf = new StringBuffer();
	String currentKeyBufStr =null;
	byte[] valuebytes;
	String valuebytesstr="";
	InputSource xmlSource = null;
	Document doc = null;
	DOMSource source = null;
//	StringWriter stringWriter = null;
//	StreamResult result = null;
	
	
	@Override
	public void map(Text key
			       ,BytesWritable value
			       ,Context context) throws IOException, InterruptedException {

		String lookUpKey = null;
		String outPutType = "zip";

	
	

		try{

			ABaC.CodeType subFileStatus = ABaC.CodeType.SUCCESSFUL;
			ABaC.CodeType subFileReason = ABaC.CodeType.MISSING;
		
			long fileLength = 0;
			String keystring=fileName;
			valuebytesstr="";
			//Processing SMG files alone
			if(!(nonmergedZipFile)){


			valuebytes = value.getBytes();
			
//			xmlBuffer = new StringBuffer(valuebytes.length);
			valuebytesstr = new String(valuebytes,"UTF-8");
			}
			
			if( key != null  && !key.toString().isEmpty()) {
				System.out.println (" map called with key " + key.toString());
				//SMG_US_95960_20150819.20150819100000.zip/SMG_Answer_US_95960_20150226.20150227100000.csv
				
				if ( processedFiles.containsKey(key.toString())) {
					return;
				}
				
				processedFiles.put(key.toString(), 1);
				
				keystring = key.toString();//.replace(".XML", "_TEST.XML");
											
				fileNameParts = (keystring+"_@@@").split("\\_");
				
				if(keystring.contains("/"))
				{
					fileNameParts = (keystring+"_@@@").split("\\/");
					fileNameParts=fileNameParts[1].split("\\_");
				}

				if ( validFileTypes.contains(fileNameParts[1]) ) {
					lookUpKey = keystring.replace(fileNameParts[1] , "XML");
					outPutType = fileNameParts[1];
					
				}else{//handle pos 3.5 files
					if(validFileTypes.contains(keystring.substring(0,2))){
//						lookUpKey = keystring.substring(0,2);
						outPutType = "STLD";
						
					} else {
						return;
					}
					
				}
				/** handle MY files without matching file naming **/
				if(!key.toString().contains("_")){
//					keystring = key.toString().replace(".XML", "_TEST.XML");
					
					
					fileNameParts = (keystring+"_@@@").split("\\_");
					
					int extbegindx =  fileNameParts[0].indexOf(".");
					
					if ( validFileTypes.contains(fileNameParts[0].substring(0, extbegindx)) ) {
						
						lookUpKey = keystring.replace(fileNameParts[1] , "XML");
						outPutType = fileNameParts[0].substring(0, extbegindx);
					}else{//handle pos 3.5 files
						if(validFileTypes.contains(keystring.substring(0,2))){
	//						lookUpKey = keystring.substring(0,2);
							outPutType = "STLD";
						} else {
							return;
						}
						
					}
				}
				
				if ( currentXmlFileListItem == null ) {
					context.getCounter("Count","NoDistCacheRefForFileFound").increment(1);
					context.write(new Text(keystring), new Text());
					return;
				} else {
					currentXmlFileListItem.setStatusTypeId(ABaC.CodeType.SUCCESSFUL);
				
					if ( currentTextKey == null ) {
						currentTextKey = new Text();
					} else {
						currentTextKey.clear();
					}
					
					if(nonmergedZipFile){
						currentTextKey.set(outPutType+"_"+currentXmlFileListItem.getTerrCd()+"_"+currentXmlFileListItem.getBusnDt());
					}else
					{
						currentTextKey.set(outPutType + "_" + currentXmlFileListItem.getLgcyLclRfrDefCd().substring(currentXmlFileListItem.getLgcyLclRfrDefCd().length()-1)+"_"+currentXmlFileListItem.getTerrCd()+"_"+currentXmlFileListItem.getBusnDt());
					}
					
//					currentTextKey.set(outPutType + "_" + currentXmlFileListItem.getLgcyLclRfrDefCd().substring(currentXmlFileListItem.getLgcyLclRfrDefCd().length()-1));
				}
			
//				if (lookUpKey == null) {
//					if(fileName.contains("Content")){
//						context.getCounter("Count","SkippingContentsFile").increment(1);
//					}else{
//						context.getCounter("Count","InValidFileType").increment(1);	
//					}
//				
//					System.out.println(" Skipping file " + keystring + " from "+ fileName);
//					return;
//				}
			
				
				
				if(nonmergedZipFile){
					
					
					if ( currentText == null ) {
						currentText = new Text();
					} else {
						currentText.clear();
						currentText.set(value.getBytes());
					}
					 mos.write(HDFSUtil.replaceMultiOutSpecialChars(outPutType + HDFSUtil.FILE_PART_SEPARATOR+ currentXmlFileListItem.getTerrCd() + HDFSUtil.FILE_PART_SEPARATOR + currentXmlFileListItem.getBusnDt()),NullWritable.get(), currentText);
					 return;
				
				}
				if ( !valuebytesstr.startsWith("FILE_SIZE_EXCEEDS_ALLOWED_LIMIT") && !valuebytesstr.startsWith("CORRUPT_FILE") ) {
					
					xmlBuffer.append(valuebytesstr);
			
//					//dereference unnecessary objects
//					valuebytes = null;
//					valuebytesstr = null;
			
					int outStart = 0;
					int outSize = xmlBuffer.length();

					boolean foundStartEndTag = false;

					while ( foundStartEndTag == false && outStart < outSize ) {
						if (xmlBuffer.charAt(outStart) == '<') {
							foundStartEndTag = true;
						} else {
							outStart++;
						}
					}

					if ( outStart > 0 ) {
						xmlBuffer = xmlBuffer.delete(0, outStart);
					}

					outStart = 0;
					outSize = xmlBuffer.length();
					int outSizeOrig = outSize;

					foundStartEndTag = false;

					while (foundStartEndTag == false && outSize > 0) {
						if (xmlBuffer.charAt(outSize - 1) == '>') {
							foundStartEndTag = true;
						} else {
							outSize--;
						}
					}

					if (outSize != outSizeOrig) {
						xmlBuffer = xmlBuffer.delete(outSize, outSizeOrig);
					}
				
					if (outStart > 0) {
						outSize -= (outStart - 1);
					}
			
					try {
						if(isXmlValid(xmlBuffer)){
						
					
							if(newtext ==null)
								newtext = new StringBuffer();
							else{
								newtext.setLength(0);
							}

							newtext.append(" gdwFileName=\"").append(keystring).append("\"");//gdwfilename
							newtext.append(" gdwBusinessDate=\"").append(currentXmlFileListItem.getBusnDt()).append("\"");//"gdwBusinessDate",
							newtext.append(" gdwFileId=\"").append(Integer.toString(currentXmlFileListItem.getFileId())).append("\"");//"gdwFileId",
							newtext.append(" gdwMcdGbalLcatIdNu=\"").append(currentXmlFileListItem.getMcdGbalBusnLcat().toString()).append("\"");//"gdwMcdGbalLcatIdNu",
							newtext.append(" gdwTerrCd=\"").append(Integer.toString(currentXmlFileListItem.getTerrCd())).append("\"");//"gdwTerrCd",
							newtext.append(" gdwLgcyLclRfrDefCd=\"").append(currentXmlFileListItem.getLgcyLclRfrDefCd()).append("\" ");//"gdwLgcyLclRfrDefCd",
					
							if ( currentKeyBuf == null ){
								currentKeyBuf = new StringBuffer();
							} else {
								currentKeyBuf.setLength(0);
							}
					
							currentKeyBuf.append(outPutType).append("\t");
							currentKeyBuf.append(currentXmlFileListItem.getBusnDt() ).append("\t");
							currentKeyBuf.append(Integer.toString(currentXmlFileListItem.getFileId())).append("\t");
							currentKeyBuf.append(currentXmlFileListItem.getMcdGbalBusnLcat().toString()).append("\t");
							currentKeyBuf.append(Integer.toString(currentXmlFileListItem.getTerrCd())).append("\t");
							currentKeyBuf.append(currentXmlFileListItem.getLgcyLclRfrDefCd()).append("\t");
							currentKeyBuf.append(currentXmlFileListItem.getCurrentOwnershipType()).append("\t");
						
							int posStartTag = xmlBuffer.indexOf("<");
					
							if ( xmlBuffer.substring(posStartTag, posStartTag+5).equalsIgnoreCase("<?xml") ) {
								posStartTag = xmlBuffer.indexOf("<", posStartTag+1);
							}
					
							int posEndTag = xmlBuffer.indexOf(">",posStartTag);

							xmlBuffer = xmlBuffer.insert(posEndTag,newtext);
					
//							if ( outPutType.equalsIgnoreCase("STLD") && ( currentXmlFileListItem.getObfuscateTypeCd().equalsIgnoreCase("M") || currentXmlFileListItem.getObfuscateTypeCd().equalsIgnoreCase("E") )) {
//								boolean encryptData = false;
//
//								if ( currentXmlFileListItem.getObfuscateTypeCd().equalsIgnoreCase("E") ) {
//									encryptData = true;
//								}
//								replaceName(xmlBuffer,"CrewName",encryptData);
//								replaceName(xmlBuffer,"ManagerName",encryptData);
//							}
					
							if ( (outPutType.equalsIgnoreCase("STLD") || outPutType.equalsIgnoreCase("SecurityData")) && ( currentXmlFileListItem.getObfuscateTypeCd().equalsIgnoreCase("M") || currentXmlFileListItem.getObfuscateTypeCd().equalsIgnoreCase("E") )) {
				                boolean encryptData = false;

				                if ( currentXmlFileListItem.getObfuscateTypeCd().equalsIgnoreCase("E") ) {
				                                encryptData = true;
				                }
				                
				                replaceName(xmlBuffer,"CrewID",encryptData);
				                replaceName(xmlBuffer,"CrewId",encryptData);
				                replaceName(xmlBuffer,"CrewName",encryptData);
				                replaceName(xmlBuffer,"ManagerID",encryptData);
				                replaceName(xmlBuffer,"ManagerId",encryptData);
				                replaceName(xmlBuffer,"ManagerName",encryptData);
							}

							currentKeyBufStr = xmlBuffer.toString().replaceAll("[\n\r]", "").replaceAll("\t", "");
							
//							currentKeyBufStr = compressString(currentKeyBufStr);
							
							currentKeyBuf.append(currentKeyBufStr);
					
							currentKeyBufStr = currentKeyBuf.toString();
					
							if ( currentText == null ) {
								currentText = new Text();
							} else {
								currentText.clear();
							}
							currentText.set(currentKeyBufStr);

							fileLength = xmlBuffer.length();

							context.write(currentTextKey,currentText);
						} else {
							subFileStatus = ABaC.CodeType.REJECTED;
							subFileReason = ABaC.CodeType.MALFORMED_XML;
							
						}

					} catch (Exception ex) {
						subFileStatus = ABaC.CodeType.FAILED;
						ex.printStackTrace(System.err);

					} finally {
						resetobjects();
					}
			
				} else {
					if ( valuebytesstr.startsWith("FILE_SIZE_EXCEEDS_ALLOWED_LIMIT") ) {
						currentXmlFileListItem.setStatusTypeId(ABaC.CodeType.REJECTED);
						currentXmlFileListItem.setRejectReasonId(ABaC.CodeType.FAILED);
						context.getCounter("Count","NumFilesExceededMaxSize").increment(1);
						subFileStatus = ABaC.CodeType.FAILED;//file size exceeded
						
					} else if( valuebytesstr.startsWith("CORRUPT_FILE") ) {
						context.getCounter("Count","NumCorruptFiles").increment(1);
						subFileStatus = ABaC.CodeType.REJECTED;
						subFileReason = ABaC.CodeType.CORRUPTED_COMPRESSED_FORMAT;
						currentXmlFileListItem.setStatusTypeId(ABaC.CodeType.REJECTED);
						currentXmlFileListItem.setRejectReasonId(ABaC.CodeType.CORRUPTED_COMPRESSED_FORMAT);
					}
				}
			} else {
				
//				System.out.println(" valuebytesstr  "+  valuebytesstr );
				subFileStatus = ABaC.CodeType.REJECTED;
				
				if ( valuebytesstr.startsWith("FILE_SIZE_EXCEEDS_ALLOWED_LIMIT") ) {
					currentXmlFileListItem.setStatusTypeId(ABaC.CodeType.REJECTED);
					currentXmlFileListItem.setRejectReasonId(ABaC.CodeType.FAILED);
					context.getCounter("Count","NumFilesExceededMaxSize").increment(1);
					subFileStatus = ABaC.CodeType.FAILED;//file size exceeded
				} else if( valuebytesstr.startsWith("CORRUPT_FILE") ) {
					context.getCounter("Count","NumCorruptFiles").increment(1);
					subFileStatus = ABaC.CodeType.REJECTED;
					subFileReason = ABaC.CodeType.CORRUPTED_COMPRESSED_FORMAT;
					currentXmlFileListItem.setStatusTypeId(ABaC.CodeType.REJECTED);
					currentXmlFileListItem.setRejectReasonId(ABaC.CodeType.CORRUPTED_COMPRESSED_FORMAT);
				}
			}

			if ( !outPutType.equals("zip") ) {
//				System.out.println("outPutType is not zip ************** ");
				ABaCListSubItem abac2ListSubItem = currentXmlFileListItem.getSubItem(outPutType);
				abac2ListSubItem.setStatusTypeId(subFileStatus);
				if ( subFileStatus == ABaC.CodeType.REJECTED ) {
					abac2ListSubItem.setRejectReasonTypeId(subFileReason);
				}
				abac2ListSubItem.setSubFileName(keystring);
				abac2ListSubItem.setFileSizeNum(fileLength);
				
				if ( xmlMsg.length() > 0 ) {
					abac2ListSubItem.setXmlErrorMsg(xmlMsg);
				}
			
//				if ( currentText == null ) {
//					currentText = new Text();
//				} else {
//					currentText.clear();
//				}
//			
//				currentText.set(ABaC.serializeListItemToHexString(currentXmlFileListItem));
//	
//				if ( currentTextKey ==null ) {
//					currentTextKey = new Text();
//				} else {
//					currentTextKey.clear();
//				}
//				currentTextKey.set("FileStatus_0_0_0");
////				currentTextKey.set("FileStatus");
//				context.write(currentTextKey, currentText);
			} else {
//				System.out.println("outPutType is  zip ************** " );
				Iterator<Entry<String, ABaCListSubItem>>  subItemIterator = currentXmlFileListItem.iterator();
			
				while( subItemIterator.hasNext() ) {
//					System.out.println( " loop 2 **********"+ subFileStatus + " " + subFileReason);
					ABaCListSubItem abac2ListSubItem = subItemIterator.next().getValue();
				
					abac2ListSubItem.setStatusTypeId(subFileStatus);
					if ( subFileStatus == ABaC.CodeType.REJECTED ) {
						abac2ListSubItem.setRejectReasonTypeId(subFileReason);
					}
				
//					if ( currentText == null ) {
//						currentText = new Text();
//					} else {
//						currentText.clear();
//					}
//					
//				
//					currentText.set(ABaC.serializeListItemToHexString(currentXmlFileListItem));
//
//					if ( currentTextKey == null ) {
//						currentTextKey = new Text();
//					} else {
//						currentTextKey.clear();
//					}
//				
//					currentTextKey.set("FileStatus_0_0_0");
////					currentTextKey.set("FileStatus");
//					context.write(currentTextKey, currentText);
				}
			}
		} catch(Exception ex) {
			ex.printStackTrace(System.err);
		} finally {
			if(value != null){
				value.setSize(0);
				value = null;
			}
			
			resetobjects();
		}
	}
	
	private void resetobjects(){
		try{
			
			
	
		if(xmlBuffer != null){
			xmlBuffer.setLength(0);
//			xmlBuffer 		 = null;
		}
		
		if(valuebytes != null){
			Arrays.fill(valuebytes, (byte)0);
			valuebytes 	     = null;
		}
		
		if(valuebytesstr != null){
			valuebytesstr    = null;
		}
		
		if(newtext != null){
			newtext.setLength(0);
//			newtext 		 = null;
		}
		
		if(currentKeyBufStr != null){
			
			currentKeyBufStr = null;
		}
		
		if(currentKeyBuf != null){
			currentKeyBuf.setLength(0);
//			currentKeyBuf 	 = null;
		}
		if(xmlSource != null){
			
			xmlSource = null;
		}
		
		if(doc != null){
			doc = null;
		}
		source = null;
		
		}catch(Exception ex){
			
		}
	
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	
		try{
			if ( currentText == null ) {
				currentText = new Text();
			} else {
				currentText.clear();
			}
			
		
			currentText.set(ABaC.serializeListItemToHexString(currentXmlFileListItem));

			if ( currentTextKey == null ) {
				currentTextKey = new Text();
			} else {
				currentTextKey.clear();
			}
		
			currentTextKey.set("FileStatus_0_0_0");
//			currentTextKey.set("FileStatus");
			
			if(nonmergedZipFile)
			{
			
			//mos.write(new Text(""), currentText,currentTextKey.toString().split("_")[0]);
			mos.write(currentTextKey.toString().split("_")[0],NullWritable.get(),currentText);
			//mos.write(NullWritable.get(),currentText,currentTextKey.toString().split("_")[0]);
			}else
			{
			 context.write(currentTextKey, currentText);
			}
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		mos.close();
		resetobjects();
		currentText 	 = null;
		currentTextKey   = null;
		validFileTypes = null;
		currentXmlFileListItem = null;
		lineparts = null;
		fileNameParts = null;
		path = null;
		super.cleanup(context);
		
	}
	
	SAXParserFactory saxfactory = null;
	SAXParser parser = null;
	XMLReader reader = null;
	StringReader stringReader = null;
	String xmlString = null;
	UnzipInputFileMapper.SaxErr errHandler = null;
	
	public boolean validateXMLUsingSAX(StringBuffer filebuf) {
		boolean validFl = true;
		
		try{
			saxfactory = SAXParserFactory.newInstance();
			saxfactory.setValidating(false);
			saxfactory.setNamespaceAware(true);

			parser = saxfactory.newSAXParser();

			reader = parser.getXMLReader();
			errHandler = new UnzipInputFileMapper.SaxErr();
			reader.setErrorHandler(errHandler);
			xmlString = filebuf.toString().trim().replaceAll("&", " ");

			filebuf.setLength(0);
			filebuf.append(xmlString);
			
			stringReader = new StringReader(xmlString);
			
			reader.parse(new InputSource(stringReader));
			
			boolean ignoreErr1 = false;
			boolean ignoreErr2 = false;
			
			if ( errHandler.errorList.size() >= 2 ) {
				for (String msg : errHandler.errorList ) {
					if ( msg.toLowerCase().contains("document root element") && msg.toLowerCase().contains("must match doctype root \"null\"") ) {
						ignoreErr1 = true;
					}
					
					if ( msg.toLowerCase().contains("document is invalid: no grammar found") ) {
						ignoreErr2 = true;
					}
				}
			}
			
			if ( ignoreErr1 && ignoreErr2 ) {
				String msg;
				for ( int idx = errHandler.errorList.size()-1; idx >= 0; idx-- ) {
					msg = errHandler.errorList.get(idx);
					if ( msg.toLowerCase().contains("document root element") && msg.toLowerCase().contains("must match doctype root \"null\"") ) {
						errHandler.errorList.remove(idx);
					}
					
					if ( msg.toLowerCase().contains("document is invalid: no grammar found") ) {
						errHandler.errorList.remove(idx);
					}
				}
			}

			if ( errHandler.errorList.size() > 0 || errHandler.fatialList.size() > 0 ) {
				validFl = false;
				xmlMsg = "";
				for ( int idx=0; idx < errHandler.errorList.size(); idx++ ) {
					if ( xmlMsg.length() > 0 ) {
						xmlMsg += " ";
					}
					xmlMsg += errHandler.errorList.get(idx);
				}
				for ( int idx=0; idx < errHandler.fatialList.size(); idx++ ) {
					if ( xmlMsg.length() > 0 ) {
						xmlMsg += " ";
					}
					xmlMsg += errHandler.fatialList.get(idx);
				}
			}
			
			if ( errHandler.warningList.size() > 0  ) {
				System.out.println("XML Parser Warnings:");
				
				for (String msg : errHandler.warningList ) {
					System.out.println("     " + msg);
				}
			}
			
			if ( errHandler.errorList.size() > 0  ) {
				System.out.println("XML Parser Errors:");
				
				for (String msg : errHandler.errorList ) {
					System.out.println("     " + msg);
				}
			}
			
			if ( errHandler.fatialList.size() > 0  ) {
				System.out.println("XML Parser Fatial Errors:");
				
				for (String msg : errHandler.fatialList ) {
					System.out.println("     " + msg);
				}
			}
		}catch(Exception ex){
			System.out.println("Encoutered SAX Exception: " + ex.getMessage());
			ex.printStackTrace();
			validFl = false;
		}finally{
			if(stringReader != null)
				stringReader.close();
			
			reader= null;
			factory = null;
			errHandler = null;
			xmlString = null;
		}
		
	
	
		if ( !validFl )  {
			System.out.println(" SAX validation returning false ");
		}
		
		return(validFl);
			
	}
 
	public boolean validateXMLUsingDom(StringBuffer filebuf) {
		
		boolean validFl = true;
		
//		System.out.println( " Dom received " + filebuf.toString());
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		Document document =null;
		InputSource xmlSource = null;
		UnzipInputFileMapper.SaxErr errHandler = null;
		String xmlString  = null;
		StringReader stringReader = null;
		try{
			
			factory.setValidating(true);
//			factory.setNamespaceAware(true);
	
			builder = factory.newDocumentBuilder();
			
			errHandler = new UnzipInputFileMapper.SaxErr();
			
			builder.setErrorHandler(errHandler);
	
			xmlString = filebuf.toString().trim().replaceAll("&", " ");
			
			filebuf.setLength(0);
			filebuf.append(xmlString);
			
			stringReader = new StringReader(xmlString);
			xmlSource = new InputSource(stringReader);
//			xmlSource = new InputSource(new StringReader(filebuf.toString().trim()));
			document = builder.parse(xmlSource);

			boolean ignoreErr1 = false;
			boolean ignoreErr2 = false;
			
			if ( errHandler.errorList.size() >= 2 ) {
				for (String msg : errHandler.errorList ) {
					if ( msg.toLowerCase().contains("document root element") && msg.toLowerCase().contains("must match doctype root \"null\"") ) {
						ignoreErr1 = true;
					}
					
					if ( msg.toLowerCase().contains("document is invalid: no grammar found") ) {
						ignoreErr2 = true;
					}
				}
			}
			
			if ( ignoreErr1 && ignoreErr2 ) {
				String msg;
				for ( int idx = errHandler.errorList.size()-1; idx >= 0; idx-- ) {
					msg = errHandler.errorList.get(idx);
					if ( msg.toLowerCase().contains("document root element") && msg.toLowerCase().contains("must match doctype root \"null\"") ) {
						errHandler.errorList.remove(idx);
					}
					
					if ( msg.toLowerCase().contains("document is invalid: no grammar found") ) {
						errHandler.errorList.remove(idx);
					}
				}
			}

			if ( errHandler.errorList.size() > 0 || errHandler.fatialList.size() > 0 ) {
				validFl = false;
				xmlMsg = "";
				for ( int idx=0; idx < errHandler.errorList.size(); idx++ ) {
					if ( xmlMsg.length() > 0 ) {
						xmlMsg += " ";
					}
					xmlMsg += errHandler.errorList.get(idx);
				}
				for ( int idx=0; idx < errHandler.fatialList.size(); idx++ ) {
					if ( xmlMsg.length() > 0 ) {
						xmlMsg += " ";
					}
					xmlMsg += errHandler.fatialList.get(idx);
				}
			}
			
			if ( errHandler.warningList.size() > 0  ) {
				System.out.println("XML Parser Warnings:");
				
				for (String msg : errHandler.warningList ) {
					System.out.println("     " + msg);
				}
			}
			
			if ( errHandler.errorList.size() > 0  ) {
				System.out.println("XML Parser Errors:");
				
				for (String msg : errHandler.errorList ) {
					System.out.println("     " + msg);
				}
			}
			
			if ( errHandler.fatialList.size() > 0  ) {
				System.out.println("XML Parser Fatial Errors:");
				
				for (String msg : errHandler.fatialList ) {
					System.out.println("     " + msg);
				}
			}
			
		}catch(Exception ex){
			System.out.println("Encoutered SAX Exception: " + ex.getMessage());
			ex.printStackTrace();
			validFl = false;
		}finally{
			if(stringReader != null)
				stringReader.close();
			
			document = null;
			builder = null;
			factory = null;
			errHandler = null;
			xmlString = null;
		}

		if ( validFl ) {
//			System.out.println(" DOM validation returning true ");
		} else { 
			System.out.println(" DOM validation returning false ");
		}
		
		return(validFl);
	}
 
	public boolean isXmlValid(StringBuffer filebuf) {
		
//		return validateXMLUsingSAX(filebuf);
		return validateXMLUsingDom(filebuf);
		
//		SAXParser parser  = null;
//		XMLReader reader = null;
//		InputSource xmlSource = null;
//		String tempxml = null;
//		StringReader sr = null;
//		try {
//			factory.setValidating(true);
//			factory.setNamespaceAware(true);
//
//			parser = factory.newSAXParser();
//
//			reader = parser.getXMLReader();
//			tempxml = filebuf.toString();
//			sr = new StringReader(tempxml);
//			xmlSource = new InputSource(sr);
//			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			return false;
//		} finally {
//			tempxml   = null;
//			xmlSource = null;
//			reader    = null;
//			parser    = null;
//		}
//
//		return true;
	}

	private void replaceName(StringBuffer xmlText
			                ,String tagName
			                ,boolean encrypt) throws Exception {
	    
	    int pos = 0;
	    int nameStart = 0;
	    int nameEnd = 0;
	    
	    pos = xmlText.indexOf("<" + tagName, pos);
	    
	    while ( pos > 0 ) {
	    	nameStart = xmlText.indexOf(">", pos+1);
	    	
	    	if ( nameStart > 0 ) {
	    		nameStart++;
	    		
	    		nameEnd = xmlText.indexOf("<", nameStart);
	    		
	    		if ( (nameEnd-nameStart) > 0 ) {
	    			if ( encrypt ) {
	    				xmlText.replace(nameStart, nameEnd, encypt.encryptAsHexString(xmlText.substring(nameStart, nameEnd)));
	    			} else {
	    				xmlText.replace(nameStart, nameEnd, "***");
	    			}
	    		}
	    		
		    	pos = xmlText.indexOf("<" + tagName, pos+1);
	    	}
	    }
	}
	
	public  String compressString(String str) {
		ByteArrayOutputStream out = null;
		try{
			
			if (str == null || str.length() == 0) {
			    return str;
			}
			out = new ByteArrayOutputStream(str.length());
			GZIPOutputStream gzip = new GZIPOutputStream(out);
			gzip.write(str.getBytes());
			gzip.close();
			out.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return Base64.encodeBase64String(out.toByteArray()).replaceAll("[\n\r]", "").replaceAll("\t", "");
	}
	
}
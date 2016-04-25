package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.GenerateAPTFormatSos;
import com.mcd.gdw.daas.util.HDFSUtil;

import java.math.*;
import java.net.URI;


/**
 * 
 * @author Khajaasmath This mapper reads the DetailedSOS records and extracts
 *         fields that are part of Cook and Wait Time Extract.
 * 
 */

public class NpAPTSosXmlMapper extends Mapper<LongWritable, Text, Text, Text> {

	BigDecimal bgzero = new BigDecimal("0.00");	
	TDAExtractMapperUtil tdsExtractMapperUtil = new TDAExtractMapperUtil();
	private HashSet<String> storeFilterSet;
	boolean filterOnStoreId = false;
	String[] recArr;
	String ownership = "";
	Text outputkey = new Text();
	Text outputvalue = new Text();
	private MultipleOutputs<Text, Text> mos;
	private TreeMap<String, HashMap<String, BigDecimal>> segOrderMFYTime = new TreeMap<String, HashMap<String, BigDecimal>>();
	private TreeMap<String, HashMap<String, BigDecimal>> segOrderFCTime = new TreeMap<String, HashMap<String, BigDecimal>>();
	private TreeMap<String, HashMap<String, BigDecimal>> segOrderDTTime = new TreeMap<String, HashMap<String, BigDecimal>>();
	HashMap<String, BigDecimal> orderMFYTime = new HashMap<String, BigDecimal>();
	HashMap<String, BigDecimal> orderDTTime = new HashMap<String, BigDecimal>();
	HashMap<String, BigDecimal> orderFCTime = new HashMap<String, BigDecimal>();
	String storeId;
	String gdwLgcyLclRfrDefCd;
	String gdwBusinessDate;
	String terrCd;
	public static final BigDecimal DEC_ZERO = new BigDecimal("0.00");
	public static final String DT="DT";
	public static final String FC="FC";
	public static final String MFY="MFY:Side";
	private FileSplit fileSplit = null;
	private String fileName = "";
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	int partitionID;
	HashSet<String> storeSet;
	int recordCount;
	

	HashMap<String, TreeMap<String, HashMap<String, String>>> ordpaItemCodeMap = new HashMap<String, TreeMap<String, HashMap<String, String>>>();

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
        int partitionID=context.getTaskAttemptID().getTaskID().getId();
		
		mosKey.setLength(0);
		mosKey.append(GenerateAPTFormatSos.APTUS_FILE_STATUS);
		
		mosValue.setLength(0);
		mosValue.append(GenerateAPTFormatSos.APTUS_SOS_TDA_Detail+GenerateAPTFormatSos.HYPHEN_DELIMITER+"m"+GenerateAPTFormatSos.HYPHEN_DELIMITER+String.format("%05d",partitionID));
		mosValue.append(DaaSConstants.PIPE_DELIMITER);
		mosValue.append(recordCount);
		mosValue.append(DaaSConstants.PIPE_DELIMITER);
		mosValue.append(storeSet.size());
		
		
		
		
		outputkey.clear();
		outputvalue.clear();
		outputkey.set(HDFSUtil.replaceMultiOutSpecialChars(mosKey.toString()));
		outputvalue.set(mosValue.toString());
		mos.write( outputkey.toString(),NullWritable.get(), outputvalue);
		
		
			
		mos.close();
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;

        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();

        System.out.println("File Name = " + fileName);
        
		mos = new MultipleOutputs<Text, Text>(context);
		storeSet=new HashSet<String>();
		recordCount=0;

		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
		    distPaths = context.getCacheFiles();
		    
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		    
		    String filterOnStoreIdStr = context.getConfiguration().get(
					DaaSConstants.JOB_CONFIG_PARM_STORE_FILTER);
			if (filterOnStoreIdStr != null
					&& filterOnStoreIdStr.equalsIgnoreCase("true")) {
				filterOnStoreId = true;
			}
		      
		    if ( distPaths != null && distPaths.length > 0 )  {
		    	  
		    	System.out.println(" number of distcache files : " + distPaths.length);
		    	  
		    	for ( int i=0; i<distPaths.length; i++ ) {
				     
			    	  System.out.println("distpaths:" + distPaths[i].toString());
			    	  
			    	  distPathParts = 	distPaths[i].toString().split("#");
			    	  
			    	  if( distPaths[i].toString().contains(GenerateAPTFormatSos.STORE_FILTER_LIST) ) {
			    		  		      	  
			    		  	    		  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  storeFilterSet = new HashSet<String>();
						  addValuesToSet(storeFilterSet, br);
				    	  System.out.println("Loaded Include List Values Set");
    	  		      	  
				      }
			      }
		      }
			
		} catch (Exception ex) {
			System.err.println("Error in initializing NPAPTSosXMLMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}

		
	}

	
	private void addValuesToSet(HashSet<String> valSet, BufferedReader br) {

		try {

			String line = "";

			while ((line = br.readLine()) != null) {
				String[] flds = line.split("\t", -1);
				if (flds.length >= 2) {
					valSet.add(flds[0] + "_" + Integer.parseInt(flds[1]));

				}
			}

			System.out.println("added " + valSet.size() + " values to valSet");

		} catch (IOException e) {
			e.printStackTrace();
			System.out
					.println("read from distributed cache: read length and instances");
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
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSource xmlSource;
		Document xmlDoc;
		Element eleDetailedSos;

		try {
			recArr = value.toString().split("\t");
			ownership = recArr[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS];
			String terrCdStoreId = recArr[DaaSConstants.XML_REC_TERR_CD_POS]
					+ "_"
					+ Integer
							.parseInt(recArr[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]);
			
			

			if (filterOnStoreId && !storeFilterSet.contains(terrCdStoreId)) {
				context.getCounter("Count", "SkippingonStoreFilter").increment(
						1);
				return;
			}
			
			/*if(Integer.parseInt(recArr[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS])!=7712)
			{
				context.getCounter("Count", "SkippingonStore").increment(
						1);
				return;
			}*/
			
		
				docFactory = DocumentBuilderFactory.newInstance();
				docBuilder = docFactory.newDocumentBuilder();
				xmlSource = new InputSource(new StringReader(
						recArr[recArr.length - 1]));
				xmlDoc = docBuilder.parse(xmlSource);

				eleDetailedSos = (Element) xmlDoc.getFirstChild();
				storeId = Integer.parseInt(recArr[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS])+"";

				segOrderMFYTime.clear();
				segOrderFCTime.clear();
				segOrderDTTime.clear();
				orderMFYTime.clear();
				orderDTTime.clear();
				orderFCTime.clear();

				if (eleDetailedSos.getNodeName().equals("DetailedSOS")) {
					npParseXml(eleDetailedSos, context, storeId, ownership);
					generateMFYStoreDetails(gdwLgcyLclRfrDefCd, gdwBusinessDate, terrCd, storeId, orderMFYTime, context);
					
				}
			

			
		} catch (Exception ex) {
			context.getCounter("Debug", "parseError").increment(1);
			Logger.getLogger(NpAPTSosXmlMapper.class.getName()).log(
					Level.SEVERE, null, ex);
		}
	}

	StringBuffer mosKey = new StringBuffer();
	StringBuffer mosValue = new StringBuffer();

	/**This method parse fields of Detailed SOS XML file.
	 * @param eleDetailedSos
	 * @param context
	 * @param storeId
	 * @param ownership
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void npParseXml(Element eleDetailedSos, Context context,
			String storeId, String ownership) throws IOException,
			InterruptedException {

		NodeList nlStoreTotals;
		Element eleStoreTotals;

		try {
			gdwLgcyLclRfrDefCd = eleDetailedSos
					.getAttribute("gdwLgcyLclRfrDefCd");
			gdwBusinessDate = eleDetailedSos.getAttribute("gdwBusinessDate");
			terrCd = eleDetailedSos.getAttribute("gdwTerrCd");
			

			nlStoreTotals = eleDetailedSos.getChildNodes();

			if (nlStoreTotals != null && nlStoreTotals.getLength() > 0) {
				for (int idxStoreTotals = 0; idxStoreTotals < nlStoreTotals
						.getLength(); idxStoreTotals++) {
					if (nlStoreTotals.item(idxStoreTotals).getNodeType() == Node.ELEMENT_NODE) {
						eleStoreTotals = (Element) nlStoreTotals
								.item(idxStoreTotals);

						if (eleStoreTotals.getNodeName().equals("StoreTotals")) {
							parseNode(eleStoreTotals, gdwLgcyLclRfrDefCd,
									gdwBusinessDate, terrCd, context, storeId,
									ownership);
						}
					}
				}
			}
		} catch (Exception ex) {
			Logger.getLogger(NpAPTSosXmlMapper.class.getName()).log(
					Level.SEVERE, null, ex);
		}
	}

	/** This method parse fields of Detailed SOS XML file.
	 * @param eleStoreTotals
	 * @param gdwLgcyLclRfrDefCd
	 * @param gdwBusinessDate
	 * @param terrCd
	 * @param context
	 * @param storeId
	 * @param ownership
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void parseNode(Element eleStoreTotals, String gdwLgcyLclRfrDefCd,
			String gdwBusinessDate, String terrCd, Context context,
			String storeId, String ownership) throws IOException,
			InterruptedException {

		String productionNodeIdText;
		String[] productionNodeIdParts;
		String productionNodeId;
		Element eleServiceTime = null;
		String orderKey;
		String totalTimeText;
		BigDecimal totalTime = new BigDecimal(0.00);
		String untilRecallText;
		BigDecimal untilRecallTime = new BigDecimal(0.00);
		BigDecimal orderTotalTime = new BigDecimal(0.00);
		String segmentId = "";

		try {
			productionNodeIdText = eleStoreTotals
					.getAttribute("productionNodeId");
			if (productionNodeIdText.equalsIgnoreCase("FC")
					|| productionNodeIdText.equalsIgnoreCase("DT")
					|| productionNodeIdText.equalsIgnoreCase("MFY:Side1")
					|| productionNodeIdText.equalsIgnoreCase("MFY:Side2"))
			
			{
				productionNodeIdParts = (productionNodeIdText + ":@@@@")
						.split(":");
				productionNodeId = productionNodeIdParts[0];

				NodeList nlstrttlsList = eleStoreTotals.getChildNodes();
				for (int nlstrttlcnt = 0; nlstrttlcnt < nlstrttlsList
						.getLength(); nlstrttlcnt++) {

					if (nlstrttlsList.item(nlstrttlcnt).getNodeType() == Node.ELEMENT_NODE) {
						eleServiceTime = (Element) nlstrttlsList
								.item(nlstrttlcnt);
						break;
					}

				}

				orderKey = eleServiceTime.getAttribute("orderKey");
				segmentId = eleServiceTime.getAttribute("segmentId");
				totalTimeText = eleServiceTime.getAttribute("totalTime");

				untilRecallText = eleServiceTime.getAttribute("untilRecall");

				try {
					if (totalTimeText != null && !totalTimeText.isEmpty())
						totalTime = new BigDecimal(totalTimeText);
					if (untilRecallText != null && !untilRecallText.isEmpty())
						untilRecallTime = new BigDecimal(untilRecallText);
					orderTotalTime = totalTime.subtract(untilRecallTime);

				} catch (NumberFormatException ex) {
					context.getCounter("Count", "NumFormatExeption").increment(
							1);
				}

				if(productionNodeIdText.startsWith("FC") || productionNodeIdText.startsWith("DT"))
				{
				   generateStoreDetails(gdwLgcyLclRfrDefCd, gdwBusinessDate, terrCd, orderKey, productionNodeIdText, productionNodeId, segmentId, totalTime, untilRecallTime, orderTotalTime, storeId, ownership, context);
				}
				else
				{
					calculateMFYTotalTime(gdwLgcyLclRfrDefCd, gdwBusinessDate, terrCd, orderKey, productionNodeIdText, productionNodeId, segmentId, totalTime, untilRecallTime, orderTotalTime, storeId, ownership, context);
				}
				
		       
			} // If Loop
		} catch (Exception ex) {
			ex.printStackTrace();
			Logger.getLogger(NpAPTSosXmlMapper.class.getName()).log(
					Level.SEVERE, null, ex);
		}
	}
	
	/** This method output fields in the output file for DT and TC orders.
	 * @param gdwLgcyLclRfrDefCd
	 * @param gdwBusinessDate
	 * @param terrCd
	 * @param orderKey
	 * @param productionNodeIdText
	 * @param productionNodeId
	 * @param segmentId
	 * @param totalTime
	 * @param untilRecallTime
	 * @param orderTotalTime
	 * @param storeId
	 * @param ownership
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void  generateStoreDetails(String gdwLgcyLclRfrDefCd, String gdwBusinessDate, String terrCd,
			String orderKey, String productionNodeIdText,
			String productionNodeId, String segmentId, BigDecimal totalTime,
			BigDecimal untilRecallTime, BigDecimal orderTotalTime,
			String storeId, String ownership, Context context)
			throws IOException, InterruptedException {
		//String gdwLgcyLclRfrDefCd_lastDigit = gdwLgcyLclRfrDefCd.substring(gdwLgcyLclRfrDefCd.length()-1);
		
		mosValue.setLength(0);
		mosValue.append(gdwLgcyLclRfrDefCd);
		mosValue.append(DaaSConstants.PIPE_DELIMITER);
		mosValue.append(gdwBusinessDate);
		mosValue.append(DaaSConstants.PIPE_DELIMITER);
		mosValue.append(terrCd);
		mosValue.append(DaaSConstants.PIPE_DELIMITER);
		mosValue.append(orderKey);
		mosValue.append(DaaSConstants.PIPE_DELIMITER);
		mosValue.append(productionNodeId);
		mosValue.append(DaaSConstants.PIPE_DELIMITER);
		mosValue.append(segmentId);
		mosValue.append(DaaSConstants.PIPE_DELIMITER);
		mosValue.append(orderTotalTime);
		
		mosKey.setLength(0);
		mosKey.append(GenerateAPTFormatSos.APTUS_SOS_TDA_Detail);
		
		
		
		outputkey.clear();
		outputvalue.clear();
		outputkey.set(HDFSUtil.replaceMultiOutSpecialChars(mosKey.toString()));
		outputvalue.set(mosValue.toString());
		
		mos.write(outputkey.toString(),NullWritable.get(), outputvalue);
		recordCount=recordCount+1;
		storeSet.add(gdwLgcyLclRfrDefCd);
	       
	}
	
	/** This method adds MFYTimes for order key. Add MFYSIDE1 and MFYSIDE2 for particular order key
	 * @param gdwLgcyLclRfrDefCd
	 * @param gdwBusinessDate
	 * @param terrCd
	 * @param orderKey
	 * @param productionNodeIdText
	 * @param productionNodeId
	 * @param segmentId
	 * @param totalTime
	 * @param untilRecallTime
	 * @param orderTotalTime
	 * @param storeId
	 * @param ownership
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void calculateMFYTotalTime(String gdwLgcyLclRfrDefCd, String gdwBusinessDate, String terrCd,
			String orderKey, String productionNodeIdText,
			String productionNodeId, String segmentId, BigDecimal totalTime,
			BigDecimal untilRecallTime, BigDecimal orderTotalTime,
			String storeId, String ownership, Context context)
			throws IOException, InterruptedException {

		try {
			if (productionNodeId.equalsIgnoreCase("MFY")) {

				
					if (orderMFYTime.containsKey(segmentId+"|"+orderKey)) {
						BigDecimal totMF=orderMFYTime.get(segmentId+ "|" + orderKey).add(
								totalTime);
						orderMFYTime.put(segmentId + "|" +orderKey, totMF);
						
					} else {
						orderMFYTime.put(segmentId + "|" +orderKey, totalTime);
						
					}
				

			}

			

		} catch (Exception ex) {
			Logger.getLogger(NpAPTSosXmlMapper.class.getName()).log(
					Level.SEVERE, null, ex);
		}

	}
	
	
	
	/** This method output fields in the output file for MFY orders.
	 * @param gdwLgcyLclRfrDefCd
	 * @param gdwBusinessDate
	 * @param terrCd
	 * @param storeId
	 * @param segOrderTimeMap
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void generateMFYStoreDetails(
			String gdwLgcyLclRfrDefCd, String gdwBusinessDate, String terrCd,
			String storeId,
			HashMap<String, BigDecimal> segOrderTimeMap,
			Context context) throws IOException, InterruptedException {
		
		String segmentId="";
		try {
			
					
			String orderKey="";
			BigDecimal orderTotalTime=DEC_ZERO;
			
			for (Map.Entry<String, BigDecimal> orderTimeMap : segOrderTimeMap.entrySet()) {
				
				segmentId=orderTimeMap.getKey().split("\\|")[0];
				orderKey=orderTimeMap.getKey().split("\\|")[1];
				
				orderTotalTime=orderTimeMap.getValue();	
				//String gdwLgcyLclRfrDefCd_lastDigit = gdwLgcyLclRfrDefCd.substring(gdwLgcyLclRfrDefCd.length()-1);
				
				outputkey.clear();
				outputvalue.clear();
				
				mosKey.setLength(0);
				mosKey.append(GenerateAPTFormatSos.APTUS_SOS_TDA_Detail);

				mosValue.setLength(0);
				mosValue.append(gdwLgcyLclRfrDefCd);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(gdwBusinessDate);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(terrCd);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(orderKey);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(MFY);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(segmentId);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(orderTotalTime);

				outputkey.set(HDFSUtil.replaceMultiOutSpecialChars(mosKey.toString()));
				outputvalue.set(mosValue.toString());

				mos.write(outputkey.toString(),NullWritable.get(), outputvalue);
				recordCount=recordCount+1;
				storeSet.add(gdwLgcyLclRfrDefCd);
			}
		
		}catch (Exception e) {
			e.printStackTrace();
		}

	}


}

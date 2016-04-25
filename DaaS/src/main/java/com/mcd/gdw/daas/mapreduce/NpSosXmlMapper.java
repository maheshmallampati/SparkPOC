package com.mcd.gdw.daas.mapreduce;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.TimeUtil;

import java.math.*;
import java.net.URI;
/**
 * 
 * @author Sateesh Pula
 * This mapper reads the DetailedSOS records and extracts fields that are part of TDA format sales.
 * 
 */
public class NpSosXmlMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static final MathContext SCALE_4 = new MathContext(4);
  private static final BigDecimal T1000 = new BigDecimal("1000",SCALE_4);
  private static final BigDecimal T0 = new BigDecimal("0",SCALE_4);
  BigDecimal bgzero = new BigDecimal("0.00");
  private String owshFltr = "*";
  
  private HashMap<String, String> lookupCodes; 
  TDAExtractMapperUtil tdsExtractMapperUtil = new TDAExtractMapperUtil();
  private MultipleOutputs<Text, Text> mos;
  private HashSet<String> storeFilterSet;
  boolean filterOnStoreId = false;
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException{
     BufferedReader br = null;

    try {
    	
    	mos = new MultipleOutputs<Text, Text>(context);
    	
    	owshFltr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER);
    	if(owshFltr == null)
    		owshFltr = "*";
    
    	String filterOnStoreIdStr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_STORE_FILTER);
    	if(filterOnStoreIdStr != null && filterOnStoreIdStr.equalsIgnoreCase("true")){
    		filterOnStoreId = true;
    	}
    	
    	 Path[] distPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
         
//       URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
       
       if (distPaths == null){
     	  System.err.println("distpath is null");
       }
       
       Path distpath = null;
       if (distPaths != null && distPaths.length > 0)  {
//       if (uris != null && uris.length > 0)  {
     	  
     	  System.out.println(" number of distcache files : " + distPaths.length);
//     	  System.out.println(" number of distcache files : " + uris.length);
     	  
 	      for(int i=0;i<distPaths.length;i++){
// 	      for(int i=0;i<uris.length;i++){
 	    	  
 	    	  distpath = distPaths[i];
// 	    	  distpath = new Path(uris[i]);
 		     
 	    	  System.out.println("distpaths:" + distPaths[i].toString());
 	    	  System.out.println("distpaths URI:" + distPaths[i].toUri());
 	    	  
// 	    	  System.out.println("distpaths:" +distpath.toString());
 	    	  
 	    	  
 	    	  br  = new BufferedReader( new FileReader(distpath.toString())); 
 	    	  
 	    	 if(distpath.toUri().toString().contains("TDA_CODE_VALUES.psv")){
 	    	  
 	    	 try { 
 	    	      this.lookupCodes = new HashMap<String, String >();
 	    	      String line = "";
 	    	      int k = 0;
 	    	      while ( (line = br.readLine() )!= null) {
 	    	        String[] flds = line.split("\\|");
 	    	        if (flds.length == 2) {
 	    	          lookupCodes.put(flds[0].trim().toUpperCase(), flds[1].trim());
 	    	          k++;
 	    	        }   
 	    	      }
 	    	      System.out.println(" Added " + k + " values to the lookupCodes map");
 	    	    } catch (IOException e1) { 
 	    	      e1.printStackTrace();
 	    	      System.out.println("read from distributed cache: read length and instances");
 	    	    }finally{
 	    	    	try{
 	    	    		if(br != null)
 	    	    			br.close();
 	    	    	}catch(Exception ex){
 	    	    		ex.printStackTrace();
 	    	    	}
 	    	    }
 	    	 } else if(distpath.toUri().toString().contains("STORE_FILTER.txt")){
 		    	  
		    	 
		    	 
 		    	  storeFilterSet = new HashSet<String>();
 		    	  addValuesToSet(storeFilterSet, br);
 		      }
 	    	 
 	      }
       }
    	
//      Path[] distPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//      URI[] uris = context.getCacheFiles();
//      String[] fileParts;
//      for(int i=0;i<uris.length;i++){
//    	  if(uris[i].toString().contains("TDA_CODE_VALUES.psv")){
//	    	  fileParts = uris[i].toString().split("#");
//	    	  
////	    	  System.out.println(" uris["+i+"]"+ uris[i].toString());
//	    	  File fl = new File("./"+fileParts[1]);
//	    	  if(fl != null){
////	    		  System.out.println("file is not null");
//	    		  br  = new BufferedReader( new FileReader(fl));
//	    	  }
//    	  }
//    	  else if(uris[i].toString().contains("STORE_FILTER.txt")){
//	    	  
//		    	 
//		    	 
//	    	  storeFilterSet = new HashSet<String>();
//	    	  addValuesToSet(storeFilterSet, br);
//	      }
//      }
//      if (distPaths == null){
//    	  System.err.println("distpath is null");
//      }
//      
//      Path distpath = null;
//      if (distPaths != null && distPaths.length > 0)  {
//	      for(int i=0;i<distPaths.length;i++){
//	    	  distpath = distPaths[i];
//		      if(distpath.toUri().toString().contains("TDA_CODE_VALUES.psv")){
//		    	  System.out.println("distpaths:" + distPaths[i].toString());
//		    	  System.out.println("distpaths:" + distPaths[i].toUri());
//		      	  br  = new BufferedReader( new FileReader(distPaths[i].toString()));
//		      	  break;
//		      }
//		      
//	      }
//      }
      
     } catch (FileNotFoundException e1) {
      e1.printStackTrace();
      System.out.println("read from distributed cache: file not found!");
      throw new InterruptedException(e1.toString());
    } catch (IOException e1) {
      e1.printStackTrace();
      System.out.println("read from distributed cache: IO exception!");
      throw new InterruptedException(e1.toString());
    }

    
  }
  
 private void addValuesToSet(HashSet<String> valSet,BufferedReader br) throws InterruptedException{
	  
	  
	  try { 

	      String line = "";
	      
	      while ( (line = br.readLine() )!= null) {
	        String[] flds = line.split("\t",-1);
	        if (flds.length >= 2) {
	        	valSet.add(flds[0]+"_"+Integer.parseInt(flds[1]));
	        	
	        }   
	      }
	      
	      System.out.println("added " + valSet.size()  + " values to valSet");
	      
	      
	    } catch (IOException e) { 
	      e.printStackTrace();
	      System.out.println("read from distributed cache: read length and instances");
	      throw new InterruptedException(e.toString());
	    }finally{
	    	try{
	    		if(br != null)
	    			br.close();
	    	}catch(Exception ex){
//	    		ex.printStackTrace();
	    		 throw new InterruptedException(ex.toString());
	    	}
	    }
  }
 
  
  @Override
  public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

    DocumentBuilderFactory docFactory;
    DocumentBuilder docBuilder;
    InputSource xmlSource;
    Document xmlDoc;
    Element eleDetailedSos;
   
    try {
      String[] recArr = value.toString().split("\t");   
      String ownership = recArr[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS];
      if(!owshFltr.equals("*") && !owshFltr.equalsIgnoreCase(ownership)){
  	    context.getCounter("Debug", "SKIP OWSH FILTER").increment(1);
			return;
	  }
      String terrCdStoreId = recArr[DaaSConstants.XML_REC_TERR_CD_POS]+"_"+Integer.parseInt(recArr[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]);
      
      if(filterOnStoreId && !storeFilterSet.contains(terrCdStoreId)){
    	  context.getCounter("Count","SkippingonStoreFilter").increment(1);
    	  return;
      }
      
      
      docFactory = DocumentBuilderFactory.newInstance();
      docBuilder = docFactory.newDocumentBuilder();
      xmlSource = new InputSource(new StringReader(recArr[recArr.length -1]));
      xmlDoc = docBuilder.parse(xmlSource);

      eleDetailedSos = (Element)xmlDoc.getFirstChild();
      String storeId =  recArr[5];

      if ( eleDetailedSos.getNodeName().equals("DetailedSOS") ) {
        npParseXml(eleDetailedSos, context,storeId,ownership);
      }
    } catch (Exception ex) {
      context.getCounter("Debug", "parseError").increment(1);
      Logger.getLogger(NpSosXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      throw new InterruptedException(ex.toString());
    }
  }

  StringBuffer mosKey = new StringBuffer();
  private void npParseXml(Element eleDetailedSos,
                          Context context,String storeId,String ownership) throws IOException, InterruptedException {

    String gdwLgcyLclRfrDefCd;
    String gdwBusinessDate;
    NodeList nlStoreTotals;
    Element eleStoreTotals;
    String terrCd;
    
    

    try {
      gdwLgcyLclRfrDefCd = eleDetailedSos.getAttribute("gdwLgcyLclRfrDefCd");
      gdwBusinessDate = eleDetailedSos.getAttribute("gdwBusinessDate");
      terrCd = eleDetailedSos.getAttribute("gdwTerrCd");
     
      nlStoreTotals = eleDetailedSos.getChildNodes();

      if (nlStoreTotals != null && nlStoreTotals.getLength() > 0 ) {
        for (int idxStoreTotals=0; idxStoreTotals < nlStoreTotals.getLength(); idxStoreTotals++ ) {
          if ( nlStoreTotals.item(idxStoreTotals).getNodeType() == Node.ELEMENT_NODE ) {
            eleStoreTotals = (Element)nlStoreTotals.item(idxStoreTotals);

            if ( eleStoreTotals.getNodeName().equals("StoreTotals") ) {
              parseNode(eleStoreTotals, gdwLgcyLclRfrDefCd, gdwBusinessDate, terrCd,context,storeId,ownership);
            }
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(NpSosXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      throw new InterruptedException(ex.toString());
    }
  }

  private void parseNode(Element eleStoreTotals,
                         String gdwLgcyLclRfrDefCd,
                         String gdwBusinessDate,
                         String terrCd,
                         Context context,
                         String storeId,
                         String ownership) throws IOException, InterruptedException {

    String productionNodeIdText;
    String[] productionNodeIdParts;
    String productionNodeId;
    Element eleServiceTime = null;
    String orderKey;
//    String totalTimeText;
//    Element eleProductionTime;
//    String heldTimeText;
    BigDecimal totalTime = new BigDecimal(0.00);
    BigDecimal heldTime  = new BigDecimal(0.00);
    String outValue;
    
    
    long untilPay   =0;
    long untilServe =0;
    long holdtime;
    String[] orderKeyParts;
    String orderNumber;

    try {
      productionNodeIdText = eleStoreTotals.getAttribute("productionNodeId");
      productionNodeIdParts = (productionNodeIdText+":@@@@").split(":");
      productionNodeId = productionNodeIdParts[0];

      
      NodeList nlstrttlsList= eleStoreTotals.getChildNodes();
      for(int nlstrttlcnt=0;nlstrttlcnt < nlstrttlsList.getLength();nlstrttlcnt++  ){
      	
      	if(nlstrttlsList.item(nlstrttlcnt).getNodeType() == Node.ELEMENT_NODE){
      		eleServiceTime = (Element)nlstrttlsList.item(nlstrttlcnt);
      	  break;
      	}
      	
      }
      
//      eleServiceTime = (Element)eleStoreTotals.getFirstChild();

      orderKey = eleServiceTime.getAttribute("orderKey");
      
    //order number
      orderKeyParts = (orderKey+":@@@@").split(":");
      orderNumber = orderKeyParts[1];
      
      
//      totalTimeText = eleServiceTime.getAttribute("totalTime");
//      totalTime = new BigDecimal(totalTimeText,SCALE_4).divide(T1000);
      
     
      
//      eleProductionTime = (Element)eleServiceTime.getFirstChild();

//      heldTimeText = eleProductionTime.getAttribute("heldTime");
      try{
    	  if(eleServiceTime.getAttribute("untilPay") != null && !eleServiceTime.getAttribute("untilPay").isEmpty())
    		  untilPay   = Long.parseLong(eleServiceTime.getAttribute("untilPay"));
    	  if(eleServiceTime.getAttribute("untilServe") != null && !eleServiceTime.getAttribute("untilServe").isEmpty())
    		  untilServe = Long.parseLong(eleServiceTime.getAttribute("untilServe"));
      
      }catch(NumberFormatException ex){
    	  context.getCounter("Count","NumFormatExeption").increment(1);
      }
     
		
		
      totalTime = new BigDecimal(untilPay);
      
      heldTime = new BigDecimal(untilServe).subtract(new BigDecimal(untilPay)).divide(T1000,0,RoundingMode.HALF_UP);
      
      holdtime = untilServe-untilPay;
      
      if(holdtime < 0)
    	  holdtime = 0;
      
      if(heldTime.compareTo(bgzero) < 0){
    	  context.getCounter("Count","NegativeHeldTime").increment(1);
      }
    	  
//      if(heldTime.intValue() < 0){
//    	  System.out.println( " until pay : " + untilPay +" until serve : " + untilServe);
//    
//      }
      
//      if ( heldTimeText.length() > 0 ) {
//        heldTime = new BigDecimal(heldTimeText,SCALE_4).divide(T1000);
//      } else {
//        heldTime = T0;
//      }
      
      String posTypeCode = lookupCodes.get(terrCd+"-POS-"+productionNodeId.toUpperCase().trim());
      if(posTypeCode == null || posTypeCode.isEmpty()){
    	  posTypeCode =  tdsExtractMapperUtil.handleMissingCode(context, lookupCodes, "POS", productionNodeId.toUpperCase().trim(), terrCd, mos,"NEWTDACODEVALUES");
      }

      outValue = gdwLgcyLclRfrDefCd + "|" +
                 gdwBusinessDate + "|" +
                 productionNodeId + "|" + 
                 posTypeCode + "|"+
                 orderNumber + "|" +
                 TimeUtil.getTimeinMMSS(untilPay)+ "|" + //total time
//                 heldTime +"|"+
                 TimeUtil.getTimeinMMSS(holdtime)+"|"+
                 terrCd +"|"+
                 storeId+"|"+
                 ownership;

      mosKey.setLength(0);
      mosKey.append("SOS").append(DaaSConstants.SPLCHARTILDE_DELIMITER);
      mosKey.append(terrCd).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
      mosKey.append(gdwBusinessDate);
     
//      context.write(NullWritable.get(), new Text(outValue));
//      mos.write("SOS"+gdwBusinessDate,NullWritable.get(), new Text(outValue));
//      mos.write("SOS"+gdwBusinessDate,null, new Text(outValue));
      mos.write(mosKey.toString(),null, new Text(outValue));
    } catch (Exception ex) {
      Logger.getLogger(NpSosXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      throw new InterruptedException(ex.toString());
    }
  }
  
  @Override 
  protected void cleanup(Context contect) throws IOException, InterruptedException {
    mos.close();
  }
}

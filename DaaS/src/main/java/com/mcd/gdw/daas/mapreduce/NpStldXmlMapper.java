package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.PaymentMethod;
/**
 * 
 * @author Sateesh Pula
 * This mapper reads the STLD records and output fields that are part of TDA format sales file. 
 * It reads 3 distributed cache files in the setup and prepares a HashMap for each file. These values are
 * used when creating the output record. It outputs 2 records. One is the HDR format and one is the  DTL format.
 * 
 */

public class NpStldXmlMapper extends Mapper<LongWritable, Text, Text, Text> {

  private HashMap<String, String> tdacodesLookupMap;
  private HashMap<String, String> itemcodesLookupMap;
  private HashMap<Integer, String> menuPriceBasisMap;
  private HashSet<String> storeFilterSet;
  
  private MultipleOutputs<Text, Text> mos;
  BigDecimal bgzero = new BigDecimal("0.00");
  
  private String OUTPUT_FIELD_SEPERATOR = "|";
  private String owshFltr = "*";
  Context thiscontext;
  
  String ts = DaaSConstants.SDF_yyyyMMddHHmmssSSSnodashes.format(new Date(System.currentTimeMillis()));
  
  TDAExtractMapperUtil tdaExtractMapperUtil = new TDAExtractMapperUtil();
  Text newkey = new Text();
  Text newvalue = new Text();
  public class TransDetails {
  
    public int qty;
    public int qtyPromo;
    public BigDecimal totalDiscount;
    public BigDecimal totalPromo;
    public String tenderName;

    public TransDetails() {
      
      qty = 0;
      qtyPromo = 0;
      totalDiscount = new BigDecimal("0.00");
      totalPromo = new BigDecimal("0.00");
      tenderName = "@@@@";
    }
  }

  boolean generatFieldsForNielsen = false;
  boolean filterOnStoreId = false;
  
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException{
     BufferedReader br = null;

    try {
    	
    	thiscontext = context;
    	
    	owshFltr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER);
    	if(owshFltr == null)
    		owshFltr = "*";
    	
    	String generatFieldsForNielsenStr = context.getConfiguration().get("generatFieldsForNielsen");
    	if(generatFieldsForNielsenStr != null && generatFieldsForNielsenStr.equalsIgnoreCase("true")){
    		generatFieldsForNielsen = true;
    	}
    	
    	String filterOnStoreIdStr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_STORE_FILTER);
    	if(filterOnStoreIdStr != null && filterOnStoreIdStr.equalsIgnoreCase("true")){
    		filterOnStoreId = true;
    	}
    	
    	
    	mos = new MultipleOutputs<Text, Text>(context);
    	
      Path[] distPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
      
//      URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
      
      if (distPaths == null){
    	  System.err.println("distpath is null");
      }
      
//      if(uris == null){
//    	  System.err.println("distpath is null");
//      }
      
      Path distpath = null;
      if (distPaths != null && distPaths.length > 0)  {
//      if (uris != null && uris.length > 0)  {
    	  
    	  System.out.println(" number of distcache files : " + distPaths.length);
//    	  System.out.println(" number of distcache files : " + uris.length);
    	  
	      for(int i=0;i<distPaths.length;i++){
//	      for(int i=0;i<uris.length;i++){
	    	  
	    	  distpath = distPaths[i];
//	    	  distpath = new Path(uris[i]);
		     
	    	  System.out.println("distpaths:" + distPaths[i].toString());
	    	  System.out.println("distpaths URI:" + distPaths[i].toUri());
	    	  
//	    	  System.out.println("distpaths:" +distpath.toString());
	    	  
	    	  
	    	  br  = new BufferedReader( new FileReader(distpath.toString())); 
//	    	  br = new BufferedReader(new InputStreamReader(fs.open(path)));
	    	  
	    	  //handle TDA code dist cache file
	    	  if(distpath.toUri().toString().contains("TDA_CODE_VALUES.psv")){
		    	  		      	  
//	    		  br  = new BufferedReader( new FileReader(distpath.toString())); 
		      	  
		      	  tdacodesLookupMap  = new HashMap<String,String>();
		      	  addKeyValuestoMap(tdacodesLookupMap, br);
		      	  
		      }else if(distpath.toUri().toString().contains("ITEMCODE_MAPPING.psv")){
		    	  
//		    	  br  = new BufferedReader( new FileReader(distpath.toString())); 
		    	  
		    	  itemcodesLookupMap = new HashMap<String,String>();
		    	  addKeyValuestoMap(itemcodesLookupMap, br);
//		    	  System.out.println( " itemcodesLookupMap size   " + itemcodesLookupMap.size());
		      }else if(distpath.toUri().toString().contains("MenuPriceBasis.psv")){
		    	  
		    	 
		    	  
		    	  menuPriceBasisMap = new HashMap<Integer,String>();
		    	  addKeyValuestoMap2(menuPriceBasisMap, br);
		      }else if(distpath.toUri().toString().contains("STORE_FILTER.txt")){
		    	  
		    	 
		    	 
		    	  storeFilterSet = new HashSet<String>();
		    	  addValuesToSet(storeFilterSet, br);
		      }
	    	  
		      
	      }
      }
      
     } catch (FileNotFoundException e1) {
      e1.printStackTrace();
      System.out.println("read from distributed cache: file not found!");
    } catch (IOException e1) {
      e1.printStackTrace();
      System.out.println("read from distributed cache: IO exception!");
    }finally{
    	try{
    		if(br != null)
    			br.close();
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}
    }

//    try { 
//      this.lookupCodes = new HashMap<String, String >();
//      String line = "";
//      while ( (line = br.readLine() )!= null) {
//        String[] flds = line.split("\\|");
//        if (flds.length == 2) {
//          lookupCodes.put(flds[0].trim().toUpperCase(), flds[1].trim());
//        }   
//      }
//    } catch (IOException e1) { 
//      e1.printStackTrace();
//      System.out.println("read from distributed cache: read length and instances");
//    }finally{
//    	try{
//    		if(br != null)
//    			br.close();
//    	}catch(Exception ex){
//    		ex.printStackTrace();
//    	}
//    }
  }
 
  //Adds key values pairs to the provided hashmap and closes the buffered reader
  private void addValuesToSet(HashSet<String> valSet,BufferedReader br){
	  
	  
	  try { 

	      String line = "";
	      while ( (line = br.readLine() )!= null) {
	        String[] flds = line.split("\t",-1);
	        if (flds.length >= 2) {
	        	valSet.add(flds[0]+"_"+Integer.parseInt(flds[1]));
	        }   
	      }
	    } catch (IOException e) { 
	      e.printStackTrace();
	      System.out.println("read from distributed cache: read length and instances");
	    }finally{
	    	try{
	    		if(br != null)
	    			br.close();
	    	}catch(Exception ex){
	    		ex.printStackTrace();
	    	}
	    }
  }
  
  
  //Adds key values pairs to the provided hashmap and closes the buffered reader
  private void addKeyValuestoMap(HashMap<String,String> keyvalMap,BufferedReader br){
	  
	  
	  try { 

	      String line = "";
	      while ( (line = br.readLine() )!= null) {
	        String[] flds = line.split("\\|");
	        if (flds.length == 2) {
	        	keyvalMap.put(flds[0].trim().toUpperCase(), flds[1].trim());
	        }   
	      }
	    } catch (IOException e) { 
	      e.printStackTrace();
	      System.out.println("read from distributed cache: read length and instances");
	    }finally{
	    	try{
	    		if(br != null)
	    			br.close();
	    	}catch(Exception ex){
	    		ex.printStackTrace();
	    	}
	    }
  }
  
 private void addKeyValuestoMap2(HashMap<Integer,String> keyvalMap,BufferedReader br){
	  
	  
	  try { 

	      String line = "";
	      while ( (line = br.readLine() )!= null) {
	        String[] flds = line.split("\\|");
	        if (flds.length == 2) {
	        	keyvalMap.put(new Integer(flds[0].trim()), flds[1].trim());
	        }   
	      }
	    } catch (IOException e) { 
	      e.printStackTrace();
	      System.out.println("read from distributed cache: read length and instances");
	    }finally{
	    	try{
	    		if(br != null)
	    			br.close();
	    	}catch(Exception ex){
	    		ex.printStackTrace();
	    	}
	    }
  }
 
  @Override
  public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

	
	
    DocumentBuilderFactory docFactory;
    DocumentBuilder docBuilder;
    InputSource xmlSource;
    Document xmlDoc;
    Element eleTLD;

    try {
      String[] recArr = value.toString().split("\t");    
      String ownership = recArr[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS];
      if(!owshFltr.equals("*") && !owshFltr.equalsIgnoreCase(recArr[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS])){
    	    context.getCounter("Count", "SKIP OWSH FILTER").increment(1);
			return;
  	  }
      
      String terrCdStoreId = recArr[DaaSConstants.XML_REC_TERR_CD_POS]+"_"+Integer.parseInt(recArr[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]);
      
      if(filterOnStoreId && !storeFilterSet.contains(terrCdStoreId)){
    	  context.getCounter("Count","SkippingonStoreFilter").increment(1);
    	  return;
      }
      
//      String decompstr = decompressString(recArr[recArr.length -1]);
      
      docFactory = DocumentBuilderFactory.newInstance();
      docBuilder = docFactory.newDocumentBuilder();
      xmlSource = new InputSource(new StringReader(recArr[recArr.length -1]));
//      xmlSource = new InputSource(new StringReader(decompstr));
      xmlDoc = docBuilder.parse(xmlSource);

      eleTLD = (Element)xmlDoc.getFirstChild();

      if ( eleTLD.getNodeName().equals("TLD") ) {
        npParseXml(eleTLD, context,ownership);
      }
    } catch (Exception ex) {
    	
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      
      throw new InterruptedException(ex.toString());
    }
  }

  private void npParseXml(Element eleTLD, Context context,String ownership) throws IOException, InterruptedException {

    String gdwLgcyLclRfrDefCd;
    String gdwBusinessDate;
    String gdwTerrCd;
    NodeList nlNode;
    Element eleNode;
    String storeId;

    try {
      gdwLgcyLclRfrDefCd = eleTLD.getAttribute("gdwLgcyLclRfrDefCd");
      gdwBusinessDate = eleTLD.getAttribute("gdwBusinessDate");
      gdwTerrCd = eleTLD.getAttribute("gdwTerrCd");
      storeId   = eleTLD.getAttribute("storeId");
//      if(!storeId.equalsIgnoreCase("865") || !gdwBusinessDate.equalsIgnoreCase("20120803"))
//    	  return;

      nlNode = eleTLD.getChildNodes();

      if (nlNode != null && nlNode.getLength() > 0 ) {
        for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
          if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {
            eleNode = (Element)nlNode.item(idxNode);

            if ( eleNode.getNodeName().equals("Node") ) {
              parseNode(eleNode, storeId,gdwLgcyLclRfrDefCd, gdwBusinessDate, gdwTerrCd, context,ownership);
            }
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      throw new InterruptedException(ex.toString());
    }
  }

  StringBuffer hdrKeyBf = new StringBuffer();
  
  private void parseNode(Element eleNode,
		  				 String storeId,
                         String gdwLgcyLclRfrDefCd,
                         String gdwBusinessDate,
                         String gdwTerrCd,
                         Context context,
                         String ownership) throws IOException, InterruptedException {

    String nodeId;
    String regId;
    NodeList nlEvent;
    Element eleEvent;
    String eventType;
    Element eleTrxSale;
    String trxSaleStatus;
    String trxSalePod;
    Element eleOrder = null;
    String orderTimestamp;
    String orderKey;
    String orderKind;
    BigDecimal totalAmount;
    BigDecimal totalTax;
    BigDecimal totalAmountPlusTax;
    String orderDate;
    String orderTime;
    String[] orderKeyParts;
    String orderNumber;
    String orderSaleType;
    String transactionType;
    String outValue;
    TransDetails tranDetails;
    String posTypeCode;
    String dlvrMethCode;
    String trnTypeCode;
    String pymtMethCode;
    
    
    //new
    Element eleTrxEvent = null;
    String takeId ;
    String splTransId;
    
    try {
      nodeId = eleNode.getAttribute("id");
      //cash register number
      regId = String.valueOf(Integer.parseInt(nodeId.substring(3)));

      nlEvent = eleNode.getChildNodes();

      if ( nlEvent !=null && nlEvent.getLength() > 0 ) {
        for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++ ) {
          if ( nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE ) {
        	  
        	  context.getCounter("Count","TotalEvents").increment(1);
        	  
            eleEvent = (Element)nlEvent.item(idxEvent);

            if ( eleEvent.getNodeName().equals("Event") ) {
              eventType = eleEvent.getAttribute("Type");

              if ( eventType.equals("TRX_Sale") || 
            		  eventType.equals("TRX_Refund") || 
            		 eventType.equals("TRX_Overring") || 
                    		  eventType.equals("TRX_Waste") ){
            	  
            	  context.getCounter("Count","TRXSALEREFUNDWASTE").increment(1);
            	  
            	  NodeList nevntchldList = eleEvent.getChildNodes();
            	  
            	  for(int chldcnt=0;chldcnt < nevntchldList.getLength();chldcnt++){
            		  if(nevntchldList.item(chldcnt).getNodeType() == Node.ELEMENT_NODE){
            			  eleTrxEvent = (Element)nevntchldList.item(chldcnt);
            			  break;
            		  }
            	  }//end child nodes loop
//            	  eleTrxEvent = (Element)eleEvent.getFirstChild();
            	  
            	  if(eleTrxEvent != null){
                  trxSaleStatus = eleTrxEvent.getAttribute("status");
                  
                  NodeList ntrxchldList = eleTrxEvent.getChildNodes();
            	  
            	  for(int trxchldcnt=0;trxchldcnt < ntrxchldList.getLength();trxchldcnt++){
            		  if(ntrxchldList.item(trxchldcnt).getNodeType() == Node.ELEMENT_NODE){
            			  eleOrder = (Element)ntrxchldList.item(trxchldcnt);
            			  break;
            		  }
            	  }//end child nodes loop
            	  	
//                  eleOrder = (Element)eleTrxEvent.getFirstChild();
            	  
                  orderTimestamp = eleOrder.getAttribute("Timestamp");
                  orderKey = eleOrder.getAttribute("key");
                  
                  context.getCounter("Count","NumOrders").increment(1);

                  //Sateesh: do we need to check for this condition. Will we always have status ( refund??)
                  if ( (eventType.equals("TRX_Sale") && trxSaleStatus.equals("Paid")) ||
                		  (eventType.equals("TRX_Refund") || 
                         		 eventType.equals("TRX_Overring") || 
                       		  eventType.equals("TRX_Waste")) ) {
                    
                	  context.getCounter("Count","NumOrdersPaidRefundStatus").increment(1);
                   
                	//order number
                      orderKeyParts = (orderKey+":@@@@").split(":");
                      orderNumber = orderKeyParts[1];
                      
                    
//                	  if(Long.parseLong(orderNumber) != 145303043){
//                      	context.getCounter("Count", "SkippingOrders").increment(1);
//                      	return;
//                      }else{
//                      	context.getCounter("Count", "ValidOrders").increment(1);
//                      }
                	  
                    //do we need to output only if there is a timestamp
                    if ( orderTimestamp.length() >= 12 ) {  
                        //order date
                        orderDate = orderTimestamp.substring(0,8);
                        
                        //order time
                        orderTime = orderTimestamp.substring(8,12);
                        
                        //register type
	                    trxSalePod = eleTrxEvent.getAttribute("POD");
	                    
	                    //Sateesh: revisit this logic do we need to consider TRX_Cashout
	                    //
	                    posTypeCode = tdacodesLookupMap.get(gdwTerrCd+"-POS-" + trxSalePod.trim().toUpperCase());
	                    
	                    
	                    if(posTypeCode == null || posTypeCode.isEmpty()){
//	                    	posTypeCode = "Missingpostyp"+trxSalePod;
	                    	posTypeCode = tdaExtractMapperUtil.handleMissingCode(thiscontext,tdacodesLookupMap,"POS", trxSalePod, gdwTerrCd,mos,"NEWTDACODEVALUES");
	                    	
	                    }
	                    
//	                    System.out.println(" looing for    *  "+  trxSalePod.trim().toUpperCase());
	                    //take id
	                    orderSaleType =  eleOrder.getAttribute("saleType");
//	                    if(orderSaleType != null && orderSaleType.equalsIgnoreCase("TakeOut")){
//	                    	takeId = 1;
//	                    }
	                    
	                    takeId = tdacodesLookupMap.get(gdwTerrCd+"-DLVR-"+orderSaleType.toUpperCase());
	                    
	                    if(takeId == null || takeId.isEmpty()){
//	                    	takeId = "Missingtakeid"+orderSaleType;
	                    	
	                    	takeId = tdaExtractMapperUtil.handleMissingCode(thiscontext,tdacodesLookupMap,"DLVR", orderSaleType, gdwTerrCd,mos,"NEWTDACODEVALUES");
	                    	
	                    	context.getCounter("Count","Missingtakeid-"+orderSaleType);
	                    }
	                    
	                    tranDetails = parseOrderDetails(eleOrder, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber,storeId,gdwTerrCd, context,ownership); 
	                    
	                    //special transaction id
	                    orderKind = eleOrder.getAttribute("kind").toUpperCase();
//	                    splTransId = "0";
	                    
//	                    if(isSaleEventRecalled(eleOrder)){
//	                    	splTransId = "1";
//	                    }
	                   
	                    //Sateesh: verify lookup codes
	                   
	                	String splLookupKey = "" ;
	                    
                    	if(isSaleEventRecalled(eleOrder)){
                    		 if(orderKind != null && 
     	                    		tranDetails.totalPromo != null && tranDetails.totalPromo.compareTo(bgzero) > 0){
                    			 
                    			 splLookupKey = "RECALLED - PROMO "+orderKind.toUpperCase();
                    			 
     	                    	splTransId = tdacodesLookupMap.get(gdwTerrCd+"-SPLTRX-RECALLED - PROMO "+orderKind.toUpperCase());
     	                    	if(splTransId == null)
     	                    		context.getCounter("Count","MissingOrderKind:"+gdwTerrCd+"-SPLTRX-RECALLED - PROMO "+orderKind.toUpperCase()).increment(1);
     	                    }else{
     	                    	
     	                    	splLookupKey = "RECALLED - "+orderKind.toUpperCase();
     	                    	splTransId = tdacodesLookupMap.get(gdwTerrCd+"-SPLTRX-RECALLED - "+orderKind.toUpperCase());//make sure a 2 digit is added in the map
     	                    	if(splTransId == null)
     	                    		context.getCounter("Count","MissingOrderKind:"+gdwTerrCd+"-SPLTRX-RECALLED - "+orderKind.toUpperCase()).increment(1);
     	                    }
                    		 
                    		 
 	                    }else{
 	                    	
// 	                    	System.out.println( "promo amt : " + tranDetails.totalPromo.toString() + " order kind " + orderKind);
 	                    
 	                    	
 	                    	if(orderKind != null && 
     	                    		tranDetails.totalPromo != null && tranDetails.totalPromo.compareTo(bgzero) > 0){
 	                    		
 	                    		splLookupKey = "PROMO "+orderKind.toUpperCase();
 	                    				
     	                    	splTransId = tdacodesLookupMap.get(gdwTerrCd+"-SPLTRX-PROMO "+orderKind.toUpperCase());
     	                    	if(splTransId == null)
     	                    		context.getCounter("Count","MissingOrderKind:"+gdwTerrCd+"-SPLTRX-PROMO "+orderKind.toUpperCase()).increment(1);
     	                    		
     	                    }else{
     	                    	
     	                    	splLookupKey = orderKind.toUpperCase();
     	                    	
     	                    	splTransId = tdacodesLookupMap.get(gdwTerrCd+"-SPLTRX-"+orderKind.toUpperCase());//make sure a 2 digit is added in the map
     	                    	if(splTransId == null)
     	                    		context.getCounter("Count","MissingOrderKind:"+gdwTerrCd+"-SPLTRX-"+orderKind.toUpperCase()).increment(1);
     	                    }
 	                    }
                    	
                    	if(splTransId == null || splTransId.isEmpty()){
                    		splTransId = "Missingspltrx"+orderKind;
                    		
                    		splTransId = tdaExtractMapperUtil.handleMissingCode(thiscontext,tdacodesLookupMap,"SPLTRX", splLookupKey, gdwTerrCd,mos,"NEWTDACODEVALUES");
                    	}
	                   
	                    
	                
	                    
	                    //discount amount
	                    String totalDiscount = tranDetails.totalDiscount.setScale(2, RoundingMode.HALF_UP).toString();
	                    
	                    //promo amount
	                    String totalPromo    = tranDetails.totalPromo.setScale(2, RoundingMode.HALF_UP).toString();
	                    
	                    //total amount 
	                    //sateesh :  do we need to add the tax?
	                 
	                    totalAmount = new BigDecimal(eleOrder.getAttribute("totalAmount")).add(new BigDecimal(totalDiscount));
//	                    totalTax = new BigDecimal(eleOrder.getAttribute("totalTax"));
//	                    totalAmountPlusTax = totalAmount.add(totalTax);
	                    
	                    
	                    //menu price basis
//	                    String menuPriceBasis = "N";
//	                    if(gdwTerrCd.equals("40") || gdwTerrCd.equals("040")){
//	                    	menuPriceBasis = "G";
//	                    }
	                    
	                    String menuPriceBasis = menuPriceBasisMap.get(new Integer(gdwTerrCd));
	                    
	                    //tender type id. what is the logic here lookup file
	                  
//                        if ( tdacodesLookupMap.get(gdwTerrCd + "-PYMT-" + tranDetails.tenderName.toUpperCase()) == null ) {
//                          pymtMethCode = tranDetails.tenderName;
//                        } else {
                          pymtMethCode = tdacodesLookupMap.get(gdwTerrCd + "-PYMT-" + tranDetails.tenderName.toUpperCase());
//                        }
	                     
                        if(pymtMethCode == null || pymtMethCode.isEmpty()){
                        	pymtMethCode = "Missing"+tranDetails.tenderName;
                        	pymtMethCode = tdaExtractMapperUtil.handleMissingCode(thiscontext,tdacodesLookupMap,"PYMT", tranDetails.tenderName.toUpperCase(), gdwTerrCd,mos,"NEWTDACODEVALUES");
                        	context.getCounter("Count","MissingPYMTType:"+gdwTerrCd+"-"+tranDetails.tenderName.toUpperCase()).increment(1);
                        	
	                    
                        }
	                    outValue = storeId + OUTPUT_FIELD_SEPERATOR +
             		    gdwLgcyLclRfrDefCd + OUTPUT_FIELD_SEPERATOR +
                        regId + OUTPUT_FIELD_SEPERATOR + 
                        orderNumber + OUTPUT_FIELD_SEPERATOR + 
                        gdwBusinessDate + OUTPUT_FIELD_SEPERATOR +
                        orderDate + OUTPUT_FIELD_SEPERATOR + 
                        orderTime + OUTPUT_FIELD_SEPERATOR + 
                        posTypeCode + OUTPUT_FIELD_SEPERATOR + //drive id
                        takeId + OUTPUT_FIELD_SEPERATOR + //take id
                        splTransId + OUTPUT_FIELD_SEPERATOR + //
                        totalDiscount + OUTPUT_FIELD_SEPERATOR +
                        totalPromo + OUTPUT_FIELD_SEPERATOR +
//                        totalAmountPlusTax.setScale(2, RoundingMode.HALF_UP).toString() + OUTPUT_FIELD_SEPERATOR +
						totalAmount.setScale(2, RoundingMode.HALF_UP).toString() + OUTPUT_FIELD_SEPERATOR +
                        menuPriceBasis+OUTPUT_FIELD_SEPERATOR + 
                        pymtMethCode + OUTPUT_FIELD_SEPERATOR +
//                        orderKey;
                        gdwTerrCd+OUTPUT_FIELD_SEPERATOR;//+
//                        ownership;
	                    
	                    

	                    hdrKeyBf.setLength(0);
	                    hdrKeyBf.append("HDR").append(DaaSConstants.SPLCHARTILDE_DELIMITER);
	                    hdrKeyBf.append(gdwTerrCd).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
	                    hdrKeyBf.append(gdwBusinessDate);
	                    
//	                    context.write(new Text("HDR"), new Text(outValue));
	                    newkey.clear();
                    	newkey.set(hdrKeyBf.toString());
                    	
	                    newvalue.clear();
	                    newvalue.set(outValue);
	                    
//	                    if(Integer.parseInt(gdwTerrCd)  != 156){
		                    if(generatFieldsForNielsen){
		                    	
		                    	context.write(newkey, newvalue);
		                    }else{
		                    	mos.write(hdrKeyBf.toString(),null, newvalue);
		                    }
//	                    }else{
//	                    	System.out.println("***************writing out SALES files *******************");
//	                    	mos.write(hdrKeyBf.toString(),null, newvalue,"/daas/tdaextracts/salesandpmix_CHINAAPT/"+gdwBusinessDate+"/");
//	                    }
	                    context.getCounter("Count", "ValidHDRRecords").increment(1);
	                    
                    }//end timestamp check
                    
                  }//end paid/refund
            	  }//end if transaction element
            	  }//end sale/refund/overring/waste
          }
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      throw new InterruptedException(ex.toString());
    }
  }

  private TransDetails parseOrderDetails(Element eleOrder, String gdwLgcyLclRfrDefCd, String regId, String gdwBusinessDate, String orderNumber,String storeId,String gdwTerrCd, Context context,String ownership) throws IOException, InterruptedException {

    TransDetails retTransDetails = new TransDetails();
    String outValue; 

    NodeList nlOrderDetails;
    Element eleOrderDetails;
 
    NodeList nlItem1;
    Element eleItem1;
    NodeList nlItem2;
    Element eleItem2;

    try {
      nlOrderDetails = eleOrder.getChildNodes();

      if ( nlOrderDetails !=null && nlOrderDetails.getLength() > 0 ) {
        for (int idxOrderDetails=0; idxOrderDetails < nlOrderDetails.getLength(); idxOrderDetails++ ) {
          if ( nlOrderDetails.item(idxOrderDetails).getNodeType() == Node.ELEMENT_NODE ) {
            eleOrderDetails = (Element)nlOrderDetails.item(idxOrderDetails);
            if ( eleOrderDetails.getNodeName().equals("Item") ) {
              nlItem1 = processItems(eleOrderDetails, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, retTransDetails, storeId,gdwTerrCd,context,ownership);

              if ( nlItem1 !=null && nlItem1.getLength() > 0 ) {
                for (int idxItem1=0; idxItem1 < nlItem1.getLength(); idxItem1++ ) {
                  if ( nlItem1.item(idxItem1).getNodeType() == Node.ELEMENT_NODE ) {
                    eleItem1 = (Element)nlItem1.item(idxItem1);
                    if ( eleItem1.getNodeName().equals("Item") ) {
                      nlItem2 = processItems(eleItem1, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, retTransDetails, storeId,gdwTerrCd,context,ownership);
 
                      if ( nlItem2 !=null && nlItem2.getLength() > 0 ) {
                        for (int idxItem2=0; idxItem2 < nlItem2.getLength(); idxItem2++ ) {
                          if ( nlItem2.item(idxItem2).getNodeType() == Node.ELEMENT_NODE ) {
                            eleItem2 = (Element)nlItem2.item(idxItem2);
                            if ( eleItem2.getNodeName().equals("Item") ) {
                              processItems(eleItem2, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, retTransDetails, storeId,gdwTerrCd,context,ownership);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }

            if ( eleOrderDetails.getNodeName().equals("Tenders") ) {
              retTransDetails.tenderName = getTenderName(eleOrderDetails,context);
            }
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      throw new InterruptedException(ex.toString());
    }

    if ( retTransDetails.tenderName.equals("@@@@") && retTransDetails.qtyPromo > 0 && retTransDetails.qtyPromo >= retTransDetails.qty ) {
      retTransDetails.tenderName = "Promo";
    }

    return(retTransDetails);
  }

  StringBuffer outValueBf = new StringBuffer();
  StringBuffer pmixKeyBf  = new StringBuffer();
  private NodeList processItems(Element eleItem,
                                String gdwLgcyLclRfrDefCd,
                                String regId,
                                String gdwBusinessDate,
                                String orderNumber,
                                TransDetails retTransDetails,
                                String storeId,
                                String gdwTerrCd,
                                Context context,
                                String ownership) throws InterruptedException{

    String outValue;
    String code;
    String mappedCode;
    String type;
    String qtyText;
    String qtyPromoText;
    BigDecimal qty;
    BigDecimal qtyPromo;
    BigDecimal totalPrice;
    BigDecimal bdPrice;
    BigDecimal bpPrice;
    BigDecimal bdTax;
    BigDecimal bpTax;
    BigDecimal vat = new BigDecimal("0.00");
    Element eleTaxChain = null;
//    NodeList nlSubItem = null;
    
    
    int displayOrder = -1; //src field
    BigDecimal netAmount = new BigDecimal(0);//calc field
    BigDecimal taxAmount = new BigDecimal(0);//calc field
    
    BigDecimal bdBaseAmount;//src field
    BigDecimal bpBaseAmount;//src field
    BigDecimal baseAmount;//src field
    
    BigDecimal bdAmount;//src field
    BigDecimal bpAmount;//src field
    BigDecimal amount;//src field
    
    String grillQtyTxt;
    String grillModifierTxt;
    
    String itemleveltxt;
    String itemaction;
    BigDecimal itemlevel;
    
    String grillModifier;
    String grillQty;
    
    
    context.getCounter("Count","NumofTimesProcessItemsCalled").increment(1);
    try { 
      qtyText = eleItem.getAttribute("qty");
      qty = new BigDecimal(qtyText);
      qtyPromoText = eleItem.getAttribute("qtyPromo");
      qtyPromo = new BigDecimal(qtyPromoText);
      type = eleItem.getAttribute("type");
      displayOrder = 0;
      
      if(eleItem.getAttribute("displayOrder") != null && !eleItem.getAttribute("displayOrder").isEmpty())
    	  displayOrder = Integer.parseInt(eleItem.getAttribute("displayOrder"));
      
      itemleveltxt = eleItem.getAttribute("level");
      itemaction   = eleItem.getAttribute("action");
      itemlevel = new BigDecimal(itemleveltxt);
      
      grillModifier = eleItem.getAttribute("grillModifier");
      grillQty = eleItem.getAttribute("grillQty");
      
      code = eleItem.getAttribute("code");
      
//      if ( !(type.equalsIgnoreCase("PRODUCT") || type.equalsIgnoreCase("VALUE_MEAL") ||
//    		  type.equalsIgnoreCase("NON_FOOD_PRODUCT")) ){
//    	  context.getCounter("Count","SkippedRecordsonType "+type).increment(1);
//      }
//      
//      if(! (qty.compareTo(new BigDecimal("0")) > 0 || 
//    				  qtyPromo.compareTo(new BigDecimal("0")) > 0) ){
//    	  context.getCounter("Count","SkippedRecordsonqty ").increment(1);
//      }
//      
//      if(!( (itemlevel != null && itemlevel.compareTo(bgzero) == 0) ||
//    				  "CHOICE".equalsIgnoreCase(itemaction)) ){
//    	  context.getCounter("Count","SkippedRecordsonaction ").increment(1);
//      }
      
      if ( (type.equalsIgnoreCase("PRODUCT") || type.equalsIgnoreCase("VALUE_MEAL") ||
    		  type.equalsIgnoreCase("NON_FOOD_PRODUCT")) && 
    	
    		  (qty.compareTo(new BigDecimal("0")) > 0 || 
    				  qtyPromo.compareTo(new BigDecimal("0")) > 0) 
    				  &&
    		  ( (itemlevel != null && itemlevel.compareTo(bgzero) == 0) ||
    				  "CHOICE".equalsIgnoreCase(itemaction)) 
    				  &&
//    		code != null && !code.isEmpty() && Integer.parseInt(code) <= 9999	&&
    				grillModifier != null && !grillModifier.isEmpty() && (Integer.parseInt(grillModifier) ==0 && 
    			    grillQty != null && !grillQty.isEmpty() &&  Integer.parseInt(grillQty) == 0)  &&
    	   displayOrder != -1
    	   ){
    	  
    	if( (Integer.parseInt(gdwTerrCd) == 840 && (code != null && !code.isEmpty() && Integer.parseInt(code) <= 9999)) || (Integer.parseInt(gdwTerrCd) != 840) ){
    		context.getCounter("Count","IncludingItemCodeGT9999").increment(1);
        retTransDetails.qty = Integer.parseInt(qtyText);
        retTransDetails.qtyPromo = Integer.parseInt(qtyPromoText);
         
        
//        System.out.println("replace item code " + context.getConfiguration().get("replace.itemcode"));
        
        if("true".equalsIgnoreCase(context.getConfiguration().get("replace.itemcode"))){
        	mappedCode = itemcodesLookupMap.get(gdwTerrCd+"-"+code.trim().toUpperCase());
	        if(mappedCode == null){
	        	mappedCode = code;
	        	context.getCounter("Count", "MissingItemCodeReplacement").increment(1);
	        }else{
	        	context.getCounter("Count", "FoundItemCodeReplacement").increment(1);
	        }
        }else{
        	mappedCode =code;
        	
        }
        
        //sateesh: should we consider tax here
        totalPrice = new BigDecimal(eleItem.getAttribute("totalPrice"));//.add(new BigDecimal(eleItem.getAttribute("totalTax")));
        bdPrice =  new BigDecimal(eleItem.getAttribute("BDPrice"));//.add(new BigDecimal(eleItem.getAttribute("BDTax")));
        bpPrice =  new BigDecimal(eleItem.getAttribute("BPPrice"));//.add(new BigDecimal(eleItem.getAttribute("BPTax")));
        bdTax 	= new BigDecimal(eleItem.getAttribute("BDTax"));
        bpTax 	= new BigDecimal(eleItem.getAttribute("BPTax"));
                     
        if ( bdPrice.compareTo(bgzero) > 0 ) {
          retTransDetails.totalDiscount = retTransDetails.totalDiscount.add(bdPrice.subtract(totalPrice));
        }
              
//        if ( bpPrice.compareTo(new BigDecimal("0.00")) > 0 ) {
//          retTransDetails.totalPromo = retTransDetails.totalPromo.add(bpPrice.subtract(totalPrice));
//        }
        
        if ( bdPrice != null && bdPrice.compareTo(bgzero) > 0 ) {
        	if(bpPrice.subtract(bdPrice).compareTo(bgzero) > 0)// ignore less than zero values
        		retTransDetails.totalPromo = retTransDetails.totalPromo.add(bpPrice.subtract(bdPrice));
        }else{
        	if(bpPrice.subtract(totalPrice).compareTo(bgzero) > 0)//ignore less than zero values
        		retTransDetails.totalPromo = retTransDetails.totalPromo.add(bpPrice.subtract(totalPrice));
        }
        
        //menu item price
        BigDecimal menuItemPrice = new BigDecimal(0);
        if(bpPrice != null && bpPrice.compareTo(bgzero) > 0 && qty.compareTo(bgzero) > 0) {
        	menuItemPrice =  bpPrice.divide(qty,2,RoundingMode.HALF_UP);
        }else if(bdPrice != null && bdPrice.compareTo(bgzero) > 0 && qty.compareTo(bgzero) > 0){
        	menuItemPrice = bdPrice.divide(qty,2,RoundingMode.HALF_UP);
        }else if(totalPrice != null && totalPrice.compareTo(bgzero) > 0 && qty.compareTo(bgzero) > 0){
        	menuItemPrice = totalPrice.divide(qty,2,RoundingMode.HALF_UP);
        }
        
                     
//        nlSubItem = eleItem.getChildNodes();
        NodeList nlSubItemList= eleItem.getChildNodes();
        
        for(int nlsubitemcnt=nlSubItemList.getLength()-1;nlsubitemcnt >= 0;nlsubitemcnt--  ){
        	
        	if(nlSubItemList.item(nlsubitemcnt).getNodeType() == Node.ELEMENT_NODE){
        	  eleTaxChain = (Element)nlSubItemList.item(nlsubitemcnt);
        	  break;
        	}
        	
        	
        	
        }
        
//        eleTaxChain = (Element)nlSubItem.item(nlSubItem.getLength()-1);
        
        if (eleTaxChain != null &&  eleTaxChain.getNodeName().equals("TaxChain") ) { 
//          vat = new BigDecimal(eleTaxChain.getAttribute("rate"));
                
        	
        	
        	
        	
//        	if(orderNumber.equalsIgnoreCase("309716386")){
//        		System.out.println(" bdBaseAmount :" + bdBaseAmount.doubleValue());
//        		System.out.println(" bpBaseAmount :" + bpBaseAmount.doubleValue());
//        		System.out.println(" baseAmount :" + baseAmount.doubleValue());
//        		
//        		
//        		System.out.println(" bdAmount :" + bdAmount.doubleValue());
//        		System.out.println(" bpAmount :" + bpAmount.doubleValue());
//        		System.out.println(" amount :" + amount.doubleValue());
//        		
//        		System.out.println(" taxAmount :"+ taxAmount.doubleValue());
//        		System.out.println(" netAmount :"+ netAmount.doubleValue());
//        	}
        	
        	double taxchnrate = Double.parseDouble(eleTaxChain.getAttribute("rate"));
        	String vatcalculated = "NC";
        	if(taxchnrate <= 0.0){
        		
        		
        	
	        	if(taxAmount != null && taxAmount.compareTo(bgzero) > 0 && 
	        			netAmount != null && netAmount.compareTo(bgzero) > 0){
	        		
	        		bdBaseAmount = new BigDecimal(eleTaxChain.getAttribute("BDBaseAmount"));
	            	bpBaseAmount = new BigDecimal(eleTaxChain.getAttribute("BPBaseAmount"));
	            	baseAmount   = new BigDecimal(eleTaxChain.getAttribute("baseAmount"));
	            	
	            	bdAmount 	 = new BigDecimal(eleTaxChain.getAttribute("BDAmount"));
	            	bpAmount 	 = new BigDecimal(eleTaxChain.getAttribute("BPAmount"));
	            	amount   	 = new BigDecimal(eleTaxChain.getAttribute("amount"));
	            	
	            	
	            	if(bpBaseAmount != null && bpBaseAmount.compareTo(bgzero) > 0){
	            		netAmount = netAmount.add(bpBaseAmount);
	            	}else if(bdBaseAmount != null && bdBaseAmount.compareTo(bgzero) > 0){
	            		netAmount = netAmount.add(bdBaseAmount);
	            	}else
	            		netAmount = netAmount.add(baseAmount);
	            	
	            	if(bpAmount != null && bpAmount.compareTo(bgzero) > 0){
	            		taxAmount = taxAmount.add(bpAmount);
	            	}else if(bdAmount != null && bdAmount.compareTo(bgzero) > 0){
	            		taxAmount = taxAmount.add(bdAmount);
	            	}else
	            		taxAmount = taxAmount.add(amount);
	            	
	        		vat = taxAmount.multiply(new BigDecimal(100.00)).divide(netAmount,2,RoundingMode.HALF_UP);
	        		vatcalculated = "C";
	        	}
        	}else{
        		
        		vat = new BigDecimal(taxchnrate).setScale(2,RoundingMode.HALF_UP);
        	}
        	
        	
        	 String vatrounding = "1";
             if(gdwTerrCd.equals("40") || gdwTerrCd.equals("040")){
            	 vatrounding = "0";
             }
             
             
             BigDecimal bpUnitPrice  = null;
             BigDecimal bdUnitPrice  = null;
             BigDecimal bpTaxperUnit = null;
             BigDecimal bdTaxperUnit = null;
             
             
             //new additional fields only required for Nielsen.
             if(generatFieldsForNielsen){
            	 
//            	 if(qtyPromo != null && qtyPromo.compareTo(bgzero)  > 0){
//            		if(bpPrice != null){
//            			 bpUnitPrice = bpPrice.divide(qtyPromo,2,RoundingMode.HALF_UP);
//            		}
//            		
//            		if(bpTax != null){
//            			bpTaxperUnit = bpTax.divide(qtyPromo,2,RoundingMode.HALF_UP);
//            		}
//            		
//            	 }
            	 if(qty != null && qty.compareTo(bgzero)  > 0){
             		if(bpPrice != null){
             			 bpUnitPrice = bpPrice.divide(qty,2,RoundingMode.HALF_UP);
             		}
             		
             		if(bpTax != null){
             			bpTaxperUnit = bpTax.divide(qty,2,RoundingMode.HALF_UP);
             		}
             		
             	 }
            	 
            	if(bpUnitPrice == null){
            		bpUnitPrice = new BigDecimal(0);
     			}
            	 if(bpTaxperUnit == null){
         			bpTaxperUnit = new BigDecimal(0);
         		}
            	 
            	 
//            	 if(qty != null && qty.compareTo(bgzero)  > 0){
//             		if(bdPrice != null){
//             			 bdUnitPrice = bdPrice.divide(qty,2,RoundingMode.HALF_UP);
//             		}
//             		
//             		if(bdTax != null){
//             			bdTaxperUnit = bdTax.divide(qty,2,RoundingMode.HALF_UP);
//             		}
//             		
//             	 }
            	
            	 
            	 if(qty != null && qty.compareTo(bgzero)  > 0 && qtyPromo != null  && qty.compareTo(qtyPromo) >= 0  ){
            		 
              		if(bdPrice != null ){
              			if(qty.compareTo(qtyPromo) > 0){
              				bdUnitPrice = bdPrice.divide(qty.subtract(qtyPromo),2,RoundingMode.HALF_UP);
              			}
              		}
              		
              		if(bdTax != null){
              			if(qty.compareTo(qtyPromo) > 0){
              				bdTaxperUnit = bdTax.divide(qty.subtract(qtyPromo),2,RoundingMode.HALF_UP);
              			}
              		}
              		
              	 }
            	 if(bdUnitPrice == null){
          			bdUnitPrice = new BigDecimal(0).setScale(2);
          		  }
            	 if(bdTaxperUnit == null){
          			bdTaxperUnit = new BigDecimal(0).setScale(2);
          		 }
            	 
            	 
            	 
             }
             
             
             
             outValueBf.setLength(0);
             
             outValueBf.append(gdwLgcyLclRfrDefCd).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(regId).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(gdwBusinessDate).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(orderNumber).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(mappedCode).append(OUTPUT_FIELD_SEPERATOR);
             if(generatFieldsForNielsen){
            	 totalPrice  = totalPrice.setScale(2,RoundingMode.HALF_UP);
            	 outValueBf.append(totalPrice.toString()).append(OUTPUT_FIELD_SEPERATOR);
             }else{
            	 outValueBf.append(menuItemPrice.toString()).append(OUTPUT_FIELD_SEPERATOR);
             }
             outValueBf.append( qty.setScale(2, RoundingMode.HALF_UP).toString() ).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(qtyPromo.setScale(2, RoundingMode.HALF_UP).toString()).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(vat.toString()).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(vatrounding).append(OUTPUT_FIELD_SEPERATOR);
             if(generatFieldsForNielsen){
            	 outValueBf.append(bpUnitPrice.toString()).append(OUTPUT_FIELD_SEPERATOR);
            	 outValueBf.append(bpTaxperUnit.toString()).append(OUTPUT_FIELD_SEPERATOR);
            	 outValueBf.append(bdUnitPrice.toString()).append(OUTPUT_FIELD_SEPERATOR);
            	 outValueBf.append(bdTaxperUnit.toString()).append(OUTPUT_FIELD_SEPERATOR);
             }
             outValueBf.append(gdwTerrCd);
             
//          outValue = 
////        		  	 storeId + OUTPUT_FIELD_SEPERATOR +
//        	  		 gdwLgcyLclRfrDefCd + OUTPUT_FIELD_SEPERATOR +
//                     regId + OUTPUT_FIELD_SEPERATOR +
//                     gdwBusinessDate + OUTPUT_FIELD_SEPERATOR +
//                     orderNumber + OUTPUT_FIELD_SEPERATOR +
//                     mappedCode + OUTPUT_FIELD_SEPERATOR +
////                     totalPrice.setScale(2, RoundingMode.HALF_UP).toString() + OUTPUT_FIELD_SEPERATOR +
//                     menuItemPrice.toString()+OUTPUT_FIELD_SEPERATOR+
//                     qty.setScale(2, RoundingMode.HALF_UP).toString() + OUTPUT_FIELD_SEPERATOR +
//                     qtyPromo.setScale(2, RoundingMode.HALF_UP).toString() + OUTPUT_FIELD_SEPERATOR +
//                     vat.toString() + OUTPUT_FIELD_SEPERATOR +
//                     vatrounding    + OUTPUT_FIELD_SEPERATOR +                     
//                     gdwTerrCd ;//+ OUTPUT_FIELD_SEPERATOR+
//                     ownership;          
//          context.write(new Text("DTL"), new Text(outValue));
           
             pmixKeyBf.setLength(0);
             pmixKeyBf.append("PMIX").append(DaaSConstants.SPLCHARTILDE_DELIMITER);
             pmixKeyBf.append(gdwTerrCd).append(DaaSConstants.SPLCHARTILDE_DELIMITER);
             pmixKeyBf.append(gdwBusinessDate);
             
             newkey.clear();
        	 newkey.set(pmixKeyBf.toString());
        	
            newvalue.clear();
            newvalue.set(outValueBf.toString());
             
            if(Integer.parseInt(gdwTerrCd) != 156){ 
	            if(generatFieldsForNielsen){
	            
	            	context.write(newkey, newvalue);
	            }else{
//	            	mos.write(pmixKeyBf.toString(),null, new Text(outValueBf.toString()));
	            	mos.write(pmixKeyBf.toString(),null, newvalue);
	            }
            }else{
            	
            	mos.write("PMIX"+DaaSConstants.SPLCHARTILDE_DELIMITER+gdwTerrCd+DaaSConstants.SPLCHARTILDE_DELIMITER+gdwBusinessDate, NullWritable.get(), newvalue, "/daas/tdaextracts/salesandpmix_CHINAAPT/GTDA_Dly_Pmix_CN_"+storeId+"_"+gdwBusinessDate+"."+ts+".psv");
            }
//          mos.write("PMIX"+gdwBusinessDate,NullWritable.get(), new Text(outValue));
          context.getCounter("Count", "ValidDTLRecords").increment(1);
        }else{
        	context.getCounter("Count", "NoTaxChainElementFound").increment(1);
        }
      }else{
    	  context.getCounter("Count","SkippedRecordsonItemCode").increment(1);
      }
        
      }else{
    	  context.getCounter("Count","SkippedRecords-"+itemaction).increment(1);
      }
    } catch (Exception ex) {
    	context.getCounter("ExceptionCount", "ProcessItems"+context.getTaskAttemptID()).increment(1);
    	
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      throw new InterruptedException(ex.toString());
    }

    return(eleItem.getChildNodes());
  }

  private String getTenderName(Element eleTenders,Context context) throws IOException, InterruptedException { 

    NodeList nlTenders;
    Element eleTender;
    String tenderKind;
    String tenderName;
    String tenderAmount;
    String tenderCardProvider;
    String tenderPaymentName;
    NodeList nlTenderSubItems;
    Element eleTenderSubItem;
    String textNodeName;
    Node textNode;

    PaymentMethod pymt = new PaymentMethod();

    try {
      nlTenders = eleTenders.getChildNodes();

      if ( nlTenders !=null && nlTenders.getLength() > 0 ) {
        for (int idxTenders=0; idxTenders < nlTenders.getLength(); idxTenders++ ) {
          if ( nlTenders.item(idxTenders).getNodeType() == Node.ELEMENT_NODE ) {
            eleTender = (Element)nlTenders.item(idxTenders);

            nlTenderSubItems = eleTender.getChildNodes();

            tenderKind = "@@@@";
            tenderName = "@@@@";
            tenderAmount = "0.00";
            tenderCardProvider = "@@@@";
                   
            if ( nlTenderSubItems != null && nlTenderSubItems.getLength() > 0 ) {
              for (int idxTenderSubItems = 0; idxTenderSubItems < nlTenderSubItems.getLength(); idxTenderSubItems++ ) {
                if ( nlTenderSubItems.item(idxTenderSubItems).getNodeType() == Node.ELEMENT_NODE ) {
                  eleTenderSubItem = (Element)nlTenderSubItems.item(idxTenderSubItems);

                  textNodeName = eleTenderSubItem.getNodeName();
                  textNode = eleTenderSubItem.getFirstChild();
 
                  if ( textNodeName.equalsIgnoreCase("TenderKind") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
                    tenderKind = textNode.getNodeValue();
                  }

                  if ( textNodeName.equalsIgnoreCase("TenderName") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
                    tenderName = textNode.getNodeValue();
                  }

                  if ( textNodeName.equalsIgnoreCase("TenderAmount") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
                    tenderAmount = textNode.getNodeValue();
                  }
                  
                  if ( textNodeName.equalsIgnoreCase("CardProviderID") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE && textNode.getNodeValue() != null && !textNode.getNodeValue().trim().isEmpty()) {
                    tenderCardProvider = textNode.getNodeValue();
                  }
                }
              }

              if ( ! tenderName.equals("@@@@") && ( tenderKind.equals("0") || tenderKind.equals("1") || tenderKind.equals("2") || tenderKind.equals("3") || tenderKind.equals("4") || tenderKind.equals("5") || tenderKind.equals("8") ) ) { 
                if ( tenderCardProvider.equals("@@@@") ) {
                  tenderPaymentName = tenderName;
                } else {
                  tenderPaymentName = "Cashless-" + tenderCardProvider;
                }

                if ( tenderKind.equals("4") ) {
                  pymt.addUpdatePayment(tenderPaymentName,tenderAmount,true);
                } else {
                  pymt.addUpdatePayment(tenderPaymentName,tenderAmount,false);
                }
              }
            }
          }
        }
      }
    } catch (Exception ex) {
    	context.getCounter("ExceptionCount", "TenderName").increment(1);
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
      throw new InterruptedException(ex.toString());
    }

    return(pymt.maxPaymentMethod());
  }
  
  
  public boolean isSaleEventRecalled(Node  eleOrder){
	  NodeList ordchldnds = eleOrder.getChildNodes();
	  
	  Node evntdtlchnd;
	  NodeList evntdtlchnds;
	  Node saleevntNd;
	  
      if ( ordchldnds != null && ordchldnds.getLength() > 0 ) {
          for (int ordSubItemsIndx = 0; ordSubItemsIndx < ordchldnds.getLength(); ordSubItemsIndx++ ) {
        	  
        	  if(ordchldnds.item(ordSubItemsIndx).getNodeName().equalsIgnoreCase("EventsDetail")){
        		  evntdtlchnd = ordchldnds.item(ordSubItemsIndx);
        		  
        		  evntdtlchnds = evntdtlchnd.getChildNodes();
        		  
        		  for (int evntdtlSubItemsIndx = 0; evntdtlSubItemsIndx < evntdtlchnds.getLength(); evntdtlSubItemsIndx++ ) {
        			  if(evntdtlchnds.item(evntdtlSubItemsIndx).getNodeName().equalsIgnoreCase("SaleEvent")){ 
        				  saleevntNd = evntdtlchnds.item(evntdtlSubItemsIndx);
        				  
        				  if(((Element)saleevntNd).getAttribute("Type").equalsIgnoreCase("Ev_SaleRecalled")){
        					  return true;
        				  }
        						  
        			  }
        			  		
        		  }
        		  
        	  }
        	
            
          }
      }
	  
	  
	  return false;
  }
  
  public boolean isSaleEventOverring(Node  eleOrder){
	  NodeList ordchldnds = eleOrder.getChildNodes();
	  
	  Node evntdtlchnd;
	  NodeList evntdtlchnds;
	  Node saleevntNd;
	  
      if ( ordchldnds != null && ordchldnds.getLength() > 0 ) {
          for (int ordSubItemsIndx = 0; ordSubItemsIndx < ordchldnds.getLength(); ordSubItemsIndx++ ) {
        	  
        	  if(ordchldnds.item(ordSubItemsIndx).getNodeName().equalsIgnoreCase("EventsDetail")){
        		  evntdtlchnd = ordchldnds.item(ordSubItemsIndx);
        		  
        		  evntdtlchnds = evntdtlchnd.getChildNodes();
        		  
        		  for (int evntdtlSubItemsIndx = 0; evntdtlSubItemsIndx < evntdtlchnds.getLength(); evntdtlSubItemsIndx++ ) {
        			  if(evntdtlchnds.item(evntdtlSubItemsIndx).getNodeName().equalsIgnoreCase("SaleEvent")){ 
        				  saleevntNd = evntdtlchnds.item(evntdtlSubItemsIndx);
        				  
        				  if(((Element)saleevntNd).getAttribute("Type").equalsIgnoreCase("Ev_overring")){
        					  return true;
        				  }
        						  
        			  }
        			  		
        		  }
        		  
        	  }
        	
            
          }
      }
	  
	  
	  return false;
  }
  
	public String decompressString(String str) throws IOException{
		StringBuffer newsbf = new StringBuffer();
		
		byte[] bytes = Base64.decodeBase64(str);
		GZIPInputStream gzis = null;
		try{
		    gzis = new GZIPInputStream(new ByteArrayInputStream(bytes));
		    
		   
		}catch(Exception ex){
			ex.printStackTrace();
		}
	    
	    return IOUtils.toString(gzis);
	}
 
  
  @Override 
  protected void cleanup(Context contect) throws IOException, InterruptedException {
    mos.close();
  }
}

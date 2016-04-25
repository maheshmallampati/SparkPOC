package com.mcd.gdw.daas.mapreduce;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
/**
 * 
 * @author Sateesh Pula
 *
 */

public class CoffeeExtractMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

  
  private String OUTPUT_FIELD_SEPERATOR = "|";
  static final int COFFEE_ITEMCD_1 = 24;
  static final int COFFEE_ITEMCD_2 = 81;
  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
  SimpleDateFormat sdfnoms = new SimpleDateFormat("yyyyMMddHHmmss");
  SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
  SimpleDateFormat sdf3 = new SimpleDateFormat("yyyyMMdd");
  
  SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd");
 
  Context thiscontext;
  
  Date datetemp;
  BigDecimal totalAmountBg;
  BigDecimal totalPriceBg;
  BigDecimal totalTaxBg;
  
  Text newValue = new Text();
  String gdwMcdGbalLcatIdNu ="";
 
  HashMap<String,String> itemCodeLevelNamesMap = new HashMap<String,String>();
 
  public static HashSet<Integer> CASHLESS_ITEM_CODES = new HashSet<Integer>();
  public static HashSet<Long> CASHLESS_MCD_GBAL_LCAT_ID_NUS = new HashSet<Long>();
  public static HashSet<Integer> GUAC_ITEM_CODES = new HashSet<Integer>();
  
  private MultipleOutputs<NullWritable, Text> mos;
  HashSet<String> processedTenders = new HashSet<String>();
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException{
     BufferedReader br = null;

    try {
    	
    	  thiscontext = context;
    
    	  mos = new MultipleOutputs<NullWritable, Text>(context);
    	  
    	  System.out.println( ((FileSplit)context.getInputSplit()).getPath());
    	
    	  Path[] distPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
          if (distPaths == null){
        	  System.err.println("distpath is null");
          }
          
          Path distpath = null;
          if (distPaths != null && distPaths.length > 0)  {
        	  
        	  System.out.println(" number of distcache files : " + distPaths.length);
    	      for(int i=0;i<distPaths.length;i++){
    	    	  distpath = distPaths[i];
    		     
    	    	  System.out.println("distpaths:" + distPaths[i].toString());
    	    	  System.out.println("distpaths URI:" + distPaths[i].toUri());
    	    	  
    	    	  if(distpath.toUri().toString().contains("US_WD_DMITM_HRCY_SNAP.psv")){
    		    	  		      	  
    	    		  br  = new BufferedReader( new FileReader(distPaths[i].toString())); 
    	    		  
    	    		  String line = null;
    	  			  String[] lineparts;
    	  			
    	  			  String terrCd;
    	  			  String  itemId;
    	  			  StringBuffer levelNamesbf = new StringBuffer();
    	  			
	    	  			while ((line = br.readLine()) != null) {
	    	  				if (line != null && !line.isEmpty()) {
	    	  					lineparts = line.split("\\|", -1);
	    	  					
	    	  					terrCd = lineparts[0];
	    	  					itemId = lineparts[1];
	    	  					
	    	  					levelNamesbf.setLength(0);
	    	  					levelNamesbf.append(lineparts[2]).append("|");//sld_menu_itm_na
	    	  					levelNamesbf.append(lineparts[4]).append("|");//itm_us1_lvl1_na
	    	  					levelNamesbf.append(lineparts[6]).append("|");//itm_us1_lvl2_na
	    	  					levelNamesbf.append(lineparts[8]).append("|");//itm_us1_lvl3_na
	    	  					levelNamesbf.append(lineparts[10]);//itm_us1_lvl4_na
	    	  					
	    	  					itemCodeLevelNamesMap.put(terrCd+"|"+itemId, levelNamesbf.toString());
	    	  					
	    	  				}
	    	  				
	    	  				
	    	  			}
    		      	  break;
    		      }
    		      
    	      }
          }
          
          //'6522','6653','6654','6695','6698','6699','6672','6676','6683'
          CASHLESS_ITEM_CODES.add(6522);
          CASHLESS_ITEM_CODES.add(6653);
          CASHLESS_ITEM_CODES.add(6654);
          CASHLESS_ITEM_CODES.add(6695);
          CASHLESS_ITEM_CODES.add(6698);
          CASHLESS_ITEM_CODES.add(6699);
          CASHLESS_ITEM_CODES.add(6672);
          CASHLESS_ITEM_CODES.add(6676);
          CASHLESS_ITEM_CODES.add(6683);
          
          
          //MCD_GBAL_LCAT_ID_NU : 195500249193,195500249186,195500246710,195500337616,195500247182
          CASHLESS_MCD_GBAL_LCAT_ID_NUS.add(new Long("195500249193"));
          CASHLESS_MCD_GBAL_LCAT_ID_NUS.add(new Long("195500249186"));
          CASHLESS_MCD_GBAL_LCAT_ID_NUS.add(new Long("195500246710"));
          CASHLESS_MCD_GBAL_LCAT_ID_NUS.add(new Long("195500337616"));
          CASHLESS_MCD_GBAL_LCAT_ID_NUS.add(new Long("195500247182"));
          
          
          //GUAC item codes '6',' 7', '4139', '4282', '4295', '4298', '4384', '5054', '5094', '5096', '5159','5164', '5283', '5594', '5597', '5894', 
          //'6432', '6530', '6652', '7599','6522','6653','6654','6695','6698','6699','6672','6676','6683'
          GUAC_ITEM_CODES.add(6);
          GUAC_ITEM_CODES.add(7);
          GUAC_ITEM_CODES.add(4139);
          GUAC_ITEM_CODES.add(4282);
          GUAC_ITEM_CODES.add(4295);
          GUAC_ITEM_CODES.add(4298);
          GUAC_ITEM_CODES.add(4384);
          GUAC_ITEM_CODES.add(5054);
          GUAC_ITEM_CODES.add(5094);
          GUAC_ITEM_CODES.add(5096);
          GUAC_ITEM_CODES.add(5159);
          GUAC_ITEM_CODES.add(5164);
          GUAC_ITEM_CODES.add(5283);
          GUAC_ITEM_CODES.add(5594);
          GUAC_ITEM_CODES.add(5597);
          GUAC_ITEM_CODES.add(5894);
          GUAC_ITEM_CODES.add(6432);
          GUAC_ITEM_CODES.add(6530);
          GUAC_ITEM_CODES.add(6652);
          GUAC_ITEM_CODES.add(7599);
          GUAC_ITEM_CODES.add(6522);
          GUAC_ITEM_CODES.add(6653);
          GUAC_ITEM_CODES.add(6654);
          GUAC_ITEM_CODES.add(6695);
          GUAC_ITEM_CODES.add(6698);
          GUAC_ITEM_CODES.add(6699);
          GUAC_ITEM_CODES.add(6672);
          GUAC_ITEM_CODES.add(6676);
          GUAC_ITEM_CODES.add(6683);
          
          
          
          
          
          
      
     } catch (Exception ex) {
      ex.printStackTrace();
      
    }finally{
		   try{
			   if (br != null)
					br.close();
		   }catch(Exception ex){
			   ex.printStackTrace();
		   }
	   }

  }
  int count = 0;
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
      gdwMcdGbalLcatIdNu = eleTLD.getAttribute("gdwMcdGbalLcatIdNu");
      gdwLgcyLclRfrDefCd = eleTLD.getAttribute("gdwLgcyLclRfrDefCd");
//      if(count < 10){
//    	  System.out.println (" gdwMcdGbalLcatIdNu " +gdwMcdGbalLcatIdNu + " gdwLgcyLclRfrDefCd  " + gdwLgcyLclRfrDefCd);
//      }
      count++;
      gdwBusinessDate = eleTLD.getAttribute("gdwBusinessDate");
      gdwTerrCd = eleTLD.getAttribute("gdwTerrCd");
      storeId   = eleTLD.getAttribute("storeId");
      nlNode = eleTLD.getChildNodes();

      if (nlNode != null && nlNode.getLength() > 0 ) {
        for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
          if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {
            eleNode = (Element)nlNode.item(idxNode);

            if ( eleNode.getNodeName().equals("Node") ) {
              parseNode(eleNode,gdwMcdGbalLcatIdNu,storeId,gdwLgcyLclRfrDefCd, gdwBusinessDate, gdwTerrCd, context,ownership);
            }
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private void parseNode(Element eleNode,
		  				 String gdwMcdGbalLcatIdNu,
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
    String trxSaleStatus;
    String trxSalePod;
    Element eleOrder = null;
    //new
    Element eleTrxEvent = null;
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

              if ( eventType.equals("TRX_Sale")){
            	  
            	
            	  
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
            	  
            	  String orderKind       =  eleOrder.getAttribute("kind");

                  //Sateesh: do we need to check for this condition. Will we always have status ( refund??)
                  if ( (eventType.equals("TRX_Sale") && trxSaleStatus.equals("Paid"))  && orderKind != null && orderKind.equalsIgnoreCase("Sale") ) {
                    
                	  context.getCounter("Count","NumOrdersPaidRefundStatus").increment(1);
  
              
                        //register type
	                    trxSalePod = eleTrxEvent.getAttribute("POD");
	                    

	                    

	                    eleOrder.getAttribute("saleType");

	                    
	                     parseOrderDetails(eleOrder,gdwMcdGbalLcatIdNu, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, storeId,gdwTerrCd,trxSalePod, context,ownership); 
	                    
	                  
	                   
	                  
                    
                  }//end paid/refund
            	  }//end if transaction element
            	  }//end sale/refund/overring/waste
          }
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private void parseOrderDetails(Element eleOrder, String gdwMcdGbalLcatIdNu, String gdwLgcyLclRfrDefCd, 
		  String regId, String gdwBusinessDate, 
		 String storeId,String gdwTerrCd, 
		 String trxSalePod,Context 
		 context,String ownership) throws IOException, InterruptedException {

  
//    NodeList nlOrderDetails;
//    Element eleOrderDetails;
// 
//    NodeList nlItem1;
//    Element eleItem1;
//    NodeList nlItem2;
//    Element eleItem2;

    try {
    	
//    	if (!eleOrder.getAttribute("key").equalsIgnoreCase("POS0002:955010336")){
//    		return;
//    	}
    	
    	 
    	 NodeList items = eleOrder.getElementsByTagName("Item");
//    	 System.out.println(" Number of items in order " +items.getLength());
    	 Element eleItem;
    	 boolean hasCoffeeinOrder = false;
    	 boolean hasGUACItemCodeinOrder = false;
    	 
    	 String orderTimestamp  = eleOrder.getAttribute("Timestamp");
    	 String orderKey 	    = eleOrder.getAttribute("key");
    	 String orderSaleType   = eleOrder.getAttribute("saleType");
    	 String orderKind       =  eleOrder.getAttribute("kind");
			
		  try{
			  datetemp = sdf.parse(orderTimestamp);
		  }catch(ParseException ex){
			  datetemp = sdfnoms.parse(orderTimestamp);
		  }
    	 
		  Calendar cal = Calendar.getInstance();
		  cal.setTime(datetemp);
		  int orderHour = cal.get(Calendar.HOUR_OF_DAY);

		  
		
			 String[] outputItems = new String[items.getLength()];
	    	 int idx =0;
	         for(int itemIdx = 0;itemIdx < items.getLength();itemIdx++){
	        	 eleItem = (Element)items.item(itemIdx);
	        	String itemCode = eleItem.getAttribute("code");
	        	
	        	  
				  
	        	if(itemCode != null && ( Integer.parseInt(itemCode) == COFFEE_ITEMCD_1 || Integer.parseInt(itemCode) == COFFEE_ITEMCD_2))
	        		hasCoffeeinOrder = true;
	        	if(itemCode != null && GUAC_ITEM_CODES.contains(Integer.parseInt(itemCode)))
	        		hasGUACItemCodeinOrder = true;
	        	
	         	String outputItem  = processItems(eleItem, eleOrder,gdwLgcyLclRfrDefCd,regId,gdwBusinessDate,storeId,gdwTerrCd,trxSalePod,context,ownership);
//	         	System.out.println( " outputItem  " +outputItem);
	         	outputItems[idx++] =  outputItem;
	         }
	         if(hasCoffeeinOrder && orderHour >= 5 && orderHour <= 10){ // consider coffee only in breakfast orders.
	         
	        	 
	        	 for(int itemIndx=0;itemIndx < outputItems.length;itemIndx++){
	        		 newValue.clear();
	             	 newValue.set(outputItems[itemIndx]);
	           
//	           		 context.write(NullWritable.get(),newValue);
	             	 mos.write("COFFEEEXTRACT",NullWritable.get(),newValue);
	        	 }
	         
	         }
	         
	         if(hasGUACItemCodeinOrder){
	        	 for(int itemIndx=0;itemIndx < outputItems.length;itemIndx++){
	        		 newValue.clear();
	             	 newValue.set(outputItems[itemIndx]);
	           
//	           		 context.write(NullWritable.get(),newValue);
	             	 mos.write("GUAC",NullWritable.get(),newValue);
	        	 }
	         }
		 
		 
		 
		 //handle cashless tenders
		 boolean hasCashlessItemCodeinOrder = false;
		 
		 for(int itemIdx = 0;itemIdx < items.getLength();itemIdx++){
			 eleItem = (Element)items.item(itemIdx);
	         String itemCode = eleItem.getAttribute("code");
	         if(itemCode != null && CASHLESS_ITEM_CODES.contains(new Integer(itemCode))){
	        		hasCashlessItemCodeinOrder = true;
	        		break;
	         }
		 }
		
			  //MCD_GBAL_LCAT_ID_NU in (195500249193,195500249186,195500246710,195500337616,195500247182)
			  if(hasCashlessItemCodeinOrder && CASHLESS_MCD_GBAL_LCAT_ID_NUS.contains(new Long(gdwMcdGbalLcatIdNu))){
//				  if(!orderKey.equalsIgnoreCase("POS0002:509125808"))
//					  return;
				  processedTenders.clear();
				  NodeList tenders = eleOrder.getElementsByTagName("Tender");
				  
				 
				  String cashlessDataValue = "" ;
				  Element tenderElement = null;
				  
				  try{
					  datetemp = sdf.parse(orderTimestamp);
				  }catch(ParseException ex){
					  datetemp = sdfnoms.parse(orderTimestamp);
				  }
				  String orderTimestampFormatted  = sdf2.format(datetemp);
				  
				  datetemp = sdf3.parse(gdwBusinessDate);
				  
				  String busDtFormatted = sdf4.format(datetemp);
				  
				  String totalamount	 = eleOrder.getAttribute("totalAmount");//99999D99
				  String totalTax		 = eleOrder.getAttribute("totalTax");//99999D99
				
				  
				  
				  totalAmountBg = new BigDecimal(totalamount);
				  totalAmountBg = totalAmountBg.setScale(2, RoundingMode.HALF_EVEN);
				      
				  totalTaxBg = new BigDecimal(totalTax);
				  totalTaxBg = totalTaxBg.setScale(2, RoundingMode.HALF_EVEN);
				  NodeList tenderChlds;
				  StringBuffer sbf = new StringBuffer();
				  
				  if(tenders == null || tenders.getLength() ==0){
					  sbf.setLength(0);
					  
//					  sbf.append(gdwBusinessDate).append(OUTPUT_FIELD_SEPERATOR);
					 
			             
			           
			          sbf.append(busDtFormatted).append(OUTPUT_FIELD_SEPERATOR);
			             
					  sbf.append(storeId).append(OUTPUT_FIELD_SEPERATOR);
					  sbf.append(orderKey).append(OUTPUT_FIELD_SEPERATOR);
					  
					  sbf.append(orderSaleType).append(OUTPUT_FIELD_SEPERATOR);
					  sbf.append(orderKind).append(OUTPUT_FIELD_SEPERATOR);
					  sbf.append(trxSalePod).append(OUTPUT_FIELD_SEPERATOR);
					  sbf.append(orderTimestampFormatted).append(OUTPUT_FIELD_SEPERATOR);
					  sbf.append(totalAmountBg.toString()).append(OUTPUT_FIELD_SEPERATOR);
//					  sbf.append(totalTaxBg.toString()).append(OUTPUT_FIELD_SEPERATOR);
					  sbf.append("").append(OUTPUT_FIELD_SEPERATOR);
					  sbf.append("");
					  
					  newValue.clear();
					  newValue.set(sbf.toString());
					  
					  mos.write("CASHLESSEXTRACT", NullWritable.get(), newValue);
					  
				  }else{
					  
					  Element tenderChld;
					  for(int indxTender = 0;indxTender < tenders.getLength();indxTender ++ ){
						  tenderElement = (Element)tenders.item(indxTender);
						  
						  
						  tenderChlds = tenderElement.getChildNodes();
						  
						  
						  String tenderChldName;
						  String tenderChldValue;
						  String tenderName = "";
						  
						  int i = 0;
						  for (int indxTenderChlds = 0; indxTenderChlds < tenderChlds.getLength(); indxTenderChlds++ ) {
							  tenderChld = (Element)tenderChlds.item(indxTenderChlds);
							  tenderChldName = tenderChld.getNodeName();
							  tenderChldValue = tenderChld.getTextContent();
	//						  System.out.println(" tenderChldName " + tenderChldName + " tenderChldValue " +tenderChldValue);
							  
							 
							  if(tenderChldName  != null && tenderChldName.equals("TenderName")){
								  tenderName = tenderChldValue;
								  
							  }else if(tenderChldName != null && tenderChldName.equalsIgnoreCase("CashlessData")){
								  cashlessDataValue = tenderChldValue;
							  }
							  
							  
						  }
						  
						  String cashlessData =  formateCashLessData(cashlessDataValue);
						  if(cashlessData == null)
							  cashlessData = "";
	//					  /BusinessDate,StoreId,OrderKey,OrderSaleType,OrderKind,OrderLocation,OrderTimestamp,OrderTotalAmount,TenderName,Payment
						      
						  if(!processedTenders.contains(tenderName)){
							  processedTenders.add(tenderName);
							  sbf.setLength(0);
					          sbf.append(busDtFormatted).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(storeId).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(orderKey).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(orderSaleType).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(orderKind).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(trxSalePod).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(orderTimestampFormatted).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(totalAmountBg.toString()).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(tenderName).append(OUTPUT_FIELD_SEPERATOR);
							  sbf.append(cashlessData);
							  
							  newValue.clear();
							  newValue.set(sbf.toString());
							  
							  mos.write("CASHLESSEXTRACT", NullWritable.get(), newValue);
						  }
					  }
				  
				  }
				  
			  }
			  
			 
    } catch (Exception ex) {
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
    }

   
  }

  private String processItems(Element eleItem,
		  Element eleOrder,
          String gdwLgcyLclRfrDefCd,
          String regId,
          String gdwBusinessDate,
          String storeId,
          String gdwTerrCd,
          String trxSalePod,
          Context context,
          String ownership) {
	  
	  StringBuffer outValueBf = new StringBuffer();

	  try { 
	 
	
			  String orderKey 	     = eleOrder.getAttribute("key");
			  String orderSaleType   = eleOrder.getAttribute("saleType");
			  String orderTimestamp  = eleOrder.getAttribute("Timestamp");
			
			  try{
				  datetemp = sdf.parse(orderTimestamp);
			  }catch(ParseException ex){
				  datetemp = sdfnoms.parse(orderTimestamp);
			  }
			  orderTimestamp  = sdf2.format(datetemp);
	
			  String itemLevel		 = eleItem.getAttribute("level");
		      String itemType 		 = eleItem.getAttribute("type");
		      String itemAction		 = eleItem.getAttribute("action");
		      String itemCode 		 = eleItem.getAttribute("code");
		      String itemSeq		 = eleItem.getAttribute("id");
		      String qty		     = eleItem.getAttribute("qty");//99999
		      String qtyPromo		 = eleItem.getAttribute("qtyPromo");//99999
		      String totalPrice		 = eleItem.getAttribute("totalPrice");//99999D99
		      String totalTax		 = eleItem.getAttribute("totalTax");//99999D99
    
     
		      totalPriceBg = new BigDecimal(totalPrice);
		      
		      totalPriceBg = totalPriceBg.setScale(2, RoundingMode.HALF_EVEN);
		      
		      totalTaxBg = new BigDecimal(totalTax);
		      
		      totalTaxBg = totalTaxBg.setScale(2, RoundingMode.HALF_EVEN);
      
             
		      datetemp = sdf3.parse(gdwBusinessDate);
             
           
             outValueBf.append(sdf4.format(datetemp)).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(StringUtils.leftPad(storeId,5,'0')).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(orderKey).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(orderSaleType).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(trxSalePod).append(OUTPUT_FIELD_SEPERATOR);
            
             outValueBf.append(orderTimestamp).append(OUTPUT_FIELD_SEPERATOR);//format
             outValueBf.append(itemLevel).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(itemType).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(itemAction).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(itemCode).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(itemSeq).append(OUTPUT_FIELD_SEPERATOR);
             
             String dmitmHrcySnapValue = itemCodeLevelNamesMap.get(gdwTerrCd+"|"+itemCode);
             if(dmitmHrcySnapValue == null){
            	 dmitmHrcySnapValue = "||||";
             }
             
             outValueBf.append(dmitmHrcySnapValue).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(qty).append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(qtyPromo).append(OUTPUT_FIELD_SEPERATOR);
//             outValueBf.append(totalPrice);//.append(OUTPUT_FIELD_SEPERATOR);
             outValueBf.append(totalPriceBg.toPlainString());//.append(OUTPUT_FIELD_SEPERATOR);
             
//             outValueBf.append(totalTax).append(OUTPUT_FIELD_SEPERATOR);
//             outValueBf.append(gdwMcdGbalLcatIdNu).append(OUTPUT_FIELD_SEPERATOR);
//             outValueBf.append(gdwLgcyLclRfrDefCd);
//             newValue.clear();
//             newValue.set(outValueBf.toString());
             
//             context.write(NullWritable.get(),newValue);
          
             
    } catch (Exception ex) {
    	context.getCounter("ExceptionCount", "ProcessItems"+context.getTaskAttemptID()).increment(1);
    	ex.printStackTrace();
  
    }
    return outValueBf.toString();
//    return(eleItem.getChildNodes());
  }

  
  private static String formateCashLessData(String cashlessData){
	  
	  String[] cashlessDataParts = null;
	  String[] cashlessDataCDParts = null;
	  String[] cdExpdateParts = null;
	  
	  StringBuffer cashLessDataValue = new StringBuffer();
	  
	  if(cashlessData != null && !cashlessData.isEmpty()){
		  cashlessDataParts = cashlessData.split(":");
		  if(cashlessDataParts != null && cashlessDataParts.length > 1
				  && cashlessDataParts[1] != null && !cashlessDataParts[1].isEmpty()){
			  cashlessDataCDParts = cashlessDataParts[1].split("\\|");
			  
			  cashLessDataValue.append(cashlessDataParts[1].substring(0, 1));
			  
			  if(cashlessDataCDParts != null && cashlessDataCDParts.length > 4){
				  
				  cashLessDataValue.append(cashlessDataCDParts[1]);//masked card number
				  
				  cdExpdateParts = cashlessDataCDParts[2].split("/");
				  cashLessDataValue.append(cdExpdateParts[0]).append(cdExpdateParts[1]);
				  
			  }
		  }
		  
		  return cashLessDataValue.toString();
	  }
	  
	  return "";
  }
  
  @Override 
  protected void cleanup(Context contect) throws IOException, InterruptedException {
    mos.close();
  } 
}

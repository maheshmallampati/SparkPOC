package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.security.NetgroupCache;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.GenerateQuarterHourDaypartPMIXFromStld;
import com.mcd.gdw.daas.util.PaymentMethod;
import com.sun.org.apache.bcel.internal.generic.NEWARRAY;

public class QuarterHourDaypartPMIXMapper extends Mapper<LongWritable, Text, Text, Text> {

  private String OUTPUT_FIELD_SEPERATOR = "|";

  private Text mapKey = new Text();
  private Text mapValue = new Text();
  private String fileTimestamp;
  
  private String posTrnStrtTs;
  private String posBusnDt;
  private String tmSegIdNu;
  
  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
	
  private TreeMap<String, TreeMap<String,String>> daypartidmap = new TreeMap<String,TreeMap<String,String>>();
  private HashMap<Integer, String> timeSegIdDescMap = new HashMap<Integer,String>();
  private HashMap<Integer, String> currencyMap = new HashMap<Integer,String>();
  private HashMap<Integer, String> menuPriceBasisMap = new HashMap<Integer,String>();
  
  BigDecimal bgzero = new BigDecimal("0.00");
  StringBuffer outValueBf = new StringBuffer(); 
  String owshFltr = "*";
  
  TDAExtractMapperUtil tdaExtractMapperUtil = new TDAExtractMapperUtil();
  private MultipleOutputs<Text, Text> mos;
  
   @Override
  public void setup(Context context) throws IOException, InterruptedException{
     BufferedReader br = null;
   
    try {
    	
    	mos = new MultipleOutputs<Text, Text>(context);
    	
    	owshFltr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_OWNERSHIP_FILTER);
    	if(owshFltr == null)
    		owshFltr = "*";
    	
    	Calendar cal = Calendar.getInstance();
    	fileTimestamp = sdf.format(cal.getTime());
    	
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
	    	  
	    	  if (distpath.toUri().toString().contains("DayPart_ID.psv")){
		    	  		      	  
	    		  br  = new BufferedReader( new FileReader(distPaths[i].toString())); 
		      	  addDaypartKeyValuestoMap(br);
		      	  
		      } else if (distpath.toUri().toString().contains("Currency.psv")){
  		      	  
				  br  = new BufferedReader( new FileReader(distPaths[i].toString())); 
				  addCurrencyKeyValuestoMap( br);
					  
		      } else if (distpath.toUri().toString().contains("MenuPriceBasis.psv")){
  		      	  
				  br  = new BufferedReader( new FileReader(distPaths[i].toString())); 
				  addMenuPriceBasisKeyValuestoMap( br);	  
		      }
		      
	      }
      }
      
     } catch (FileNotFoundException e1) {
      e1.printStackTrace();
      System.out.println("read from distributed cache: file not found!");
    } catch (IOException e1) {
      e1.printStackTrace();
      System.out.println("read from distributed cache: IO exception!");
    } finally {
    	try {
    		if(br != null)
    			br.close();
    	} catch(Exception ex) {
    		ex.printStackTrace();
    	}
    }
  }
   
   private  void addMenuPriceBasisKeyValuestoMap(
		  	BufferedReader br) {
	   try{
		    String line = null;
			String[] lineparts;
			
			Integer terrCd;
			String  currency;
			
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					lineparts = line.split("\\|", -1);
					
					terrCd = new Integer(lineparts[0]);
					currency = lineparts[1];
					
					menuPriceBasisMap.put(terrCd,currency.trim().toUpperCase());
					
				}	
			}
			
	   } catch(Exception ex) {
		  ex.printStackTrace();
	   } finally {
		   try {
			   if (br != null)
					br.close();
		   } catch(Exception ex) {
			   ex.printStackTrace();
		   }
	   }
  }
   private  void addCurrencyKeyValuestoMap(
		  	BufferedReader br) {
	   try{
		    String line = null;
			String[] lineparts;
			
			Integer terrCd;
			String  currency;
			
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					lineparts = line.split("\\|", -1);
					
					terrCd = new Integer(lineparts[0]);
					currency = lineparts[1];
					
					currencyMap.put(terrCd,currency.trim().toUpperCase());
					
				}
			}
			
	   } catch(Exception ex) {
		  ex.printStackTrace();
	   } finally {
		   try {
			   if (br != null)
					br.close();
		   } catch(Exception ex) {
			   ex.printStackTrace();
		   }
	   }
   }
 
  private void addDaypartKeyValuestoMap(
		  	BufferedReader br) {

		try {
		
			String line = null;
			String[] lineparts;
			
			String cntry_day_timeseg_key;
			String cntrycd;
			String hrkey;
			String value;
			String startTime;
			String endTime;
			String daypartId;
			
			String dayofweek;
			String timesegment;

			String starthour;
			String startminutes;
			String startseconds;

			String endhour;
			String endminutes;
			String endseconds;
			String timesegDesc;

			TreeMap<String, String> minutes_daypartid_map;
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					lineparts = line.split("\\|", -1);

						cntrycd = lineparts[0];
						
						dayofweek   = lineparts[1];
						timesegment = lineparts[2];
						startTime   = lineparts[3];
						endTime     = lineparts[4];

						daypartId   = lineparts[5];
						timesegDesc = lineparts[6];
						
						starthour    = startTime.split(":")[0];
						startminutes = startTime.split(":")[1];
						startseconds = startTime.split(":")[2];

						endhour = endTime.split(":")[0];
						endminutes = endTime.split(":")[1];
						endseconds = endTime.split(":")[2];

						cntry_day_timeseg_key = cntrycd + "_" + dayofweek+"_"+timesegment;
						
						minutes_daypartid_map = daypartidmap
								.get(cntry_day_timeseg_key);

						if (minutes_daypartid_map == null) {
							minutes_daypartid_map = new TreeMap<String, String>();
							daypartidmap.put(cntry_day_timeseg_key,minutes_daypartid_map);							
						}
						
						minutes_daypartid_map.put(starthour+"_"+startminutes + "_"+ endminutes, daypartId);
						
						if(!timeSegIdDescMap.containsKey(new Integer(daypartId))){
							timeSegIdDescMap.put(new Integer(daypartId), timesegDesc);
						}
					
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
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
  public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

    DocumentBuilderFactory docFactory;
    DocumentBuilder docBuilder;
    InputSource xmlSource;
    Document xmlDoc;
    Element eleTLD;

    try {
    	
    	String[] recArr = value.toString().split("\t");   
    	
    	if(!owshFltr.equals("*") && !owshFltr.equalsIgnoreCase(recArr[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS])){
     	    context.getCounter("Debug", "SKIP OWSH FILTER").increment(1);
 			return;
   	    }
    	 
    	String filename = recArr[0];
    	
	    docFactory    = DocumentBuilderFactory.newInstance();
	    docBuilder    = docFactory.newDocumentBuilder();
	    xmlSource     = new InputSource(new StringReader(recArr[recArr.length -1]));
	    xmlDoc        = docBuilder.parse(xmlSource);
	
	    eleTLD 		  = (Element)xmlDoc.getFirstChild();

	    String xmlstr = recArr[recArr.length -1];
	    xmlstr = xmlstr.toString().replaceAll("[\n\r]", "").replaceAll("\t", "");
	    
	    if ( eleTLD.getNodeName().equals("TLD") ) {
	    	npParseXml(eleTLD, context);
	    }

    } catch (Exception ex) {
      Logger.getLogger(QuarterHourDaypartPMIXMapper.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private void npParseXml(Element eleTLD, Context context) throws IOException, InterruptedException {

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

      nlNode = eleTLD.getChildNodes();

      if (nlNode != null && nlNode.getLength() > 0 ) {
        for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
          if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {
            eleNode = (Element)nlNode.item(idxNode);

            if ( eleNode.getNodeName().equals("Node") ) {
              parseNode(eleNode, storeId,gdwLgcyLclRfrDefCd, gdwBusinessDate, gdwTerrCd, context);
            }
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(QuarterHourDaypartPMIXMapper.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private void parseNode(Element eleNode,
		  				 String storeId,
                         String gdwLgcyLclRfrDefCd,
                         String gdwBusinessDate,
                         String gdwTerrCd,
                         Context context) throws IOException, InterruptedException {

    String nodeId;
    String regId;
    NodeList nlEvent;
    Element eleEvent;
    String eventType;

    String trxSaleStatus;

    Element eleOrder = null;
    String orderTimestamp;
    String orderKey;

    String[] orderKeyParts;
    String orderNumber;
   
    Element eleTrxEvent = null;
    String orderKind;
    String trxSalePod;
    String orderSaleType;
    
    
    try {
      nodeId = eleNode.getAttribute("id");
      //cash register number
      regId = String.valueOf(Integer.parseInt(nodeId.substring(3)));

      nlEvent = eleNode.getChildNodes();

      if ( nlEvent !=null && nlEvent.getLength() > 0 ) {
        for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++ ) {
          if ( nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE ) {
        	  
        	  	
        	  
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
	            	  }
	            	  
		            	  if(eleTrxEvent != null){
			                  trxSaleStatus = eleTrxEvent.getAttribute("status");
			                  
			                  NodeList ntrxchldList = eleTrxEvent.getChildNodes();
			            	  
			            	  for(int trxchldcnt=0;trxchldcnt < ntrxchldList.getLength();trxchldcnt++){
			            		  if(ntrxchldList.item(trxchldcnt).getNodeType() == Node.ELEMENT_NODE){
			            			  eleOrder = (Element)ntrxchldList.item(trxchldcnt);
			            			  break;
			            		  }
			            	  }
			            	  
			                  orderTimestamp = eleOrder.getAttribute("Timestamp");

			                  orderKey = eleOrder.getAttribute("key");
			                  
			                  context.getCounter("Count","NumOrders").increment(1);
			
			                  orderKind = eleOrder.getAttribute("kind").toUpperCase();
			                  trxSalePod = eleTrxEvent.getAttribute("POD");
			                  orderSaleType =  eleOrder.getAttribute("saleType");
			                  
			                  //Sateesh: do we need to check for this condition. Will we always have status ( refund??)
			                  if (  ((eventType.equals("TRX_Sale") && trxSaleStatus.equals("Paid")) ||
			                		  (eventType.equals("TRX_Refund") || eventType.equals("TRX_Overring") || 
			                       		  eventType.equals("TRX_Waste"))) &&
			                       		isValidOrderKind(orderKind) &&
			                       		isValidPOD(trxSalePod)) {
			                    
			                	  context.getCounter("Count","NumOrdersPaidRefundStatus").increment(1);
			                   
			                    
				                    //do we need to output only if there is a timestamp
				                    if ( orderTimestamp.length() >= 12 ) {
				                       
				                    	//order number
				                        orderKeyParts = (orderKey+":@@@@").split(":");
				                        orderNumber = orderKeyParts[1];
					                    parseOrderDetails(eleOrder, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, orderTimestamp, storeId, orderKind, trxSalePod, orderSaleType, gdwTerrCd, context); 
					                    
				                    }//end timestamp check
			                    
			                  }//end paid/refund
		            	  }//end if transaction element
	            	  }//end sale/refund/overring/waste
	          }
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(QuarterHourDaypartPMIXMapper.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
  
  
private boolean isValidPOD(String pod){
	  
	  if(pod != null && ("DESSERT KIOSK".equalsIgnoreCase(pod) ||
			  "FRONT COUNTER".equalsIgnoreCase(pod) ||
			  "MCCAFE".equalsIgnoreCase(pod) ||
			  "DRIVE THRU".equalsIgnoreCase(pod) ||
			  "DELIVERY".equalsIgnoreCase(pod))){
		  
		  return true;
	  }
	  
  
  	return false;
  }

  private boolean isValidOrderKind(String orderKind){
	  
	  if(orderKind != null && ("SALE".equalsIgnoreCase(orderKind) ||
			  "REFUND".equalsIgnoreCase(orderKind) ||
			  "WASTE".equalsIgnoreCase(orderKind) ||
			  "PROMOTION".equalsIgnoreCase(orderKind) ||
			  "MANAGER DISCOUNT".equalsIgnoreCase(orderKind) ||
			  "CREW DISCOUNT".equalsIgnoreCase(orderKind) ||
			  "DISCOUNT".equalsIgnoreCase(orderKind))){
		  
		  return true;
	  }
	  
  
  	return false;
  }

  private void parseOrderDetails(Element eleOrder, 
		  						String gdwLgcyLclRfrDefCd, 
		  						String regId, 
		  						String gdwBusinessDate, 
		  						String orderNumber,
		  						String orderTimestamp,
		  						String storeId,
		  						String orderKind,
		  						String trxSalePod,
		  						String orderSaleType,
		  						String gdwTerrCd, 
		  						Context context) throws IOException, InterruptedException {

   
    //String outValue; 

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
              nlItem1 = processItems(eleOrderDetails, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, orderTimestamp, storeId, orderKind, trxSalePod, orderSaleType, gdwTerrCd, context);

              if ( nlItem1 !=null && nlItem1.getLength() > 0 ) {
                for (int idxItem1=0; idxItem1 < nlItem1.getLength(); idxItem1++ ) {
                  if ( nlItem1.item(idxItem1).getNodeType() == Node.ELEMENT_NODE ) {
                    eleItem1 = (Element)nlItem1.item(idxItem1);
                    if ( eleItem1.getNodeName().equals("Item") ) {
                      nlItem2 = processItems(eleItem1, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, orderTimestamp, storeId, orderKind, trxSalePod, orderSaleType, gdwTerrCd, context);
 
                      if ( nlItem2 !=null && nlItem2.getLength() > 0 ) {
                        for (int idxItem2=0; idxItem2 < nlItem2.getLength(); idxItem2++ ) {
                          if ( nlItem2.item(idxItem2).getNodeType() == Node.ELEMENT_NODE ) {
                            eleItem2 = (Element)nlItem2.item(idxItem2);
                            if ( eleItem2.getNodeName().equals("Item") ) {
                              processItems(eleItem2, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, orderTimestamp, storeId, orderKind, trxSalePod, orderSaleType, gdwTerrCd, context);
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }

           
          }
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(QuarterHourDaypartPMIXMapper.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private NodeList processItems(Element eleItem,
                                String gdwLgcyLclRfrDefCd,
                                String regId,
                                String gdwBusinessDate,
                                String orderNumber,
                                String orderTimestamp,
                                String storeId,
                                String orderKind,
                                String trxSalePod,
                                String orderSaleType,
                                String gdwTerrCd,
                                Context context) {

    String outValue;
    String code;
  
    String type;
    String qtyText;
    String qtyPromoText;
    BigDecimal qty;
    BigDecimal qtyPromo;
    
    BigDecimal totalPrice;
    BigDecimal totalTax;

    BigDecimal bdPrice;
    BigDecimal bdTax;    
    
    int displayOrder = -1; //src field
  
    String itemleveltxt;
    String itemaction;
    BigDecimal itemlevel;
    
    String grillModifier;
    String grillQty;
    
    try { 
    	
      qtyText = eleItem.getAttribute("qty");
      qty = new BigDecimal(qtyText);
      qtyPromoText = eleItem.getAttribute("qtyPromo");
      qtyPromo = new BigDecimal(qtyPromoText);
      
      type = eleItem.getAttribute("type");
      displayOrder = Integer.parseInt(eleItem.getAttribute("displayOrder"));
      
      itemleveltxt = eleItem.getAttribute("level");
      itemaction   = eleItem.getAttribute("action");
      itemlevel = new BigDecimal(itemleveltxt);
      code = eleItem.getAttribute("code");
      
      grillModifier = eleItem.getAttribute("grillModifier");
      grillQty = eleItem.getAttribute("grillQty");
      
      if ( (type.equalsIgnoreCase("PRODUCT") || type.equalsIgnoreCase("VALUE_MEAL") ||
    		  type.equalsIgnoreCase("NON_FOOD_PRODUCT")) && 
    	
    		  (qty.compareTo(new BigDecimal("0")) > 0 || 
    				  qtyPromo.compareTo(new BigDecimal("0")) > 0) 
    				  &&
    		  ( (itemlevel != null && itemlevel.compareTo(bgzero) == 0) ||
    				  "CHOICE".equalsIgnoreCase(itemaction)) 
    				  &&
    		Integer.parseInt(code) <= 9999	&&
    		(Integer.parseInt(grillModifier) ==0 && Integer.parseInt(grillQty) == 0)  &&
    	   displayOrder != -1
    	   ) {
     
	    String daypartinfo = findDayPartInfo(orderTimestamp,gdwTerrCd,daypartidmap,timeSegIdDescMap);
     	
     	BigDecimal netUnitPrice;
     	BigDecimal grossUnitPrice;
     	BigDecimal taxUnitPrice;
     	
     	BigDecimal netSalesAmount;
     	BigDecimal grossSalesAmount;
     	BigDecimal salesTaxAmount;
     	
     	BigDecimal netAmountBeforeDiscount;
     	BigDecimal grossAmountBeforeDiscount;
     	BigDecimal taxBeforeDiscount;
     	
        
        totalPrice = new BigDecimal(eleItem.getAttribute("totalPrice"));
	    totalTax = new BigDecimal(eleItem.getAttribute("totalTax"));
    
	    bdPrice  = new BigDecimal(eleItem.getAttribute("BDPrice"));
        bdTax    = new BigDecimal(eleItem.getAttribute("BDTax"));
        
     	//if net
     	if( "N".equalsIgnoreCase(menuPriceBasisMap.get(new Integer(gdwTerrCd)))){
     		netUnitPrice  			  = totalPrice.divide(qty,2,RoundingMode.HALF_UP);
     		grossUnitPrice			  = totalPrice.add(totalTax).divide(qty,2,RoundingMode.HALF_UP);
     		taxUnitPrice   			  = totalTax.divide(qty,2,RoundingMode.HALF_UP);
     		
     		netSalesAmount   		  = totalPrice.setScale(2,RoundingMode.HALF_UP);
     		grossSalesAmount 		  = totalPrice.add(totalTax).setScale(2,RoundingMode.HALF_UP);
     		salesTaxAmount   		  = totalTax.setScale(2,RoundingMode.HALF_UP);
     		
     		netAmountBeforeDiscount   = bdPrice.setScale(2,RoundingMode.HALF_UP);
     		grossAmountBeforeDiscount = bdPrice.add(bdTax).setScale(2,RoundingMode.HALF_UP);
     		taxBeforeDiscount         = bdTax.setScale(2, RoundingMode.HALF_UP);
     		
     	} else {	//gross
     		
     		netUnitPrice   			  = totalPrice.subtract(totalTax).divide(qty,2,RoundingMode.HALF_UP);
     		grossUnitPrice 			  = totalPrice.divide(qty,2,RoundingMode.HALF_UP);
     		taxUnitPrice   			  = totalTax.divide(qty,2,RoundingMode.HALF_UP);
     		
     		netSalesAmount   		  = totalPrice.subtract(totalTax).setScale(2,RoundingMode.HALF_UP);
     		grossSalesAmount 		  = totalPrice.setScale(2,RoundingMode.HALF_UP);
     		salesTaxAmount     		  = totalTax.setScale(2,RoundingMode.HALF_UP);
     		
     		netAmountBeforeDiscount   = bdPrice.subtract(bdTax).setScale(2,RoundingMode.HALF_UP);
     		grossAmountBeforeDiscount = bdPrice.setScale(2,RoundingMode.HALF_UP);
     		taxBeforeDiscount         = bdTax.setScale(2, RoundingMode.HALF_UP);
     	}
     	
     	posTrnStrtTs = formatAsTs(orderTimestamp);
     	
     	tmSegIdNu = getTimeSegment(posTrnStrtTs);
     	
     	posBusnDt = formatDateAsIsoDate(gdwBusinessDate);
     	       
     	outValueBf.setLength(0);
     	
        outValueBf.append(posBusnDt).append(OUTPUT_FIELD_SEPERATOR);								//pos_busn_dt
        outValueBf.append(gdwTerrCd).append(OUTPUT_FIELD_SEPERATOR);								//terr_cd
        outValueBf.append(gdwLgcyLclRfrDefCd).append(OUTPUT_FIELD_SEPERATOR);						//lgcy_lcl_rfr_def_cd
        outValueBf.append(code).append(OUTPUT_FIELD_SEPERATOR); 									//sld_menu_itm_id
        outValueBf.append(netUnitPrice.toString()).append(OUTPUT_FIELD_SEPERATOR);					//pos_itm_act_unt_prc_am
        outValueBf.append(currencyMap.get(new Integer(gdwTerrCd))).append(OUTPUT_FIELD_SEPERATOR);	//curn_ds
        outValueBf.append(tmSegIdNu).append(OUTPUT_FIELD_SEPERATOR);								//Keep
        outValueBf.append(daypartinfo.split("_")[0]).append(OUTPUT_FIELD_SEPERATOR);				//tm_seg_strt_tm
        outValueBf.append(daypartinfo.split("_")[1]).append(OUTPUT_FIELD_SEPERATOR);				//tm_seg_end_tm
        outValueBf.append(daypartinfo.split("_")[2]).append(OUTPUT_FIELD_SEPERATOR);				//dypt_id_nu						
        outValueBf.append(orderSaleType).append(OUTPUT_FIELD_SEPERATOR);							//prd_dlvr_meth_ds 
        outValueBf.append(orderKind).append(OUTPUT_FIELD_SEPERATOR);								//pos_trn_typ_ds
        outValueBf.append(trxSalePod).append(OUTPUT_FIELD_SEPERATOR);								//pos_area_ds
        outValueBf.append(qty).append(OUTPUT_FIELD_SEPERATOR);										//pos_itm_qty
        outValueBf.append(qtyPromo).append(OUTPUT_FIELD_SEPERATOR);									//pos_itm_promo_qty
        outValueBf.append(taxUnitPrice).append(OUTPUT_FIELD_SEPERATOR);								//pos_itm_act_tax_am
        outValueBf.append(orderNumber).append(OUTPUT_FIELD_SEPERATOR);								//Order Number
        outValueBf.append(fileTimestamp).append(OUTPUT_FIELD_SEPERATOR);
        outValueBf.append(netSalesAmount).append(OUTPUT_FIELD_SEPERATOR);							//netSalesAmount
        outValueBf.append(netAmountBeforeDiscount).append(OUTPUT_FIELD_SEPERATOR);					//netAmountBeforeDiscount
        outValueBf.append(type).append(OUTPUT_FIELD_SEPERATOR);										//type
        outValueBf.append(daypartinfo.split("_")[3]);												//segment description
        
          
        //mapKey.clear();
		
        //String keytemp = gdwBusinessDate + OUTPUT_FIELD_SEPERATOR + gdwTerrCd + OUTPUT_FIELD_SEPERATOR + gdwLgcyLclRfrDefCd + OUTPUT_FIELD_SEPERATOR + code + OUTPUT_FIELD_SEPERATOR + netUnitPrice.toString() + OUTPUT_FIELD_SEPERATOR + currencyMap.get(new Integer(gdwTerrCd));
        
        //if("2014-10-06|840|00971|3582|1.79|USD".equalsIgnoreCase(keytemp)){        	
        //mapKey.set(keytemp);
        
        mapKey.clear();
        mapKey.set(posBusnDt + OUTPUT_FIELD_SEPERATOR + gdwTerrCd + OUTPUT_FIELD_SEPERATOR + gdwLgcyLclRfrDefCd + OUTPUT_FIELD_SEPERATOR + code + OUTPUT_FIELD_SEPERATOR + netUnitPrice.toString() + OUTPUT_FIELD_SEPERATOR + currencyMap.get(new Integer(gdwTerrCd)));
		mapValue.clear();
		mapValue.set(outValueBf.toString());

        context.write(mapKey, mapValue);
          
        context.getCounter("Count", "ValidItemRecords").increment(1);
        //}
      }
    } catch (Exception ex) {
    	context.getCounter("ExceptionCount", "ProcessItems").increment(1);
    	ex.printStackTrace();
      Logger.getLogger(QuarterHourDaypartPMIXMapper.class.getName()).log(Level.SEVERE, null, ex);
    }

    return(eleItem.getChildNodes());
  }
  
  
  
  private static String findDayPartInfo(String timestamp,String terrCd,
		  TreeMap<String, TreeMap<String, String>> daypart_cntry_id_map,
		  HashMap<Integer,String> timeSegIdDescMap) {

		String datepart ;
		int daypartid  = -1;
		String timesegdesc  = null;
		String daypartinfo = null;
		String starttime = null;
		String endtime = null;
		
	
		if (timestamp != null && timestamp.trim().length() >= 12) {
			datepart = timestamp.substring(0,8);
			
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.YEAR, Integer.parseInt(datepart.substring(0,4)));
			cal.set(Calendar.MONTH, Integer.parseInt(datepart.substring(4,6))-1);
			cal.set(Calendar.DATE, Integer.parseInt(datepart.substring(6,8)));
			
			String key = terrCd+"_"+cal.get(Calendar.DAY_OF_WEEK)+"_"+"Quarter Hourly";
			
			TreeMap<String, String> hourmin_id_map = daypart_cntry_id_map.get(key);
			
			daypartinfo = lookupdaypartinfo(timestamp,hourmin_id_map,timeSegIdDescMap);
			
			if(daypartinfo != null){
				starttime  = daypartinfo.split("_")[0];
				endtime    = daypartinfo.split("_")[1];
				daypartid  = Integer.parseInt(daypartinfo.split("_")[2]);
				timesegdesc = timeSegIdDescMap.get(daypartid);
			}
			
			if(daypartid == -1){
				key = terrCd+"_"+cal.get(Calendar.DAY_OF_WEEK)+"_"+"Half Hourly";
				hourmin_id_map = daypart_cntry_id_map.get(key);
				daypartinfo = lookupdaypartinfo(timestamp,hourmin_id_map,timeSegIdDescMap);
				
				if(daypartinfo != null){
					starttime  = daypartinfo.split("_")[0];
					endtime    = daypartinfo.split("_")[1];
					daypartid  = Integer.parseInt(daypartinfo.split("_")[2]);
					timesegdesc = timeSegIdDescMap.get(daypartid);
				}
				
			}
			
			if(daypartid == -1){
				key = terrCd+"_"+cal.get(Calendar.DAY_OF_WEEK)+"_"+"Hourly";
				hourmin_id_map = daypart_cntry_id_map.get(key);
				daypartinfo = lookupdaypartinfo(timestamp,hourmin_id_map,timeSegIdDescMap);
				
				if(daypartinfo != null){
					starttime  = daypartinfo.split("_")[0];
					endtime    = daypartinfo.split("_")[1];
					daypartid  = Integer.parseInt(daypartinfo.split("_")[2]);
					timesegdesc = timeSegIdDescMap.get(daypartid);
				}
				
			}			
		}
		
		return starttime+"_"+endtime+"_"+daypartid+"_"+timesegdesc;
	}
	
	private static String lookupdaypartinfo(String timestamp,TreeMap<String, String> hourmin_id_map,HashMap<Integer,String> timeSegIdDescMap) {
		
	
		if(hourmin_id_map != null){
			Iterator< String> it = hourmin_id_map.keySet().iterator();
			
			String hour    = timestamp.substring(8, 10);
			String minutes = timestamp.substring(10, 12);
//			String seconds = timestamp.substring(12, 14);
			
			String hourminkey;
			int mins = Integer.parseInt(minutes);
			int hr   = Integer.parseInt(hour);
			
			String starttime = null;
			String endtime = null;
			
			while(it.hasNext()){
				hourminkey = it.next();
				int hrkey    = Integer.parseInt(hourminkey.split("_")[0]);
				int stminkey = Integer.parseInt(hourminkey.split("_")[1]);
				int endminkey= Integer.parseInt(hourminkey.split("_")[2]);
				
				if(hr == hrkey){
					if(mins >= stminkey && mins <= endminkey){
						
						starttime = String.format("%1$02d:%2$02d", hrkey,stminkey,"00");
						endtime = String.format("%1$02d:%2$02d", hrkey,endminkey,"59");
						
						
						return starttime+"_"+endtime+"_"+hourmin_id_map.get(hourminkey);
						
					}
				}
				
			}
					
		}
		
		return null;
	}
	
	private String getTimeSegment(String in) {
		
		String timeSegmentKey;
		String hour;
		String minute;
		int minuteInt;
		
		hour = in.substring(0, 2);
		minuteInt = Integer.parseInt(in.substring(3,5));
		
		if ( minuteInt < 15 ) {
			minute = "00";
		} else if ( minuteInt < 30 ) {
			minute = "15";
		} else if ( minuteInt < 45 ) {
			minute = "30";
		} else {
			minute = "45";
		}
		
		timeSegmentKey = hour + minute;

		return timeSegmentKey;		
	}
	
	private String formatAsTs(String in) {
		
		String retTs = "";
		
		if ( in.length() >= 14 ) {
			retTs = in.substring(8, 10) + ":" + in.substring(10, 12);
		}

		return(retTs);
	}
	
	private String formatDateAsIsoDate(String in) {
		
		String retIsoDate = "";
		
		if ( in.length() >= 8 ) {
			retIsoDate = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8);
		}

		return(retIsoDate);
		
	}

	  @Override 
	  protected void cleanup(Context contect) throws IOException, InterruptedException {
	    mos.close();
	  }
}

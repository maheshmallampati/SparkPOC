package com.mcd.gdw.daas.mapreduce;


import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
		

public class TMSHaviXmlMapper extends Mapper<LongWritable, Text, Text, Text> {

	 Text newkey = new Text();
	 Text newvalue = new Text();
	 
	  
	 
	 String fileName ="";
	 Configuration conf;
	 int cnt = 0;
	 DocumentBuilderFactory docFactory;
	 DocumentBuilder docBuilder;
  @Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
	  try{
	  docFactory = DocumentBuilderFactory.newInstance();
      docBuilder = docFactory.newDocumentBuilder();
	  fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	  conf = context.getConfiguration();
	  super.setup(context);
	  }catch (Exception ex) {
	    	
	      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
	    }
	}

@Override
  public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

	
	
   
    InputSource xmlSource;
    Document xmlDoc;
    Element eleTLD;

    try {
      String[] recArr = value.toString().split("\t");    
      String ownership = recArr[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS];
      
      
      
      
//      String decompstr = decompressString(recArr[recArr.length -1]);
      
     
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

String gdwBusinessDateFormatted = "";

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
      gdwBusinessDateFormatted = DaaSConstants.SDF_yyyy_MM_dd.format(DaaSConstants.SDF_yyyyMMdd.parse(gdwBusinessDate));
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
    }
  }

  StringBuffer hdrKeyBf = new StringBuffer();
  HashMap<String,String> offerIdPAIdMap = new HashMap<String,String>();
  Element eleOffer;
  String orderKey;
  String[] orderKeyParts;
  String orderNumber;
  String orderOfferId;
  String orderPromotionId;
  String orderOverrideF1;
  String orderAppliedFl;
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
    String trxSaleStatus;
    Element eleOrder = null;
    String orderTimestamp;
    
    
    
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

//            if(eleEvent.getAttribute("RegId").equals("582")){
//            	System.out.println ( "  oooo " + ((Element)eleEvent.getElementsByTagName("Order").item(0)).getAttribute("key"));
//            }

            if ( eleEvent.getNodeName().equals("Event") ) {
            	 context.getCounter("Count","2TotalEvents").increment(1);
           	  
               eventType = eleEvent.getAttribute("Type");
//               System.out.println (" event type "  + eleEvent.getAttribute("RegId") + " - " + eventType);

               if ( eventType.equalsIgnoreCase("TRX_Sale") || 
            		  eventType.equalsIgnoreCase("TRX_Refund") || 
            		 eventType.equalsIgnoreCase("TRX_Overring") || 
                    		  eventType.equalsIgnoreCase("TRX_Waste") ){
            	  
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
            		  if( (ntrxchldList.item(trxchldcnt).getNodeType() == Node.ELEMENT_NODE) &&
            				  (((Element)ntrxchldList.item(trxchldcnt)).getNodeName().equalsIgnoreCase("Order"))
            				  ){
            			  eleOrder = (Element)ntrxchldList.item(trxchldcnt);
            			  break;
            		  }
            	  }//end child nodes loop
            	  	
//                  eleOrder = (Element)eleTrxEvent.getFirstChild();
            	  
                  orderTimestamp = eleOrder.getAttribute("Timestamp");
                  orderKey = eleOrder.getAttribute("key");
                 /* if(!storeId.equalsIgnoreCase("32797"))
                  {
                	  return ;
                  }*/
                  /*if(!orderKey.equalsIgnoreCase("POS0001:35588273"))
                  {
                	  return ;
                  }
                  System.out.println("OrderKey------------>"+orderKey);*/
//                  if(cnt == 0)
//                	  System.out.println( " USE_FILTER_BY_STORE_ORDER " + conf.get("USE_FILTER_BY_STORE_ORDER") + conf.get("FILTER_BY_STORE")+ conf.get("FILTER_BY_ORDER"));
//                  cnt++;
//                  
//                  System.out.println(" storeid " + storeId + " orderkey " + orderKey);
                  
//                  if(orderKey.equalsIgnoreCase("POS0003:395153600"))
//                	  context.getCounter("Count","xxxxx"+fileName).increment(1);
//                  if(Integer.parseInt(storeId) != 2973){
//                	  return;
//                  }
//                  
                  if(conf.get("USE_FILTER_BY_STORE_ORDER").equalsIgnoreCase("TRUE")){
                  //|| !orderKey.equalsIgnoreCase("POS0002:419317810")
	                  if( !(Integer.parseInt(storeId) ==Integer.parseInt(conf.get("FILTER_BY_STORE")))){
	                	  context.getCounter("Count","ReturningonStoreID").increment(1);
	                	  return;
	                  }
	                  if(!(conf.get("FILTER_BY_ORDER") != null && orderKey.equalsIgnoreCase(conf.get("FILTER_BY_ORDER"))) ){
	                	  context.getCounter("Count","ReturningonOrder").increment(1);
	                	  return;
	                  }
                  }
                	  
                  context.getCounter("Count","NumOrders").increment(1);
                 // if(orderKey.equalsIgnoreCase("POS0003:395153600"))    	  context.getCounter("Count",fileName).increment(1);

                  //Sateesh: do we need to check for this condition. Will we always have status ( refund??)
                  if ( (eventType.equals("TRX_Sale") && trxSaleStatus.equals("Paid")) ||
                		  (eventType.equals("TRX_Refund") || 
                         		 eventType.equals("TRX_Overring") || 
                       		  eventType.equals("TRX_Waste")) ) {
                    
                	  context.getCounter("Count","NumOrdersPaidRefundStatus").increment(1);
                   
                	//order number
                      orderKeyParts = (orderKey+":@@@@").split(":");
                      orderNumber = orderKeyParts[1];
                      
                	  
                    //do we need to output only if there is a timestamp
                    if ( orderTimestamp.length() >= 12 ) {  
                    	
                    	
                    	NodeList ordOffers = eleOrder.getElementsByTagName("Offers");
                    	if(ordOffers != null && ordOffers.getLength() > 0){
	                    
                    		offerIdPAIdMap.clear();
                    		
                    		for(int i =0;i<ordOffers.getLength();i++){
                    			
                    			eleOffer = ((Element)ordOffers.item(i));
                    			
                    			offerIdPAIdMap.put(eleOffer.getAttribute("promotionId"), eleOffer.getAttribute("offerId"));
                    			
                    			orderOfferId = eleOffer.getAttribute("offerId");
                    			orderPromotionId = eleOffer.getAttribute("promotionId");
                    			orderOverrideF1 = getValue(
										eleOffer,
										"override");
								orderAppliedFl = getValue(
										eleOffer, "applied");
                    		}
                    		parseOrderDetails(eleOrder, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber,storeId,gdwTerrCd, context,ownership); 
	                    

                    		context.getCounter("Count", "OrderswithOffer").increment(1);
                    	}else{
                    		context.getCounter("Count", "OrderswithoutOffer").increment(1);
                    	}
                    	
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
    }
  }

  private void parseOrderDetails(Element eleOrder, String gdwLgcyLclRfrDefCd, String regId, String gdwBusinessDate, String orderNumber,String storeId,String gdwTerrCd, Context context,String ownership) throws IOException, InterruptedException {

   
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
              nlItem1 = processItems(eleOrderDetails, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, storeId,gdwTerrCd,context,ownership);

              if ( nlItem1 !=null && nlItem1.getLength() > 0 ) {
                for (int idxItem1=0; idxItem1 < nlItem1.getLength(); idxItem1++ ) {
                  if ( nlItem1.item(idxItem1).getNodeType() == Node.ELEMENT_NODE ) {
                    eleItem1 = (Element)nlItem1.item(idxItem1);
                    if ( eleItem1.getNodeName().equals("Item") ) {
                      nlItem2 = processItems(eleItem1, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, storeId,gdwTerrCd,context,ownership);
 
                      if ( nlItem2 !=null && nlItem2.getLength() > 0 ) {
                        for (int idxItem2=0; idxItem2 < nlItem2.getLength(); idxItem2++ ) {
                          if ( nlItem2.item(idxItem2).getNodeType() == Node.ELEMENT_NODE ) {
                            eleItem2 = (Element)nlItem2.item(idxItem2);
                            if ( eleItem2.getNodeName().equals("Item") ) {
                              processItems(eleItem2, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, storeId,gdwTerrCd,context,ownership);
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
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  StringBuffer outValueBf = new StringBuffer();
  StringBuffer pmixKeyBf  = new StringBuffer();
  private NodeList processItems(Element eleItem,
                                String gdwLgcyLclRfrDefCd,
                                String regId,
                                String gdwBusinessDate,
                                String orderNumber,
                                String storeId,
                                String gdwTerrCd,
                                Context context,
                                String ownership) {

    String type;
    String qtyText;
    String qtyPromoText;
    BigDecimal qty;
    BigDecimal qtyPromo;
 
    int displayOrder = -1; //src field
   
    String itemleveltxt;
    String itemaction;
    BigDecimal itemlevel;
    
    String grillModifier;
    String grillQty;
    String code;
    Boolean redeemedFlg=false;
    Boolean purchasedFlg=false;
    BigDecimal totalPrice=null;
    String discountType="";
    
     
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
      
      NodeList itemPA;
      String paPromotionId = "";

      
//      if ( (type.equalsIgnoreCase("PRODUCT") || type.equalsIgnoreCase("VALUE_MEAL") ||
//    		  type.equalsIgnoreCase("NON_FOOD_PRODUCT")) && 
//    	
//    		  (qty.compareTo(new BigDecimal("0")) > 0 || 
//    				  qtyPromo.compareTo(new BigDecimal("0")) > 0) 
//    				  &&
//    		  ( (itemlevel != null && itemlevel.compareTo(BigDecimal.ZERO) == 0) ||
//    				  "CHOICE".equalsIgnoreCase(itemaction)) 
//    				  &&
//    		code != null && !code.isEmpty() && Integer.parseInt(code) <= 9999	&&
//    				grillModifier != null && !grillModifier.isEmpty() && (Integer.parseInt(grillModifier) ==0 && 
//    			    grillQty != null && !grillQty.isEmpty() &&  Integer.parseInt(grillQty) == 0)  &&
//    	   displayOrder != -1
//    	   ){
    	  
    	  	
    	  	itemPA =eleItem.getElementsByTagName("PromotionApplied");
    	  	if(itemPA == null){
    	  		itemPA = eleItem.getElementsByTagName("Promotion");
    	  	}
    	  	if(itemPA != null && itemPA.getLength() > 0){
    	  		for(int i=0;i<itemPA.getLength();i++){
    	  			paPromotionId = ((Element)itemPA.item(i)).getAttribute("promotionId");
    	  			discountType=((Element)itemPA.item(i)).getAttribute("discountType");
    	  			if(discountType.equalsIgnoreCase("Promo") || discountType.equalsIgnoreCase("Price")){redeemedFlg=true;};
    	  			if(discountType.equalsIgnoreCase("")){purchasedFlg=true;};
    	  			
//    	  			offerId = offerIdPAIdMap.get(paPromotionId);
    	  			boolean reedemed=false;
   	  			
    	  			if("FALSE".equalsIgnoreCase(orderOverrideF1)){
    	  				if("TRUE".equalsIgnoreCase(orderAppliedFl) && paPromotionId != null && Integer.parseInt(paPromotionId) > 0) {
    	  					reedemed=true;
    	  					//allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
    	  				}else{
    	  					//allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
    	  				}
    	  			}else if("TRUE".equalsIgnoreCase(orderOverrideF1)){
    	  				
    	  				
    	  				if( "TRUE".equalsIgnoreCase(orderAppliedFl)){
    	  					if( paPromotionId != null  && Integer.parseInt(paPromotionId) > 0){
    	  						reedemed=true;
    	  						//allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
    	  					}else{
    	  						//allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
    	  					}
    	  				}else if("FALSE".equalsIgnoreCase(orderAppliedFl)){
    	  					if( paPromotionId != null  && Integer.parseInt(paPromotionId) > 0){
    	  						reedemed=true;
    	  						//allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
    	  					}else{
    	  						//allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
    	  					}
    	  						
    	  				}else{
    	  					//allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
    	  				}
    	  			}
    	  			
    	  		
    	  			if (reedemed) {
    	  			newkey.clear();
    	  			newkey.set(paPromotionId+DaaSConstants.PIPE_DELIMITER+storeId+DaaSConstants.PIPE_DELIMITER+gdwBusinessDateFormatted+DaaSConstants.PIPE_DELIMITER+gdwTerrCd+DaaSConstants.PIPE_DELIMITER+orderKey);
    	  			
    	  			newvalue.clear();
    	  			newvalue.set(orderKey+DaaSConstants.PIPE_DELIMITER+paPromotionId+DaaSConstants.PIPE_DELIMITER+code+ DaaSConstants.PIPE_DELIMITER+
    						orderOverrideF1+ DaaSConstants.PIPE_DELIMITER+
    						orderAppliedFl+ DaaSConstants.PIPE_DELIMITER+
    						redeemedFlg+ DaaSConstants.PIPE_DELIMITER+
    						purchasedFlg+ DaaSConstants.PIPE_DELIMITER+
    						discountType);
    	  			
    	  			context.write(newkey, newvalue);
//    	  			context.write(key, value);
    	  		}
    	  		}
//    	  	}
//    	  	else{
//    	  		newkey.clear();
//	  			newkey.set(offerId+"_"+storeId+"_"+gdwBusinessDate);
//	  			
//	  			newvalue.clear();
//	  			newvalue.set(orderKey+DaaSConstants.TAB_DELIMITER+""+DaaSConstants.TAB_DELIMITER+code);
//    	  		
//    	  	}
    	  	
    	  	
          context.getCounter("Count", "ValidDTLRecords").increment(1);
        }else{

  	      /*  Offer ID	OverrideFlag	Applied Flag	     				Promotion ID	         Considered Redeemed ? (Y/N)
  			Exists	    FALSE	           TRUE	             				Exists (Non-Zero)	        Y
  			Exists	    TRUE	           TRUE	          					Exists (Non-Zero)	        Y
  			Exists	    TRUE	           FALSE	        				0	                   	    Y
  			Exists	    TRUE	           FALSE							Exists (Non-Zero)	        Y
  			Exists	    FALSE	           FALSE							Exists (Non-Zero)		    N
  			Exists	    FALSE	           FALSE							0						    N
  			Exists	    Doesn’t exist	   Doesn’t exist					Doesn’t Exist 				N*/
  			
        
		boolean reedemed=false;
		/*if (!orderOfferId.isEmpty() && (!orderAppliedFl.equalsIgnoreCase("false") && (!orderPromotionId.isEmpty() && Integer.parseInt(orderPromotionId)!=0))) {reedemed=true;}
		if (!orderOfferId.isEmpty() && (!orderOverrideF1.equalsIgnoreCase("false")) &&  (!orderPromotionId.isEmpty())) {reedemed=true;}
		
		if (!orderOfferId.isEmpty() && (!orderAppliedFl.equalsIgnoreCase("false") && (!orderPromotionId.isEmpty() && Integer.parseInt(orderPromotionId)!=0))) {reedemed=true;}
		if (!orderOfferId.isEmpty() && (!orderOverrideF1.equalsIgnoreCase("false")) &&  (!orderPromotionId.isEmpty())) {reedemed=true;}
		*/
		
		if("FALSE".equalsIgnoreCase(orderOverrideF1)){
			if("TRUE".equalsIgnoreCase(orderAppliedFl) && orderPromotionId != null && Integer.parseInt(orderPromotionId) > 0) {
				reedemed=true;
				//allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
			}else{
				//allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
			}
		}else if("TRUE".equalsIgnoreCase(orderOverrideF1)){
			
			
			if( "TRUE".equalsIgnoreCase(orderAppliedFl)){
				if( orderPromotionId != null  && Integer.parseInt(orderPromotionId) > 0){
					reedemed=true;
					//allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
				}else{
					//allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
				}
			}else if("FALSE".equalsIgnoreCase(orderAppliedFl)){
				if( orderPromotionId != null  && Integer.parseInt(orderPromotionId) > 0){
					reedemed=true;
					//allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
				}else{
					//allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
				}
					
			}else{
				//allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
			}
		}
		
	
		if (reedemed) {
			//if (!offerId.isEmpty()) {
			//System.out.println("here with order"+orderKey );
			newkey.clear();
			newkey.set(orderPromotionId + DaaSConstants.PIPE_DELIMITER
					+ storeId + DaaSConstants.PIPE_DELIMITER
					+ gdwBusinessDateFormatted
					+ DaaSConstants.PIPE_DELIMITER + gdwTerrCd+DaaSConstants.PIPE_DELIMITER+orderKey);

		newvalue.clear();
			// newvalue.set(orderKey+DaaSConstants.PIPE_DELIMITER+""+DaaSConstants.PIPE_DELIMITER+code);
		newvalue.set(orderKey + DaaSConstants.PIPE_DELIMITER
					+ orderPromotionId + DaaSConstants.PIPE_DELIMITER
					+ code+ DaaSConstants.PIPE_DELIMITER+
					orderOverrideF1+ DaaSConstants.PIPE_DELIMITER+
					orderAppliedFl+ DaaSConstants.PIPE_DELIMITER+
					redeemedFlg+ DaaSConstants.PIPE_DELIMITER+
					purchasedFlg+ DaaSConstants.PIPE_DELIMITER+
					discountType);
			
			context.write(newkey, newvalue);

			context.getCounter("Count", "NoTaxChainElementFound")
					.increment(1);
		}
	
        }
//      }
      
    } catch (Exception ex) {
    	context.getCounter("ExceptionCount", "ProcessItems"+context.getTaskAttemptID()).increment(1);
    	ex.printStackTrace();
      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
    }

    return(eleItem.getChildNodes());
  }
  private String getValue(Element ele, String attribute) {

		String retValue = "";

		try {
			retValue = ele.getAttribute(attribute);

		} catch (Exception ex) {
		}

		return (retValue.trim());
	}
}

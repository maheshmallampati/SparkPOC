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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.fs.FileSystem;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.driver.APTDriver;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.PaymentMethod;
import com.mcd.gdw.daas.util.SimpleEncryptSHA1;
/**
 * 
 * @author Sateesh Pula
 * This mapper reads the STLD records and output fields that are part of TDA format sales file. 
 * It reads 3 distributed cache files in the setup and prepares a HashMap for each file. These values are
 * used when creating the output record. It outputs 2 records. One is the HDR format and one is the  DTL format.
 * 
 */

public class NpStldXmlExtendedMapper extends Mapper<LongWritable, Text, Text, Text> {

	 
	  
	  private MultipleOutputs<Text, Text> mos;
	  BigDecimal bgzero = new BigDecimal("0.00");
	  
	  //  private String owshFltr = "*";
	  Context thiscontext;
	  
	  TDAExtractMapperUtil tdaExtractMapperUtil = new TDAExtractMapperUtil();
	  Text newkey = new Text();
	  Text newvalue = new Text();
	  
	  Text outputValue = new Text();
	  Text outputKey = new Text();
	  
	  JSONObject itemJSObj = new JSONObject();
	  JSONArray itemColsArray = new JSONArray();
	  Set itemValues = new HashSet();
	  int mapcount = 0;
	  boolean hasRecordsToWrite = false;
	  
	  HashSet<String> validStores = new HashSet<String>();
	  JSONArray itemValsJSArr = new JSONArray();
	  boolean filterOnStoreId = false;
	  private HashSet<String> storeFilterSet;
	  String timestamp="";
	  //String terrCD="";
	    
	  String fileName;
	   //private StringBuffer tdaHeader=new StringBuffer();
	  //private StringBuffer allInfo = new StringBuffer();
	  //private StringBuffer tdaData=new StringBuffer();
	  String format="";
	  DocumentBuilderFactory docFactory;
	  DocumentBuilder docBuilder;
	  StringBuffer tdaData=new StringBuffer();
	  public static SimpleDateFormat SDF_yyyyMMddHHmmssSSS = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");
	  public static final String GDW_DATE_SEPARATOR = "";
	  public static final String TIME_SEPARATOR = ":";
	  public static final String TIME_MILLISEC_SEPARATOR = ":";
	  String generateHeaders="";
	  String generateExtraFields="";
	  int seqNum;
	  
	  
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException{
	     BufferedReader br = null;

	    try {
	    	docFactory = DocumentBuilderFactory.newInstance();
		    docBuilder = docFactory.newDocumentBuilder();
		    format=context.getConfiguration().get("OUTPUT_FILE_FORMAT");
	    	fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
	    	
	    	thiscontext = context;
	    	mos = new MultipleOutputs<Text, Text>(context);
	    	
	    	String vldStores = context.getConfiguration().get("VALID_STORES");
	    	//terrCD=context.getConfiguration().get("TERR_CD");
	    	
	    	if(vldStores == null || (vldStores != null && vldStores.equalsIgnoreCase("*"))){
	    		validStores.add("*");
	    	}else if(vldStores != null){
	    		String[] vldStoresArr = vldStores.split(",",-1);
	    		for(int indx=0;indx < vldStoresArr.length;indx++){
	    			validStores.add(vldStoresArr[indx]);
	    			System.out.println(" adding valid stores " + vldStoresArr[indx]);
	    		}
	    	}
	    	
	    	String filterOnStoreIdStr = context.getConfiguration().get(DaaSConstants.JOB_CONFIG_PARM_STORE_FILTER);
	    	if(filterOnStoreIdStr != null && filterOnStoreIdStr.equalsIgnoreCase("true")){
	    		filterOnStoreId = true;
	    	}
	    	
	    	timestamp=context.getConfiguration().get("TIMESTAMP");
	    	generateHeaders=context.getConfiguration().get("GENERATE_HEADERS");
	    	generateExtraFields=context.getConfiguration().get("GENERATE_EXTRA_FIELDS");
	    		
//	    	Path[] distPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());

			URI[] distPaths =  context.getCacheFiles();

			//AWS START
			//FileSystem fs = FileSystem.get(context.getConfiguration());
			FileSystem fs = HDFSUtil.getFileSystem(context.getConfiguration().get(DaaSConstants.HDFS_ROOT_CONFIG), context.getConfiguration());
			//AWS END


//	      if (distPaths == null){
//	    	  System.err.println("distpath is null");
//	      }

			if(distPaths == null){
				System.err.println("distpath is null");
			}

			Path distpath = null;
			String[] distPathParts;
			if ( distPaths != null && distPaths.length > 0 )  {

				System.out.println(" number of distcache files : " + distPaths.length);

				for ( int i=0; i<distPaths.length; i++ ) {

					//distpath = distPaths[i];

					System.out.println("distpaths:" + distPaths[i].toString());
					//System.out.println("distpaths URI:" + distPaths[i].toUri());

					distPathParts = 	distPaths[i].toString().split("#");
					if( distPaths[i].toString().contains("STORE_FILTER.txt") ) {
						//AWS Start
						br = new BufferedReader(new InputStreamReader(fs.open(new Path("./" + distPathParts[1]))));

						//br  = new BufferedReader(new FileReader("./" + distPathParts[1]));

						storeFilterSet = new HashSet<String>();
						addValuesToSet(storeFilterSet, br);
					}


				}
			}
	    	
	    	mapcount = 0;

	    if ( format.equalsIgnoreCase("tsv") ) {
	    	itemColsArray.add("TERR_CD");
	    	itemColsArray.add("MCD_GBAL_LCAT_ID_NU");
	    	itemColsArray.add("POS_BUSN_DT");
	    	itemColsArray.add("POS_ORD_KEY_ID");
	    	itemColsArray.add("pos_itm_line_seq_nu");
	    	itemColsArray.add("LGCY_LCL_RFR_DEF_CD");
	    	itemColsArray.add("POS_EVNT_RGIS_ID");
	    	itemColsArray.add("POS_EVNT_TS");
	    	itemColsArray.add("POS_TRN_STUS_CD");
	    	itemColsArray.add("POS_AREA_TYP_SHRT_DS");
	    	itemColsArray.add("POS_TRN_STRT_TS");
	    	itemColsArray.add("POS_ORD_UNIQ_ID");
	    	itemColsArray.add("POS_TRN_KIND_CD");
	    	itemColsArray.add("POS_PRD_DLVR_METH_CD");
	    	itemColsArray.add("POS_TOT_NET_TRN_AM");
	    	itemColsArray.add("POS_TOT_TAX_AM");
	    	itemColsArray.add("SLD_MENU_ITM_ID");
	    	itemColsArray.add("SLD_MENU_ITM_ID");
	    	itemColsArray.add("POS_ITM_TOT_QT");
	    	itemColsArray.add("POS_ITM_PRMO_QT");
	    	itemColsArray.add("POS_ITM_BP_PRC_AM");
	    	itemColsArray.add("POS_ITM_BP_TAX_AM");
	    	itemColsArray.add("POS_ITM_BD_TAX_AM");
	    	itemColsArray.add("POS_ITM_BD_PRC_AM");
	    	itemColsArray.add("POS_ITM_NET_TOT_AM");
	    	itemColsArray.add("POS_ITM_TOT_TAX_AM");
	    	itemColsArray.add("POS_ITM_UNT_PRC_AM");
	    	itemColsArray.add("POS_ITM_UNT_TAX_AM");
	    	itemColsArray.add("ITM_PRMO_APPD_DIGL_OFFR_ID");
	    	itemColsArray.add("ITM_PRMO_APPD_OFFR_CTER_QT");
	    	itemColsArray.add("ITM_PRMO_APPD_OFFR_ELIG_FL");
	    	itemColsArray.add("ITM_PRMO_APPD_ORGN_PRC_AM");
	    	itemColsArray.add("ITM_PRMO_APPD_DISC_AM");
	    	itemColsArray.add("ITM_PRMO_APPD_DISC_TYP_CD");
	    	itemColsArray.add("ITM_PRMO_APPD_ORGN_OFFR_QT");
	    	itemColsArray.add("ITM_PRMO_APPD_ORGN_PRD_CD");
	    	itemColsArray.add("ITM_PRMO_APPD_CUST_OFFR_ID");
	    	itemColsArray.add("POS_PRMO_DIGL_OFFR_ID");
	    	itemColsArray.add("POS_PRMO_CTER_QT");
	    	itemColsArray.add("POS_PRMO_DISC_TYP_CD");
	    	itemColsArray.add("POS_PRMO_DISC_AM");
	    	itemColsArray.add("POS_PRMO_CUST_OFFR_ID");
	    	itemColsArray.add("POS_PRMO_XCLU_FL");
	    	itemColsArray.add("CUST_OFFR_TAG_ID");
	    	itemColsArray.add("CUST_OFFR_ID");
	    	itemColsArray.add("CUST_OFFR_OVRD_FL");
	    	itemColsArray.add("CUST_OFFR_APPD_FL");
	    	itemColsArray.add("CUST_OFFR_CLER_AFT_OVRD_FL");
	    	itemColsArray.add("CUST_OFFR_DIGL_OFFR_ID");
	    	itemColsArray.add("CMIN_CUST_ID");
	    	itemColsArray.add("CMIN_ORD_ID");
	    	itemColsArray.add("CMIN_PAID_MOBL_ORD_FL");
	    	itemColsArray.add("CMIN_TEND_CNT_QT");
	    	itemColsArray.add("CMIN_TEND_VAL_AM");
	    	itemColsArray.add("CMIN_CSHL_CARD_PVDR_TYP_CD");
	    	itemColsArray.add("CMIN_CSHL_AM");
	    	itemColsArray.add("CMIN_TOT_DISC_AM");
	    	itemColsArray.add("CMIN_TOT_B4_DISC_AM");
	    	itemColsArray.add("CMIN_DIGL_OFFR_ID");
	    	itemColsArray.add("CMIN_DISC_TYP_CD");
	    	itemColsArray.add("POS_TEND_TYP_CD");
	    	itemColsArray.add("POS_PYMT_METH_TYP_CD");
	    	itemColsArray.add("POS_PYMT_METH_DS_TX");
	    	itemColsArray.add("POS_TEND_QT");
	    	itemColsArray.add("POS_TEND_FACE_VAL_AM");
	    	itemColsArray.add("POS_TEND_AM");

	    } else {
	      itemColsArray.add("out_parent_node");
	   	  itemColsArray.add("storeId");
	   	  itemColsArray.add("businessDate");
	   	  //itemColsArray.add("");
	   	  itemColsArray.add("EventRegisterId");
	   	  itemColsArray.add("EventTime");
	   	  itemColsArray.add("status");
	   	  itemColsArray.add("POD");
	   	  itemColsArray.add("OrderTimestamp");
	   	  itemColsArray.add("OrderuniqueID");
	   	  itemColsArray.add("Orderkind");
	   	  itemColsArray.add("Orderkey");
	   	  itemColsArray.add("OrdersaleType");
	   	  itemColsArray.add("OrderTotalAmount");
	   	  itemColsArray.add("OrderTotalTax");
	   	  itemColsArray.add("ItemCode");
	   	  itemColsArray.add("ItemQty");
	   	  itemColsArray.add("ItemQtyPromo");
	   	  itemColsArray.add("ItemBPPrice");
	   	  itemColsArray.add("ItemBPTax");
	   	  itemColsArray.add("ItemBDPrice");
	   	  itemColsArray.add("ItemBDTax");
	   	  itemColsArray.add("ItemTotalPrice");
	   	  itemColsArray.add("ItemTotalTax");
	   	  itemColsArray.add("ItemDescription");
	   	  itemColsArray.add("ItemUnitPrice");
	   	  itemColsArray.add("ItemUnitTax");
	   	  itemColsArray.add("promotionID");
	   	  itemColsArray.add("promotionCounter");
	   	  itemColsArray.add("eligible");
	   	  itemColsArray.add("originalPrice");
	   	  itemColsArray.add("discountAmount");
	   	  itemColsArray.add("discountType");
	   	  itemColsArray.add("originalPromoQty");
	   	  itemColsArray.add("originalProductCode");
	   	  itemColsArray.add("offerId");
	   	  itemColsArray.add("promotionId");
	   	  itemColsArray.add("promotionCounter");
	   	  itemColsArray.add("discountType");
	   	  itemColsArray.add("discountAmount");
	   	  itemColsArray.add("offerId");
	   	  itemColsArray.add("exlusive");
	   	  itemColsArray.add("tagId");
	   	  itemColsArray.add("offerId");
	   	  itemColsArray.add("override");
	   	  itemColsArray.add("applied");
	   	  itemColsArray.add("clearAfterOverride");
	   	  itemColsArray.add("promotionID");
	   	  itemColsArray.add("customerId");
	   	  itemColsArray.add("OrderId");
	   	  itemColsArray.add("isPaidMobileOrder");
	   	  itemColsArray.add("TenderQty");
	   	  itemColsArray.add("TenderValue");
	   	  itemColsArray.add("TenderCashlessCardProv");
	   	  itemColsArray.add("TenderCashlessAmount");
	   	  itemColsArray.add("totalDiscount");
	   	  itemColsArray.add("totalBD");
	   	  itemColsArray.add("discountId");
	   	  itemColsArray.add("discountType");
	   	  itemColsArray.add("TenderID");
	   	  itemColsArray.add("TenderKind");
	   	  itemColsArray.add("TenderName");
	   	  itemColsArray.add("TenderQuantity");
	   	  itemColsArray.add("TenderFacevalue");
	   	  itemColsArray.add("TenderAmount");
	    }
/*	   	   tdaHeader.setLength(0);
	   	   tdaHeader.append("Out_parent_node");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	   	   tdaHeader.append("StoreId");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("BusinessDate");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	  // psvHeader.append("");
	 	   tdaHeader.append("EventRegisterId");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("EventTime");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("Status");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("POD");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderTimestamp");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderuniqueID");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("Orderkind");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("Orderkey");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrdersaleType");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderTotalAmount");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderTotalTax");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemCode");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemQty");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemQtyPromo");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemBPPrice");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemBPTax");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemBDPrice");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemBDTax");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemTotalPrice");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemTotalTax");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemDescription");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemUnitPrice");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemUnitTax");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemPromoAppliedPromoID");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemPromoAppliedPromotionCounter");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("Eligible");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OriginalPrice");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemPromoAppliedDiscountAmount");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemPromotionAppliedDiscountType");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OriginalPromoQty");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OriginalProductCode");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ItemPromoAppliedOfferID");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderPromotionPromoID");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderPromotionPromotionCounter");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderPromotionDiscountType");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderPromotionDiscountAmount");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderPromotionOfferID");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("Exlusive");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TagId");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderOfferOfferID");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("Override");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("Applied");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("ClearAfterOverride");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderOfferPromoID");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("CustomerId");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("OrderId");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("IsPaidMobileOrder");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderQty");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderValue");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderCashlessCardProv");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderCashlessAmount");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TotalDiscount");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TotalBD");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("DiscountId");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("DiscountType");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderID");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderKind");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderName");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderQuantity");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderFacevalue");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("TenderAmount");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("CardType");tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("CardNumber");
	 	   
	 	   if(Integer.parseInt(terrCD)!=36){	 	   
	 	   tdaHeader.append(DaaSConstants.PIPE_DELIMITER);
	 	   tdaHeader.append("CardExpirationDate");
	 	   }
	 	   
	 	  // tdaData.setLength(0);
	 	   allInfo.setLength(0);
	 	   allInfo.append(tdaHeader.toString());*/
	   	  
	   	  
	      
	     }catch (Exception ex) {
	      ex.printStackTrace();
	     
	    }finally{
	    	try{
	    		if(br != null)
	    			br.close();
	    	}catch(Exception ex){
	    		ex.printStackTrace();
	    	}
	    }
	  }
	  
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

	  String gdwBusinessDate =null;
	  String gdwTerrCd = null;
	  String gdwMcdGbalLcatIdNu = null;
	 
	  @Override
	  public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		
		
	   
	    InputSource xmlSource;
	    Document xmlDoc;
	    Element eleTLD;

	    try {
	      String[] recArr = value.toString().split("\t");
	      
	      String terrCdStoreId = recArr[DaaSConstants.XML_REC_TERR_CD_POS]+"_"+Integer.parseInt(recArr[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]);
	      
	      if(filterOnStoreId && !storeFilterSet.contains(terrCdStoreId)){
	    	  context.getCounter("Count","SkippingonStoreFilter").increment(1);
	    	  return;
	      }else{
	    	  context.getCounter("Count","ALBRLHVALIDSTORES").increment(1);
	      }
	    
	      
//	      String ownership = recArr[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS];
//	      if(!owshFltr.equals("*") && !owshFltr.equalsIgnoreCase(recArr[DaaSConstants.XML_REC_REST_OWSH_TYP_SHRT_DS_POS])){
//	    	    context.getCounter("Count", "SKIP OWSH FILTER").increment(1);
//				return;
//	  	  }
	      
//	      String decompstr = decompressString(recArr[recArr.length -1]);
	      
	      docFactory = DocumentBuilderFactory.newInstance();
	      docBuilder = docFactory.newDocumentBuilder();
	      xmlSource = new InputSource(new StringReader(recArr[recArr.length -1]));
//	      xmlSource = new InputSource(new StringReader(decompstr));
	      xmlDoc = docBuilder.parse(xmlSource);

	      eleTLD = (Element)xmlDoc.getFirstChild();

	      if (eleTLD.getNodeName().equals("TLD") ) {

	    	  gdwBusinessDate = eleTLD.getAttribute("gdwBusinessDate");
	    	  gdwMcdGbalLcatIdNu = eleTLD.getAttribute("gdwMcdGbalLcatIdNu");
//	    	  mos.write("USRxD126TASSEORxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp,NullWritable.get(), tdaHeader.toString());
	          eleTLD.getAttribute("gdwLgcyLclRfrDefCd"); 
	          itemValsJSArr = new JSONArray();
	    	  itemValues = new HashSet();
	          npParseXml(eleTLD, context);                 
	    	  
	          
	          if(itemValues.size() > 0 && format.equalsIgnoreCase("json")){
	        	 
	        	  hasRecordsToWrite = true;
	        	  if(mapcount == 0){
		        	  itemJSObj = new JSONObject();
//		        	  itemJSObj.put("columns", (Object)itemColsArray);
		        	  
		          }
		          
//	        	  if(Integer.parseInt(gdwTerrCd) == 36){
	        		  JSONArray jarr = new JSONArray();
	        		  jarr.addAll(itemValues);
		          
	    	          itemJSObj.put("data", (Object)jarr);
	        		  
	    	          /**
	        	  }else{
	        		  JSONObject itemobj = new JSONObject();
//	    	          itemobj.put("Item",(Object)jarr);
//	    	          itemobj.put("Item",itemValues);
	    	          
//	    	          System.out.println(" itemValsJSArr **************** " + itemValsJSArr.toJSONString());
	    	          itemobj.put("Item",itemValsJSArr);
	    	          
//	    	          itemJSObj.put("Data", (Object)jarr);
	    	          itemJSObj.put("Data", itemobj);	  
	        	  }
		          **/                      
		                		          
		          mapcount++;
	          }
	         
	      }
	    } catch (Exception ex) {
	    	
	      Logger.getLogger(NpStldXmlExtendedMapper.class.getName()).log(Level.SEVERE, null, ex);
	    }
	  }
	  
	  

	  private void npParseXml(Element eleTLD, Context context) throws IOException, InterruptedException {

	    String gdwLgcyLclRfrDefCd;
	    String gdwBusinessDate;
	    
	    NodeList nlNode;
	    Element eleNode;
	    String storeId;

	    try {
	      gdwLgcyLclRfrDefCd = eleTLD.getAttribute("gdwLgcyLclRfrDefCd");
	      gdwBusinessDate = eleTLD.getAttribute("gdwBusinessDate");
	      gdwTerrCd = eleTLD.getAttribute("gdwTerrCd");
	      storeId   = eleTLD.getAttribute("storeId");
	      
//	      if( validStores != null && !validStores.contains("*") && !validStores.contains(storeId)){
//	    	  System.out.println( "  invalid store " + storeId);
//	    	  context.getCounter("Count", "InValidStores").increment(1);
//	    	  return;
//	      }else{
//	    	  
//	    	  context.getCounter("Count", "ValidStores").increment(1);
//	      }
	      
//	      if(!storeId.equalsIgnoreCase("865") || !gdwBusinessDate.equalsIgnoreCase("20120803"))
//	    	  return;

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
	      Logger.getLogger(NpStldXmlExtendedMapper.class.getName()).log(Level.SEVERE, null, ex);
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
	        	  
	        	  
	        	  
	            eleEvent = (Element)nlEvent.item(idxEvent);

	            if ( eleEvent.getNodeName().equals("Event") ) {
	            	context.getCounter("Count","TotalEvents").increment(1);
	              eventType = eleEvent.getAttribute("Type");
//	              System.out.println(" eventType " + eventType);
	              if ( eventType.equalsIgnoreCase("TRX_Sale") || 
	            		  eventType.equalsIgnoreCase("TRX_Refund") || 
	            		 eventType.equalsIgnoreCase("TRX_Overring") || 
	                    		  eventType.equalsIgnoreCase("TRX_Waste") ){
	            	  
	            	  seqNum = 0;
	            	  
	            	  context.getCounter("Count","TRXSALEREFUNDWASTE").increment(1);
	            	  
	            	  NodeList nevntchldList = eleEvent.getChildNodes();
	            	  
	            	  for(int chldcnt=0;chldcnt < nevntchldList.getLength();chldcnt++){
	            		  if(nevntchldList.item(chldcnt).getNodeType() == Node.ELEMENT_NODE){
	            			  eleTrxEvent = (Element)nevntchldList.item(chldcnt);
	            			  break;
	            		  }
	            	  }//end child nodes loop
//	            	  eleTrxEvent = (Element)eleEvent.getFirstChild();
	            	  
	            	  if(eleTrxEvent != null){
	                  trxSaleStatus = eleTrxEvent.getAttribute("status");
	                  
	                  NodeList ntrxchldList = eleTrxEvent.getChildNodes();
	            	  
	            	  for(int trxchldcnt=0;trxchldcnt < ntrxchldList.getLength();trxchldcnt++){
	            		  if(ntrxchldList.item(trxchldcnt).getNodeType() == Node.ELEMENT_NODE){
	            			  eleOrder = (Element)ntrxchldList.item(trxchldcnt);
	            			  break;
	            		  }
	            	  }//end child nodes loop
	            	  
	            	  
	            	  orderTimestamp = eleOrder.getAttribute("Timestamp");
	                  orderKey = eleOrder.getAttribute("key");
	                  
	                  context.getCounter("Count","NumOrders").increment(1);
	                  
	                  if("true".equalsIgnoreCase(context.getConfiguration().get("CHECK_FOR_CUSTINFO"))){
		            	  //process orders only if custominfo is present for an order
		            	  if(!hasCustomerInfo(eleOrder)){
		            		  context.getCounter("Count","NumOrdersWithoutCustomInfo").increment(1);
		            		  
		            		  continue;
		            	  }else{
		            		  context.getCounter("Count","NumOrdersWithCustomInfo").increment(1);
		            		  context.getCounter("Count",fileName);
		            	  }
	                  }
	            	  	
//	                  eleOrder = (Element)eleTrxEvent.getFirstChild();
	            	  
	                 
	                  
	                 

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
	                        orderTimestamp.substring(0,8);
	                        
	                        orderTimestamp.substring(8,12);
	                        
	                        eleTrxEvent.getAttribute("POD");
		                    
		                    
		                    
	                        eleOrder.getAttribute("saleType");
		                    
		               
		                     parseOrderDetails(eleEvent,eleTrxEvent,eleOrder, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber,storeId,gdwTerrCd, context  ); 
		                    
		                 
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
	      Logger.getLogger(NpStldXmlExtendedMapper.class.getName()).log(Level.SEVERE, null, ex);
	      ex.printStackTrace();
	    }
	  }
	  
	  

	  private void parseOrderDetails(Element eleEvent,Element eleTrxEvent,Element eleOrder, String gdwLgcyLclRfrDefCd, String regId, 
			  String gdwBusinessDate, String orderNumber,String storeId,String gdwTerrCd, Context context) throws IOException, InterruptedException {

	    
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
	              nlItem1 = processItems(eleEvent,eleTrxEvent,eleOrder,eleOrderDetails, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, storeId,gdwTerrCd,context);

	              if ( nlItem1 !=null && nlItem1.getLength() > 0 ) {
	                for (int idxItem1=0; idxItem1 < nlItem1.getLength(); idxItem1++ ) {
	                  if ( nlItem1.item(idxItem1).getNodeType() == Node.ELEMENT_NODE ) {
	                    eleItem1 = (Element)nlItem1.item(idxItem1);
	                    if ( eleItem1.getNodeName().equals("Item") ) {
	                      nlItem2 = processItems(eleEvent,eleTrxEvent,eleOrder,eleItem1, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, storeId,gdwTerrCd,context);
	 
	                      if ( nlItem2 !=null && nlItem2.getLength() > 0 ) {
	                        for (int idxItem2=0; idxItem2 < nlItem2.getLength(); idxItem2++ ) {
	                          if ( nlItem2.item(idxItem2).getNodeType() == Node.ELEMENT_NODE ) {
	                            eleItem2 = (Element)nlItem2.item(idxItem2);
	                            if ( eleItem2.getNodeName().equals("Item") ) {
	                              processItems(eleEvent,eleTrxEvent,eleOrder,eleItem2, gdwLgcyLclRfrDefCd, regId, gdwBusinessDate, orderNumber, storeId,gdwTerrCd,context);
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
	      Logger.getLogger(NpStldXmlExtendedMapper.class.getName()).log(Level.SEVERE, null, ex);
	      ex.printStackTrace();
	    }


	  }

	  
	  private NodeList processItems(Element eleEvent,
			  						Element eleTrxEvent,
			  						Element eleOrder,
			  						Element eleItem,
	                                String gdwLgcyLclRfrDefCd,
	                                String regId,
	                                String gdwBusinessDate,
	                                String orderNumber,
	                                String storeId,
	                                String gdwTerrCd,
	                                Context context) {
		  
		  
		  

	    String code;
	    String type;
	    String qtyText;
	    String qtyPromoText;
	    BigDecimal qty;
	    BigDecimal qtyPromo;
	    BigDecimal itemTotalPrice;
	    BigDecimal itemTotalTax;
	    BigDecimal itemBdPrice;
	    BigDecimal itemBpPrice;
	    BigDecimal itemBdTax;
	    BigDecimal itemBpTax;
	    BigDecimal itemUnitPrice;
	    BigDecimal itemUnitTax;
	    String itemDescription = "";
	   
	    
	//    
//	    int displayOrder = -1; //src field
//	    BigDecimal netAmount = new BigDecimal(0);//calc field
//	    BigDecimal taxAmount = new BigDecimal(0);//calc field
	    
////	    BigDecimal bdBaseAmount;//src field
//	    BigDecimal bpBaseAmount;//src field
//	    BigDecimal baseAmount;//src field
	//    
//	    BigDecimal bdAmount;//src field
//	    BigDecimal bpAmount;//src field
//	    BigDecimal amount;//src field
	    
	    String itemleveltxt;
	    String itemaction;
	    BigDecimal itemlevel;
	    
	    //event details
	    String eventTimestamp 	= eleEvent.getAttribute("Time");
	    String eventRegisterId 		= eleEvent.getAttribute("RegId");
	    
	    //TRX_Sale details
	    
	    String trxSaleStatus    = eleTrxEvent.getAttribute("status");
	    String  trxSalePod 		= eleTrxEvent.getAttribute("POD");
	    
	    //order details
	    String orderTimestamp 	 = eleOrder.getAttribute("Timestamp");
	    String orderUniqueId 	 = eleOrder.getAttribute("uniqueId");
	    String orderKind		 = eleOrder.getAttribute("kind");
	    String orderKey			 = eleOrder.getAttribute("key");
	    String orderTotalAmount  = eleOrder.getAttribute("totalAmount");
	    String orderTotalTax 	 = eleOrder.getAttribute("totalTax");
	    String orderSaleType 	 = eleOrder.getAttribute("saleType");
	    
	    
	    
	    context.getCounter("Count","NumofTimesProcessItemsCalled").increment(1);
	    try { 
	      qtyText = eleItem.getAttribute("qty");
	      qty = new BigDecimal(qtyText);
	      qtyPromoText = eleItem.getAttribute("qtyPromo");
	      qtyPromo = new BigDecimal(qtyPromoText);
	      type = eleItem.getAttribute("type");
////	      displayOrder = 0;
//	      
//	      if(eleItem.getAttribute("displayOrder") != null && !eleItem.getAttribute("displayOrder").isEmpty())
//	    	  displayOrder = Integer.parseInt(eleItem.getAttribute("displayOrder"));
	      
	      itemleveltxt = eleItem.getAttribute("level");
	      itemaction   = eleItem.getAttribute("action");
	      itemlevel = new BigDecimal(itemleveltxt);
	      
	      eleItem.getAttribute("grillModifier");
	      eleItem.getAttribute("grillQty");
	      
	      code = eleItem.getAttribute("code");
	      
	      if ( (type.equalsIgnoreCase("PRODUCT") || type.equalsIgnoreCase("VALUE_MEAL") ||
	    		  type.equalsIgnoreCase("NON_FOOD_PRODUCT") || type.equalsIgnoreCase("CHOICE")) && 
	    	 
	    		  ( (itemlevel != null && (itemlevel.compareTo(bgzero) == 0 || itemlevel.compareTo(bgzero) == 1 || itemlevel.compareTo(bgzero) == 2 )) ||
	    				  "CHOICE".equalsIgnoreCase(itemaction) || "RECIPE".equalsIgnoreCase(itemaction))
	    				  &&
	    				  !(type.equalsIgnoreCase("CHOICE") && "CHOICE".equalsIgnoreCase(itemaction) )
	    				  &&
	    		code != null && !code.isEmpty() && Integer.parseInt(code) <= 9999	
	    	   ){
	    	  	
	    	  
	    	//item details

	    	    itemTotalPrice  = new BigDecimal(eleItem.getAttribute("totalPrice"));
	    	    itemTotalTax    = new BigDecimal(eleItem.getAttribute("totalTax"));
	    	    itemBdPrice 	= new BigDecimal(eleItem.getAttribute("BDPrice"));
	    	    itemBpPrice 	= new BigDecimal(eleItem.getAttribute("BPPrice"));
	    	    itemBdTax 		= new BigDecimal(eleItem.getAttribute("BDTax"));
	    	    itemBpTax 		= new BigDecimal(eleItem.getAttribute("BPTax"));
	    	    if(eleItem.getAttribute("unitPrice") != null && eleItem.getAttribute("unitPrice").trim().length() > 0)
	    	    	itemUnitPrice 	= new BigDecimal(eleItem.getAttribute("unitPrice"));
	    	    else
	    	    	itemUnitPrice 	= BigDecimal.ZERO;
	    	    
	    	    if(eleItem.getAttribute("unitTax") != null & eleItem.getAttribute("unitTax").trim().length() > 0)
	    	    	itemUnitTax 	= new BigDecimal(eleItem.getAttribute("unitTax"));
	    	    else
	    	    	itemUnitTax = BigDecimal.ZERO;
	    	    
	    	    if(eleItem.getAttribute("description") != null){
	    	    	itemDescription	= eleItem.getAttribute("description");
	    	    }else{
	    	    	context.getCounter("Count", "MissingItemDescription").increment(1);
	    	    }
	    	    
	    	    //promotionapplied details
	    	    String paPromotionID		="";
	    	    String paPromotionCounter	="";
	    	    String paEligible		   	="";
	    	    String paOriginalPrice	 	="";
	    	    String paDiscountAmount		="";
	    	    String paDiscountType		="";
	    	    String paOriginalPromoQty	="";
	    	    String paOriginalProductCode="";
	    	    String paOfferId			="";
	    	    
	    	    NodeList itemChldNodes = eleItem.getChildNodes();
	    	    Element itemChldNode = null;
	    	    
	    	    
	    	    for(int itmCldIndx=0;itmCldIndx < itemChldNodes.getLength();itmCldIndx++){
	    	    	
	    	    	if((itemChldNodes.item(itmCldIndx)).getNodeType() == Node.ELEMENT_NODE){
	    	    	itemChldNode = (Element)itemChldNodes.item(itmCldIndx);
	    	    	
	    	    	if(itemChldNode.getNodeName().equalsIgnoreCase("PromotionApplied")){
	    	    		if(itemChldNode.getAttribute("promotionId") != null)
	    	    			paPromotionID = itemChldNode.getAttribute("promotionId");
	    	    		if(itemChldNode.getAttribute("promotionCounter") != null)
	    	    			paPromotionCounter = itemChldNode.getAttribute("promotionCounter");
	    	    		if(itemChldNode.getAttribute("eligible") != null)
	    	    			paEligible = itemChldNode.getAttribute("eligible");
	    	    		if(itemChldNode.getAttribute("originalPrice") != null)
	    	    			paOriginalPrice = itemChldNode.getAttribute("originalPrice");
	    	    		if(itemChldNode.getAttribute("discountAmount") != null)
	    	    			paDiscountAmount = itemChldNode.getAttribute("discountAmount");
	    	    		if(itemChldNode.getAttribute("discountType") != null)
	    	    			paDiscountType = itemChldNode.getAttribute("discountType");
	    	    		if(itemChldNode.getAttribute("originalItemPromoQty") != null)
	    	    			paOriginalPromoQty = itemChldNode.getAttribute("originalItemPromoQty");
	    	    		if(itemChldNode.getAttribute("originalProductCode")!= null)
	    	    			paOriginalProductCode = itemChldNode.getAttribute("originalProductCode");
	    	    		if(itemChldNode.getAttribute("offerId") != null)
	    	    			paOfferId = itemChldNode.getAttribute("offerId");
	    	    		
	    	    		break;
	    	    	}
	    	    	}
	    	    	
	    	    }
	    	    
	    	    
	    	    //promotion details
	    	    String promotionId 					= "" ;
	    	    String promotionCounter				= "" ;
	    	    String promotionDiscountType		= "" ;
	    	    String promotionDiscountAmount 		= "" ;
	    	    String promotionOfferId				= "" ;
	    	    String promotionExlusive			= "" ;
	    	    
	    	    NodeList orderChldNodes = eleOrder.getChildNodes();
	    	    Element orderChldNode = null;
	    	    
	    	    NodeList promotionsChldNodes = null;
	    	    Element promotionsChldNode = null;
	    	    
	    	    for(int ordCldIndx=0;ordCldIndx < orderChldNodes.getLength();ordCldIndx++){
	    	    	if((orderChldNodes.item(ordCldIndx)).getNodeType() == Node.ELEMENT_NODE){
		    	    	orderChldNode = (Element)orderChldNodes.item(ordCldIndx);
		    	    	
		    	    	if(orderChldNode.getNodeName().equalsIgnoreCase("Promotions")){
		    	    		promotionsChldNodes = orderChldNode.getChildNodes();
		    	    		
		    	    		for(int promotionsCldIndx=0;promotionsCldIndx < promotionsChldNodes.getLength();promotionsCldIndx++){
		    	    			if((promotionsChldNodes.item(promotionsCldIndx)).getNodeType() == Node.ELEMENT_NODE){
			    	    			promotionsChldNode = (Element)promotionsChldNodes.item(promotionsCldIndx);
			    	    			promotionId = promotionsChldNode.getAttribute("promotionId");
			    	    			if(paPromotionID != null && paPromotionID.equals(promotionId)){
			    	    				if(promotionsChldNode.getAttribute("promotionCounter") != null)
			    	    					promotionCounter 		 = promotionsChldNode.getAttribute("promotionCounter");
			    	    				if(promotionsChldNode.getAttribute("discountType") != null)
			    	    					promotionDiscountType  	 = promotionsChldNode.getAttribute("discountType");
			    	    				if(promotionsChldNode.getAttribute("discountAmount") != null)
			    	    					promotionDiscountAmount  = promotionsChldNode.getAttribute("discountAmount");
			    	    				if(promotionsChldNode.getAttribute("offerId") != null)
			    	    					promotionOfferId  		 = promotionsChldNode.getAttribute("offerId");
			    	    				if(promotionsChldNode.getAttribute("exclusive")!= null)
			    	    					promotionExlusive  		 = promotionsChldNode.getAttribute("exclusive");
			    	    				
			    	    				
			    	    				break;
			    	    			}else{
			    	    				promotionId= "";
			    	    			}
		    	    			
		    	    			}
		    	    		}
		    	    		
		    	    	}
	    	    	}
	    	    }
	    	    
	    	    //offer details
	    	   
	    	    String offersPromotionId 		= "";
	    	    String offersTagId 				= "";
	    	    String offersOfferId			= "";
	    	    String offersOverride			= "";
	    	    String offersApplied			= "";
	    	    String offersClearAfterOverride = "";
	    	    
	    	    for(int ordCldIndx=0;ordCldIndx < orderChldNodes.getLength();ordCldIndx++){
	    	    	if((orderChldNodes.item(ordCldIndx)).getNodeType() == Node.ELEMENT_NODE){
		    	    	orderChldNode = (Element)orderChldNodes.item(ordCldIndx);
		    	    	
		    	    	if(orderChldNode.getNodeName().equalsIgnoreCase("Offers")){
		    	    		
		    	    		
		    	    		offersPromotionId = orderChldNode.getAttribute("promotionId");
		    	    		
		    	    		if(paPromotionID.equalsIgnoreCase(offersPromotionId)){
		    	    			if(orderChldNode.getAttribute("tagId") != null)
		    	    				offersTagId 	= orderChldNode.getAttribute("tagId");
		    	    			if(orderChldNode.getAttribute("offerId") != null)
		    	    				offersOfferId   = orderChldNode.getAttribute("offerId");
		    	    			if(orderChldNode.getAttribute("override") != null)
		    	    				offersOverride  = orderChldNode.getAttribute("override");
		    	    			if(orderChldNode.getAttribute("applied") != null)
		    	    				offersApplied   = orderChldNode.getAttribute("applied");
		    	    			if(orderChldNode.getAttribute("clearAfterOverride") != null)
		    	    				offersClearAfterOverride = orderChldNode.getAttribute("clearAfterOverride");
		    	    			
		    	    			break;
		    	    		}else{
		    	    			offersPromotionId = "";
		    	    		}
		    	    	}
	    	    	}
	    	    }
	    	    
	    	    //custominfo details
	    	    

	    		String custinfoCustomerId 				= "";
	    		String custinfoOrderId 					= "";
	    		String custinfoIsPaidMobileOrder  		= "";
	    		String custinfoTenderQty 				= "";
	    		String custinfoTenderValue 				= "";
	    		String custinfoTenderCashlessCardProv  	= "";
	    		String custinfoTenderCashlessAmount 	= "";
	    		String custinfoTotalDiscount 			= "";
	    		String custinfoTotalBD 					= "";
	    		String custinfoDiscountId 				= "";
	    		String custinfoDiscountType 			= "";
	    		
	    	    HashMap<String,String> customInfoMap = new HashMap<String,String>();
//	    	    Element customInfoNode = null;
	    	    NodeList customInfoCldNodes = null;
	    	    Element infoNode = null;
	    	    
	    	    for(int ordCldIndx=0;ordCldIndx < orderChldNodes.getLength();ordCldIndx++){
	    	    	if((orderChldNodes.item(ordCldIndx)).getNodeType() == Node.ELEMENT_NODE){
		    	    	orderChldNode = (Element)orderChldNodes.item(ordCldIndx);
		    	    	
		    	    	
		    	    	if(orderChldNode.getNodeName().equalsIgnoreCase("CustomInfo")){
		    	    		customInfoCldNodes = orderChldNode.getChildNodes();
		    	    		for(int ciCldIndx=0;ciCldIndx < customInfoCldNodes.getLength();ciCldIndx++){
		    	    			infoNode = (Element)customInfoCldNodes.item(ciCldIndx);
		    	    			if( infoNode.getNodeName().equalsIgnoreCase("Info")){
		    	    				customInfoMap.put(infoNode.getAttribute("name"), infoNode.getAttribute("value"));
		    	    			}
		    	    		}
		    	    		
		    	    	}else{
		    	    		if ( orderChldNode.getNodeName().equals("Customer") ) {
		    	    			custinfoCustomerId = orderChldNode.getAttribute("id");
							}
		    	    	}
	    	    	}
	    	    }
	    	    
	    	    if(customInfoMap != null && customInfoMap.size() > 0){
	    	    	if(customInfoMap.get("customerId") != null)
	    	    		custinfoCustomerId 					= customInfoMap.get("customerId");
	    	    	if(customInfoMap.get("OrderId") != null)
	    	    		custinfoOrderId						= customInfoMap.get("OrderId");
	    	    	if(customInfoMap.get("isPaidMobileOrder") != null)
	    	    		custinfoIsPaidMobileOrder			= customInfoMap.get("isPaidMobileOrder");
	    	    	if(customInfoMap.get("tenderQty") != null)
	    	    		custinfoTenderQty					= customInfoMap.get("tenderQty");
	    	    	if(customInfoMap.get("tenderValue") != null)
	    	    		custinfoTenderValue					= customInfoMap.get("tenderValue");
	    	    	if(customInfoMap.get("tenderCashlessCardProv") != null)
	    	    		custinfoTenderCashlessCardProv		= customInfoMap.get("tenderCashlessCardProv");
	    	    	if(customInfoMap.get("tenderCashlessAccount") != null)
	    	    		custinfoTenderCashlessAmount		= customInfoMap.get("tenderCashlessAccount");
	    	    	if(customInfoMap.get("totalDiscount") != null)
	    	    		custinfoTotalDiscount				= customInfoMap.get("totalDiscount");
	    	    	if(customInfoMap.get("totalBD") != null)
	    	    		custinfoTotalBD						= customInfoMap.get("totalBD");
	    	    	if(customInfoMap.get("discountId") != null)
	    	    		custinfoDiscountId					= customInfoMap.get("discountId");
	    	    	if(customInfoMap.get("discountType") != null)
	    	    		custinfoDiscountType				= customInfoMap.get("discountType");
	    	    	
	    	    }
	    	    
	    	    //tender details
	    	    PaymentMethod orderPaymentMethod = null;
	    	    Element tendersNode = null;
	    	    boolean foundTenders = false;
	    	    for(int ordCldIndx=0;ordCldIndx < orderChldNodes.getLength();ordCldIndx++){
	    	    	if((orderChldNodes.item(ordCldIndx)).getNodeType() == Node.ELEMENT_NODE){
		    	    	tendersNode = (Element)orderChldNodes.item(ordCldIndx);
		    	    	
		    	    	
		    	    	if(tendersNode.getNodeName().equalsIgnoreCase("Tenders")){
		    	    		orderPaymentMethod = getTender(tendersNode, context);
		    	    		foundTenders = true;
		    	    		break;
		    	    	}
	    	    	}
	    	    }
	    	    if(!foundTenders){
	    	    	context.getCounter("Count","MissingTenderSections").increment(1);
	    	    }
	    	    Integer tenderId 			= null;
	    	    Integer tenderKind  	    = null;
	    	    Integer tenderQuantity 	    = null;
	    	    String  paymentMethodName 	= null;
	    	    BigDecimal tenderFaceAmount = null;
	    	    BigDecimal paymentAmount    = null;
	    	  //mc41946-CashlessData
	    	    String cashlessData="";
	    	    if(orderPaymentMethod != null && orderPaymentMethod.maxTender() != null)
	    	    {
		    	    tenderId 			= orderPaymentMethod.maxTender().getTendeId();
		    	    tenderKind  	    = orderPaymentMethod.maxTender().getTenderKind();
		    	    tenderQuantity 	    = orderPaymentMethod.maxTender().getTenderQuantity();
		    	    paymentMethodName 	= orderPaymentMethod.maxTender().getPaymentMethodName();
		    	    tenderFaceAmount 	= orderPaymentMethod.maxTender().getTenderFaceValue();
		    	    paymentAmount    	= orderPaymentMethod.maxTender().getPaymentAmount();
		    	  //mc41946 -CashlessData
		    	    cashlessData    	= orderPaymentMethod.maxTender().getCashlessData();
	    	    }
	    	    
	    	    cashlessData=(cashlessData==null?"":cashlessData);
	    	    String[] cashlessDataArr;
	    	    String cardType="";
	    	    String cardNumber="";
	    	    StringBuffer cardExpiration=new StringBuffer();
	    	   
	    	    String tenderIdStr 				= "";
	    	    String tenderKindStr  	    	= "";
	    	    String tenderQuantityStr 	    = "";
	    	    String  paymentMethodNameStr 	= "";
	    	    String tenderFaceAmountStr	 	= "";
	    	    String paymentAmountStr	    	= "";
	    	    
	    	    if(tenderId != null){
	    	    	tenderIdStr = tenderId.toString();
	    	    }
	    	    if(tenderKind != null){
	    	    	tenderKindStr = tenderKind.toString();
	    	    }
	    	    if(tenderQuantity != null){
	    	    	tenderQuantityStr = tenderQuantity.toString();
	    	    }
	    	    if(paymentMethodName != null){
	    	    	paymentMethodNameStr = paymentMethodName;
	    	    }
	    	    if(tenderFaceAmount != null){
	    	    	tenderFaceAmountStr = tenderFaceAmount.toString();
	    	    }
	    	    if(paymentAmount != null){
	    	    	paymentAmountStr = paymentAmount.toString();
	    	    }
	    	    
	    	  //mc41946 -CashlessData
	    	    if(cashlessData != null){
	    	    	cashlessData = cashlessData.toString();
	    	    }
	    	    
	    	    String month;
	    	    String year;
              if(StringUtils.isNotBlank(cashlessData) ){
            	  cashlessDataArr=null;
            	  if(Integer.parseInt(gdwTerrCd) == 36){
	            	cashlessDataArr = cashlessData.split("\\\\n");
                    int cardIndex=0;
                    if(cashlessData.contains("(t)")){
                    cardIndex =  cashlessData.indexOf("(t)");
                    }
                    else if( cashlessData.contains("(s)")){
                     cardIndex =  cashlessData.indexOf("(s)");
                     }
                     else if (cashlessData.contains("(c)")){
                      cardIndex =  cashlessData.indexOf("(c)");
                   }
                     else if (cashlessData.contains("(e)")){
                         cardIndex =  cashlessData.indexOf("(e)");
                      }
                    if (cardIndex > 0 && cashlessData.contains("ACCOUNT TYPE")){
                    	cardNumber=cashlessData.substring(cardIndex-4, cardIndex);
                    	}
                    for(int i=0; i<cashlessDataArr.length;i++){
                        if(cashlessDataArr[i].contains("ACCOUNT TYPE") ){
                         cardType = cashlessDataArr[i+1].trim();
                         break;
                       // System.out.println("Card type is "+cardType);
                        }
                    	}
                                  	
            	  }
            	  if(Integer.parseInt(gdwTerrCd) == 840){
  	            	cashlessDataArr = cashlessData.split("#",-1)[0].split("\\|");
  	            	cardNumber=cashlessDataArr[1];//1_POS_CSHL_CARD_NU
	            	cardType=paymentMethodNameStr;//1_POS_CSHL_ATHZ_CD
	            	cardNumber = (cardNumber == null || cardNumber.length() ==0 ) ?"" : cardNumber.substring(cardNumber.length() - 4);
	            	
//	            	System.out.println(" cashlessDataArr -----  "+ cashlessDataArr[2] + "XXXX"+cashlessData + " ------");
	            	if(StringUtils.isNotBlank(cashlessDataArr[2])){
	            		 month=cashlessDataArr[2].substring(0, 2);//month
	            		 year=cashlessDataArr[2].substring(3);//year
	            		cardExpiration.append(month).append("/").append(year);
	            	}
              	  }
	            }
   	     
	      	
	      	context.getCounter("Count","AddingJSONToArray").increment(1);
//	      	  
//	      	   if(Integer.parseInt(gdwTerrCd) == 36){
	      	   if(format.equalsIgnoreCase("json"))
	      	   {
	    	     itemValsJSArr.clear();
	    	  	 itemValsJSArr.add("Item");
	             itemValsJSArr.add(gdwLgcyLclRfrDefCd);
	             itemValsJSArr.add(gdwBusinessDate);
	             itemValsJSArr.add(eventRegisterId);
	             itemValsJSArr.add(eventTimestamp);
	             itemValsJSArr.add(trxSaleStatus);
	             itemValsJSArr.add(trxSalePod);
	             itemValsJSArr.add(orderTimestamp);
	             itemValsJSArr.add(orderUniqueId);
	             itemValsJSArr.add(orderKind);
	             itemValsJSArr.add(orderKey);
	             itemValsJSArr.add(orderSaleType);
	             itemValsJSArr.add(orderTotalAmount.toString());
	             itemValsJSArr.add(orderTotalTax.toString());
	             itemValsJSArr.add(code);
	             itemValsJSArr.add(qty);
	             itemValsJSArr.add(qtyPromo);
	             itemValsJSArr.add(itemBpPrice.toString());
	             itemValsJSArr.add(itemBpTax.toString());
	             itemValsJSArr.add(itemBdPrice.toString());
	             itemValsJSArr.add(itemBdTax.toString());
	             itemValsJSArr.add(itemTotalPrice.toString());
	             itemValsJSArr.add(itemTotalTax.toString());
	             itemValsJSArr.add(itemDescription);
	             itemValsJSArr.add(itemUnitPrice.toString());
	             itemValsJSArr.add(itemUnitTax.toString());
	             itemValsJSArr.add(paPromotionID);
	             itemValsJSArr.add(paPromotionCounter);
	             itemValsJSArr.add(paEligible);
	             itemValsJSArr.add(paOriginalPrice);
	             itemValsJSArr.add(paDiscountAmount);
	             itemValsJSArr.add(paDiscountType);
	             itemValsJSArr.add(paOriginalPromoQty);
	             itemValsJSArr.add(paOriginalProductCode);
	             itemValsJSArr.add(paOfferId);
	             itemValsJSArr.add(promotionId);
	             itemValsJSArr.add(promotionCounter);
	             itemValsJSArr.add(promotionDiscountType);
	             itemValsJSArr.add(promotionDiscountAmount);
	             itemValsJSArr.add(promotionOfferId);
	             itemValsJSArr.add(promotionExlusive);
	             itemValsJSArr.add(offersTagId);
	             itemValsJSArr.add(offersOfferId);
	             itemValsJSArr.add(offersOverride);
	             itemValsJSArr.add(offersApplied);
	             itemValsJSArr.add(offersClearAfterOverride);
	             itemValsJSArr.add(offersPromotionId);
	             itemValsJSArr.add(custinfoCustomerId);
	             itemValsJSArr.add(custinfoOrderId);
	             itemValsJSArr.add(custinfoIsPaidMobileOrder);
	             itemValsJSArr.add(custinfoTenderQty);
	             itemValsJSArr.add(custinfoTenderValue);
	             itemValsJSArr.add(custinfoTenderCashlessCardProv);
	             itemValsJSArr.add(custinfoTenderCashlessAmount);
	             itemValsJSArr.add(custinfoTotalDiscount);
	             itemValsJSArr.add(custinfoTotalBD);
	             itemValsJSArr.add(custinfoDiscountId);
	             itemValsJSArr.add(custinfoDiscountType);
	             itemValsJSArr.add(tenderIdStr);
	             itemValsJSArr.add(tenderKindStr);
	             itemValsJSArr.add(paymentMethodNameStr);
	             itemValsJSArr.add(tenderQuantityStr);
	             itemValsJSArr.add(tenderFaceAmountStr);
	             itemValsJSArr.add(paymentAmountStr);
	             itemValues.add(itemValsJSArr);
	      	   }
	             
	             seqNum++;
	      	     
	      	     tdaData.setLength(0);
	             
	      	     if ( format.equalsIgnoreCase("tsv") ) {
	      	    	tdaData.append(gdwTerrCd);tdaData.append("\t");
	      	    	tdaData.append(gdwMcdGbalLcatIdNu);tdaData.append("\t");
	      	    	tdaData.append(gdwBusinessDate);tdaData.append("\t");
	      	    	tdaData.append(orderKey);tdaData.append("\t");
	      	    	tdaData.append(seqNum);tdaData.append("\t");
	      	    	tdaData.append(gdwLgcyLclRfrDefCd);tdaData.append("\t");
	      	    	tdaData.append(eventRegisterId);tdaData.append("\t");
	      	    	tdaData.append(getTMSFormattedDate(eventTimestamp));tdaData.append("\t");
	      	    	tdaData.append(trxSaleStatus);tdaData.append("\t");
	      	    	tdaData.append(trxSalePod);tdaData.append("\t");
	      	    	tdaData.append(getTMSFormattedDate(orderTimestamp));tdaData.append("\t");
	      	    	tdaData.append(orderUniqueId);tdaData.append("\t");
	      	    	tdaData.append(orderKind);tdaData.append("\t");
	      	    	tdaData.append(orderSaleType);tdaData.append("\t");
	      	    	tdaData.append(orderTotalAmount.toString());tdaData.append("\t");
	      	    	tdaData.append(orderTotalTax.toString());tdaData.append("\t");
	      	    	tdaData.append(code);tdaData.append("\t");
	      	    	tdaData.append(itemDescription);tdaData.append("\t");
	      	    	tdaData.append(qty);tdaData.append("\t");
	      	    	tdaData.append(qtyPromo);tdaData.append("\t");
	      	    	tdaData.append(itemBpPrice.toString());tdaData.append("\t");
	      	    	tdaData.append(itemBpTax.toString());tdaData.append("\t");
	      	    	tdaData.append(itemBdTax.toString());tdaData.append("\t");
	      	    	tdaData.append(itemBdPrice.toString());tdaData.append("\t");
	      	    	tdaData.append(itemTotalPrice.toString());tdaData.append("\t");
	      	    	tdaData.append(itemTotalTax.toString());tdaData.append("\t");
	      	    	tdaData.append(itemUnitPrice.toString());tdaData.append("\t");
	      	    	tdaData.append(itemUnitTax.toString());tdaData.append("\t");
	      	    	tdaData.append(paPromotionID);tdaData.append("\t");
	      	    	tdaData.append(paPromotionCounter);tdaData.append("\t");
	      	    	tdaData.append(paEligible);tdaData.append("\t");
	      	    	tdaData.append(paOriginalPrice);tdaData.append("\t");
	      	    	tdaData.append(paDiscountAmount);tdaData.append("\t");
	      	    	tdaData.append(paDiscountType);tdaData.append("\t");
	      	    	tdaData.append(paOriginalPromoQty);tdaData.append("\t");
	      	    	tdaData.append(paOriginalProductCode);tdaData.append("\t");
	      	    	tdaData.append(paOfferId);tdaData.append("\t");
	      	    	tdaData.append(promotionId);tdaData.append("\t");
	      	    	tdaData.append(promotionCounter);tdaData.append("\t");
	      	    	tdaData.append(promotionDiscountType);tdaData.append("\t");
	      	    	tdaData.append(promotionDiscountAmount);tdaData.append("\t");
	      	    	tdaData.append(promotionOfferId);tdaData.append("\t");
	      	    	tdaData.append(promotionExlusive);tdaData.append("\t");
	      	    	tdaData.append(offersTagId);tdaData.append("\t");
	      	    	tdaData.append(offersOfferId);tdaData.append("\t");
	      	    	tdaData.append(offersOverride);tdaData.append("\t");
	      	    	tdaData.append(offersApplied);tdaData.append("\t");
	      	    	tdaData.append(offersClearAfterOverride);tdaData.append("\t");
	      	    	tdaData.append(offersPromotionId);tdaData.append("\t");
	      	    	tdaData.append(custinfoCustomerId);tdaData.append("\t");
	      	    	tdaData.append(custinfoOrderId);tdaData.append("\t");
	      	    	tdaData.append(custinfoIsPaidMobileOrder);tdaData.append("\t");
	      	    	tdaData.append(custinfoTenderQty);tdaData.append("\t");
	      	    	tdaData.append(custinfoTenderValue);tdaData.append("\t");
	      	    	tdaData.append(custinfoTenderCashlessCardProv);tdaData.append("\t");
	      	    	tdaData.append(custinfoTenderCashlessAmount);tdaData.append("\t");
	      	    	tdaData.append(custinfoTotalDiscount);tdaData.append("\t");
	      	    	tdaData.append(custinfoTotalBD);tdaData.append("\t");
	      	    	tdaData.append(custinfoDiscountId);tdaData.append("\t");
	      	    	tdaData.append(custinfoDiscountType);tdaData.append("\t");
	      	    	tdaData.append(tenderIdStr);tdaData.append("\t");
	      	    	tdaData.append(tenderKindStr);tdaData.append("\t");
	      	    	tdaData.append(paymentMethodNameStr);tdaData.append("\t");
	      	    	tdaData.append(tenderQuantityStr);tdaData.append("\t");
	      	    	tdaData.append(tenderFaceAmountStr);tdaData.append("\t");
	      	    	tdaData.append(paymentAmountStr);
	      	     } else {
		      	     tdaData.append("Item");tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(gdwLgcyLclRfrDefCd);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(gdwBusinessDate);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(eventRegisterId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             if(generateExtraFields.equalsIgnoreCase("TRUE")){
		             tdaData.append(getTMSFormattedDate(eventTimestamp));tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             }else
		             {
		            	 tdaData.append(eventTimestamp);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             }
		             tdaData.append(trxSaleStatus);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(trxSalePod);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             if(generateExtraFields.equalsIgnoreCase("TRUE")){
		             tdaData.append(getTMSFormattedDate(orderTimestamp));tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             }else
		             {
		            	 tdaData.append(orderTimestamp);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             }	             
		             //tdaData.append(SDF_yyyyMMddHHmmssSSS.parse(orderTimestamp));tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(orderUniqueId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(orderKind);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(orderKey);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(orderSaleType);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(orderTotalAmount.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(orderTotalTax.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(code);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(qty);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(qtyPromo);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemBpPrice.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemBpTax.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemBdPrice.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemBdTax.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemTotalPrice.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemTotalTax.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemDescription);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemUnitPrice.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(itemUnitTax.toString());tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paPromotionID);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paPromotionCounter);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paEligible);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paOriginalPrice);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paDiscountAmount);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paDiscountType);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paOriginalPromoQty);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paOriginalProductCode);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paOfferId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(promotionId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(promotionCounter);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(promotionDiscountType);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(promotionDiscountAmount);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(promotionOfferId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(promotionExlusive);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(offersTagId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(offersOfferId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(offersOverride);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(offersApplied);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(offersClearAfterOverride);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(offersPromotionId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoCustomerId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoOrderId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoIsPaidMobileOrder);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoTenderQty);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoTenderValue);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoTenderCashlessCardProv);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoTenderCashlessAmount);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoTotalDiscount);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoTotalBD);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoDiscountId);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(custinfoDiscountType);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(tenderIdStr);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(tenderKindStr);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paymentMethodNameStr);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(tenderQuantityStr);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(tenderFaceAmountStr);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(paymentAmountStr);
		           //@mc41946 -CashlessData
		             if(generateExtraFields.equalsIgnoreCase("TRUE")){
		             tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(cardType);tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(cardNumber.trim().equalsIgnoreCase("")?"":SimpleEncryptSHA1.SHA1(cardNumber));
		             if(Integer.parseInt(gdwTerrCd) != 36){
		             tdaData.append(DaaSConstants.PIPE_DELIMITER);
		             tdaData.append(cardExpiration.toString().trim().equalsIgnoreCase("")?"":SimpleEncryptSHA1.SHA1(cardExpiration.toString()));
		             }
		             
		             //if ( format.equalsIgnoreCase("tsv") ) {
			         //    tdaData.append(DaaSConstants.PIPE_DELIMITER);
			         //    tdaData.append(gdwTerrCd);
			         //    tdaData.append(DaaSConstants.PIPE_DELIMITER);
			         //    tdaData.append(gdwMcdGbalLcatIdNu);
			         //    tdaData.append(DaaSConstants.PIPE_DELIMITER);
			         //    tdaData.append(seqNum);
		             //}
		             
		             }
	      	     }
	             /*if(Integer.parseInt(gdwTerrCd) == 36){
	             allInfo.append("\n");
	             allInfo.append(tdaData.toString());
	             }*/
	             
//	      	   }
	      	   /** coded for US TMS since they wanted the attributes repeated. but now they want the old format.
	      	   else{
	      		 JSONObject joj = new JSONObject();
	     	    
	   		      joj.put("OutParentNode","Item");
	         	  joj.put("StoreId",gdwLgcyLclRfrDefCd);
	         	  joj.put("BusinessDate",gdwBusinessDate);
	         	  joj.put("EventRegisterId",eventRegisterId);
	         	  joj.put("EventTime",eventTimestamp);
	         	  joj.put("Status",trxSaleStatus);
	         	  joj.put("POD",trxSalePod);
	         	  joj.put("OrderTimestamp",orderTimestamp);
	         	  joj.put("OrderuniqueID",orderUniqueId);
	         	  joj.put("Orderkind",orderKind);
	         	  joj.put("Orderkey",orderKey);
	         	  joj.put("OrdersaleType",orderSaleType);
	         	  joj.put("OrderTotalAmount",orderTotalAmount.toString());
	         	  joj.put("OrderTotalTax",orderTotalTax.toString());
	         	  joj.put("ItemCode",code);
	         	  joj.put("ItemQty",qty);
	         	  joj.put("ItemQtyPromo",qtyPromo);
	         	  joj.put("ItemBPPrice",itemBpPrice.toString());
	         	  joj.put("ItemBPTax",itemBpTax.toString());
	         	  joj.put("ItemBDPrice",itemBdPrice.toString());
	         	  joj.put("ItemBDTax",itemBdTax.toString());
	         	  joj.put("ItemTotalPrice",itemTotalPrice.toString());
	         	  joj.put("ItemTotalTax",itemTotalTax.toString());
	         	  joj.put("ItemDescription",itemDescription);
	         	  joj.put("ItemUnitPrice",itemUnitPrice.toString());
	         	  joj.put("ItemUnitTax",itemUnitTax.toString());
	         	  joj.put("PromoAppPromotionID",paPromotionID);
	         	  joj.put("PromoAppPromotionCounter",paPromotionCounter);
	         	  joj.put("PromoAppEligible",paEligible);
	         	  joj.put("PromoAppOriginalPrice",paOriginalPrice);
	         	  joj.put("PromoAppDiscountAmount",paDiscountAmount);
	         	  joj.put("PromoAppDiscountType",paDiscountType);
	         	  joj.put("PromoAppOriginalPromoQty",paOriginalPromoQty);
	         	  joj.put("PromoAppOriginalProductCode",paOriginalProductCode);
	         	  joj.put("PromoAppOfferId",paOfferId);
	         	  joj.put("PromoPromotionId",promotionId);
	         	  joj.put("PromoPromotionCounter",promotionCounter);
	         	  joj.put("PromoDiscountType",promotionDiscountType);
	         	  joj.put("PromoDiscountAmount",promotionDiscountAmount);
	         	  joj.put("PromoOfferId",promotionOfferId);
	         	  joj.put("PromoExlusive",promotionExlusive);
	         	  joj.put("OffersTagId",offersTagId);
	         	  joj.put("OffersOfferId",offersOfferId);
	         	  joj.put("OffersOverride",offersOverride);
	         	  joj.put("OffersApplied",offersApplied);
	         	  joj.put("OffersClearAfterOverride",offersClearAfterOverride);
	         	  joj.put("OffersPromotionID",offersPromotionId);
	         	  joj.put("CustInfoCustomerId",custinfoCustomerId);
	         	  joj.put("CustInfoOrderId",custinfoOrderId);
	         	  joj.put("CustInfoIsPaidMobileOrder",custinfoIsPaidMobileOrder);
	         	  joj.put("CustInfoTenderQty",custinfoTenderQty);
	         	  joj.put("CustInfoTenderValue",custinfoTenderValue);
	         	  joj.put("CustInfoTenderCashlessCardProv",custinfoTenderCashlessCardProv);
	         	  joj.put("CustInfoTenderCashlessAmount",custinfoTenderCashlessAmount);
	         	  joj.put("CustInfoTotalDiscount",custinfoTotalDiscount);
	         	  joj.put("CustInfoTotalBD",custinfoTotalBD);
	         	  joj.put("CustInfoDiscountId",custinfoDiscountId);
	         	  joj.put("CustInfoDiscountType",custinfoDiscountType);
	         	  joj.put("TenderID",tenderIdStr);
	         	  joj.put("TenderKind",tenderKindStr);
	         	  joj.put("TenderName",paymentMethodNameStr);
	         	  joj.put("TenderQuantity",tenderQuantityStr);
	         	  joj.put("TenderFacevalue",tenderFaceAmountStr);
	         	  joj.put("TenderAmount",paymentAmountStr);
	     	  
	         	  itemValsJSArr.add(joj);
	      	   }
	      	   */
	      	   
	//
//	    	   	  	  itemValsJSArr.add("out_parent_node: Item");
//	    	      	  itemValsJSArr.add("storeId: "+gdwLgcyLclRfrDefCd);
//	    	      	  itemValsJSArr.add("businessDate: "+gdwBusinessDate);
//	    	      	  itemValsJSArr.add("EventRegisterId: "+eventRegisterId);
//	    	      	  itemValsJSArr.add("EventTime: "+eventTimestamp);
//	    	      	  itemValsJSArr.add("status: "+trxSaleStatus);
//	    	      	  itemValsJSArr.add("POD: "+trxSalePod);
//	    	      	  itemValsJSArr.add("OrderTimestamp: "+orderTimestamp);
//	    	      	  itemValsJSArr.add("OrderuniqueID: "+orderUniqueId);
//	    	      	  itemValsJSArr.add("Orderkind: "+orderKind);
//	    	      	  itemValsJSArr.add("Orderkey: "+orderKey);
//	    	      	  itemValsJSArr.add("OrdersaleType: "+orderSaleType);
//	    	      	  itemValsJSArr.add("OrderTotalAmount: "+orderTotalAmount.toString());
//	    	      	  itemValsJSArr.add("OrderTotalTax: "+orderTotalTax.toString());
//	    	      	  itemValsJSArr.add("ItemCode: "+code);
//	    	      	  itemValsJSArr.add("ItemQty: "+qty);
//	    	      	  itemValsJSArr.add("ItemQtyPromo: "+qtyPromo);
//	    	      	  itemValsJSArr.add("ItemBPPrice: "+itemBpPrice.toString());
//	    	      	  itemValsJSArr.add("ItemBPTax: "+itemBpTax.toString());
//	    	      	  itemValsJSArr.add("ItemBDPrice: "+itemBdPrice.toString());
//	    	      	  itemValsJSArr.add("ItemBDTax: "+itemBdTax.toString());
//	    	      	  itemValsJSArr.add("ItemTotalPrice: "+itemTotalPrice.toString());
//	    	      	  itemValsJSArr.add("ItemTotalTax: "+itemTotalTax.toString());
//	    	      	  itemValsJSArr.add("ItemDescription: "+itemDescription);
//	    	      	  itemValsJSArr.add("ItemUnitPrice: "+itemUnitPrice.toString());
//	    	      	  itemValsJSArr.add("ItemUnitTax: "+itemUnitTax.toString());
//	    	      	  itemValsJSArr.add("promotionID: "+paPromotionID);
//	    	      	  itemValsJSArr.add("promotionCounter: "+paPromotionCounter);
//	    	      	  itemValsJSArr.add("eligible: "+paEligible);
//	    	      	  itemValsJSArr.add("originalPrice: "+paOriginalPrice);
//	    	      	  itemValsJSArr.add("discountAmount: "+paDiscountAmount);
//	    	      	  itemValsJSArr.add("discountType: "+paDiscountType);
//	    	      	  itemValsJSArr.add("originalPromoQty: "+paOriginalPromoQty);
//	    	      	  itemValsJSArr.add("originalProductCode: "+paOriginalProductCode);
//	    	      	  itemValsJSArr.add("offerId: "+paOfferId);
//	    	      	  itemValsJSArr.add("promotionId: "+promotionId);
//	    	      	  itemValsJSArr.add("promotionCounter: "+promotionCounter);
//	    	      	  itemValsJSArr.add("discountType: "+promotionDiscountType);
//	    	      	  itemValsJSArr.add("discountAmount: "+promotionDiscountAmount);
//	    	      	  itemValsJSArr.add("offerId: "+promotionOfferId);
//	    	      	  itemValsJSArr.add("exlusive: "+promotionExlusive);
//	    	      	  itemValsJSArr.add("tagId: "+offersTagId);
//	    	      	  itemValsJSArr.add("offerId: "+offersOfferId);
//	    	      	  itemValsJSArr.add("override: "+offersOverride);
//	    	      	  itemValsJSArr.add("applied: "+offersApplied);
//	    	      	  itemValsJSArr.add("clearAfterOverride: "+offersClearAfterOverride);
//	    	      	  itemValsJSArr.add("promotionID: "+offersPromotionId);
//	    	      	  itemValsJSArr.add("customerId: "+custinfoCustomerId);
//	    	      	  itemValsJSArr.add("OrderId: "+custinfoOrderId);
//	    	      	  itemValsJSArr.add("isPaidMobileOrder: "+custinfoIsPaidMobileOrder);
//	    	      	  itemValsJSArr.add("TenderQty: "+custinfoTenderQty);
//	    	      	  itemValsJSArr.add("TenderValue: "+custinfoTenderValue);
//	    	      	  itemValsJSArr.add("TenderCashlessCardProv: "+custinfoTenderCashlessCardProv);
//	    	      	  itemValsJSArr.add("TenderCashlessAmount: "+custinfoTenderCashlessAmount);
//	    	      	  itemValsJSArr.add("totalDiscount: "+custinfoTotalDiscount);
//	    	      	  itemValsJSArr.add("totalBD: "+custinfoTotalBD);
//	    	      	  itemValsJSArr.add("discountId: "+custinfoDiscountId);
//	    	      	  itemValsJSArr.add("discountType: "+custinfoDiscountType);
//	    	      	  itemValsJSArr.add("TenderID: "+tenderIdStr);
//	    	      	  itemValsJSArr.add("TenderKind: "+tenderKindStr);
//	    	      	  itemValsJSArr.add("TenderName: "+paymentMethodNameStr);
//	    	      	  itemValsJSArr.add("TenderQuantity: "+tenderQuantityStr);
//	    	      	  itemValsJSArr.add("TenderFacevalue: "+tenderFaceAmountStr);
//	    	      	  itemValsJSArr.add("TenderAmount: "+paymentAmountStr);
	             
	          //itemValues.add(itemValsJSArr);
	                 
	             
//	             StringBuffer sbf = new StringBuffer();
//	             sbf.append("Item").append("|");
//	             sbf.append(gdwLgcyLclRfrDefCd).append("|");
//	             sbf.append(gdwBusinessDate).append("|");
//	             sbf.append(eventRegisterId).append("|");
//	             sbf.append(eventTimestamp).append("|");
//	             sbf.append(trxSaleStatus).append("|");
//	             sbf.append(trxSalePod).append("|");
//	             sbf.append(orderTimestamp).append("|");
//	             sbf.append(orderUniqueId).append("|");
//	             sbf.append(orderKind).append("|");
//	             sbf.append(orderKey).append("|");
//	             sbf.append(orderSaleType).append("|");
//	             sbf.append(orderTotalAmount.toString()).append("|");
//	             sbf.append(orderTotalTax.toString()).append("|");
//	             sbf.append(code).append("|");
//	             sbf.append(qty).append("|");
//	             sbf.append(qtyPromo).append("|");
//	             sbf.append(itemBpPrice.toString()).append("|");
//	             sbf.append(itemBpTax.toString()).append("|");
//	             sbf.append(itemBdPrice.toString()).append("|");
//	             sbf.append(itemBdTax.toString()).append("|");
//	             sbf.append(itemTotalPrice.toString()).append("|");
//	             sbf.append(itemTotalTax.toString()).append("|");
//	             sbf.append(itemDescription).append("|");
//	             sbf.append(itemUnitPrice.toString()).append("|");
//	             sbf.append(itemUnitTax.toString()).append("|");
//	             sbf.append(paPromotionID).append("|");
//	             sbf.append(paPromotionCounter).append("|");
//	             sbf.append(paEligible).append("|");
//	             sbf.append(paOriginalPrice).append("|");
//	             sbf.append(paDiscountAmount).append("|");
//	             sbf.append(paDiscountType).append("|");
//	             sbf.append(paOriginalPromoQty).append("|");
//	             sbf.append(paOriginalProductCode).append("|");
//	             sbf.append(paOfferId).append("|");
//	             sbf.append(promotionId).append("|");
//	             sbf.append(promotionCounter).append("|");
//	             sbf.append(promotionDiscountType).append("|");
//	             sbf.append(promotionDiscountAmount).append("|");
//	             sbf.append(promotionOfferId).append("|");
//	             sbf.append(promotionExlusive).append("|");
//	             sbf.append(offersTagId).append("|");
//	             sbf.append(offersOfferId).append("|");
//	             sbf.append(offersOverride).append("|");
//	             sbf.append(offersApplied).append("|");
//	             sbf.append(offersClearAfterOverride).append("|");
//	             sbf.append(offersPromotionId).append("|");
//	             sbf.append(custinfoCustomerId).append("|");
//	             sbf.append(custinfoOrderId).append("|");
//	             sbf.append(custinfoIsPaidMobileOrder).append("|");
//	             sbf.append(custinfoTenderQty).append("|");
//	             sbf.append(custinfoTenderValue).append("|");
//	             sbf.append(custinfoTenderCashlessCardProv).append("|");
//	             sbf.append(custinfoTenderCashlessAmount).append("|");
//	             sbf.append(custinfoTotalDiscount).append("|");
//	             sbf.append(custinfoTotalBD).append("|");
//	             sbf.append(custinfoDiscountId).append("|");
//	             sbf.append(custinfoDiscountType).append("|");
//	             sbf.append(tenderIdStr).append("|");
//	             sbf.append(tenderKindStr).append("|");
//	             sbf.append(paymentMethodNameStr).append("|");
//	             sbf.append(tenderQuantityStr).append("|");
//	             sbf.append(tenderFaceAmountStr).append("|");
//	             sbf.append(paymentAmountStr).append("|");
//	             
//	             
//	             newvalue.clear();
//	             newvalue.set(sbf.toString());
//	             
//	             newkey.clear();
//	             newkey.set("");
//	             
////	             context.write(NullWritable.get(), newvalue);
//	             mos.write("PIPERxD126DELIMRxD126"+gdwTerrCd+"RxD126"+gdwBusinessDate,NullWritable.get(), newvalue);
	          
	         // context.getCounter("Count","NumOutputforUS").increment(1);
				
			  
			  if((Integer.parseInt(gdwTerrCd) == 36) && ("psv".equalsIgnoreCase(context.getConfiguration().get("OUTPUT_FILE_FORMAT")))){
				  outputValue.clear();
				  outputValue.set(tdaData.toString());
				  outputKey.clear();
				  outputKey.set("AURxD126TASSEORxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp);
				  
				  context.getCounter("Count","NumOutputforAUPSV").increment(1);
				  if(generateHeaders.equalsIgnoreCase("TRUE"))
				  {
					  context.write(outputKey,outputValue);
				  }else
				  {
				  mos.write("AURxD126TASSEORxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp,NullWritable.get(), outputValue);
				  }
				  //mos.write("AURxD126TASSEORxD126TDARxD126",NullWritable.get(), finalOutText);
			  }
			  
			  if((Integer.parseInt(gdwTerrCd) == 36) && ("tsv".equalsIgnoreCase(context.getConfiguration().get("OUTPUT_FILE_FORMAT")))){
				  outputValue.clear();
				  outputValue.set(tdaData.toString());
				  outputKey.clear();
				  outputKey.set("AURxD126DBRxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp);
				  
				  context.getCounter("Count","NumOutputforAUTSV").increment(1);
				  if(generateHeaders.equalsIgnoreCase("TRUE"))
				  {
					  context.write(outputKey,outputValue);
				  }else
				  {
				  mos.write("AURxD126DBRxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp,NullWritable.get(), outputValue);
				  }
				  //mos.write("AURxD126TASSEORxD126TDARxD126",NullWritable.get(), finalOutText);
			  }
			  
			  if((Integer.parseInt(gdwTerrCd) == 124) && ("tsv".equalsIgnoreCase(context.getConfiguration().get("OUTPUT_FILE_FORMAT")))){
				  outputValue.clear();
				  outputValue.set(tdaData.toString());
				  outputKey.clear();
				  outputKey.set("CNRxD126DBRxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp);
				  
				  context.getCounter("Count","NumOutputforCNTSV").increment(1);
				  if(generateHeaders.equalsIgnoreCase("TRUE"))
				  {
					  context.write(outputKey,outputValue);
				  }else
				  {
				  mos.write("CNRxD126DBRxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp,NullWritable.get(), outputValue);
				  }
				  //mos.write("AURxD126TASSEORxD126TDARxD126",NullWritable.get(), finalOutText);
			  }
			  
			  if((Integer.parseInt(gdwTerrCd) == 840) && ("psv".equalsIgnoreCase(context.getConfiguration().get("OUTPUT_FILE_FORMAT")))){
				  outputValue.clear();
				  outputValue.set(tdaData.toString());
				  outputKey.clear();
				  outputKey.set("USRxD126TASSEORxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp);
				  if(generateHeaders.equalsIgnoreCase("TRUE"))
				  {
					  context.write(outputKey,outputValue);
				  }
				  else
				  {
				  mos.write("USRxD126TASSEORxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp,NullWritable.get(), outputValue);
				  }
				  context.getCounter("Count","NumOutputforUSPSV").increment(1);
				  //mos.write("AURxD126TASSEORxD126TDARxD126",NullWritable.get(), finalOutText);
			  }

			  if((Integer.parseInt(gdwTerrCd) == 840) && ("tsv".equalsIgnoreCase(context.getConfiguration().get("OUTPUT_FILE_FORMAT")))){
				  outputValue.clear();
				  outputValue.set(tdaData.toString());
				  outputKey.clear();
				  outputKey.set("USRxD126DBRxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp);
				  if(generateHeaders.equalsIgnoreCase("TRUE"))
				  {
					  context.write(outputKey,outputValue);
				  }
				  else
				  {
				  mos.write("USRxD126DBRxD126TDARxD126"+gdwBusinessDate+"RxD126"+timestamp,NullWritable.get(), outputValue);
				  }
				  context.getCounter("Count","NumOutputforUSTSV").increment(1);
				  //mos.write("AURxD126TASSEORxD126TDARxD126",NullWritable.get(), finalOutText);
			  }
			  
			  /*else {
				  outputValue.clear();
				  outputValue.set(itemJSObj.toString());
				  context.getCounter("Count","NumOutputforJSON").increment(1);
				  mos.write("MCDRxD126TMSRxD126"+gdwBusinessDate,NullWritable.get(), outputValue);
				  //mos.write("AURxD126TASSEORxD126TDARxD126",NullWritable.get(), finalOutText);
			  }*/
			  			  
			 // mos.write("MCDRxD126TMSRxD126"+gdwTerrCd+DaaSConstants.SPLCHARTILDE_DELIMITER+gdwBusinessDate,NullWritable.get(), partfileNewvalue);
	             		
	          context.getCounter("Count", "ValidDTLRecords").increment(1);
	        
	        
	      }else{
	    	  context.getCounter("Count","SkippedRecords-"+itemaction).increment(1);
//	    	  context.getCounter("Count","SkippedRecords-"+type+"-"+itemlevel+"-"+itemaction+"-"+code).increment(1);
	      }
	    } catch (Exception ex) {
	    	context.getCounter("ExceptionCount", "ProcessItems"+context.getTaskAttemptID()).increment(1);
	    	ex.printStackTrace();
	      Logger.getLogger(NpStldXmlExtendedMapper.class.getName()).log(Level.SEVERE, null, ex);
	    }

	    return(eleItem.getChildNodes());
	  }
	  
	  private boolean hasCustomerInfo(Element orderElement){
		  
		  NodeList orderChldNodes = orderElement.getChildNodes();
		  Element orderChldNode = null;
		  boolean hasCustomerData = false;
		  
		  for(int ordCldIndx=0;ordCldIndx < orderChldNodes.getLength();ordCldIndx++){
			  
			  if(orderChldNodes.item(ordCldIndx).getNodeType() == Node.ELEMENT_NODE){
				  	orderChldNode = (Element)orderChldNodes.item(ordCldIndx);
			    	if(orderChldNode.getNodeName().equalsIgnoreCase("CustomInfo")){
			    		hasCustomerData = true;
			    		break;
			    	}else if(orderChldNode.getNodeName().equalsIgnoreCase("Customer")){
			    		hasCustomerData = true;
			    		break;
			    	}
			  }
		    	
		  }
		
		  
		  return hasCustomerData;
		  
	  }
	  NodeList nlTenders;
	  
	  private PaymentMethod getTender(Element eleTenders,Context context)  { 

	    
	    Element eleTender;
	    String tenderId ="";
	    String tenderKind = "";
	    String tenderName = "";
	    String tenderQuantity ="";
	    String tenderFaceValue = "";
	    String tenderAmount = "" ;
	    String tenderCardProvider = "";
	    String tenderPaymentName = "" ;
	    //mc41946 --> CashlessData
	    String cashlessData="";
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
	 
	                  if ( textNodeName.equalsIgnoreCase("TenderId") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
	                      tenderId = textNode.getNodeValue();
	                    }
	                  
	                  if ( textNodeName.equalsIgnoreCase("TenderKind") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
	                    tenderKind = textNode.getNodeValue();
	                  }

	                  if ( textNodeName.equalsIgnoreCase("TenderName") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
	                    tenderName = textNode.getNodeValue();
	                  }
	                  
	                  if ( textNodeName.equalsIgnoreCase("TenderQuantity") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
	                      tenderQuantity = textNode.getNodeValue();
	                    }
	                  if ( textNodeName.equalsIgnoreCase("FaceValue") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
	                      tenderFaceValue = textNode.getNodeValue();
	                    }

	                  if ( textNodeName.equalsIgnoreCase("TenderAmount") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
	                    tenderAmount = textNode.getNodeValue();
	                  }
	                  
	                  if ( textNodeName.equalsIgnoreCase("CardProviderID") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE && textNode.getNodeValue() != null && !textNode.getNodeValue().trim().isEmpty()) {
	                    tenderCardProvider = textNode.getNodeValue();
	                  }
	                //mc41946- Cashless Data
	                  if ( textNodeName.equalsIgnoreCase("CashlessData") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
	                	  cashlessData = textNode.getNodeValue();
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
	                	//pymt.addUpdatePayment(tenderId,tenderKind,tenderPaymentName,tenderQuantity,tenderFaceValue,tenderAmount,true);
	                	//mc41946 -cashless data
	                  pymt.addUpdatePayment(tenderId,tenderKind,tenderPaymentName,tenderQuantity,tenderFaceValue,tenderAmount,cashlessData,true);
	                } else {
	                	//pymt.addUpdatePayment(tenderId,tenderKind,tenderPaymentName,tenderQuantity,tenderFaceValue,tenderAmount,false);
	                	//mc41946 -cashless data
	                	pymt.addUpdatePayment(tenderId,tenderKind,tenderPaymentName,tenderQuantity,tenderFaceValue,tenderAmount,cashlessData,false);
	                }
	              }
	            }
	          }
	        }
	      }
	    } catch (Exception ex) {
	    	context.getCounter("ExceptionCount", "TenderName").increment(1);
	      Logger.getLogger(NpStldXmlExtendedMapper.class.getName()).log(Level.SEVERE, null, ex);
	    }

	    return pymt;
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
			new StringBuffer();
			
			byte[] bytes = Base64.decodeBase64(str);
			GZIPInputStream gzis = null;
			try{
			    gzis = new GZIPInputStream(new ByteArrayInputStream(bytes));
			    
			   
			}catch(Exception ex){ 
				ex.printStackTrace();
			}
		    
		    return IOUtils.toString(gzis);
		}
	 
	  Text finalOutText = new Text();
	  
	  public String getTMSFormattedDate(String timestamp)
	  {
		  String modifiedTimeStamp;
		  
		  if(timestamp.trim().length()>=14)
		  {
			  
			  /*modifiedTimeStamp = timestamp.substring(0, 4) + GDW_DATE_SEPARATOR + timestamp.substring(4, 6) + GDW_DATE_SEPARATOR + timestamp.substring(6, 8) + " " +
			    		 timestamp.substring(8, 10) + TIME_SEPARATOR  + timestamp.substring(10, 12) + TIME_SEPARATOR + timestamp.substring(12, 14);*/
			  
			  modifiedTimeStamp = timestamp.substring(0, 14);
			  
		  }
		  else
		  {
			  modifiedTimeStamp=timestamp;
		  }
		  return modifiedTimeStamp;
		  
		  /*else if(timestamp.length()==17)
		  {
			  modifiedTimeStamp = timestamp.substring(0, 4) + GDW_DATE_SEPARATOR + timestamp.substring(4, 6) + GDW_DATE_SEPARATOR + timestamp.substring(6, 8) + " " +
			    		 timestamp.substring(8, 10) + TIME_SEPARATOR  + timestamp.substring(10, 12) + TIME_SEPARATOR + timestamp.substring(12, 14)+ TIME_MILLISEC_SEPARATOR+ timestamp.substring(14, 17);
			  
		  }else*/
		  
		  
	  }
		
	  @Override 
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		
			if (this.hasRecordsToWrite) {
				if (Integer.parseInt(this.gdwTerrCd) == 36
						&& ("json".equalsIgnoreCase(context.getConfiguration()
								.get("OUTPUT_FILE_FORMAT")))) {
					this.itemJSObj.put((Object) "columns",
							(Object) this.itemColsArray);
					this.finalOutText.clear();
					this.finalOutText.set(this.itemJSObj.toString());
					context.getCounter("Count", "NumOutputforAUS").increment(1);
					this.mos.write("MCDRxD126TMSRxD126" + this.gdwBusinessDate,NullWritable.get(),this.finalOutText);
				} if (Integer.parseInt(this.gdwTerrCd) != 36
						&& ("json".equalsIgnoreCase(context.getConfiguration()
								.get("OUTPUT_FILE_FORMAT")))) {
					context.getCounter("Count", "NumOutputforUS").increment(1);
					this.itemJSObj.put((Object) "columns",
							(Object) this.itemColsArray);
					this.finalOutText.clear();
					this.finalOutText.set(this.itemJSObj.toString());
					this.mos.write("MCDRxD126TMSRxD126" + this.gdwTerrCd
							+ DaaSConstants.SPLCHARTILDE_DELIMITER
							+ this.gdwBusinessDate,NullWritable.get(),this.finalOutText);
				}
			}
			mos.close();
		
	}
}

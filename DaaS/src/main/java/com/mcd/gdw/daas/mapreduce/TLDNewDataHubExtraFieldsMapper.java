package com.mcd.gdw.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.Path;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;
public class TLDNewDataHubExtraFieldsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
//	public class TLDNewDataHubMapperHCat extends Mapper<LongWritable, Text, NullWritable, Text> {
//		public class TLDNewDataHubMapperHCat extends Mapper<LongWritable, Text, NullWritable, OrcFile> {

		private static String ORDER_KEY					= "key";
		private static String ORDER_KIND 				= "kind";
		private static String ORDER_SIDE 				= "side";
		private static String ORDER_SALETYPE 			= "saleType";
		private static String ORDER_TOTALAMOUNT 		= "totalAmount";
		private static String ORDER_NONPRODUCTAMOUNT 	= "nonProductAmount";
		private static String ORDER_TOTALTAX			= "totalTax";
		private static String ORDER_NONPRODUCTTAX		= "nonProductTax";
		private static String ORDER_UNIQUEID 		 	= "uniqueId";
		private static String ORDER_STARTSALEDATE 		= "startSaleDate";
		private static String ORDER_START_SALETIME 		= "startSaleTime";
		private static String ORDER_ENDSALEDATE    		= "endSaleDate";
		private static String ORDER_ENDSALETIME    		= "endSaleTime";
		
		
		
	
		private static final String ORDER_ITEM_ID  				= "id";
		private static final String ORDER_ITEM_LEVEL 			= "level";
		private static final String ORDER_ITEM_CODE				= "code";
		private static final String ORDER_ITEM_TYPE				= "type";
		private static final String ORDER_ITEM_ACTION 			= "action";
		private static final String ORDER_ITEM_DESCRIPTION 		= "description";
		private static final String ORDER_ITEM_QTY				= "qty";
		private static final String ORDER_ITEM_POSITMTOTQTY		= "posItmTotQt";
		private static final String ORDER_ITEM_GRILLQTY			= "grillQty";
		private static final String ORDER_ITEM_GRILLMODIFER		= "grillModifer";
		private static final String ORDER_ITEM_QTYPROMO			= "qtyPromo";
		private static final String ORDER_ITEM_CHGAFTERTOTAL	= "chgAfterTotal";
		private static final String ORDER_ITEM_BPPRICE			= "BPPrice";
		private static final String ORDER_ITEM_BPTAX			= "BPTax";
		private static final String ORDER_ITEM_BDPRICE			= "BDPrice";
		private static final String ORDER_ITEM_BDTAX			= "BDTax";
		private static final String ORDER_ITEM_TOTALPRICE		= "totalPrice";
		private static final String ORDER_ITEM_TOTALTAX			= "totalTax";
		private static final String ORDER_ITEM_CATEGORY			= "category";
		private static final String ORDER_ITEM_FAMILYGROUP		= "familyGroup";
		private static final String ORDER_ITEM_QTYVOIDED		= "qtyVoided";
		private static final String ORDER_ITEM_UNITPRICE		= "unitPrice";
		private static final String ORDER_ITEM_UNITTAX			= "unitTax";
		 



		
		private static String ORDER_ITEM_POSTIMINGS_ITEMSCOUNT 		= "itemsCount";
		private static String ORDER_ITEM_POSTIMINGS_UNTILPAY   		= "untilPay";
		private static String ORDER_ITEM_POSTIMINGS_UNTILTOTAL 		= "untilTotal";
		private static String ORDER_ITEM_POSTIMINGS_UNTILSTORE 		= "untilStore";


		private static String ORDER_ITEM_OFFERS_APPLIED 			= "applied";
		private static String ORDER_ITEM_OFFERS_OVERRIDE 			= "override";
		private static String ORDER_ITEM_OFFERS_TAGID	 			= "tagId";
		private static String ORDER_ITEM_OFFERS_OFFERID 			= "offerId";
		private static String ORDER_ITEM_OFFERS_CLEARAFTEROVERRIDE 	= "clearAfterOverride";
		private static String ORDER_ITEM_OFFERS_PROMOTIONID 		= "promotionId";

		private static String ORDER_ITEM_PROMOTIONS_PROMOTIONID			= "promotionId";
		private static String ORDER_ITEM_PROMOTIONS_PROMOTIONCOUNTER	= "promotionCounter";
		private static String ORDER_ITEM_PROMOTIONS_DISCOUNTTYPE		= "discountType";
		private static String ORDER_ITEM_PROMOTIONS_DISCOUNTAMOUNT		= "discountAmount";
		private static String ORDER_ITEM_PROMOTIONS_OFFERID				= "offerId";
		private static String ORDER_ITEM_PROMOTIONS_EXCLUSIVE			= "exclusive";
		private static String ORDER_ITEM_PROMOTIONS_PROMOTIONONTENDER	= "promotionOnTender";
		private static String ORDER_ITEM_PROMOTIONS_RETURNEDVALUE		= "returnedValue";

		private static String ORDER_ITEM_CUSTOMINFO_ORDERID						= "orderId";
		private static String ORDER_ITEM_CUSTOMINFO_ORDERID_CUSTOMERID 			= "customerId";
		private static String ORDER_ITEM_CUSTOMINFO_ORDERID_ISPAIDMOBILEORDER 	= "IsPaidMobileOrder";
		private static String ORDER_ITEM_CUSTOMINFO_ORDERID_CHECKINDATA 		= "checkInData";

		private static String ORDER_TENDERS_TENDERID		= "TenderId";
		private static String ORDER_TENDERS_TENDERKIND 		= "TenderKind";
		private static String ORDER_TENDERS_TENDERNAME		= "TenderName";
		private static String ORDER_TENDERS_TENDERAMOUNT	= "TenderAmount";
		private static String ORDER_TENDERS_CARDPROVIDERID	= "CardProviderID";
		private static String ORDER_TENDERS_TENDERQUANTITY	= "TenderQuantity";
		private static String ORDER_TENDERS_FACEVALUE		= "FaceValue";
		private static String ORDER_TENDERS_CASHLESSDATA	= "CashlessData";
		
		
		
		private static final String SOS_SERVICETIME_UNTILTOTAL 					= "untilTotal";
		private static final String SOS_SERVICETIME_UNTILSTORE					= "untilStore";
		private static final String SOS_SERVICETIME_UNTILRECALL					= "untilRecall";
		private static final String SOS_SERVICETIME_UNTILCLOSEDRAWER			= "untilCloseDrawer";
		private static final String SOS_SERVICETIME_UNTILPAY					= "untilPay";
		private static final String SOS_SERVICETIME_UNTILSERVE					= "untilServe";
		private static final String SOS_SERVICETIME_TOTALTIME					= "totalTime";
		private static final String SOS_SERVICETIME_TCOVERPRESENTATIONPRESET	= "tcOverPresentationPreset";
		private static final String SOS_SERVICETIME_TCOVERTOTALPRESET			= "tcOverTotalPreset";
		private static final String SOS_SERVICETIME_TCOVERTOTALMFY				= "tcOverTotalMFY";
		private static final String SOS_SERVICETIME_TCOVERTOTALFC				= "tcOverTotalFC";
		private static final String SOS_SERVICETIME_TCOVERTOTALDT				= "tcOverTotalDT";

		private static final String SOS_SERVICETIME_PT_TCOVER50 		 		= "tcOver50";
		private static final String SOS_SERVICETIME_PT_TCUNDER25				= "tcUnder25";
		private static final String SOS_SERVICETIME_PT_HELDTIME		 			= "heldTime";
		private static final String SOS_SERVICETIME_PT_TCHELD		 			= "tcHeld";
		
	HashSet<String> TERRCD_STOREID_BUSNDT_ORDKEY_SET = new HashSet<String>();
	

	  String  orderTarget ="";
	  String  cashTarget ="";
	  String  storeTarget ="";
	  String  lineTarget ="";
	  String  holdTarget ="";
	  String  presentationTarget ="";
	  String  cashpresentationTarget ="";
	  String  totalTarget ="";
	  String  totalTargetMFY ="";
	  String  totalTargetFC ="";
	  String  totalTargetDT ="";
	  String  underTotalTargetMFY ="";
	  String  underTotalTargetDT ="";
	  String  UnderTotalTargetFC ="";
	  String  underTotalTargetCBB ="";
	  String  overTotalTargetMFY ="";
	  String  overTotalTargetDT ="";
	  String  overTotalTargetFC ="";
	  String  overTotalTargetCBB ="";
	  String  underPresentationTargetMFY ="";
	  String  underPresentationTargetDT ="";
	  String  underPresentationTargetFC ="";
	  String  underPresentationTargetCBB ="";
	  String  overPresentationTargetMFY ="";
	  String  overPresentationTargetDT ="";
	  String  overPresentationTargetFC ="";
	  String  overPresentationTargetCBB ="";

	  StringBuffer additionalFieldsbf = new StringBuffer();
	
	public class IncludeListKey {
	
		private String terrCd;
		private String lgcyLclRfrDefCd;
		
		public IncludeListKey(String terrCd
				             ,String lgcyLclRfrDefCd) {
			
			this.terrCd = terrCd;
			this.lgcyLclRfrDefCd = lgcyLclRfrDefCd;
			
		}
		
		public String getTerrCd() {
			
			return(this.terrCd);
			
		}
		
		public String getLgcyLclRfrDefCd() {
			
			return(this.lgcyLclRfrDefCd);
			
		}
		
		public String toString() {
			
			return(this.terrCd + "_" + this.lgcyLclRfrDefCd);
			
		}
	}
	
	public class IncludeListDates {
		
		private Calendar fromDt = Calendar.getInstance();
		private Calendar toDt = Calendar.getInstance();
		
		public IncludeListDates(String fromDt
				               ,String toDt) {
			
			String[] parts;
			
			parts = (fromDt+"-1-1").split("-");
			this.fromDt.set(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));

			parts = (toDt+"-1-1").split("-");
			this.toDt.set(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
			
		}
		
		public boolean isIsoDateBetween(String date) {
			
			String[] parts;

			parts = (date+"-1-1").split("-");
			
			return(isDateBetween(parts[0] + parts[1] + parts[2]));
			
		}

		public boolean isDateBetween(String date) {
			
			Calendar dt = Calendar.getInstance();
			boolean isBetween = false;

			dt.set(Integer.parseInt(date.substring(0, 4)), Integer.parseInt(date.substring(4, 6)), Integer.parseInt(date.substring(6, 8)));

			if ( (dt.equals(fromDt) || dt.after(fromDt)) && ( dt.equals(toDt) || dt.before(toDt)) ) {
				isBetween = true;
			}
			return(isBetween);
			
		}
	}
	
	private final static String SEPARATOR_CHARACTER = "\t";
	private final static String REC_POSTRN          = "TRN";
	private final static String REC_POSTRNOFFR      = "OFR";
	private final static String REC_POSTRNITM       = "ITM";
	private final static String REC_POSTRNITMOFFR   = "IOF";
	
	private String[] parts = null;
	private Text mapKey = new Text();
	private Text mapValue = new Text();

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;

	private FileSplit fileSplit = null;
	private String fileName = StringUtils.EMPTY;
	private String subfileType = StringUtils.EMPTY;
	
	private Calendar cal = Calendar.getInstance();

	private String posBusnDt;
    private String mcdGbalLcatIdNu;
    private String lgcyLclRfrDefCd;
    private String terrCd;
	private String posOrdKey;
	private String posRestId;
	private String posDvceId;
	private String posAreaTypShrtDs;
	private String posTrnStrtTs;
	private String posTrnTypCd;
	private String posMfySideCd;
	private String posPrdDlvrMethCd;
	private String posTotNetTrnAm;
	private String posTotNprdNetTrnAm;
	private String posTotTaxAm;
	private String posTotNprdTaxAm;
	private String posTotItmQt;
	private String posPaidForOrdTs;
	private String posTotKeyPrssTs;
	private String posOrdStrInSysTs;
	private String posOrdUniqId;
	private String posOrdStrtDt;
	private String posOrdStrtTm;
	private String posOrdEndDt;
	private String posOrdEndTm;
	private String offrCustId;
	private String ordOffrApplFl;
	private String dyptIdNu;
	private String dyptDs;
	private String dyOfCalWkDs;
 	private String hrIdNu; 

	private String untlTotKeyPrssScQt;
	private String untlStrInSysScQt;
	private String untlOrdRcllScQt;
	private String untlDrwrClseScQt;
	private String untlPaidScQt;
	private String untlSrvScQt;
	private String totOrdTmScQt;
	private String abovPsntTmTrgtFl;
	private String abovTotTmTrgtFl;
	private String abovTotMfyTrgtTmTmFl;
	private String abovTotFrntCterTrgtTmFl;
	private String abovTotDrvTrgtTmFl;
	private String abov50ScFl;
	private String bel25ScFl;
	private String heldTmScQt;
	private String ordHeldFl;

	private String posTrnItmSeqNu;
	private String posItmLvlNu;
	private String sldMenuItmId;
	private String posPrdTypCd;
	private String posItmActnCd;
	private String posItmDesc;
	private String posItmTotQt;
	private String posItmGrllQt;
	private String posItmGrllModCd;
	private String posItmPrmoQt;
	private String posChgAftTotCd;
	private String posItmNetUntPrcB4PrmoAm;
	private String posItmTaxB4PrmoAm;
	private String posItmNetUntPrcB4DiscAm;
	private String posItmTaxB4DiscAm;
	private String posItmActUntPrcAm;
	private String posItmActTaxAm;
	private String posItmCatCd;
	private String posItmFmlyGrpCd;
	private String posItmVoidQt;
	private String posItmTaxRateAm;
	private String posItmTaxBasAm;
	private String itmOffrAppdFl;
	
	private boolean orderCustomerFoundF1 = false;
	
	private boolean orderOfferFoundF1 	 = false;
	private String orderOverrideF1 	 = "FALSE";
	private boolean itmOfferAppdFoundFl  = false;
	private boolean itmPromoFoundFl  = false;
	private String orderAppliedFl       = "FALSE";
	private String orderPromotionId      = "";
	private String offersPromotionId      = null;
	
	private String itmPromoAppdFoundFlg = "0";
	private boolean isItemOfferFl = false;
	private boolean isPromoAppledNode = false;
	
	private HashMap<String,String> dayPartMap = new HashMap<String,String>();
//	private HashMap<String,IncludeListDates> includeListMap = new HashMap<String,IncludeListDates>();
	private HashMap<String,Integer> promoIdMap = new HashMap<String,Integer>();

	private StringBuffer outputKey = new StringBuffer();
	private StringBuffer outputTextValue = new StringBuffer();
	
	
	private Text uniqueTerrCdBusnDtStoreIdVal = new Text();
	
	private HashMap<String,IncludeListDates> includeListMap = new HashMap<String,IncludeListDates>();
	
	
	HashSet<String> uniqueTerrCdBusDtStoreIdSet = new HashSet<String>();
	
	private StringBuffer orderInfo 			  		= new StringBuffer();
	private StringBuffer allInfo 			 		= new StringBuffer();
	private StringBuffer itemInfo 			 	 	= new StringBuffer();
	private StringBuffer itemPromotionAppliedInfo 	= new StringBuffer();
	private StringBuffer itemPromoInfo 			    = new StringBuffer();
	private StringBuffer orderCustomerInfo			= new StringBuffer();
	private StringBuffer orderPromotionsInfo 		= new StringBuffer();
	private StringBuffer orderOfferInfo 			= new StringBuffer();
	private StringBuffer orderPOSTimingsInfo 		= new StringBuffer();
	private StringBuffer orderCustomInfoInfo 		= new StringBuffer();
	private StringBuffer orderReductionInfo 		= new StringBuffer();
	
	private HashMap<Integer,String> tendersInfoMap = new HashMap<Integer,String>();
	
	private StringBuffer tenderInfo		    		= new StringBuffer();
	
	String useStoreFilter = "FALSE";
	
	String menuPriceBasis = "N";
	
	String multioutBaseOutputPath = StringUtils.EMPTY;
//	private MultipleOutputs<NullWritable, Text> mos;
//	private MultipleOutputs<NullWritable, OrcFile> mos;
	
//	HCatSchema stldschema;
//	HCatSchema sosschema;
	
	@Override
	public void setup(Context context) {
		
		multioutBaseOutputPath = context.getConfiguration().get("MULTIOUT_BASE_OUTPUT_PATH");
//		mos = new MultipleOutputs<NullWritable, Text>(context);
//		mos = new MultipleOutputs<NullWritable, OrcFile>(context);
		URI[] distPaths;
	    //Path distpath = null;
	    BufferedReader br = null;
	    String[] distPathParts;
	    
        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
        useStoreFilter = context.getConfiguration().get("USE_STORE_FILTER");
        System.out.println(" FileName " + fileName );
//        subfileType = fileName.split("~")[0];
        
        if(useStoreFilter == null)
        	useStoreFilter = "FALSE";
        
		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			
		    //distPaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		    distPaths = context.getCacheFiles();
		    
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		      
		    if ( distPaths != null && distPaths.length > 0 )  {
		    	  
		    	System.out.println(" number of distcache files : " + distPaths.length);
		    	  
		    	for ( int i=0; i<distPaths.length; i++ ) {
			    	  
			    	  //distpath = distPaths[i];
				     
			    	  System.out.println("distpaths:" + distPaths[i].toString());
			    	  //System.out.println("distpaths URI:" + distPaths[i].toUri());
			    	  
			    	  distPathParts = 	distPaths[i].toString().split("#");
			    	  
			    	  if( distPaths[i].toString().contains("DayPart_ID.psv") ) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	  addDaypartKeyValuestoMap(br);
				      	  System.out.println("Loaded Daypart Values Map");
				      	  
				      } 
			    	  else if ( distPaths[i].toString().contains("offers_include_list.txt") ) {
    	  		      	  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addIncludeListToMap(br);
				      	  System.out.println("Loaded Include List Values Map");
				      }
			      }
		      }
		    
		    //temp
		    HiveConf hconf = new HiveConf(context.getConfiguration(),this.getClass());
//			
//			hconf.addResource(new Path("/etc/hive/conf/hive-site.xml"));
//			hconf.set("hive.exec.dynamic.partition", "true");
//			hconf.set("hive.exec.dynamic.partition.mode","nonstrict");
//			
//			
//			HiveMetaStoreClient hmscli = new HiveMetaStoreClient(hconf);
//			
//			Table table = HCatUtil.getTable(hmscli, "daas", "tld_datahub");
//			
//			stldschema = HCatUtil.getTableSchemaWithPtnCols(table);
//			
//			table = HCatUtil.getTable(hmscli, "daas", "detailedsos_datahub");
//			sosschema = HCatUtil.getTableSchemaWithPtnCols(table);
			
		} catch (Exception ex) {
			System.err.println("Error in initializing TLDNewDataHubMapperHCat:");
			System.err.println(ex.toString());
			System.exit(8);
		}
		
	}
	
	private void addIncludeListToMap(BufferedReader br) throws Exception {
		
		String line = null;
		String[] parts;
		
		String terrCd;
		String lgcyLclRfrDefCd;
		String fromDt;
		String toDt;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\t", -1);

					terrCd          = String.format("%03d", Integer.parseInt(parts[0]));
					lgcyLclRfrDefCd = parts[1];
					fromDt          = parts[2];
					toDt            = parts[3];

					includeListMap.put(new IncludeListKey(terrCd,lgcyLclRfrDefCd).toString(),new IncludeListDates(fromDt,toDt));
					
					//System.out.println(terrCd + " " + lgcyLclRfrDefCd + " " + fromDt + " " + toDt );
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
//			System.exit(8);
		} finally {
			try {
				if (br != null)
					br.close();
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	private void addDaypartKeyValuestoMap(BufferedReader br) {
	
		String line = null;
		String[] parts;
		
		String dayPartKey;
		String terrCd;
		String dayOfWeek;
		String startTime;
		String daypartId;
		String daypartDs;
		
		String timeSegment;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\|", -1);

					terrCd      = String.format("%03d", Integer.parseInt(parts[0]));
					dayOfWeek   = parts[1];
					timeSegment = parts[2];
					startTime   = parts[3];

					daypartId   = parts[5];
					daypartDs   = parts[6];
						
					if ( timeSegment.equalsIgnoreCase("QUARTER HOURLY") ) {
						dayPartKey = terrCd + SEPARATOR_CHARACTER + dayOfWeek + SEPARATOR_CHARACTER + startTime.substring(0, 2) + startTime.substring(3, 5);
							
						dayPartMap.put(dayPartKey,daypartId+SEPARATOR_CHARACTER+daypartDs);
					}
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
	
	
	Date curDate = new Date(System.currentTimeMillis());
	
	
	Text terrcdbusndtstoreid = new Text("terrcdbusndtstoreid");
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		boolean includeFile = true;
		IncludeListKey includeListKey;
		uniqueTerrCdBusDtStoreIdSet.clear();
		TERRCD_STOREID_BUSNDT_ORDKEY_SET.clear();
		
		try {
			
			if ( fileName.toUpperCase().contains("STLD") || fileName.toUpperCase().contains("DETAILEDSOS") ) {
				parts = value.toString().split("\t");
				
				if ( parts.length >= 8 ) {
					includeFile = true;
				}
			}
		
			
			if ( includeFile && "TRUE".equalsIgnoreCase(useStoreFilter)) {
					
				includeListKey = new IncludeListKey(parts[DaaSConstants.XML_REC_TERR_CD_POS],parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS]);
				
				if ( includeListMap.containsKey(includeListKey.toString()) ) {
					if ( !includeListMap.get(includeListKey.toString()).isDateBetween(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]) ) {
						includeFile = false;
					}
				} else {
					includeFile = false;
				}
			}
			
			if ( includeFile ) {
				subfileType = parts[DaaSConstants.XML_REC_FILE_TYPE_POS];
				terrCd = String.format("%03d", Integer.parseInt(parts[DaaSConstants.XML_REC_TERR_CD_POS]));
				posBusnDt = formatDateAsTsDtOnly(parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS]);
				mcdGbalLcatIdNu = parts[DaaSConstants.XML_REC_MCD_GBAL_LCAT_ID_NU_POS];
				lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
				
//				
				//comment this out after testing
				if(! terrCd.equalsIgnoreCase("840") || !posBusnDt.equalsIgnoreCase("2015-06-01") || !(lgcyLclRfrDefCd.equalsIgnoreCase("32796"))){
					context.getCounter("Count","SkippingonStore").increment(1);
					return;
				}
				context.getCounter("Count", fileName).increment(1);

				
				if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("STLD") ) {
			    	getOrderDataStld(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);

				} else if ( parts[DaaSConstants.XML_REC_FILE_TYPE_POS].equalsIgnoreCase("DETAILEDSOS") ) {
					getOrderDataDetailedSos(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
				}
//				if("WORK_LAYER".equalsIgnoreCase(context.getConfiguration().get("PROCESSING_DATA"))){
					String uniquekey = terrCd+"~"+posBusnDt+"~"+lgcyLclRfrDefCd;
					
					if(!uniqueTerrCdBusDtStoreIdSet.contains(uniquekey)){
						uniqueTerrCdBusnDtStoreIdVal.clear();
						uniqueTerrCdBusnDtStoreIdVal.set(uniquekey);
						
//						context.write(NullWritable.get(), uniqueTerrCdBusnDtStoreIdVal);
						context.write(terrcdbusndtstoreid, uniqueTerrCdBusnDtStoreIdVal);//todo
						uniqueTerrCdBusDtStoreIdSet.add(uniquekey);
					}
//				}
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in TLDNewDataHubMapperHCat.Map:");
			ex.printStackTrace(System.err);
			throw new InterruptedException("Exception in TLDNewDataHubMapper" +ex.toString());
//			System.exit(8);
		}finally{
			parts = null;
		}
		
	}
	
	Element eleRoot;

	Element eleNode;
	Element eleEvent;
	String eventType;
	Element eleTrx;
	Element eleOrder;
	
	private void getOrderDataStld(String xmlText
			                     ,Context context) throws Exception{
		

		
		
		try {
			try {
				strReader  = new StringReader(xmlText);
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			} catch (Exception ex1) {
				strReader  = new StringReader(xmlText.replaceAll("&#x1F" , "_"));
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);
			}

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("TLD") ) {
				posRestId = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");

				processNode(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDNewDataHubMapperHCat.getOrderData:");
			ex.printStackTrace(System.err);
			throw  ex;
//			System.exit(8); 
		}finally{
			xmlText = null;
			doc = null;
			xmlSource = null;
			strReader.close();
			
		}
		
	}
	
	private void processNode(NodeList nlNode
			                ,Context context) throws Exception {

		eleNode = null;
		
		if (nlNode != null && nlNode.getLength() > 0 ) {
			for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
				if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {  
					eleNode = (Element)nlNode.item(idxNode);
					if ( eleNode.getNodeName().equals("Node") ) {
						posDvceId = eleNode.getAttribute("id");
						
						processEvent(eleNode.getChildNodes(),context);
					}
				}
			}
		}
		
		
	}

	
	
	private void processEvent(NodeList nlEvent
			                 ,Context context) throws Exception {

		
		
		if (nlEvent != null && nlEvent.getLength() > 0 ) {
			for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++ ) {
				eleEvent = null;
				eventType = StringUtils.EMPTY;
				if ( nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE ) {  
					eleEvent = (Element)nlEvent.item(idxEvent);
					
					if( eleEvent.getNodeName().equals("Event")){
						if(eleEvent.getAttribute("Type").equalsIgnoreCase("TRX_BaseConfig")){
							processTrxBaseConfig(eleEvent,context);
						}else
						if ( (eleEvent.getAttribute("Type").equalsIgnoreCase("TRX_Sale") ||
						    		 eleEvent.getAttribute("Type").equalsIgnoreCase("TRX_Refund") ||
						    		 eleEvent.getAttribute("Type").equalsIgnoreCase("TRX_Overring") ||
						    		 eleEvent.getAttribute("Type").equalsIgnoreCase("TRX_Waste") )) {
							eventType = eleEvent.getAttribute("Type");
							processTrxSale(eleEvent,eleEvent.getChildNodes(),context);
						}
					}
				}
				
				orderInfo.setLength(0);
				allInfo.setLength(0);
				itemInfo.setLength(0);
				itemPromotionAppliedInfo.setLength(0);
				itemPromoInfo.setLength(0);
				orderCustomerInfo.setLength(0);
				orderPromotionsInfo.setLength(0);
				orderOfferInfo.setLength(0);
				orderPOSTimingsInfo.setLength(0);
				orderCustomInfoInfo.setLength(0);
				orderReductionInfo.setLength(0);
			}
		}
		
	}
	StringBuffer trxBaseConfigBf = new StringBuffer();
	
	private void processTrxBaseConfig(Element eleEvent,Context context) throws Exception{
		
		NodeList bcLst = eleEvent.getChildNodes();
		NodeList cLst = null;
		trxBaseConfigBf.setLength(0);
		Element tmp;
		Element configEle;
		NodeList cChlLst = null;
		String[] configArray = null;
		
		trxBaseConfigBf.append(terrCd).append(DaaSConstants.TAB_DELIMITER).append(posRestId).append(DaaSConstants.TAB_DELIMITER).append(posBusnDt).append(DaaSConstants.TAB_DELIMITER);
		trxBaseConfigBf.append(eleEvent.getAttribute("RegId")).append(DaaSConstants.TAB_DELIMITER).append(eleEvent.getAttribute("Time")).append(DaaSConstants.TAB_DELIMITER);
		
		for(int bcLstIdx=0;bcLstIdx<bcLst.getLength();bcLstIdx++){
			if(bcLst.item(bcLstIdx).getNodeType() == Node.ELEMENT_NODE){
			Element trxBCEle = (Element)bcLst.item(bcLstIdx);
			if(trxBCEle.getNodeName().equalsIgnoreCase("TRX_BaseConfig")){
				
				trxBaseConfigBf.append(trxBCEle.getAttribute("POS")).append(DaaSConstants.TAB_DELIMITER).append(trxBCEle.getAttribute("POD")).append(DaaSConstants.TAB_DELIMITER);
				cLst = trxBCEle.getElementsByTagName("Config");
				System.out.println(" CONFIG children length "  + cLst.getLength());
				for(int cLstIdx=0;cLstIdx < cLst.getLength();cLstIdx++){
					
					configEle =((Element)cLst.item(cLstIdx)); 
					
					
					cChlLst =configEle.getChildNodes();
					int numChildren = 0;
					
					for(int cChLstIdx=0;cChLstIdx<cChlLst.getLength();cChLstIdx++){
						if(cChlLst.item(cChLstIdx).getNodeType() == Node.ELEMENT_NODE){
							numChildren++;
						}
					}
					configArray = new String[(numChildren*2 -1)];
					
					for(int cChLstIdx=0;cChLstIdx<cChlLst.getLength();cChLstIdx++){
						
						System.out.println( " chlement at " + cChLstIdx + " - " + cChlLst.item(cChLstIdx).getNodeName());
						
						if(cChlLst.item(cChLstIdx).getNodeType() == Node.ELEMENT_NODE){
							tmp = (Element)cChlLst.item(cChLstIdx);
							
							if("MenuPriceBasis".equalsIgnoreCase(tmp.getNodeName())){
								configArray[0] = tmp.getFirstChild().getNodeValue();
								configArray[1] = DaaSConstants.TAB_DELIMITER;
							}else if("WeekEndBreakfastStartTime".equalsIgnoreCase(tmp.getNodeName())){
								configArray[2] = tmp.getFirstChild().getNodeValue();
								configArray[3] = DaaSConstants.TAB_DELIMITER;
							}else if("WeekEndBreakfastStopTime".equalsIgnoreCase(tmp.getNodeName())){
								configArray[4] = tmp.getFirstChild().getNodeValue();
								configArray[5] = DaaSConstants.TAB_DELIMITER;
							}else if("WeekDayBreakfastStartTime".equalsIgnoreCase(tmp.getNodeName())){
								configArray[6] = tmp.getFirstChild().getNodeValue();
								configArray[7] = DaaSConstants.TAB_DELIMITER;
							}else if("WeekDayBreakfastStopTime".equalsIgnoreCase(tmp.getNodeName())){
								configArray[8] = tmp.getFirstChild().getNodeValue();
								configArray[9] = DaaSConstants.TAB_DELIMITER;
							}else if("DecimalPlaces".equalsIgnoreCase(tmp.getNodeName())){
								configArray[10] = tmp.getFirstChild().getNodeValue();
								configArray[11] = DaaSConstants.TAB_DELIMITER;
							}else if("CheckRefund".equalsIgnoreCase(tmp.getNodeName())){
								configArray[12] = tmp.getFirstChild().getNodeValue();
								configArray[13] = DaaSConstants.TAB_DELIMITER;
							}else if("GrandTotalFlag".equalsIgnoreCase(tmp.getNodeName())){
								configArray[14] = tmp.getFirstChild().getNodeValue();
								configArray[15] = DaaSConstants.TAB_DELIMITER;
							}else if("StoreId".equalsIgnoreCase(tmp.getNodeName())){
								configArray[16] = tmp.getFirstChild().getNodeValue();
								configArray[17] = DaaSConstants.TAB_DELIMITER;
							}else if("StoreName".equalsIgnoreCase(tmp.getNodeName())){
								configArray[18] = tmp.getFirstChild().getNodeValue();
								configArray[19] = DaaSConstants.TAB_DELIMITER;
							}else if("AcceptNegativeQty".equalsIgnoreCase(tmp.getNodeName())){
								configArray[20] = tmp.getFirstChild().getNodeValue();
								configArray[21] = DaaSConstants.TAB_DELIMITER;
							}else if("AcceptZeroPricePMix".equalsIgnoreCase(tmp.getNodeName())){
								configArray[22] = tmp.getFirstChild().getNodeValue();
								configArray[23] = DaaSConstants.TAB_DELIMITER;
							}else if("FloatPriceTenderId".equalsIgnoreCase(tmp.getNodeName())){
								configArray[24] = tmp.getFirstChild().getNodeValue();
							}
						
						}	
					}
					
					for(int idx=0;idx < configArray.length;idx++){
						trxBaseConfigBf.append(configArray[idx]);
					}
					break;//Assuming only one Config element
				}
				
				cLst = trxBCEle.getElementsByTagName("POSConfig");
		
				for(int pcLstIdx=0;pcLstIdx < cLst.getLength();pcLstIdx++){
					
					configEle =((Element)cLst.item(pcLstIdx)); 
					
					cChlLst =configEle.getChildNodes();
					int numChildren = 0;
					for(int cChLstIdx=0;cChLstIdx<cChlLst.getLength();cChLstIdx++){
						if(cChlLst.item(cChLstIdx).getNodeType() == Node.ELEMENT_NODE){
							numChildren++;
						}
					}
					
					configArray = new String[(numChildren*2 -1)];
					for(int cChLstIdx=0;cChLstIdx<cChlLst.getLength();cChLstIdx++){
						if(cChlLst.item(cChLstIdx).getNodeType() == Node.ELEMENT_NODE){
						tmp = (Element)cChlLst.item(cChLstIdx);
						
						if("CountTCsFullDiscEM".equalsIgnoreCase(tmp.getNodeName())){
							configArray[0] = tmp.getFirstChild().getNodeValue();
							configArray[1] = DaaSConstants.TAB_DELIMITER;
						}else if("RefundBehaviour".equalsIgnoreCase(tmp.getNodeName())){
							configArray[2] = tmp.getFirstChild().getNodeValue();
							configArray[3] = DaaSConstants.TAB_DELIMITER;
						}else if("OverringBehaviour".equalsIgnoreCase(tmp.getNodeName())){
							configArray[4] = tmp.getFirstChild().getNodeValue();
							
						}
						}
					}
					
					trxBaseConfigBf.append(DaaSConstants.TAB_DELIMITER);
					
					for(int idx=0;idx < configArray.length;idx++){
						trxBaseConfigBf.append(configArray[idx]);
					}
					trxBaseConfigBf.append(DaaSConstants.TAB_DELIMITER);
					break;//assuming only one POSConfig element
				}
				context.write(new Text("BaseConfig"), new Text(trxBaseConfigBf.toString()));
			}
			
			}
			
		}
				
		
	}
	private void processTrxSale(Element eleEvent,NodeList nlTrxSale
			                   ,Context context) throws Exception {


		eleTrx = null;
		
		if (nlTrxSale != null && nlTrxSale.getLength() > 0 ) {
			for (int idxTrxSale=0; idxTrxSale < nlTrxSale.getLength(); idxTrxSale++ ) {
				if ( nlTrxSale.item(idxTrxSale).getNodeType() == Node.ELEMENT_NODE ) {
					eleTrx = (Element)nlTrxSale.item(idxTrxSale);

//					if ( eleEvent.getAttribute("Type").equalsIgnoreCase("TRX_Sale")){
//							if(eleTrx.getAttribute("status").equals("Paid") ) {
								posAreaTypShrtDs = eleTrx.getAttribute("POD");
						
								if ( eleEvent.getAttribute("Type").equalsIgnoreCase("TRX_Sale")){
									if(!eleTrx.getAttribute("status").equals("Paid") ) {
										return;//if TRX_Sale, consider only the elements with status Paid.
									}
								}
								processOrder(eleTrx.getChildNodes(),context);
//							}
//					}
				}
			}
		}

	}

	int itemSequenceWithinOrder = 1;
	StringBuffer terrcdStoreIdBusnDtOrdKey = new StringBuffer();
	BigDecimal totalAmtbg;
	BigDecimal totalTaxbg;
	BigDecimal posTotNprdNetTrnAmbg;
	BigDecimal posTotNprdTaxAmbg;
	
	private void processOrder(NodeList nlOrder
			                 ,Context context) throws Exception {

		eleOrder = null;
		itemSequenceWithinOrder = 1;
		terrcdStoreIdBusnDtOrdKey.setLength(0);
		
		try {
			if (nlOrder != null && nlOrder.getLength() > 0 ) {
				for (int idxOrder=0; idxOrder < nlOrder.getLength(); idxOrder++ ) {
					if ( nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {
						eleOrder = (Element)nlOrder.item(idxOrder);
						
						context.getCounter("Count","NumOrders1").increment(1);
						posOrdKey = getValue(eleOrder,ORDER_KEY);
//						if(!posOrdKey.equalsIgnoreCase("POS0001:148634574")) {
//							context.getCounter("Count","SkippingOnOrder1").increment(1);
//							return;
//						}else{
//							context.getCounter("Count", ((FileSplit)context.getInputSplit()).getPath().getName()).increment(1);
//						}
						
						terrcdStoreIdBusnDtOrdKey.append(terrCd).append(SEPARATOR_CHARACTER);
						terrcdStoreIdBusnDtOrdKey.append(posRestId).append(SEPARATOR_CHARACTER);
						terrcdStoreIdBusnDtOrdKey.append(posBusnDt).append(SEPARATOR_CHARACTER);
						terrcdStoreIdBusnDtOrdKey.append(posOrdKey).append(SEPARATOR_CHARACTER);
						// handle duplicate orders within a store for the same business date.
						//this is a bug in the RDI system
						if(TERRCD_STOREID_BUSNDT_ORDKEY_SET.contains(terrcdStoreIdBusnDtOrdKey.toString().toUpperCase())){ 
							context.getCounter("Count","DUPLICATE_STLD_ORDERS").increment(1);
							return;
						}
						
						TERRCD_STOREID_BUSNDT_ORDKEY_SET.add(terrcdStoreIdBusnDtOrdKey.toString().toUpperCase());
						
//						if(!posOrdKey.equalsIgnoreCase("POS0001:1001435871")) 
//							return;
//						
						
						posTrnStrtTs = formatAsTs(eleOrder.getAttribute("Timestamp"));

						getDaypart();
						
						itmOfferAppdFoundFl = false;
						
						posTrnTypCd = getValue(eleOrder,ORDER_KIND);//kind
						posMfySideCd = getValue(eleOrder,ORDER_SIDE);//side
						posPrdDlvrMethCd = getValue(eleOrder,ORDER_SALETYPE);//saleType
						
						posTotNetTrnAm = getValue(eleOrder,ORDER_TOTALAMOUNT);//totalAmount
						
						totalAmtbg = new BigDecimal(posTotNetTrnAm);
								
						posTotNprdNetTrnAm = getValue(eleOrder,ORDER_NONPRODUCTAMOUNT);//nonProductAmount
						posTotNprdNetTrnAmbg = new BigDecimal(posTotNprdNetTrnAm);
						
						posTotTaxAm = getValue(eleOrder,ORDER_TOTALTAX);//totalTax
						totalTaxbg  = new BigDecimal(posTotTaxAm);
						
						
						posTotNprdTaxAm = getValue(eleOrder,ORDER_NONPRODUCTTAX);//nonProductTax
						posTotNprdTaxAmbg = new BigDecimal(posTotNprdTaxAm);
						
						posOrdUniqId = getValue(eleOrder,ORDER_UNIQUEID);//uniqueId
						posOrdStrtDt = eleOrder.getAttribute(ORDER_STARTSALEDATE);//startSaleDate
						posOrdStrtTm = eleOrder.getAttribute(ORDER_START_SALETIME);//startSaleTime
						posOrdEndDt = eleOrder.getAttribute(ORDER_ENDSALEDATE);//endSaleDate
						posOrdEndTm = eleOrder.getAttribute(ORDER_ENDSALETIME);//endSaleTime

					

//						context.getCounter("COUNT","POS_TRN_ITM_UNIQUE").increment(1);

						
						orderInfo.setLength(0);
						orderInfo.append(posBusnDt).append(SEPARATOR_CHARACTER);
						orderInfo.append(posOrdKey).append(SEPARATOR_CHARACTER);//@TODO verify
						orderInfo.append(posRestId).append(SEPARATOR_CHARACTER);
						orderInfo.append(mcdGbalLcatIdNu).append(SEPARATOR_CHARACTER);
						orderInfo.append(terrCd).append(SEPARATOR_CHARACTER);
						orderInfo.append(eventType).append(SEPARATOR_CHARACTER);
						orderInfo.append(posAreaTypShrtDs).append(SEPARATOR_CHARACTER);
						orderInfo.append(posTrnStrtTs).append(SEPARATOR_CHARACTER);
						orderInfo.append(posOrdUniqId).append(SEPARATOR_CHARACTER);
						orderInfo.append(posTrnTypCd).append(SEPARATOR_CHARACTER);
						orderInfo.append(posDvceId).append(SEPARATOR_CHARACTER);
						orderInfo.append(posMfySideCd).append(SEPARATOR_CHARACTER);
						orderInfo.append(posPrdDlvrMethCd).append(SEPARATOR_CHARACTER);
						
						if(menuPriceBasis.equalsIgnoreCase("N")){
							orderInfo.append(posTotNetTrnAm).append(SEPARATOR_CHARACTER);//POS_TOT_NET_TRN_AM
						}else{
							orderInfo.append(totalAmtbg.subtract(totalTaxbg)).append(SEPARATOR_CHARACTER);////POS_TOT_NET_TRN_AM
						}
						
						orderInfo.append(totalAmtbg.add(totalTaxbg)).append(SEPARATOR_CHARACTER);//POS_TOT_GRSS_TRN_AM
						
						if(menuPriceBasis.equalsIgnoreCase("N")){
							orderInfo.append(posTotNprdNetTrnAm).append(SEPARATOR_CHARACTER);//POS_TOT_NPRD_NET_TRN_AM
						}else{
							orderInfo.append(posTotNprdNetTrnAmbg.subtract(posTotNprdTaxAmbg)).append(SEPARATOR_CHARACTER);//POS_TOT_NPRD_NET_TRN_AM
						}
						
//						if(menuPriceBasis.equalsIgnoreCase("N")){
//							orderInfo.append(totalAmtbg.add(totalTaxbg)).append(SEPARATOR_CHARACTER);//POS_TOT_GRSS_TRN_AM
//						}else{
//							orderInfo.append(totalAmtbg).append(SEPARATOR_CHARACTER);//POS_TOT_GRSS_TRN_AM
//						}
						if(menuPriceBasis.equalsIgnoreCase("N")){
							orderInfo.append(posTotNprdNetTrnAmbg.add(posTotNprdTaxAmbg)).append(SEPARATOR_CHARACTER);//POS_TOT_NPRD_GRSS_TRN_AM
						}else{
							orderInfo.append(posTotNprdNetTrnAm).append(SEPARATOR_CHARACTER);//POS_TOT_NPRD_GRSS_TRN_AM
						}
						orderInfo.append(posTotTaxAm).append(SEPARATOR_CHARACTER);
						orderInfo.append(posTotNprdTaxAm).append(SEPARATOR_CHARACTER);
						orderInfo.append(formatAsTs(posOrdStrtDt+" "+posOrdStrtTm)).append(SEPARATOR_CHARACTER);
//						orderInfo.append(posOrdStrtTm).append(SEPARATOR_CHARACTER);
						orderInfo.append(formatAsTs(posOrdEndDt+" "+posOrdEndTm));
//						orderInfo.append(posOrdEndTm);

						
						allInfo.setLength(0);
						
//						System.out.println ( "orderInfo :" + orderInfo.toString());
						
						processOrderItems(eleOrder.getChildNodes(),context);
						processItem(eleOrder.getChildNodes(),context);
						
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDNewDataHubMapperHCat.processOrder:");
			ex.printStackTrace(System.err);
			throw ex;
//			System.exit(8);
		}

	}
	
	String[] cashlessDataArr;
	Element eleOrderItems;
	private void processOrderItems(NodeList nlOrderItems
			                      ,Context context) {

		eleOrderItems = null;
		
		posTotItmQt = StringUtils.EMPTY;
		posPaidForOrdTs = StringUtils.EMPTY;
		posTotKeyPrssTs = StringUtils.EMPTY;
		posOrdStrInSysTs = StringUtils.EMPTY;
		offrCustId = StringUtils.EMPTY;
		ordOffrApplFl = "0";
		
		String offrOverride;
		String offrApplied;
		String tmpOffrCustId = StringUtils.EMPTY;
		String promoId = StringUtils.EMPTY;
		
		orderOfferFoundF1 = false;
		orderCustomerFoundF1 = false;
		itmPromoAppdFoundFlg = "0";
		
		orderAppliedFl = "FALSE";
		orderOverrideF1 = "FALSE";
		orderPromotionId = "";
		offersPromotionId = null;
		
		
		promoIdMap.clear();
		
		try {
			

			orderCustomerInfo.setLength(0);
			orderCustomerInfo.append(StringUtils.EMPTY);
			
			orderPOSTimingsInfo.setLength(0);
			orderPOSTimingsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//itemsCount
			orderPOSTimingsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//until pay
			orderPOSTimingsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//until total
			orderPOSTimingsInfo.append(StringUtils.EMPTY);//until store
			
			orderCustomInfoInfo.setLength(0);
			
			orderCustomInfoInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderCustomInfoInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderCustomInfoInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderCustomInfoInfo.append(StringUtils.EMPTY);
			
			orderOfferInfo.setLength(0);
			orderOfferInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderOfferInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderOfferInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderOfferInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderOfferInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderOfferInfo.append(StringUtils.EMPTY);
			
			orderPromotionsInfo.setLength(0);
			orderPromotionsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderPromotionsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderPromotionsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderPromotionsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderPromotionsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderPromotionsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderPromotionsInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
			orderPromotionsInfo.append(StringUtils.EMPTY);
			
			tenderInfo.setLength(0);
		    tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//TenderId
            tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//TenderKind
            tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//TenderName
            tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//TenderQuantity
//            tenderInfo.append("TI").append(SEPARATOR_CHARACTER);//FaceValue
            tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//TenderAmount
            tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//cardproviderid
            tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//cashlessdata
            tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHL_CARD_NU
        	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHL_ATHZ_CD
        	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHC_PYMT_AM
        	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHL_TOKN_CD
        	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
        	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
    
            orderReductionInfo.setLength(0);
//            orderReductionInfo.append("RI").append(SEPARATOR_CHARACTER);
            orderReductionInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
            orderReductionInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
            orderReductionInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
            orderReductionInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
            orderReductionInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
            orderReductionInfo.append(StringUtils.EMPTY);
			
			
	           
			for(int i=1;i<=5;i++){   
		           tendersInfoMap.put(i, tenderInfo.toString());
		            
			}
			
			
			
			if (nlOrderItems != null && nlOrderItems.getLength() > 0 ) {
				for (int idxOrderItems=0; idxOrderItems < nlOrderItems.getLength(); idxOrderItems++ ) {
					if ( nlOrderItems.item(idxOrderItems).getNodeType() == Node.ELEMENT_NODE ) {
						eleOrderItems = (Element)nlOrderItems.item(idxOrderItems);
					
						
						if ( eleOrderItems.getNodeName().equals("Customer") ) {
							
							orderCustomerFoundF1 = true;
							offrCustId = getValue(eleOrderItems,"id");
							
							orderCustomerInfo.setLength(0);
							orderCustomerInfo.append(offrCustId);
						}else
						if ( eleOrderItems.getNodeName().equals("POSTimings") ) {
							posTotItmQt = getValue(eleOrderItems,ORDER_ITEM_POSTIMINGS_ITEMSCOUNT);//itemsCount
							posPaidForOrdTs = formatAsTs(eleOrderItems.getAttribute(ORDER_ITEM_POSTIMINGS_UNTILPAY));//untilPay
							posTotKeyPrssTs = formatAsTs(eleOrderItems.getAttribute(ORDER_ITEM_POSTIMINGS_UNTILTOTAL));//untilTotal
							posOrdStrInSysTs = formatAsTs(eleOrderItems.getAttribute(ORDER_ITEM_POSTIMINGS_UNTILSTORE));//untilStore
							
							
							orderPOSTimingsInfo.setLength(0);
							orderPOSTimingsInfo.append(posTotItmQt).append(SEPARATOR_CHARACTER);//itemsCount
							orderPOSTimingsInfo.append(posPaidForOrdTs).append(SEPARATOR_CHARACTER);//until pay
							orderPOSTimingsInfo.append(posTotKeyPrssTs).append(SEPARATOR_CHARACTER);//until total
							orderPOSTimingsInfo.append(posOrdStrInSysTs);//until store
							
						}
						
						
						else if ( eleOrderItems.getNodeName().equals("CustomInfo") ) {
							
							orderCustomInfoInfo.setLength(0);
							
							orderCustomInfoInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_CUSTOMINFO_ORDERID)).append(SEPARATOR_CHARACTER);//orderId
							orderCustomInfoInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_CUSTOMINFO_ORDERID_CUSTOMERID)).append(SEPARATOR_CHARACTER);//customerId
							orderCustomInfoInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_CUSTOMINFO_ORDERID_ISPAIDMOBILEORDER)).append(SEPARATOR_CHARACTER);//IsPaidMobileOrder
							orderCustomInfoInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_CUSTOMINFO_ORDERID_CHECKINDATA));//checkInData
							
							
						}
						
						else if ( eleOrderItems.getNodeName().equals("Offers") ) {
							
							orderOfferFoundF1 = true;
							ordOffrApplFl = "1";
							
							
							orderAppliedFl = eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_APPLIED);//applied
							orderOverrideF1 = eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_OVERRIDE);//override
							
							
							orderOfferInfo.setLength(0);
							orderOfferInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_TAGID)).append(SEPARATOR_CHARACTER);//tagId
							orderOfferInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_OFFERID)).append(SEPARATOR_CHARACTER);//offerId
							orderOfferInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_OVERRIDE)).append(SEPARATOR_CHARACTER);//override
							orderOfferInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_APPLIED)).append(SEPARATOR_CHARACTER);//applied
							orderOfferInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_CLEARAFTEROVERRIDE)).append(SEPARATOR_CHARACTER);//clearAfterOverride
							orderOfferInfo.append(eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_PROMOTIONID));//promotionId
							
							offersPromotionId = eleOrderItems.getAttribute(ORDER_ITEM_OFFERS_PROMOTIONID);
							
							
							
							promoId = getValue(eleOrderItems,"promotionId");
							if ( promoId.length() > 0 ) {
								if ( promoIdMap.containsKey(promoId) ) {
									promoIdMap.put(promoId, promoIdMap.get(promoId) + 1);
								} else {
									promoIdMap.put(promoId, 1);
								}
							}
						}else if(eleOrderItems.getNodeName().equals("Promotions") ){
							
							Element promotion = (Element)(eleOrderItems.getElementsByTagName("Promotion").item(0));
							
							orderPromotionId = promotion.getAttribute(ORDER_ITEM_PROMOTIONS_PROMOTIONID);//promotionId
//							if(StringUtils.isBlank(orderPromotionId))
//								orderPromotionId = "0";
							
							orderPromotionsInfo.setLength(0);
							orderPromotionsInfo.append(promotion.getAttribute(orderPromotionId)).append(SEPARATOR_CHARACTER);//promotionId
							orderPromotionsInfo.append(promotion.getAttribute(ORDER_ITEM_PROMOTIONS_PROMOTIONCOUNTER)).append(SEPARATOR_CHARACTER);//promotionCounter
							orderPromotionsInfo.append(promotion.getAttribute(ORDER_ITEM_PROMOTIONS_DISCOUNTTYPE)).append(SEPARATOR_CHARACTER);//discountType
							orderPromotionsInfo.append(promotion.getAttribute(ORDER_ITEM_PROMOTIONS_DISCOUNTAMOUNT)).append(SEPARATOR_CHARACTER);//discountAmount
							orderPromotionsInfo.append(promotion.getAttribute(ORDER_ITEM_PROMOTIONS_OFFERID)).append(SEPARATOR_CHARACTER);//offerId
							orderPromotionsInfo.append(promotion.getAttribute(ORDER_ITEM_PROMOTIONS_EXCLUSIVE)).append(SEPARATOR_CHARACTER);//exclusive
							orderPromotionsInfo.append(promotion.getAttribute(ORDER_ITEM_PROMOTIONS_PROMOTIONONTENDER)).append(SEPARATOR_CHARACTER);//promotionOnTender
							orderPromotionsInfo.append(promotion.getAttribute(ORDER_ITEM_PROMOTIONS_RETURNEDVALUE));//returnedValue
							
							
						}else if ( eleOrderItems.getNodeName().equals("Reduction") ) {
							orderReductionInfo.setLength(0);
							
							 NodeList nlReductions = eleOrderItems.getChildNodes();
							 Element eleTender;
							
							 Node textNode;
							 String textNodeName;
							 Element eleReduction;
							
							 
							 if ( nlReductions != null && nlReductions.getLength() > 0 ) {
					              for (int idxReductions = 0; idxReductions < nlReductions.getLength(); idxReductions++ ) {
					                if ( nlReductions.item(idxReductions).getNodeType() == Node.ELEMENT_NODE ) {
					                	eleReduction = (Element)nlReductions.item(idxReductions);

					                  textNodeName  = eleReduction.getNodeName();
					                  textNode 		= eleReduction.getFirstChild();
					 
					                  String qty = StringUtils.EMPTY;
					                  String afterTotal = StringUtils.EMPTY;
					                  String beforeTotal = StringUtils.EMPTY;
					                  String amount =StringUtils.EMPTY;
					                  String amountAfterTotal = StringUtils.EMPTY;
					                  String amountBeforeTotal = StringUtils.EMPTY;
					                  
					                  if ( textNodeName.equalsIgnoreCase("Qty") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
					                	  qty = textNode.getNodeValue();
						               }else if ( textNodeName.equalsIgnoreCase("AfterTotal") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
						            	   afterTotal = textNode.getNodeValue();
							           }else if ( textNodeName.equalsIgnoreCase("BeforeTotal") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
							        	   beforeTotal = textNode.getNodeValue();
							           }else if ( textNodeName.equalsIgnoreCase("Amount") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
							        	   amount = textNode.getNodeValue();
							           }else if ( textNodeName.equalsIgnoreCase("AmountAfterTotal") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
							        	   amountAfterTotal = textNode.getNodeValue();
							           }else if ( textNodeName.equalsIgnoreCase("AmountBeforeTotal") && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {
							        	   amountBeforeTotal = textNode.getNodeValue();
							           }
					               
					                  orderReductionInfo.setLength(0);
//						              orderReductionInfo.append("1").append(SEPARATOR_CHARACTER);
						              orderReductionInfo.append(qty).append(SEPARATOR_CHARACTER);
						              orderReductionInfo.append(afterTotal).append(SEPARATOR_CHARACTER);
						              orderReductionInfo.append(beforeTotal).append(SEPARATOR_CHARACTER);
						              orderReductionInfo.append(amount).append(SEPARATOR_CHARACTER);
						              orderReductionInfo.append(amountAfterTotal).append(SEPARATOR_CHARACTER);
						              orderReductionInfo.append(amountBeforeTotal);
						              
						              break;
					                
					                }
					             
					              
					                 
					              }
					              
					              
					              
							 }
							
						}
						
						else if(eleOrderItems.getNodeName().equals("Tenders") ){
							
							 NodeList nlTenders = eleOrderItems.getChildNodes();
							 Element eleTender;
							 NodeList nlTenderSubItems;
							 Node textNode;
							 String textNodeName;
							 Element eleTenderSubItem;
							 
							 int numtenders = 0;
							 
						      if ( nlTenders !=null && nlTenders.getLength() > 0 ) {
						        for (int idxTenders=0; idxTenders < nlTenders.getLength(); idxTenders++ ) {
						          if ( nlTenders.item(idxTenders).getNodeType() == Node.ELEMENT_NODE ) {
						        	  
						        	  numtenders ++;
						        	  
						            eleTender = (Element)nlTenders.item(idxTenders);

						            nlTenderSubItems = eleTender.getChildNodes();
						            String tenderId  = StringUtils.EMPTY;
						            String tenderKind = "@@@@";
						            String tenderName = "@@@@";
						            String tenderAmount = "0.00";
						            String tenderCardProvider = "@@@@";
						            String tenderPaymentName =StringUtils.EMPTY;
						            String tenderQuantity = StringUtils.EMPTY;
						            String faceValue = StringUtils.EMPTY;
						            String cashlessData = StringUtils.EMPTY;
						                   
						            tenderInfo.setLength(0);
						            int numberTenders = 0;
						            
						            if ( nlTenderSubItems != null && nlTenderSubItems.getLength() > 0 ) {
						              for (int idxTenderSubItems = 0; idxTenderSubItems < nlTenderSubItems.getLength(); idxTenderSubItems++ ) {
						                if ( nlTenderSubItems.item(idxTenderSubItems).getNodeType() == Node.ELEMENT_NODE ) {
						                  eleTenderSubItem = (Element)nlTenderSubItems.item(idxTenderSubItems);

						                  textNodeName = eleTenderSubItem.getNodeName();
						                  textNode = eleTenderSubItem.getFirstChild();
						 
						                  if ( textNodeName.equalsIgnoreCase(ORDER_TENDERS_TENDERID) && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {//TenderId
						                	  tenderId = textNode.getNodeValue();
							                  }
						                  
						                  else if ( textNodeName.equalsIgnoreCase(ORDER_TENDERS_TENDERKIND) && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {//TenderKind
						                    tenderKind = textNode.getNodeValue();
						                  }

						                  else if ( textNodeName.equalsIgnoreCase(ORDER_TENDERS_TENDERNAME) && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {//TenderName
						                    tenderName = textNode.getNodeValue();
						                  }

						                  else if ( textNodeName.equalsIgnoreCase(ORDER_TENDERS_TENDERAMOUNT) && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {//TenderAmount
						                    tenderAmount = textNode.getNodeValue();
						                  }
						                  
						                  else if ( textNodeName.equalsIgnoreCase(ORDER_TENDERS_CARDPROVIDERID) && textNode != null && textNode.getNodeType() == Node.TEXT_NODE && 
						                		  textNode.getNodeValue() != null && !textNode.getNodeValue().trim().isEmpty()) {//CardProviderID
						                    tenderCardProvider = textNode.getNodeValue();
						                  }
						                  
						                  else if ( textNodeName.equalsIgnoreCase(ORDER_TENDERS_TENDERQUANTITY) && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {//TenderQuantity
						                	  tenderQuantity = textNode.getNodeValue();
							                  }
						                  else if ( textNodeName.equalsIgnoreCase(ORDER_TENDERS_FACEVALUE) && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {//FaceValue
						                	  faceValue = textNode.getNodeValue();
							                  }
						                  else if ( textNodeName.equalsIgnoreCase(ORDER_TENDERS_CASHLESSDATA) && textNode != null && textNode.getNodeType() == Node.TEXT_NODE ) {//CashlessData
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

						               
						              }
						            }
						            
						           
						            tenderInfo.setLength(0);
						            
						            tenderInfo.append(tenderId).append(SEPARATOR_CHARACTER);//TenderId
						            tenderInfo.append(tenderKind).append(SEPARATOR_CHARACTER);//TenderKind
						            tenderInfo.append(tenderPaymentName).append(SEPARATOR_CHARACTER);//TenderName
						            tenderInfo.append(tenderQuantity).append(SEPARATOR_CHARACTER);//TenderQuantity
//						            tenderInfo.append(faceValue).append(SEPARATOR_CHARACTER);//FaceValue
						            tenderInfo.append(tenderAmount).append(SEPARATOR_CHARACTER);//TenderAmount
						            tenderInfo.append(tenderCardProvider).append(SEPARATOR_CHARACTER);//CardProviderID
						            tenderInfo.append(cashlessData).append(SEPARATOR_CHARACTER);//1_POS_CSHL_DATA_TX
						            if(StringUtils.isNotBlank(cashlessData) ){
						            	
						            	
						            	cashlessDataArr = cashlessData.split("#",-1)[0].split("\\|");
						            	if(cashlessDataArr.length < 10 )
						            		System.out.println(" cashlessData -----  "+  posOrdKey + " --- " + posRestId + " XXXX"+cashlessData + " ------");
						            	
						            	tenderInfo.append(cashlessDataArr[1]).append(SEPARATOR_CHARACTER);//1_POS_CSHL_CARD_NU
						            	tenderInfo.append(cashlessDataArr[3]).append(SEPARATOR_CHARACTER);//1_POS_CSHL_ATHZ_CD
						            	if(cashlessDataArr.length > 10)
						            		tenderInfo.append(cashlessDataArr[10]).append(SEPARATOR_CHARACTER);//1_POS_CSHC_PYMT_AM
						            	else
						            		tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHC_PYMT_AM
						            	if(cashlessDataArr.length >= 10)
						            		tenderInfo.append(cashlessDataArr[cashlessDataArr.length-1]).append(SEPARATOR_CHARACTER);//1_POS_CSHL_TOKN_CD
						            	else
						            		tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHL_TOKN_CD
//						            	System.out.println(" cashlessDataArr -----  "+ cashlessDataArr[2] + "XXXX"+cashlessData + " ------");
						            	if(StringUtils.isNotBlank(cashlessDataArr[2])){
						            		tenderInfo.append(cashlessDataArr[2].substring(0, 2)).append(SEPARATOR_CHARACTER);//month
						            		tenderInfo.append(cashlessDataArr[2].substring(3)).append(SEPARATOR_CHARACTER);//year
						            	}else{
						            		tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//month
						            		tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//year
						            	
						            	}
						            }else{
						            	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHL_CARD_NU
						            	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHL_ATHZ_CD
						            	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHC_PYMT_AM
						            	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//1_POS_CSHL_TOKN_CD
						            	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//month
						            	tenderInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//year
						            }
						            
						            tendersInfoMap.put(numtenders, tenderInfo.toString());
						            
						            numberTenders++;
						          }//tenders
						        }
						      }
						      
						}
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDNewDataHubMapperHCat.processOrderItems:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	BigDecimal posItmActUntPrcAmbg;
	BigDecimal posItmActTaxAmbg;
	BigDecimal posItmTotQtbg;
	Element itemTaxChain;
	private BigDecimal posItmNetUntPrcB4PrmoAmbg;
	BigDecimal itemPromotionAppliedOriginalPricebg;
	BigDecimal POS_ITM_NET_PRMO_AMbg;
	BigDecimal POS_ITM_GRSS_PRMO_AMbg;
	BigDecimal POS_ITM_NET_PRC_DISC_AMbg;
	BigDecimal POS_ITM_GRSS_PRC_DISC_AMbg;
	String[] itemPromotionAppliedInfoArr;
	BigDecimal posItmNetUntPrcB4DiscAmbg;

	private void processItem(NodeList nlItem
                            ,Context context) {
		
		Element eleItem;
		@SuppressWarnings("unused")
		int qty;
		posItmActUntPrcAmbg = null;
		posItmActTaxAmbg = null;
		posItmTotQtbg = null;
		String itemTaxRate = StringUtils.EMPTY;
		BigDecimal itemTaxRatebg = null;
		posItmNetUntPrcB4PrmoAmbg = null;
		POS_ITM_NET_PRMO_AMbg = null;
		itemPromotionAppliedInfoArr = null;
		posItmNetUntPrcB4DiscAmbg = null;
		POS_ITM_NET_PRC_DISC_AMbg = null;
		itemPromotionAppliedOriginalPricebg = null;
		POS_ITM_GRSS_PRMO_AMbg = null;
		
		
		
		try {
			if (nlItem != null && nlItem.getLength() > 0 ) {
				for (int idxItem=0; idxItem < nlItem.getLength(); idxItem++ ) {
					if ( nlItem.item(idxItem).getNodeType() == Node.ELEMENT_NODE ) {
						eleItem = (Element)nlItem.item(idxItem);
						if ( eleItem.getNodeName().equals("Item") ) {
							
							itemTaxChain = (Element)eleItem.getElementsByTagName("TaxChain").item(0);
							if(itemTaxChain != null){
								itemTaxRate = ""+Double.parseDouble(itemTaxChain.getAttribute("rate"))/100;
								itemTaxRatebg = new BigDecimal(itemTaxChain.getAttribute("rate")).divide(new BigDecimal(100),2,RoundingMode.HALF_EVEN);
							}

							posTrnItmSeqNu = getValue(eleItem,ORDER_ITEM_ID);//id
							posItmLvlNu = getValue(eleItem,ORDER_ITEM_LEVEL);//level
							sldMenuItmId = getValue(eleItem,ORDER_ITEM_CODE);//code
							posPrdTypCd = getValue(eleItem,ORDER_ITEM_TYPE);//type
							posItmActnCd = getValue(eleItem,ORDER_ITEM_ACTION);//action
							posItmDesc = getValue(eleItem,ORDER_ITEM_DESCRIPTION);//description
							posItmTotQt = getValue(eleItem,ORDER_ITEM_QTY);//qty
							posItmTotQtbg = new BigDecimal(posItmTotQt);//posItmTotQt
							
							posItmGrllQt = getValue(eleItem,ORDER_ITEM_GRILLQTY);//grillQty
							posItmGrllModCd = getValue(eleItem,ORDER_ITEM_GRILLMODIFER);//grillModifer
							posItmPrmoQt = getValue(eleItem,ORDER_ITEM_QTYPROMO);//qtyPromo
							posChgAftTotCd = getValue(eleItem,ORDER_ITEM_CHGAFTERTOTAL);//chgAfterTotal
							posItmNetUntPrcB4PrmoAm = getValue(eleItem,ORDER_ITEM_BPPRICE);//BPPrice
							posItmNetUntPrcB4PrmoAmbg = new BigDecimal(posItmNetUntPrcB4PrmoAm);
							posItmTaxB4PrmoAm = getValue(eleItem,ORDER_ITEM_BPTAX);//BPTax
							posItmNetUntPrcB4DiscAm = getValue(eleItem,ORDER_ITEM_BDPRICE);//BDPrice
							posItmNetUntPrcB4DiscAmbg = new BigDecimal(posItmNetUntPrcB4DiscAm);
							
							posItmTaxB4DiscAm = getValue(eleItem,ORDER_ITEM_BDTAX);//BDTax
							posItmActUntPrcAm = getValue(eleItem,ORDER_ITEM_TOTALPRICE);//totalPrice
							
							posItmActUntPrcAmbg = new BigDecimal(posItmActUntPrcAm);
							
							posItmActTaxAm = getValue(eleItem,ORDER_ITEM_TOTALTAX);//totalTax
							if(StringUtils.isBlank(posItmActTaxAm))
								posItmActTaxAm = "0";
							posItmActTaxAmbg = new BigDecimal(posItmActTaxAm);
							
							posItmCatCd = getValue(eleItem,ORDER_ITEM_CATEGORY);//category
							posItmFmlyGrpCd = getValue(eleItem,ORDER_ITEM_FAMILYGROUP);//familyGroup
							posItmVoidQt = getValue(eleItem,ORDER_ITEM_QTYVOIDED);//qtyVoided
							posItmTaxRateAm = getValue(eleItem,ORDER_ITEM_UNITPRICE);//unitPrice
							posItmTaxBasAm = getValue(eleItem,ORDER_ITEM_UNITTAX);//unitTax
							itmOffrAppdFl = "0";

							try {
								qty = Integer.parseInt(posItmTotQt);
							} catch (Exception ex) {
								posItmTotQt = "0";
							}

							try {
								qty = Integer.parseInt(posItmGrllQt);
							} catch (Exception ex) {
								posItmGrllQt = "0";
							}

							try {
								qty = Integer.parseInt(posItmPrmoQt);
							} catch (Exception ex) {
								posItmPrmoQt = "0";
							}

							
							itemInfo.setLength(0);
							itemInfo.append(sldMenuItmId).append(SEPARATOR_CHARACTER);//code
							itemInfo.append(posPrdTypCd).append(SEPARATOR_CHARACTER);//type
							itemInfo.append(posItmActnCd).append(SEPARATOR_CHARACTER);//action
							itemInfo.append(posItmDesc).append(SEPARATOR_CHARACTER);//item description
							itemInfo.append(posItmLvlNu).append(SEPARATOR_CHARACTER);//level
							itemInfo.append(posTrnItmSeqNu).append(SEPARATOR_CHARACTER);//id
							itemInfo.append(""+itemSequenceWithinOrder).append(SEPARATOR_CHARACTER);
							itemSequenceWithinOrder++;
							
							itemInfo.append(posItmTotQt).append(SEPARATOR_CHARACTER);//qty
							itemInfo.append(posItmGrllQt).append(SEPARATOR_CHARACTER);//grillQty
							itemInfo.append(posItmGrllModCd).append(SEPARATOR_CHARACTER);//grillModifier
							itemInfo.append(posItmPrmoQt).append(SEPARATOR_CHARACTER);//qtyPromo
							itemInfo.append(posChgAftTotCd).append(SEPARATOR_CHARACTER);//chgAfterTotal
							itemInfo.append(posItmNetUntPrcB4PrmoAm).append(SEPARATOR_CHARACTER);//bpPrice
							
							itemInfo.append(posItmTaxB4PrmoAm).append(SEPARATOR_CHARACTER);//bpTax
							itemInfo.append(posItmNetUntPrcB4DiscAm).append(SEPARATOR_CHARACTER);//bdPrice
							itemInfo.append(posItmTaxB4DiscAm).append(SEPARATOR_CHARACTER);//bdTax
							itemInfo.append(posItmTaxRateAm).append(SEPARATOR_CHARACTER);//unitPrice
							itemInfo.append(posItmTaxBasAm).append(SEPARATOR_CHARACTER);//unitTax
							
//							POS_ITM_GRSS_TOT_PRC_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								itemInfo.append(posItmActUntPrcAmbg.add(posItmActTaxAmbg)).append(SEPARATOR_CHARACTER);
							}else{
								itemInfo.append(posItmActUntPrcAmbg).append(SEPARATOR_CHARACTER);
							}
							
//							POS_ITM_NET_TOT_PRC_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								itemInfo.append(posItmActUntPrcAmbg).append(SEPARATOR_CHARACTER);
							}else{
								itemInfo.append(posItmActUntPrcAmbg.subtract(posItmActTaxAmbg)).append(SEPARATOR_CHARACTER);
							}
							
							
//							POS_ITM_TOT_TAX_AM
							itemInfo.append(posItmActTaxAm).append(SEPARATOR_CHARACTER);

							
							//POS_ITM_NET_UNT_PRC_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								if( (posItmActUntPrcAmbg.compareTo(BigDecimal.ZERO ) > 0) && (posItmTotQtbg.compareTo(BigDecimal.ZERO) > 0))
									itemInfo.append(posItmActUntPrcAmbg.divide(posItmTotQtbg,RoundingMode.HALF_EVEN).setScale(2)).append(SEPARATOR_CHARACTER);
								else
									itemInfo.append("0").append(SEPARATOR_CHARACTER);
							}else{
								if( ( (posItmActUntPrcAmbg.subtract(posItmActTaxAmbg)).compareTo(BigDecimal.ZERO) > 0) && 
										(posItmTotQtbg.compareTo(BigDecimal.ZERO) > 0 )){
									itemInfo.append( (posItmActUntPrcAmbg.subtract(posItmActTaxAmbg) ).divide(posItmTotQtbg,RoundingMode.HALF_EVEN).setScale(2)).append(SEPARATOR_CHARACTER);
								}else{
									itemInfo.append("0").append(SEPARATOR_CHARACTER);
								}
							}
							
							//POS_ITM_GRSS_UNT_PRC_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								if(posItmActUntPrcAmbg.add(posItmActTaxAmbg).compareTo( BigDecimal.ZERO) > 0 &&
										(posItmTotQtbg.compareTo(BigDecimal.ZERO) > 0)){
									itemInfo.append( (posItmActUntPrcAmbg.add(posItmActTaxAmbg) ).divide(posItmTotQtbg,RoundingMode.HALF_EVEN).setScale(2)).append(SEPARATOR_CHARACTER);
								}else{
									itemInfo.append("0").append(SEPARATOR_CHARACTER);
								}
							}else{
								if( (posItmActUntPrcAmbg.compareTo(BigDecimal.ZERO) > 0) &&
										(posItmTotQtbg.compareTo(BigDecimal.ZERO) > 0))
									itemInfo.append(posItmActUntPrcAmbg.divide(posItmTotQtbg,RoundingMode.HALF_EVEN).setScale(2)).append(SEPARATOR_CHARACTER);
								else
									itemInfo.append("0").append(SEPARATOR_CHARACTER);
							}
							
//							itemInfo.append(posItmTotQt).append(SEPARATOR_CHARACTER);//POS_ITM_TOT_QT @tocheck
							
							//POS_ITEM_NET_TOT_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								itemInfo.append(posItmActUntPrcAm).append(SEPARATOR_CHARACTER);
							}else{
								itemInfo.append(posItmActUntPrcAmbg.subtract(posItmActTaxAmbg)).append(SEPARATOR_CHARACTER);
							}
							
							//POS_ITEM_GRSS_TOT_AM@tocheck
							if(menuPriceBasis.equalsIgnoreCase("N")){
								itemInfo.append(posItmActUntPrcAmbg.add(posItmActTaxAmbg)).append(SEPARATOR_CHARACTER);
							}else{
								itemInfo.append(posItmActUntPrcAm).append(SEPARATOR_CHARACTER);
							}
							
//							itemInfo.append(posItmActUntPrcAm).append(SEPARATOR_CHARACTER);//totalPrice
							//POS_ITM_TOT_TAX_AM
//							itemInfo.append(posItmActTaxAm).append(SEPARATOR_CHARACTER);//totalTax@tocheck repeated
							
							//POS_ITM_TAX_PC
							itemInfo.append(itemTaxRate).append(SEPARATOR_CHARACTER);//itemTaxRate 
							
							//POS_ITM_NET_PRMO_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								if(posItmPrmoQt != null && Integer.parseInt(posItmPrmoQt) > 0){
									POS_ITM_NET_PRMO_AMbg = posItmNetUntPrcB4PrmoAmbg.subtract(posItmActUntPrcAmbg);
								}else{
									POS_ITM_NET_PRMO_AMbg = BigDecimal.ZERO; 
								}
							}else{
								POS_ITM_NET_PRMO_AMbg = BigDecimal.ZERO;//@TBD
							}
							itemInfo.append(POS_ITM_NET_PRMO_AMbg.doubleValue()).append(SEPARATOR_CHARACTER);
							

							//POS_ITM_GRSS_PRMO_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								if(posItmPrmoQt != null && Integer.parseInt(posItmPrmoQt) > 0){
									POS_ITM_GRSS_PRMO_AMbg = (posItmNetUntPrcB4PrmoAmbg.subtract(posItmActUntPrcAmbg)).multiply(itemTaxRatebg.add(new BigDecimal(1)));
								}else{
									POS_ITM_GRSS_PRMO_AMbg = BigDecimal.ZERO;
								}
								
							}else{//@TODO for grossmarkets
								POS_ITM_GRSS_PRMO_AMbg = BigDecimal.ZERO;
							}
							itemInfo.append(POS_ITM_GRSS_PRMO_AMbg.doubleValue()).append(SEPARATOR_CHARACTER);

							processItemOffers(eleItem.getChildNodes(),context);
							
							itemPromotionAppliedInfoArr = itemPromotionAppliedInfo.toString().split("\\t",-1);
							if(itemPromotionAppliedInfoArr != null && StringUtils.isNotBlank(itemPromotionAppliedInfoArr[3] ))
								itemPromotionAppliedOriginalPricebg = new BigDecimal(itemPromotionAppliedInfoArr[3]);
							else
								itemPromotionAppliedOriginalPricebg =BigDecimal.ZERO;
							
							//POS_ITM_NET_DISCOUNT_AM							
							//POS_ITM_NET_PRC_DISC_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								if(itemPromotionAppliedInfoArr != null && "PRICE".equalsIgnoreCase(itemPromotionAppliedInfoArr[5])){
									POS_ITM_NET_PRC_DISC_AMbg = itemPromotionAppliedOriginalPricebg.multiply(posItmTotQtbg).subtract(posItmActUntPrcAmbg);
									itemInfo.append(POS_ITM_NET_PRC_DISC_AMbg.doubleValue()).append(SEPARATOR_CHARACTER);
								}else{
									POS_ITM_NET_PRC_DISC_AMbg =posItmNetUntPrcB4DiscAmbg.subtract(posItmActUntPrcAmbg);
									itemInfo.append(POS_ITM_NET_PRC_DISC_AMbg.doubleValue()).append(SEPARATOR_CHARACTER);
								}
							}else{//for gross markets
								POS_ITM_NET_PRC_DISC_AMbg = BigDecimal.ZERO;
								itemInfo.append(POS_ITM_NET_PRC_DISC_AMbg.doubleValue()).append(SEPARATOR_CHARACTER);//@todo
							}
							
							//POS_ITM_GRSS_PRC_DISC_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								if(itemPromotionAppliedInfoArr != null && "PRICE".equalsIgnoreCase(itemPromotionAppliedInfoArr[5])){
									POS_ITM_GRSS_PRC_DISC_AMbg = itemPromotionAppliedOriginalPricebg.multiply(posItmTotQtbg).subtract(posItmActUntPrcAmbg);
									itemInfo.append(POS_ITM_GRSS_PRC_DISC_AMbg.doubleValue()).append(SEPARATOR_CHARACTER);
								}else{
									POS_ITM_GRSS_PRC_DISC_AMbg = posItmNetUntPrcB4DiscAmbg.subtract(posItmActUntPrcAmbg).multiply(itemTaxRatebg.add(new BigDecimal(1)));
									itemInfo.append(POS_ITM_GRSS_PRC_DISC_AMbg.doubleValue()).append(SEPARATOR_CHARACTER);
								}
							}else{//for gross markets
								POS_ITM_GRSS_PRC_DISC_AMbg = BigDecimal.ZERO;
								itemInfo.append(POS_ITM_GRSS_PRC_DISC_AMbg.doubleValue()).append(SEPARATOR_CHARACTER);//@todo
							}
							
							
							//POS_ITM_NET_TOT_DISC_AM
							itemInfo.append(POS_ITM_NET_PRMO_AMbg.add(POS_ITM_NET_PRC_DISC_AMbg)).append(SEPARATOR_CHARACTER);
							
							//POS_ITM_GRSS_TOT_DISC_AM
							itemInfo.append(POS_ITM_GRSS_PRMO_AMbg.add(POS_ITM_GRSS_PRC_DISC_AMbg)).append(SEPARATOR_CHARACTER);
							
							
							//POS_ITM_ORGN_NET_UNT_PRC_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								if(posItmNetUntPrcB4DiscAmbg.compareTo(BigDecimal.ZERO ) > 0){
									if("PRICE".equalsIgnoreCase(itemPromotionAppliedInfoArr[5])){
										itemInfo.append(itemPromotionAppliedOriginalPricebg.doubleValue()).append(SEPARATOR_CHARACTER);
									}else{
										itemInfo.append(posItmTaxRateAm).append(SEPARATOR_CHARACTER);
									}
								}else{
									itemInfo.append(posItmNetUntPrcB4PrmoAmbg.doubleValue()).append(SEPARATOR_CHARACTER);
								}
							}else{//for gross markets
								itemInfo.append("0").append(SEPARATOR_CHARACTER);//@todo
							}
							
							//POS_ITM_ORGN_GRSS_UNT_PRC_AM
							if(menuPriceBasis.equalsIgnoreCase("N")){
								if(posItmNetUntPrcB4DiscAmbg.compareTo(BigDecimal.ZERO ) > 0){
									if("PRICE".equalsIgnoreCase(itemPromotionAppliedInfoArr[5])){
										itemInfo.append(itemPromotionAppliedOriginalPricebg.doubleValue()).append(SEPARATOR_CHARACTER);
									}else{
										itemInfo.append(posItmTaxRateAm).append(SEPARATOR_CHARACTER);
									}
								}else{
									itemInfo.append(posItmNetUntPrcB4PrmoAmbg.multiply(itemTaxRatebg.add(new BigDecimal(1)))).append(SEPARATOR_CHARACTER);
								}
							}else{
								if("PRICE".equalsIgnoreCase(itemPromotionAppliedInfoArr[5])){
									itemInfo.append(itemPromotionAppliedOriginalPricebg.doubleValue()).append(SEPARATOR_CHARACTER);
								}else{
									itemInfo.append(posItmTaxRateAm).append(SEPARATOR_CHARACTER);
								}
							}
							
							
//							//POS_ITM_TOT_DISC_AM
//							itemInfo.append("POS_ITM_TOT_DISC_AM").append(SEPARATOR_CHARACTER);
//							//POS_ITM_GRSS_TOT_DISC_AM
//							itemInfo.append("POS_ITM_GRSS_TOT_DISC_AM").append(SEPARATOR_CHARACTER);
//							//POS_ITM_ORGN_NET_UNT_PRC_AM
//							itemInfo.append("POS_ITM_ORGN_NET_UNT_PRC_AM").append(SEPARATOR_CHARACTER);
//							//POS_ITM_ORGN_GRSS_UNT_PRC_AM
//							itemInfo.append("POS_ITM_ORGN_GRSS_UNT_PRC_AM").append(SEPARATOR_CHARACTER);
							
							itemInfo.append(posItmCatCd).append(SEPARATOR_CHARACTER);//category
							itemInfo.append(posItmFmlyGrpCd).append(SEPARATOR_CHARACTER);//familyGroup
							itemInfo.append(posItmVoidQt).append(SEPARATOR_CHARACTER);//qtyVoided
							
							itemInfo.append(posItmTaxRateAm).append(SEPARATOR_CHARACTER);//POS_ITM_TAX_RATE_AM
							itemInfo.append(posItmTaxBasAm).append(SEPARATOR_CHARACTER);//POS_ITM_TAX_BAS_AM
							
											
							
							itemInfo.append(itmPromoAppdFoundFlg);//ITM_PROMO_APPD_FL
							
//							itemInfo.append(itmOfferAppdFoundFl).append(SEPARATOR_CHARACTER);			
							
//							System.out.println ( "orderInfo in processItem :" + orderInfo.toString());
//							System.out.println ( "itemInfo in processItem :" + itemInfo.toString());
//							System.out.println ( "itemPromotionAppliedInfo in processItem :" + itemPromotionAppliedInfo.toString());
//							System.out.println ( "orderPromotionsInfo in processItem :" + orderPromotionsInfo.toString());
//							System.out.println ( "orderOfferInfo in processItem :" + orderOfferInfo.toString());
//							System.out.println ( "tenderInfo in processItem :" + tenderInfo.toString());
//							System.out.println ( "orderPOSTimingsInfo in processItem :" + orderPOSTimingsInfo.toString());
//							System.out.println ( "orderCustomInfoInfo in processItem :" + orderCustomInfoInfo.toString());
							
							allInfo.setLength(0);
							allInfo.append("1").append(SEPARATOR_CHARACTER);//split the hub. no reducer
							allInfo.append(orderInfo).append(SEPARATOR_CHARACTER);
							allInfo.append(itemInfo).append(SEPARATOR_CHARACTER);
							allInfo.append(itemPromoInfo).append(SEPARATOR_CHARACTER);
							allInfo.append(itemPromotionAppliedInfo).append(SEPARATOR_CHARACTER);
							allInfo.append(orderPromotionsInfo).append(SEPARATOR_CHARACTER);
							allInfo.append(orderOfferInfo).append(SEPARATOR_CHARACTER);
							
							for(int i=1;i<=5;i++){
								
								allInfo.append(tendersInfoMap.get(i));
							
							}
						
							
							allInfo.append(orderPOSTimingsInfo).append(SEPARATOR_CHARACTER);
							allInfo.append(orderReductionInfo).append(SEPARATOR_CHARACTER);
							allInfo.append(orderCustomerInfo).append(SEPARATOR_CHARACTER);
							
//							if(orderOfferFoundF1 || orderCustomerFoundF1 || isItemOfferFl){
//								allInfo.append("1").append(SEPARATOR_CHARACTER);
//							}else{
//								allInfo.append("0").append(SEPARATOR_CHARACTER);
//							}
							
							/**
							   Offer ID				OverrideFlag	Applied Flag	Promotion ID			Considered Redeemed ? (Y/N)
								Exists				FALSE			TRUE			Exists (Non-Zero)			Y
								Exists				TRUE			TRUE			Exists (Non-Zero)			Y
								Exists				TRUE			FALSE			0							Y
								Exists				TRUE			FALSE			Exists (Non-Zero)			Y
								Exists				FALSE			FALSE			Exists (Non-Zero)			N
								Exists				FALSE			FALSE			0							N
								Exists				Doesn’t exist	Doesn’t exist	Doesn’t Exist 				N
								Doesn’t exist		Doesn’t exist	Doesn’t exist	Doesn’t Exist 				N

							 */
							
//							if(StringUtils.isBlank(orderPromotionId))
//								orderPromotionId = "0";
							
//							System.out.println(" orderOfferFoundF1 "+orderOfferFoundF1 + " orderOverrideF1 "+ orderOverrideF1+" orderAppliedFl "+orderAppliedFl+" offersPromotionId "+ offersPromotionId);
							
							
							if(orderOfferFoundF1){//if Order Offer ID Exists
								if("FALSE".equalsIgnoreCase(orderOverrideF1)){
									if("TRUE".equalsIgnoreCase(orderAppliedFl) && StringUtils.isNotBlank(offersPromotionId) && Integer.parseInt(offersPromotionId) > 0) {
										allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
									}else{
										allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
									}
								}else if("TRUE".equalsIgnoreCase(orderOverrideF1)){
									
									if( "TRUE".equalsIgnoreCase(orderAppliedFl)){
									
										if( StringUtils.isNotBlank(offersPromotionId)  && Integer.parseInt(offersPromotionId) > 0){
									
											allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
										}else{
									
											allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
										}
									}else if("FALSE".equalsIgnoreCase(orderAppliedFl)){
									
										if( StringUtils.isNotBlank(offersPromotionId)  && Integer.parseInt(offersPromotionId) >= 0){
									
											allInfo.append("1").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
										}else{
											
											allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
										}
											
									}else{
										
										allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
									}
								}else{
									
									allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
								}
							}else{//if Order Offer ID Does Not Exist
								
								allInfo.append("0").append(SEPARATOR_CHARACTER); //ORD_DIGL_OFFR_APPD_FL
							}
							
							allInfo.append(dyptIdNu);
//							allInfo.append(SEPARATOR_CHARACTER);
//							allInfo.append(DaaSConstants.SDF_yyyyMMddHHmmssSSS.format(curDate)).append(SEPARATOR_CHARACTER);
//							allInfo.append(DaaSConstants.SDF_yyyyMMddHHmmssSSS.format(curDate));
							
//							allInfo.append(orderCustomInfoInfo);
							
							
//							allInfo.append(SEPARATOR_CHARACTER).append(getValue(eleItem, "description"));
							
							outputKey.setLength(0);						
							outputKey.append(REC_POSTRN);
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(posOrdKey);
							outputKey.append(SEPARATOR_CHARACTER);
							outputKey.append(mcdGbalLcatIdNu);
							
							mapKey.clear();
							mapKey.set(outputKey.toString());
							
							mapValue.clear();
							mapValue.set(allInfo.toString());
							
							
							context.write(mapKey,mapValue);//todo
							
							
//							String outputfileName = subfileType+"~"+terrCd+"~"+posBusnDt;
//							
//							mos.write(HDFSUtil.replaceMultiOutSpecialChars(outputfileName),NullWritable.get(),mapValue,multioutBaseOutputPath+Path.SEPARATOR+subfileType+"/terr_cd="+terrCd+"/pos_busn_dt="+posBusnDt+"/"+HDFSUtil.replaceMultiOutSpecialChars(outputfileName));
							
							
//							mos.write(NullWritable.get(),mapValue,multioutBaseOutputPath+Path.SEPARATOR+subfileType+"/terr_cd="+terrCd+"/pos_busn_dt="+posBusnDt+"/"+HDFSUtil.replaceMultiOutSpecialChars(outputfileName));
							
							
							context.getCounter("COUNT","POS_TRN_ITM").increment(1);
							processItem(eleItem.getChildNodes(),context);
							
							
							
						}
					}
				}
				
				
				
			}
			
		} catch (Exception ex) {
			System.err.println("Error occured in TLDNewDataHubMapperHCat.processItem:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}

	private void processItemOffers(NodeList nlItemOffers,
			                       Context context) {

		Element eleItemOffers;
		String promoId = StringUtils.EMPTY;
	
	    isItemOfferFl = false;
		isPromoAppledNode = false;
		itmOfferAppdFoundFl = false;
		itmPromoFoundFl = false;
		
		try {
			itemPromotionAppliedInfo.setLength(0);
			itemPromoInfo.setLength(0);
			
			if (nlItemOffers != null && nlItemOffers.getLength() > 0 ) {
				for (int idxItemOffers=0; idxItemOffers < nlItemOffers.getLength(); idxItemOffers++ ) {
					if ( nlItemOffers.item(idxItemOffers).getNodeType() == Node.ELEMENT_NODE ) {
						eleItemOffers = (Element)nlItemOffers.item(idxItemOffers);
						
						isItemOfferFl = false;
						isPromoAppledNode = false; 
						
						if(eleItemOffers.getNodeName().equalsIgnoreCase("Promo")){
							itmPromoFoundFl = true;
							itemPromoInfo.append(eleItemOffers.getAttribute("id")).append(SEPARATOR_CHARACTER);//ITEM_PROMO_ID
							itemPromoInfo.append(eleItemOffers.getAttribute("name")).append(SEPARATOR_CHARACTER);//ITEM_PROMO_NM
							itemPromoInfo.append(eleItemOffers.getAttribute("qty"));//ITEM_PROMO_QT
						}
						else if ( eleItemOffers.getNodeName().equals("PromotionApplied") ) {
							itmPromoAppdFoundFlg = "1";
							promoId = getValue(eleItemOffers,"promotionId");
							if ( promoId.length() > 0 && promoIdMap.containsKey(promoId) ) {
								isItemOfferFl = true;
								isPromoAppledNode = true;
							}
						} else if ( eleItemOffers.getNodeName().equals("Offers") ) {
								isItemOfferFl = true;
						}
							
						if ( isItemOfferFl ) { 
							
							itmOffrAppdFl = "1";
							itmOfferAppdFoundFl = true;
							
							itemPromotionAppliedInfo.append(promoId).append(SEPARATOR_CHARACTER);//promotionId
							itemPromotionAppliedInfo.append(eleItemOffers.getAttribute("promotionCounter")).append(SEPARATOR_CHARACTER);//promotionCounter
							itemPromotionAppliedInfo.append(eleItemOffers.getAttribute("eligible")).append(SEPARATOR_CHARACTER);
							itemPromotionAppliedInfo.append(eleItemOffers.getAttribute("originalPrice")).append(SEPARATOR_CHARACTER);
							itemPromotionAppliedInfo.append(eleItemOffers.getAttribute("discountAmount")).append(SEPARATOR_CHARACTER);
							itemPromotionAppliedInfo.append(eleItemOffers.getAttribute("discountType")).append(SEPARATOR_CHARACTER);
							itemPromotionAppliedInfo.append(eleItemOffers.getAttribute("originalItemPromoQty")).append(SEPARATOR_CHARACTER);
							itemPromotionAppliedInfo.append(eleItemOffers.getAttribute("originalProductCode"));
							break;//assuming only one promotionApplied for a given item
						} 
						
							
					}
				}
			}
			if(!isPromoAppledNode){
				itemPromotionAppliedInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//promotionId
				itemPromotionAppliedInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
				itemPromotionAppliedInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
				itemPromotionAppliedInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
				itemPromotionAppliedInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
				itemPromotionAppliedInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
				itemPromotionAppliedInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);
				itemPromotionAppliedInfo.append(StringUtils.EMPTY);
			}
			if(!itmPromoFoundFl){
				itemPromoInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//ITEM_PROMO_ID
				itemPromoInfo.append(StringUtils.EMPTY).append(SEPARATOR_CHARACTER);//ITEM_PROMO_NM
				itemPromoInfo.append(StringUtils.EMPTY);//ITEM_PROMO_QT
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDNewDataHubMapperHCat.processItemOffers:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void getOrderDataDetailedSos(String xmlText
                                        ,Context context) {


		eleRoot = null;

		try {
			strReader  = new StringReader(xmlText);
			xmlSource = new InputSource(strReader);
			doc = docBuilder.parse(xmlSource);

			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("DetailedSOS") ) {
				posRestId = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");

				processStoreTotals(eleRoot.getChildNodes(),context);
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDNewDataHubMapperHCat.getOrderDataDetailedSos:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}finally{
			doc = null;
			xmlSource = null;
			strReader.close();
			strReader = null;
		}
	

	}
	
	Element detailedSOSChldNode;
	Element eleServiceTime;
	Element eleProductionTime;
	private void processStoreTotals(NodeList detailedSOSNodeList
                                   ,Context context) {

		detailedSOSChldNode = null;

		if (detailedSOSNodeList != null && detailedSOSNodeList.getLength() > 0 ) {
			for (int detailedSOSNLIdx=0; detailedSOSNLIdx < detailedSOSNodeList.getLength(); detailedSOSNLIdx++ ) {
				if ( detailedSOSNodeList.item(detailedSOSNLIdx).getNodeType() == Node.ELEMENT_NODE ) {  
					detailedSOSChldNode = (Element)detailedSOSNodeList.item(detailedSOSNLIdx);
					
					  if(detailedSOSChldNode.getNodeName().equalsIgnoreCase("PresetsTable")){
			            	
			            	
			            	
			            	orderTarget = getValue(detailedSOSChldNode,"orderTarget");
			            	cashTarget  = getValue(detailedSOSChldNode,"cashTarget");
			            	storeTarget = getValue(detailedSOSChldNode,"storeTarget");
			            	lineTarget = getValue(detailedSOSChldNode,"lineTarget");
			            	holdTarget = getValue(detailedSOSChldNode,"holdTarget");
			            	presentationTarget = getValue(detailedSOSChldNode,"presentationTarget");
			            	cashpresentationTarget = getValue(detailedSOSChldNode,"cashpresentationTarget");
			            	totalTarget = getValue(detailedSOSChldNode,"totalTarget");
			            	totalTargetMFY = getValue(detailedSOSChldNode,"totalTargetMFY");
			            	totalTargetFC = getValue(detailedSOSChldNode,"totalTargetFC");
			            	totalTargetDT = getValue(detailedSOSChldNode,"totalTargetDT");
			            	underTotalTargetMFY = getValue(detailedSOSChldNode,"underTotalTargetMFY");
			            	underTotalTargetDT = getValue(detailedSOSChldNode,"underTotalTargetDT");
			            	UnderTotalTargetFC = getValue(detailedSOSChldNode,"UnderTotalTargetFC");
			            	underTotalTargetCBB = getValue(detailedSOSChldNode,"underTotalTargetCBB");
			            	overTotalTargetMFY = getValue(detailedSOSChldNode,"overTotalTargetMFY");
			            	overTotalTargetDT = getValue(detailedSOSChldNode,"overTotalTargetDT");
			            	overTotalTargetFC = getValue(detailedSOSChldNode,"overTotalTargetFC");
			            	overTotalTargetCBB = getValue(detailedSOSChldNode,"overTotalTargetCBB");
			            	underPresentationTargetMFY = getValue(detailedSOSChldNode,"underPresentationTargetMFY");
			            	overPresentationTargetDT = getValue(detailedSOSChldNode,"overPresentationTargetDT");
			            	overPresentationTargetFC = getValue(detailedSOSChldNode,"overPresentationTargetFC");
			            	overPresentationTargetCBB = getValue(detailedSOSChldNode,"overPresentationTargetCBB");
			            	overPresentationTargetMFY = getValue(detailedSOSChldNode,"overPresentationTargetMFY");
			            	overPresentationTargetDT = getValue(detailedSOSChldNode,"overPresentationTargetDT");
			            	overPresentationTargetFC = getValue(detailedSOSChldNode,"overPresentationTargetFC");
			            	overPresentationTargetCBB = getValue(detailedSOSChldNode,"overPresentationTargetCBB");
			            	
			            	
			            	additionalFieldsbf.append(terrCd).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(posRestId).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(posBusnDt).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(orderTarget).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(cashTarget).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(storeTarget).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(lineTarget).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(holdTarget).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(presentationTarget).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(cashpresentationTarget).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(totalTarget).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(totalTargetMFY).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(totalTargetFC).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(totalTargetDT).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(underTotalTargetMFY).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(underTotalTargetDT).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(UnderTotalTargetFC).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(underTotalTargetCBB).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overTotalTargetMFY).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overTotalTargetDT).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overTotalTargetFC).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overTotalTargetCBB).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(underPresentationTargetMFY).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overPresentationTargetDT).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overPresentationTargetFC).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overPresentationTargetCBB).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overPresentationTargetMFY).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overPresentationTargetDT).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overPresentationTargetFC).append(DaaSConstants.PIPE_DELIMITER);
			            	additionalFieldsbf.append(overPresentationTargetCBB);
			            	
			            }else if ( detailedSOSChldNode.getNodeName().equals("StoreTotals") && (detailedSOSChldNode.getAttribute("productionNodeId").equalsIgnoreCase("DT") || detailedSOSChldNode.getAttribute("productionNodeId").equalsIgnoreCase("FC")) ) {

			            	processServiceTime(detailedSOSChldNode.getChildNodes(),context);
			            }
					}
			}
		}

	}
	
	StringBuffer outputfileNamebf = new StringBuffer();
	
	private void processServiceTime(NodeList nlServiceTime
                                   ,Context context) {

		eleServiceTime = null;

//		untlTotKeyPrssScQt = StringUtils.EMPTY;
//		untlStrInSysScQt = StringUtils.EMPTY;
//		untlOrdRcllScQt = StringUtils.EMPTY;
//		untlDrwrClseScQt = StringUtils.EMPTY;
//		untlPaidScQt = StringUtils.EMPTY;
//		untlSrvScQt = StringUtils.EMPTY;
//		totOrdTmScQt = StringUtils.EMPTY;
//		abovPsntTmTrgtFl = StringUtils.EMPTY;
//		abovTotTmTrgtFl = StringUtils.EMPTY;
//		abovTotMfyTrgtTmTmFl = StringUtils.EMPTY;
//		abovTotFrntCterTrgtTmFl = StringUtils.EMPTY;
//		abovTotDrvTrgtTmFl = StringUtils.EMPTY;
//		abov50ScFl = StringUtils.EMPTY;
//		bel25ScFl = StringUtils.EMPTY;
//		heldTmScQt = StringUtils.EMPTY;
//		ordHeldFl = StringUtils.EMPTY;

		try {
			
			if (nlServiceTime != null && nlServiceTime.getLength() > 0 ) {
				for (int idxServiceTime=0; idxServiceTime < nlServiceTime.getLength(); idxServiceTime++ ) {
					if ( nlServiceTime.item(idxServiceTime).getNodeType() == Node.ELEMENT_NODE ) {  
						eleServiceTime = (Element)nlServiceTime.item(idxServiceTime);
						if ( eleServiceTime.getNodeName().equals("ServiceTime") ) {
							
							untlTotKeyPrssScQt = StringUtils.EMPTY;
							untlStrInSysScQt = StringUtils.EMPTY;
							untlOrdRcllScQt = StringUtils.EMPTY;
							untlDrwrClseScQt = StringUtils.EMPTY;
							untlPaidScQt = StringUtils.EMPTY;
							untlSrvScQt = StringUtils.EMPTY;
							totOrdTmScQt = StringUtils.EMPTY;
							abovPsntTmTrgtFl = StringUtils.EMPTY;
							abovTotTmTrgtFl = StringUtils.EMPTY;
							abovTotMfyTrgtTmTmFl = StringUtils.EMPTY;
							abovTotFrntCterTrgtTmFl = StringUtils.EMPTY;
							abovTotDrvTrgtTmFl = StringUtils.EMPTY;
							abov50ScFl = StringUtils.EMPTY;
							bel25ScFl = StringUtils.EMPTY;
							heldTmScQt = StringUtils.EMPTY;
							ordHeldFl = StringUtils.EMPTY;
							
							posOrdKey = eleServiceTime.getAttribute("orderKey");
							context.getCounter("Count","NumOrders2").increment(1);
							
//							if(!posOrdKey.equalsIgnoreCase("POS0001:148634574")){
//								context.getCounter("Count","SkippingOrders2").increment(1);
//								return;
//							}
////							
//							context.getCounter("Count",((FileSplit)context.getInputSplit()).getPath().getName()).increment(1);
//							
							
							terrcdStoreIdBusnDtOrdKey.setLength(0);
							terrcdStoreIdBusnDtOrdKey.append(terrCd).append(SEPARATOR_CHARACTER);
							terrcdStoreIdBusnDtOrdKey.append(posRestId).append(SEPARATOR_CHARACTER);
							terrcdStoreIdBusnDtOrdKey.append(posBusnDt).append(SEPARATOR_CHARACTER);
							terrcdStoreIdBusnDtOrdKey.append(posOrdKey).append(SEPARATOR_CHARACTER);
							// handle duplicate orders within a store for the same business date.
							//this is a bug in the RDI system
							if(TERRCD_STOREID_BUSNDT_ORDKEY_SET.contains(terrcdStoreIdBusnDtOrdKey.toString().toUpperCase())){ 
								context.getCounter("Count","DUPLICATE_SOS_ORDERS").increment(1);
								return;
							}
							
							TERRCD_STOREID_BUSNDT_ORDKEY_SET.add(terrcdStoreIdBusnDtOrdKey.toString().toUpperCase());
							
							

							untlTotKeyPrssScQt = getScQt(eleServiceTime.getAttribute(SOS_SERVICETIME_UNTILTOTAL));//untilTotal
							
//							System.out.println(" until Total  "+  eleServiceTime.getAttribute("untilTotal") + " untlTotKeyPrssScQt " + untlTotKeyPrssScQt);
							
							untlStrInSysScQt = getScQt(eleServiceTime.getAttribute(SOS_SERVICETIME_UNTILSTORE));//untilStore
							untlOrdRcllScQt = getScQt(eleServiceTime.getAttribute(SOS_SERVICETIME_UNTILRECALL));//untilRecall
							untlDrwrClseScQt = getScQt(eleServiceTime.getAttribute(SOS_SERVICETIME_UNTILCLOSEDRAWER));//untilCloseDrawer
							untlPaidScQt = getScQt(eleServiceTime.getAttribute(SOS_SERVICETIME_UNTILPAY));//untilPay
							untlSrvScQt = getScQt(eleServiceTime.getAttribute(SOS_SERVICETIME_UNTILSERVE));//untilServe
							totOrdTmScQt = getScQt(eleServiceTime.getAttribute(SOS_SERVICETIME_TOTALTIME));//totalTime
							
							abovPsntTmTrgtFl = getValue(eleServiceTime,SOS_SERVICETIME_TCOVERPRESENTATIONPRESET);//tcOverPresentationPreset
							abovTotTmTrgtFl = getValue(eleServiceTime,SOS_SERVICETIME_TCOVERTOTALPRESET);//tcOverTotalPreset
							abovTotMfyTrgtTmTmFl = getValue(eleServiceTime,SOS_SERVICETIME_TCOVERTOTALMFY);//tcOverTotalMFY
							abovTotFrntCterTrgtTmFl = getValue(eleServiceTime,SOS_SERVICETIME_TCOVERTOTALFC);//tcOverTotalFC
							abovTotDrvTrgtTmFl = getValue(eleServiceTime,SOS_SERVICETIME_TCOVERTOTALDT);//tcOverTotalDT
							
							processProductionTime(eleServiceTime.getChildNodes(),context);

							outputKey.setLength(0);						
							outputKey.append(REC_POSTRN);
							outputKey.append(terrCd);
							outputKey.append(posBusnDt);
							outputKey.append(posOrdKey);
							outputKey.append(SEPARATOR_CHARACTER);
							outputKey.append(mcdGbalLcatIdNu);
							
					    	mapKey.clear();
							mapKey.set(outputKey.toString());

							outputTextValue.setLength(0);
							outputTextValue.append("2"); //split hub to 2. no reducer.
//							outputTextValue.append(terrCd);
//							outputTextValue.append(SEPARATOR_CHARACTER);
//							outputTextValue.append(posBusnDt);
//							outputTextValue.append(SEPARATOR_CHARACTER);
//							outputTextValue.append(mcdGbalLcatIdNu);
//							outputTextValue.append(SEPARATOR_CHARACTER);
//							outputTextValue.append(lgcyLclRfrDefCd);
//							outputTextValue.append(SEPARATOR_CHARACTER);
//							outputTextValue.append(posOrdKey);
//							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlTotKeyPrssScQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlStrInSysScQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlOrdRcllScQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlDrwrClseScQt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlPaidScQt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(untlSrvScQt); 
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(totOrdTmScQt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovPsntTmTrgtFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovTotTmTrgtFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovTotMfyTrgtTmTmFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovTotFrntCterTrgtTmFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abovTotDrvTrgtTmFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(abov50ScFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(bel25ScFl);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(heldTmScQt);
							outputTextValue.append(SEPARATOR_CHARACTER);
							outputTextValue.append(ordHeldFl).append(SEPARATOR_CHARACTER);
							
//							outputTextValue.append(DaaSConstants.SDF_yyyyMMddHHmmssSSS.format(curDate)).append(SEPARATOR_CHARACTER);
//							outputTextValue.append(DaaSConstants.SDF_yyyyMMddHHmmssSSS.format(curDate));
							
							mapValue.clear();
							mapValue.set(outputTextValue.toString());
							
//							System.out.println(" sos " + outputTextValue.toString());
							
//							context.write(mapKey, mapValue);	
//							context.getCounter("COUNT","POS_TRN_DetailedSOS").increment(1);

						
//					    	mapKey.clear();
//							mapKey.set(outputKey.toString());
							context.write(mapKey, mapValue);	//todo
							
//							outputfileNamebf.setLength(0);
//							outputfileNamebf.append(subfileType).append(DaaSConstants.TILDE_DELIMITER).append(terrCd).append(DaaSConstants.TILDE_DELIMITER).append(posBusnDt);
//							String outputfileName = HDFSUtil.replaceMultiOutSpecialChars(outputfileNamebf.toString());
//							
//							outputfileNamebf.setLength(0);
//							outputfileNamebf.append(multioutBaseOutputPath).append(Path.SEPARATOR).append(subfileType).append("/terr_cd=").append(terrCd).append("/pos_busn_dt=").append(posBusnDt).append(Path.SEPARATOR).append(outputfileName);
//							mos.write(outputfileName,NullWritable.get(),mapValue,multioutBaseOutputPath+Path.SEPARATOR+subfileType+"/terr_cd="+terrCd+"/pos_busn_dt="+posBusnDt+"/"+outputfileName);
							
							
							//mos.write(outputfileName,NullWritable.get(),mapValue,outputfileNamebf.toString());undo
							
							
//							mos.write(NullWritable.get(),mapValue,outputfileNamebf.toString());
							context.getCounter("COUNT","POS_TRN_ITM_DetailedSOS").increment(1);

						}
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in TLDNewDataHubMapperHCat.processServiceTime:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}
	
	private void processProductionTime(NodeList nlProductionTime
                                      ,Context context) {

		eleProductionTime = null;

		if (nlProductionTime != null && nlProductionTime.getLength() > 0 ) {
			for (int idxProductionTime=0; idxProductionTime < nlProductionTime.getLength(); idxProductionTime++ ) {
				if ( nlProductionTime.item(idxProductionTime).getNodeType() == Node.ELEMENT_NODE ) {  
					eleProductionTime = (Element)nlProductionTime.item(idxProductionTime);
					if ( eleProductionTime.getNodeName().equals("ProductionTime") ) {
						abov50ScFl = getValue(eleProductionTime,SOS_SERVICETIME_PT_TCOVER50);//tcOver50
						bel25ScFl = getValue(eleProductionTime,SOS_SERVICETIME_PT_TCUNDER25);//tcUnder25
						heldTmScQt = getScQt(eleProductionTime.getAttribute(SOS_SERVICETIME_PT_HELDTIME));//heldTime
						ordHeldFl = getValue(eleProductionTime,SOS_SERVICETIME_PT_TCHELD);//tcHeld
					}
				}
			}
		}

	}

	private void getDaypart() {
		
		String dayPartKey;
		String hour;
		String minute;
		int minuteInt;
		
		dyptIdNu = StringUtils.EMPTY;
		dyptDs = StringUtils.EMPTY;
		dyOfCalWkDs = StringUtils.EMPTY;
		
		if(posTrnStrtTs == null || posTrnStrtTs.isEmpty()) return;
		
		if(posTrnStrtTs.trim().length() < 4){
			System.out.println(" posTrnStrtTs " +posTrnStrtTs  + "XXX" );
		}
		
		cal.set(Integer.parseInt(posTrnStrtTs.substring(0, 4)), Integer.parseInt(posTrnStrtTs.substring(5, 7))-1, Integer.parseInt(posTrnStrtTs.substring(8, 10)));
		hour = posTrnStrtTs.substring(11, 13);
		hrIdNu = String.valueOf(hour);
		minuteInt = Integer.parseInt(posTrnStrtTs.substring(14, 16));
		
		if ( minuteInt < 15 ) {
			minute = "00";
		} else if ( minuteInt < 30 ) {
			minute = "15";
		} else if ( minuteInt < 45 ) {
			minute = "30";
		} else {
			minute = "45";
		}
		
		switch ( cal.get(Calendar.DAY_OF_WEEK) ) {
			case 1: 
				dyOfCalWkDs = "Sunday";
			    break;
			
			case 2: 
				dyOfCalWkDs = "Monday";
			    break;
			
			case 3: 
				dyOfCalWkDs = "Tuesday";
			    break;
			
			case 4: 
				dyOfCalWkDs = "Wednesday";
			    break;
			
			case 5: 
				dyOfCalWkDs = "Thursday";
			    break;
			
			case 6: 
				dyOfCalWkDs = "Friday";
			    break;
			
			case 7: 
				dyOfCalWkDs = "Saturday";
			    break;
			
			default: 
				dyOfCalWkDs = "**";
			    break;
		}

		dayPartKey = terrCd + SEPARATOR_CHARACTER + String.valueOf(cal.get(Calendar.DAY_OF_WEEK)) + SEPARATOR_CHARACTER + hour + minute;
		
		if ( dayPartMap.containsKey(dayPartKey) ) {
			parts = dayPartMap.get(dayPartKey).split("\t",-1);
			dyptIdNu = parts[0];
			dyptDs = parts[1];
		}
		
	}
	
	private String formatAsTs(String in) {
		
		String retTs = StringUtils.EMPTY;
		
		if ( in.length() >= 14 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8) + " " + in.substring(8, 10) + ":" + in.substring(10, 12) + ":" + in.substring(12, 14);
		}

		return(retTs);
	}
	
	private String formatDateAsTsDtOnly(String in) {

		String retTs = StringUtils.EMPTY;
		
		if ( in.length() >= 8 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8);
		}

		return(retTs);
		
	}
	
	private String formatDateAsTs(String in) {

		String retTs = StringUtils.EMPTY;
		
		if ( in.length() >= 8 ) {
			retTs = in.substring(0, 4) + "-" + in.substring(4, 6) + "-" + in.substring(6, 8) + " 00:00:00";
		}

		return(retTs);
		
	}
	
	private String formatTimeAsTs(String in
			                     ,String inDate) {

		String retTs = StringUtils.EMPTY;
		
		if ( in.length() >= 6 ) {
			retTs = inDate.substring(0, 10) + " " + in.substring(0, 2) + ":" + in.substring(3, 4) + ":" + in.substring(4, 6);
		}

		return(retTs);
		
	}
	
	private String getScQt(String time) {
		
		if(StringUtils.isEmpty(time))
			return "0";
		String retScQt = StringUtils.EMPTY;
		
		Double tmpScQt;
		
		try {
			tmpScQt = Double.parseDouble(time) / 1000.0;
			retScQt = String.valueOf(tmpScQt);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return(retScQt);
		
	}
	
	private String getValue(Element ele
			               ,String attribute) {
		
		String retValue = StringUtils.EMPTY;

		try {
			retValue = ele.getAttribute(attribute);
			
		} catch (Exception ex) {
		}
		
		return(retValue.trim());
	}
	

	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

//		mos.close();

	}
	
}

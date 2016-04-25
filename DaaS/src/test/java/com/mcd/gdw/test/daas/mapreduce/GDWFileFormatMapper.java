package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.test.daas.driver.GenerateGDWFileFormat;

public class GDWFileFormatMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	public class GDWSalesItem {
		
		public final static String SALE_TYPE_TOTAL             = "Total";
		public final static String SALE_TYPE_DRIVE_THRU        = "Drive Thru";
		public final static String SALE_TYPE_FRONT_COUNTER     = "Frount Counter";
		public final static String SALE_TYPE_PRODUCT           = "Product";
		public final static String SALE_TYPE_NON_PRODUCT       = "Non Product";
		public final static String SALE_TYPE_GIFT_CERT_SOLD    = "Gift Certificates Sold";
		public final static String SALE_TYPE_GIFT_CERT_REDEMED = "Gift Certificates Redemed";
		
		private boolean isSet = false;
		private String saleType;
		private BigDecimal saleAmount;
		private int qty;
		
		public GDWSalesItem(String saleType) {
			
			this(saleType, new BigDecimal("0.00"), 0);
		}

		public GDWSalesItem(String saleType
				           ,BigDecimal saleAmount
				           ,int qty) {
			
			this.isSet = false;
			this.saleType = saleType;
			this.saleAmount = saleAmount;
			this.qty = qty;
			
		}
		
		public void initValues() {

			this.isSet = false;
			this.saleAmount = new BigDecimal("0.00");
			this.qty = 0;
			
		}
		
		public boolean valueIsSet() {
			
			return(this.isSet);
			
		}
		
		public String getSaleType() {
			
			return(this.saleType);
		}
		
		public void setSaleAmount(BigDecimal saleAmount) {
		
			this.isSet = true;
			this.saleAmount = saleAmount;
			
		}
		
		public BigDecimal getSaleAmount() {
			
			return(this.saleAmount);
			
		}
		
		public void setQty(int qty) {
		
			this.isSet = true;
			this.qty = qty;
			
		}
		
		public int getQty() {
			
			return(this.qty);
			
		}
		
		public String toString(int digits) {
			
			if ( saleType.equals(SALE_TYPE_TOTAL) ||  
				 saleType.equals(SALE_TYPE_DRIVE_THRU) ||
				 saleType.equals(SALE_TYPE_FRONT_COUNTER) ) {
				
				return(saleType + "|" + saleAmount.setScale(digits, RoundingMode.HALF_UP).toString() + "|" + qty);
				
			} else if ( saleType.equals(SALE_TYPE_PRODUCT) || 
					    saleType.equals(SALE_TYPE_NON_PRODUCT) ) {
				
				return(saleType + "|" + saleAmount.setScale(digits, RoundingMode.HALF_UP).toString() + "|");
				
			} else if ( saleType.equals(SALE_TYPE_GIFT_CERT_SOLD) || 
					    saleType.equals(SALE_TYPE_GIFT_CERT_REDEMED) ) {
				
				return(saleType + "|" + saleAmount.setScale(digits, RoundingMode.HALF_UP).toString() + "|" + qty);
				
			}
			
			return(saleType + "|ERROR|");
		}
	}
	
	public class PmixItem {
	
		private String menuItem;
		private BigDecimal unitPrice;
		private int alacartQty;
		private int comboQty;
		private int promoAlacartQty;
		private int promoComboQty;
		private BigDecimal addlPrice;
		private boolean addlItemFl = false;
		
		public PmixItem(String menuItem
				       ,BigDecimal unitPrice) {
			
			this.menuItem = menuItem;
			this.unitPrice = unitPrice;
			this.alacartQty = 0;
			this.comboQty = 0;
			this.promoAlacartQty = 0;
			this.promoComboQty = 0;
			this.addlPrice = DEC_ZERO;
			
		}
		
		public String getMenuItem() {
			
			return(this.menuItem);
			
		}
		
		public BigDecimal getUnitPrice() {
			
			return(this.unitPrice);
			
		}
		
		public String getPmixItemKey() {
			
			return(this.menuItem + "|" + this.unitPrice.setScale(2, RoundingMode.HALF_UP).toString());
			
		}
		
		public String generatePmixItemKey(String menuItem
				                         ,BigDecimal unitPrice) {
			
			return(menuItem + "|" + unitPrice.setScale(2, RoundingMode.HALF_UP).toString());
			
		}
		
		public void setAlacartQty(int alacartQty) {
			
			this.alacartQty = alacartQty;
			
		}
		
		public void setPromoAlacartQty(int promoAlacartQty) {
			
			this.promoAlacartQty = promoAlacartQty;
			
		}
		
		public int getAlacartQty() {
			
			return(this.alacartQty);
			
		}
		
		public int getPromoAlacartQty() {
			
			return(this.promoAlacartQty);
			
		}
		
		public void setComboQty(int comboQty) {
			
			this.comboQty = comboQty;
			
		}
		
		public void setPromoComboQty(int promoComboQty) {
			
			this.promoComboQty = promoComboQty;
			
		}
		
		public int getComboQty() {
			
			return(this.comboQty);
			
		}		
		
		public int getPromoComboQty() {
			
			return(this.promoComboQty);
			
		}		
		
		public void setAddlPrice(BigDecimal addlPrice) {
			
			if ( !addlItemFl && !addlPrice.equals(DEC_ZERO) ) {
				addlItemFl = true;
				this.addlPrice = addlPrice;
			}
			
		}
		
		public String toStringTotal() {
			
			return("Total|" + this.menuItem + "|" + (this.unitPrice.add(this.addlPrice)).setScale(2, RoundingMode.HALF_UP).toString() + "|" + this.alacartQty + "|" + this.comboQty + "|" + (this.alacartQty + this.comboQty));
			
		}
		
		public String toStringAsZeroTotal() {
			
			return("Total|" + this.menuItem + "|0.00|" + this.alacartQty + "|" + this.comboQty + "|" + (this.alacartQty + this.comboQty));
			
		}
		
		public String toStringPromo() {
			
			return("Promo|" + this.menuItem + "|" + (this.unitPrice.add(this.addlPrice)).setScale(2, RoundingMode.HALF_UP).toString() + "|" + this.promoAlacartQty + "|" + this.promoComboQty + "|" + (this.promoAlacartQty + this.promoComboQty));
			
		}
		
		public String toStringAsZeroPromo() {
			
			return("Promo|" + this.menuItem + "|0.00|" + this.promoAlacartQty + "|" + this.promoComboQty + "|" + (this.promoAlacartQty + this.promoComboQty));
			
		}
	}
	
	private final static String SEPARATOR_CHARACTER = "|";
	
	private final static String TRX_SALE     = "TRX_Sale";
	private final static String TRX_REFUND   = "TRX_Refund";
	private final static String TRX_OVERRING = "TRX_Waste";
	
	private final static BigDecimal DEC_ZERO    = new BigDecimal("0.00");
	private final static BigDecimal DEC_ONE     = new BigDecimal("1.00");
	private final static BigDecimal DEC_NEG_ONE = new BigDecimal("-1.00");
	private final static BigDecimal DEC_NEG_9999 = new BigDecimal("-9999.99");
	
	private final static int SALES_IDX_TOTAL             = 0; 
	private final static int SALES_IDX_DRIVE_THRU        = 1;
	private final static int SALES_IDX_FRONT_COUNTER     = 2;
	private final static int SALES_IDX_PRODUCT           = 3;
	private final static int SALES_IDX_NON_PRODUCT       = 4;
	private final static int SALES_IDX_GIFT_CERT_SOLD    = 5;
	private final static int SALES_IDX_GIFT_CERT_REDEMED = 6;

	private final static int POD_TYPES_TERR_CD   = 0;
	private final static int POD_TYPES_POD_TYPE  = 1;
	private final static int POD_TYPES_POD_DESC  = 2;
	
	private final static int CARD_ITEM_TERR_CD   = 0;
	private final static int CARD_ITEM_MENU_ITEM = 1;

	private final static int CARD_TENDER_TERR_CD = 0;
	private final static int CARD_TENDER_DESC    = 1;

	private final static int CTRY_CTRY_ISO_NU       = 0;
	private final static int CTRY_CTRY_ISO3_ABBR_CD = 1;
	private final static int CTRY_CTRY_ISO2_ABBR_CD = 2;
	private final static int CTRY_CTRY_NA           = 3;
	private final static int CTRY_CTRY_SHRT_NA      = 4;
	
	private final static int VM_COMPONENT_DATE         = 0;
	private final static int VM_COMPONENT_CTRY_ISO_NU  = 1;
	private final static int VM_COMPONENT_MENU_ITEM_NU = 2;
	private final static int VM_COMPONENT_COMPONENTS   = 3;
	

	private MultipleOutputs<NullWritable, Text> mos;
	
	private String fileDailyOutName; 
	private String fileHalfHourlyOutName; 
	private String filePmixOutName;
	
	private int idxDailySales;
	private int idxHH;
	
	private String keyPrefix; 
	
	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private String[] parts;
	private String[] components;
	private String[] componentParts;
	private StringReader strReader;
	private String line;

	private HashMap<String,String> podTypeMap = new HashMap<String,String>();
	private HashMap<String,String> cardItemMap = new HashMap<String,String>();
	private HashMap<String,String> cardTenderMap = new HashMap<String,String>();
	private HashMap<String,String> ctryMap = new HashMap<String,String>();
	private HashMap<String,String> valueMealComponentMap = new HashMap<String,String>();
	private HashMap<String,Integer> componentsMap = new HashMap<String,Integer>();
	private HashMap<String,String> ignoreItemMap = new HashMap<String,String>();

	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private Element eleTRX;
	private Element eleOrder;
	private Element eleOrderItem;
	private Element eleTender;
	private Element eleTenderItem;
	
	private Node nodeText;
	
	private String trxType;

	private String ctryIso2AbbrCd;
	private String terrCd;
	private String lgcyLclRfrDefCd;
	private String calDt;
	private String pod;
	private String podType;
	private String kind;
	private String timestamp;
	private int hour;
	private int minute;
	
	private String itemCode;
	private int itemQty;
	private int promoQty;
	private BigDecimal unitPrice;
	private BigDecimal itemPrice;
	private boolean isValueMeal;
	private boolean isCombo;
	private int valueMealQty;
	private String pmixMapKey;
	private boolean skipItem;
	private String parentPmixMapKey;
	
	private PmixItem pmixItem;
	
	private String time;
	
	private BigDecimal totalAmount;
	private BigDecimal nonProductAmount;
	private String tenderName;
	private BigDecimal tenderAmount;
	
	private GDWSalesItem[] dailyItems        = new GDWSalesItem[7];
	private GDWSalesItem[][] halfHourlyItems = new GDWSalesItem[48][3];
	private HashMap<String,PmixItem> pmixMap = new HashMap<String,PmixItem>();
	
	private Text textValue = new Text();
	
	private PmixItem pmixValueMealComponentEntry;
	private PmixItem findPmixValueMealComponentEntry;
	private PmixItem tmpPmixItem;
	private String pmixValueMealItem;
	
	
	@Override
	public void setup(Context context) {

		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;

	    mos = new MultipleOutputs<NullWritable, Text>(context);
	    
		try {
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();

			dailyItems[SALES_IDX_TOTAL]             = new GDWSalesItem(GDWSalesItem.SALE_TYPE_TOTAL);
			dailyItems[SALES_IDX_DRIVE_THRU]        = new GDWSalesItem(GDWSalesItem.SALE_TYPE_DRIVE_THRU);
			dailyItems[SALES_IDX_FRONT_COUNTER]     = new GDWSalesItem(GDWSalesItem.SALE_TYPE_FRONT_COUNTER);
			dailyItems[SALES_IDX_PRODUCT]           = new GDWSalesItem(GDWSalesItem.SALE_TYPE_PRODUCT);
			dailyItems[SALES_IDX_NON_PRODUCT]       = new GDWSalesItem(GDWSalesItem.SALE_TYPE_NON_PRODUCT);
			dailyItems[SALES_IDX_GIFT_CERT_SOLD]    = new GDWSalesItem(GDWSalesItem.SALE_TYPE_GIFT_CERT_SOLD);
			dailyItems[SALES_IDX_GIFT_CERT_REDEMED] = new GDWSalesItem(GDWSalesItem.SALE_TYPE_GIFT_CERT_REDEMED);

			for ( idxHH=0; idxHH < halfHourlyItems.length; idxHH++ ) {
				halfHourlyItems[idxHH][SALES_IDX_TOTAL]             = new GDWSalesItem(GDWSalesItem.SALE_TYPE_TOTAL);
				halfHourlyItems[idxHH][SALES_IDX_DRIVE_THRU]        = new GDWSalesItem(GDWSalesItem.SALE_TYPE_DRIVE_THRU);
				halfHourlyItems[idxHH][SALES_IDX_FRONT_COUNTER]     = new GDWSalesItem(GDWSalesItem.SALE_TYPE_FRONT_COUNTER);
				//halfHourlyItems[idxHH][SALES_IDX_PRODUCT]           = new GDWSalesItem(GDWSalesItem.SALE_TYPE_PRODUCT);
				//halfHourlyItems[idxHH][SALES_IDX_NON_PRODUCT]       = new GDWSalesItem(GDWSalesItem.SALE_TYPE_NON_PRODUCT);
				//halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_SOLD]    = new GDWSalesItem(GDWSalesItem.SALE_TYPE_GIFT_CERT_SOLD);
				//halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_REDEMED] = new GDWSalesItem(GDWSalesItem.SALE_TYPE_GIFT_CERT_REDEMED);
			}
			
		    distPaths = context.getCacheFiles();
		    
		    if (distPaths == null){
		    	System.err.println("distpath is null");
		    	System.exit(8);
		    }
		      
		    if ( distPaths != null && distPaths.length > 0 )  {
		    	  
		    	System.out.println(" number of distcache files : " + distPaths.length);
		    	  
		    	for ( int i=0; i<distPaths.length; i++ ) {
				     
			    	  System.out.println("distpaths:" + distPaths[i].toString());
			    	  
			    	  distPathParts = 	distPaths[i].toString().split("#");
			    	  
			    	  if( distPaths[i].toString().contains(GenerateGDWFileFormat.CACHE_POD_TYPES) ) {
			    		  		      	  
			    		  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				      	  addPodTypesKeyValuestoMap(br);
				      	  System.out.println("Loaded POD Types Values Map");
				      	  
				      } else if ( distPaths[i].toString().contains(GenerateGDWFileFormat.CACHE_GIFT_CARD_ITEMS) ) {
    	  		      	  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addCardItemsKeyValuestoMap(br);
				      	  System.out.println("Loaded Gift Card Items Values Map");
			      	  
				      } else if ( distPaths[i].toString().contains(GenerateGDWFileFormat.CACHE_GIFT_CARD_TENDERS) ) {
	  		      	  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addCardTendersKeyValuestoMap(br);
				    	  System.out.println("Loaded Gift Card Tenders Values Map");
			      	  
				      } else if ( distPaths[i].toString().contains(GenerateGDWFileFormat.CACHE_CTRY) ) {
	  		      	  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addCtryKeyValuestoMap(br);
				    	  System.out.println("Loaded Country Values Map");
				      	  
				      } else if ( distPaths[i].toString().contains(GenerateGDWFileFormat.CACHE_VALUE_MEAL_COMPONENTS) ) {
		  		      	  
				    	  br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
				    	  addValueMealComponentsKeyValuestoMap(br);
				    	  System.out.println("Loaded Value Meal Components Values Map");
				      }
			      }
		      }

			
		} catch (Exception ex) {
			System.err.println("Error in initializing GDWFileFormatMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
		
	}
	  
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

		//cc = context; 
		
		parts = value.toString().split("\\t");

		if ( parts.length >= 8 ) {
			if ( ctryMap.containsKey(parts[DaaSConstants.XML_REC_TERR_CD_POS]) ) {
				ctryIso2AbbrCd = ctryMap.get(parts[DaaSConstants.XML_REC_TERR_CD_POS]);
			} else {
				ctryIso2AbbrCd = parts[DaaSConstants.XML_REC_TERR_CD_POS];
			}
			fileDailyOutName = "Dly_Sls_" + ctryIso2AbbrCd + "_" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS];
			fileHalfHourlyOutName = "Dy_Tm_Seg_Sls_" + ctryIso2AbbrCd + "_" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS];
			filePmixOutName = "Dly_Pmix_" + ctryIso2AbbrCd + "_" + parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS];
			if ( parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS].equals("31329") ) {
				getData(parts[DaaSConstants.XML_REC_XML_TEXT_POS],context);
			}
		}
		
	}
		
	private void getData(String xmlText
                        ,Context context) {
		
		try {
			strReader  = new StringReader(xmlText);
			xmlSource = new InputSource(strReader);
			doc = docBuilder.parse(xmlSource);
			eleRoot = (Element) doc.getFirstChild();

			if ( eleRoot.getNodeName().equals("TLD") ) {
				terrCd = eleRoot.getAttribute("gdwTerrCd");
				lgcyLclRfrDefCd = eleRoot.getAttribute("gdwLgcyLclRfrDefCd");
				calDt = eleRoot.getAttribute("gdwBusinessDate");
				calDt = calDt.substring(0, 4) + "-" + calDt.substring(4, 6) + "-" + calDt.substring(6, 8);


				for ( idxDailySales=0; idxDailySales < dailyItems.length; idxDailySales++ ) {
					dailyItems[idxDailySales].initValues();
				}

				for ( idxHH=0; idxHH < halfHourlyItems.length; idxHH++ ) {
					halfHourlyItems[idxHH][SALES_IDX_TOTAL].initValues();
					halfHourlyItems[idxHH][SALES_IDX_DRIVE_THRU].initValues();
					halfHourlyItems[idxHH][SALES_IDX_FRONT_COUNTER].initValues();
					//halfHourlyItems[idxHH][SALES_IDX_PRODUCT].initValues();
					//halfHourlyItems[idxHH][SALES_IDX_NON_PRODUCT].initValues();
					//halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_SOLD].initValues();
					//halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_REDEMED].initValues();
				}

				pmixMap.clear();
				
				processNodes(eleRoot.getChildNodes(),context);

				keyPrefix = terrCd + "|" + lgcyLclRfrDefCd + "|" + calDt + "|";
				
				for ( idxDailySales=0; idxDailySales < dailyItems.length; idxDailySales++ ) {
					
					if ( dailyItems[idxDailySales].valueIsSet() ) {
						textValue.clear();
						textValue.set(keyPrefix + dailyItems[idxDailySales].toString(2));
						mos.write(NullWritable.get(),textValue,fileDailyOutName);
					}
				}
	
				
				ignoreItemMap.clear();
				
				for (Map.Entry<String, PmixItem> pmixEntry : pmixMap.entrySet()) {
					pmixValueMealComponentEntry = pmixEntry.getValue();
					pmixValueMealItem = pmixValueMealComponentEntry.getMenuItem();
					
					if ( pmixValueMealComponentEntry.getUnitPrice().equals(DEC_NEG_9999) ) {
						
						for (Map.Entry<String, PmixItem> pmixFindEntry : pmixMap.entrySet()) {
							
							findPmixValueMealComponentEntry = pmixFindEntry.getValue();
							
							if ( findPmixValueMealComponentEntry.getMenuItem().equals(pmixValueMealItem) && !findPmixValueMealComponentEntry.getUnitPrice().equals(DEC_NEG_9999) )  {
								findPmixValueMealComponentEntry.setComboQty(findPmixValueMealComponentEntry.getComboQty() + pmixValueMealComponentEntry.getComboQty());
								ignoreItemMap.put(pmixValueMealItem, pmixValueMealItem);
								break;
							}
						}
					}
				}

				for (Map.Entry<String, PmixItem> pmixEntry2 : pmixMap.entrySet()) {
					tmpPmixItem = pmixEntry2.getValue();
					pmixValueMealItem = tmpPmixItem.getMenuItem();
					
					if ( !(tmpPmixItem.getUnitPrice().equals(DEC_NEG_9999) && ignoreItemMap.containsKey(pmixValueMealItem)) ) {
						if ( tmpPmixItem.getUnitPrice().equals(DEC_NEG_9999) ) {
							if ( tmpPmixItem.alacartQty != 0 || tmpPmixItem.comboQty !=0 ) {
								textValue.clear();
								textValue.set(keyPrefix + tmpPmixItem.toStringAsZeroTotal());
								mos.write(NullWritable.get(),textValue,filePmixOutName);
							}
							if ( tmpPmixItem.promoAlacartQty != 0 || tmpPmixItem.promoComboQty !=0 ) {
								textValue.clear();
								textValue.set(keyPrefix + tmpPmixItem.toStringAsZeroPromo());
								mos.write(NullWritable.get(),textValue,filePmixOutName);
							}
						} else {
							if ( tmpPmixItem.alacartQty != 0 || tmpPmixItem.comboQty !=0 ) {
								textValue.clear();
								textValue.set(keyPrefix + tmpPmixItem.toStringTotal());
								mos.write(NullWritable.get(),textValue,filePmixOutName);
							}
							if ( tmpPmixItem.promoAlacartQty != 0 || tmpPmixItem.promoComboQty !=0 ) {
								textValue.clear();
								textValue.set(keyPrefix + tmpPmixItem.toStringPromo());
								mos.write(NullWritable.get(),textValue,filePmixOutName);
							}
						}
					} 
				}

				keyPrefix += "Half Hourly|";
				
				for ( idxHH=0; idxHH < halfHourlyItems.length; idxHH++ ) {
					time = String.format("%02d", (idxHH / 2));
					if ( (idxHH % 2) == 0 ) {
						time += "00";
					} else {
						time += "30";
					}
				
					for ( idxDailySales=0; idxDailySales <= SALES_IDX_FRONT_COUNTER; idxDailySales++ ) {
						if ( halfHourlyItems[idxHH][idxDailySales].isSet ) {
							textValue.clear();
							textValue.set(keyPrefix + time + "|" + halfHourlyItems[idxHH][idxDailySales].toString(2));
							mos.write(NullWritable.get(),textValue,fileHalfHourlyOutName);
						}
					}
					
				}

			}
			
			
		} catch ( Exception ex ) {
			eleRoot = (Element) doc.getFirstChild();
			System.err.println("Error in GDWFileFormatMapper.getData:");
			ex.printStackTrace(System.err);
			System.exit(8);

		} finally {
			doc = null;
			xmlSource = null;
				
			if(strReader != null){
				strReader.close();
				strReader = null;
				
			}
		}
		
	}
	
	private void processNodes(NodeList nlNode, Context context) throws Exception {

		if (nlNode != null && nlNode.getLength() > 0 ) {
			for (int idxNode=0; idxNode < nlNode.getLength(); idxNode++ ) {
				if ( nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE ) {  
					eleNode = (Element)nlNode.item(idxNode);
					if ( eleNode.getNodeName().equals("Node") ) {
						processEvents(eleNode.getChildNodes(),context);
					}
				}
			}
		}

	}
	
	private void processEvents(NodeList nlEvent,Context context) throws Exception {

		if (nlEvent != null && nlEvent.getLength() > 0 ) {
			for (int idxEvent=0; idxEvent < nlEvent.getLength(); idxEvent++ ) {
				if ( nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE ) {  
					eleEvent = (Element)nlEvent.item(idxEvent);
					
					trxType = eleEvent.getAttribute("Type");
					
					if ( trxType.equals(TRX_SALE) || 
					     trxType.equals(TRX_REFUND) || 
					     trxType.equals(TRX_OVERRING) ) {
					
						processTRX(eleEvent.getChildNodes());
					}
				}
			}
		}

	}

	private void processTRX(NodeList nlTRX) throws Exception {

		if (nlTRX != null && nlTRX.getLength() > 0 ) {
			for (int idxTRX=0; idxTRX < nlTRX.getLength(); idxTRX++ ) {
				if ( nlTRX.item(idxTRX).getNodeType() == Node.ELEMENT_NODE ) {  
					eleTRX = (Element)nlTRX.item(idxTRX);
					
					pod = eleTRX.getAttribute("POD");
					
					if ( podTypeMap.containsKey(terrCd + SEPARATOR_CHARACTER + pod) ) {
						podType = podTypeMap.get(terrCd + SEPARATOR_CHARACTER + pod);
					} else {
						podType = "";
					}

					if ( trxType.equals(TRX_SALE) && eleTRX.getAttribute("status").equals("Paid") ) {
						processOrder(eleTRX.getChildNodes(),DEC_ONE,1);
					} else if ( trxType.equals(TRX_REFUND) ) {
						processOrder(eleTRX.getChildNodes(),DEC_NEG_ONE,0);
					} else if ( trxType.equals(TRX_OVERRING) ) {
						processOrder(eleTRX.getChildNodes(),DEC_NEG_ONE,-1);
					}

				}
			}
		}
		
	}

	private void processOrder(NodeList nlOrder
			                 ,BigDecimal salesFactor
			                 ,int tcFactor) throws Exception {

		if (nlOrder != null && nlOrder.getLength() > 0 ) {
			for (int idxOrder=0; idxOrder < nlOrder.getLength(); idxOrder++ ) {
				if ( nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE ) {  
					eleOrder = (Element)nlOrder.item(idxOrder);
					
					if ( eleOrder.getNodeName().equals("Order") ) {
						timestamp = eleOrder.getAttribute("Timestamp");
						
						try {
							hour = Integer.parseInt(timestamp.substring(8,10));
						} catch (Exception ex) {
							hour = 0;
						}
						
						try {
							minute = Integer.parseInt(timestamp.substring(10,12));
						} catch (Exception ex) {
							minute = 0;
						}
									
						idxHH = hour * 2; 
						if ( minute >= 30 ) {
							idxHH++;
						}
						
						//key  = eleOrder.getAttribute("key");
						kind = eleOrder.getAttribute("kind");
						totalAmount = new BigDecimal(eleOrder.getAttribute("totalAmount"));
						nonProductAmount = new BigDecimal(eleOrder.getAttribute("nonProductAmount"));
						
						dailyItems[SALES_IDX_TOTAL].setSaleAmount(dailyItems[SALES_IDX_TOTAL].getSaleAmount().add(totalAmount.subtract(nonProductAmount).multiply(salesFactor)));
						
						halfHourlyItems[idxHH][SALES_IDX_TOTAL].setSaleAmount(halfHourlyItems[idxHH][SALES_IDX_TOTAL].getSaleAmount().add(totalAmount.subtract(nonProductAmount).multiply(salesFactor)));
					
						if ( pod.equalsIgnoreCase("DRIVE THRU") ) {
							dailyItems[SALES_IDX_DRIVE_THRU].setSaleAmount(dailyItems[SALES_IDX_DRIVE_THRU].getSaleAmount().add(totalAmount.subtract(nonProductAmount).multiply(salesFactor)));
							halfHourlyItems[idxHH][SALES_IDX_DRIVE_THRU].setSaleAmount(halfHourlyItems[idxHH][SALES_IDX_DRIVE_THRU].getSaleAmount().add(totalAmount.subtract(nonProductAmount).multiply(salesFactor)));
						}

						if ( pod.equalsIgnoreCase("FRONT COUNTER") ) {
							dailyItems[SALES_IDX_FRONT_COUNTER].setSaleAmount(dailyItems[SALES_IDX_FRONT_COUNTER].getSaleAmount().add(totalAmount.subtract(nonProductAmount).multiply(salesFactor)));
							halfHourlyItems[idxHH][SALES_IDX_FRONT_COUNTER].setSaleAmount(halfHourlyItems[idxHH][SALES_IDX_FRONT_COUNTER].getSaleAmount().add(totalAmount.subtract(nonProductAmount).multiply(salesFactor)));
						}
						
						dailyItems[SALES_IDX_PRODUCT].setSaleAmount(dailyItems[SALES_IDX_PRODUCT].getSaleAmount().add(totalAmount.subtract(nonProductAmount).multiply(salesFactor)));
						dailyItems[SALES_IDX_NON_PRODUCT].setSaleAmount(dailyItems[SALES_IDX_NON_PRODUCT].getSaleAmount().add(nonProductAmount.multiply(salesFactor)));

						//halfHourlyItems[idxHH][SALES_IDX_PRODUCT].setSaleAmount(halfHourlyItems[idxHH][SALES_IDX_PRODUCT].getSaleAmount().add(totalAmount.subtract(nonProductAmount).multiply(salesFactor)));
						//halfHourlyItems[idxHH][SALES_IDX_NON_PRODUCT].setSaleAmount(halfHourlyItems[idxHH][SALES_IDX_NON_PRODUCT].getSaleAmount().add(nonProductAmount.multiply(salesFactor)));
						
						if ( kind.toUpperCase().contains("CREW") && kind.toUpperCase().contains("MANAGER") ) {
							if ( !totalAmount.equals(DEC_ZERO)) {
								dailyItems[SALES_IDX_TOTAL].setQty(dailyItems[SALES_IDX_TOTAL].getQty() + tcFactor);

								halfHourlyItems[idxHH][SALES_IDX_TOTAL].setQty(halfHourlyItems[idxHH][SALES_IDX_TOTAL].getQty() + tcFactor);
								
								if ( podType.equalsIgnoreCase("DRIVE THRU") ) {
									dailyItems[SALES_IDX_DRIVE_THRU].setQty(dailyItems[SALES_IDX_DRIVE_THRU].getQty() + tcFactor);
									halfHourlyItems[idxHH][SALES_IDX_DRIVE_THRU].setQty(halfHourlyItems[idxHH][SALES_IDX_DRIVE_THRU].getQty() + tcFactor);
								}

								if ( podType.equalsIgnoreCase("FRONT COUNTER") ) {
									dailyItems[SALES_IDX_FRONT_COUNTER].setQty(dailyItems[SALES_IDX_FRONT_COUNTER].getQty() + tcFactor);
									halfHourlyItems[idxHH][SALES_IDX_FRONT_COUNTER].setQty(halfHourlyItems[idxHH][SALES_IDX_FRONT_COUNTER].getQty() + tcFactor);
								}
							}
						} else {
							dailyItems[SALES_IDX_TOTAL].setQty(dailyItems[SALES_IDX_TOTAL].getQty() + tcFactor);

							halfHourlyItems[idxHH][SALES_IDX_TOTAL].setQty(halfHourlyItems[idxHH][SALES_IDX_TOTAL].getQty() + tcFactor);

							if ( podType.equalsIgnoreCase("DRIVE THRU") ) {
								dailyItems[SALES_IDX_DRIVE_THRU].setQty(dailyItems[SALES_IDX_DRIVE_THRU].getQty() + tcFactor);
								halfHourlyItems[idxHH][SALES_IDX_DRIVE_THRU].setQty(halfHourlyItems[idxHH][SALES_IDX_DRIVE_THRU].getQty() + tcFactor);
							}

							if ( podType.equalsIgnoreCase("FRONT COUNTER") ) {
								dailyItems[SALES_IDX_FRONT_COUNTER].setQty(dailyItems[SALES_IDX_FRONT_COUNTER].getQty() + tcFactor);
								halfHourlyItems[idxHH][SALES_IDX_FRONT_COUNTER].setQty(halfHourlyItems[idxHH][SALES_IDX_FRONT_COUNTER].getQty() + tcFactor);
							}
						}
						
						processItem(eleOrder.getChildNodes());
						
						processOrderItems(eleOrder.getChildNodes());
					}
				}
			}
		}
	}

	private void processItem(NodeList nlItem) {
		
		Element eleItem;
		
		if (nlItem != null && nlItem.getLength() > 0 ) {
			for (int idxItem=0; idxItem < nlItem.getLength(); idxItem++ ) {
				if ( nlItem.item(idxItem).getNodeType() == Node.ELEMENT_NODE ) {
					eleItem = (Element)nlItem.item(idxItem);
					if ( eleItem.getNodeName().equals("Item") ) {
						itemCode = eleItem.getAttribute("code");
						
						try {
							itemQty = Integer.parseInt(eleItem.getAttribute("qty"));
						} catch (Exception ex) {
							itemQty = 0;
						}

						skipItem = false;
						
						if ( itemQty <= 0 ) {
							skipItem = true;
						} else {
							if ( eleItem.getAttribute("type").equals("COMMENT") ) {
								skipItem = true;
							} else {
								if ( eleItem.getAttribute("level").equals("0") || eleItem.getAttribute("level").equals("1") ) {
									skipItem = false;
								} else {
									skipItem = true;
								}
							}
						}

							
						if ( !skipItem ) {
							if ( eleItem.getAttribute("level").equals("0") ) {
								isValueMeal = ( eleItem.getAttribute("type").equals("VALUE_MEAL"));
								isCombo = false;
								
								if ( isValueMeal ) {
									valueMealQty = itemQty;
									componentsMap.clear();
									if ( valueMealComponentMap.containsKey(calDt + "|" + terrCd + "|" + itemCode) ) {
										components = valueMealComponentMap.get(calDt + "|" + terrCd + "|" + itemCode).split("~");
										for ( int idxComponents=0; idxComponents < components.length; idxComponents++ ) {
											componentParts = components[idxComponents].split("\\|");
											componentsMap.put(componentParts[0], new Integer(componentParts[1]));
											
											unitPrice = DEC_NEG_9999;
											pmixMapKey = componentParts[0] + "|" + unitPrice.setScale(2, RoundingMode.HALF_UP).toString();
											
											if ( pmixMap.containsKey(pmixMapKey) ) {
												pmixItem = pmixMap.get(pmixMapKey);
												pmixItem.comboQty += valueMealQty * Integer.parseInt(componentParts[1]);
												
											} else {
												pmixItem = new PmixItem(componentParts[0],unitPrice);
												pmixItem.comboQty += valueMealQty * Integer.parseInt(componentParts[1]);
												pmixMap.put(pmixMapKey, pmixItem);
											}
										}
									}

								}
							} else { 
								if ( isValueMeal ) {
									isCombo = true;
								} else {
									isCombo = false;
								}
							}
								
							if ( isCombo && componentsMap.containsKey(itemCode) ) {
								skipItem = true;
							}
						
							if ( !skipItem ) {
								try {
									promoQty = Integer.parseInt(eleItem.getAttribute("qtyPromo"));
								} catch (Exception ex) {
									promoQty = 0;
								}
								
								if ( promoQty > 0 ) {
									try {
										itemPrice = new BigDecimal(eleItem.getAttribute("BPPrice"));
									} catch (Exception ex) {
										itemPrice = DEC_ZERO;
									}
								} else {
									try {
										itemPrice = new BigDecimal(eleItem.getAttribute("BDPrice"));
									} catch (Exception ex) {
										itemPrice = DEC_ZERO;
									}
								}

								if ( promoQty > 0 ) {
									unitPrice = itemPrice.divide(new BigDecimal(promoQty), 2, RoundingMode.HALF_UP);
								} else {
									unitPrice = itemPrice.divide(new BigDecimal(itemQty), 2, RoundingMode.HALF_UP);
								}
								
								pmixMapKey = itemCode + "|" + unitPrice.setScale(2, RoundingMode.HALF_UP).toString();
								
								if ( isValueMeal && !isCombo ) {
									parentPmixMapKey = pmixMapKey;
								}

								if ( isCombo ) {
									if ( pmixMap.containsKey(parentPmixMapKey) ) {
										pmixItem = pmixMap.get(parentPmixMapKey);
										pmixItem.setAddlPrice(unitPrice);
									}
								}
								
								if ( pmixMap.containsKey(pmixMapKey) ) {
									pmixItem = pmixMap.get(pmixMapKey);
									if ( isCombo ) {
										pmixItem.comboQty += (itemQty - promoQty);
										pmixItem.promoComboQty += promoQty;
									} else {
										pmixItem.alacartQty += (itemQty - promoQty);
										pmixItem.promoAlacartQty += promoQty;
									}
								} else {
									pmixItem = new PmixItem(itemCode,unitPrice);
									if ( isCombo ) {
										pmixItem.comboQty += (itemQty - promoQty);
										pmixItem.promoComboQty += promoQty;
									} else {
										pmixItem.alacartQty += (itemQty - promoQty);
										pmixItem.promoAlacartQty += promoQty;
									}
									pmixMap.put(pmixMapKey, pmixItem);
								}
								
								if ( cardItemMap.containsKey(terrCd + SEPARATOR_CHARACTER + itemCode) ) {
								
									dailyItems[SALES_IDX_GIFT_CERT_SOLD].setSaleAmount(dailyItems[SALES_IDX_GIFT_CERT_SOLD].getSaleAmount().add(itemPrice));
									dailyItems[SALES_IDX_GIFT_CERT_SOLD].setQty(dailyItems[SALES_IDX_GIFT_CERT_SOLD].getQty() + itemQty);

									//halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_SOLD].setSaleAmount(halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_SOLD].getSaleAmount().add(itemPrice));
									//halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_SOLD].setQty(halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_SOLD].getQty() + itemQty);

								}
							}
						
							processItem(eleItem.getChildNodes());
							
						}
					}
				}
			}
		}

	}

	private void processOrderItems(NodeList nlOrderItem) throws Exception {

		if (nlOrderItem != null && nlOrderItem.getLength() > 0 ) {
			for (int idxOrderItem=0; idxOrderItem < nlOrderItem.getLength(); idxOrderItem++ ) {
				if ( nlOrderItem.item(idxOrderItem).getNodeType() == Node.ELEMENT_NODE ) {  
					eleOrderItem = (Element)nlOrderItem.item(idxOrderItem);
					
					if ( eleOrderItem.getNodeName().equals("Tenders") ) {
						processTenders(eleOrderItem.getChildNodes());
					}
				}
			}
		}
	}

	private void processTenders(NodeList nlTender) throws Exception {

		if (nlTender != null && nlTender.getLength() > 0 ) {
			for (int idxTender=0; idxTender < nlTender.getLength(); idxTender++ ) {
				if ( nlTender.item(idxTender).getNodeType() == Node.ELEMENT_NODE ) {  
					eleTender = (Element)nlTender.item(idxTender);
					
					if ( eleTender.getNodeName().equals("Tender") ) {
						processTenderItems(eleTender.getChildNodes());
					}
				}
			}
		}
	}

	private void processTenderItems(NodeList nlTenderItem) throws Exception {

		tenderName = "";
		
		if (nlTenderItem != null && nlTenderItem.getLength() > 0 ) {
			for (int idxTenderItem=0; idxTenderItem < nlTenderItem.getLength(); idxTenderItem++ ) {
				if ( nlTenderItem.item(idxTenderItem).getNodeType() == Node.ELEMENT_NODE ) {  
					eleTenderItem = (Element)nlTenderItem.item(idxTenderItem);
					
					nodeText = eleTenderItem.getFirstChild();
					
					if ( eleTenderItem.getNodeName().equals("TenderName") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
						tenderName = nodeText.getNodeValue();
					}

					if ( eleTenderItem.getNodeName().equals("TenderAmount") && nodeText != null && nodeText.getNodeType() == Node.TEXT_NODE ) {
						tenderAmount = new BigDecimal(nodeText.getNodeValue());
					}
						
				}
			}
		}
		
		if ( cardTenderMap.containsKey(terrCd + SEPARATOR_CHARACTER + tenderName) ) {
			dailyItems[SALES_IDX_GIFT_CERT_REDEMED].setSaleAmount(dailyItems[SALES_IDX_GIFT_CERT_REDEMED].getSaleAmount().add(tenderAmount));
			dailyItems[SALES_IDX_GIFT_CERT_REDEMED].setQty(dailyItems[SALES_IDX_GIFT_CERT_REDEMED].getQty() + 1);

			//halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_REDEMED].setSaleAmount(halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_REDEMED].getSaleAmount().add(tenderAmount));
			//halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_REDEMED].setQty(halfHourlyItems[idxHH][SALES_IDX_GIFT_CERT_REDEMED].getQty() + 1);
		}
	}
		
	private void addPodTypesKeyValuestoMap(BufferedReader br) {

		line = null;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\t", -1);

					podTypeMap.put(parts[POD_TYPES_TERR_CD] + SEPARATOR_CHARACTER + parts[POD_TYPES_POD_DESC], parts[POD_TYPES_POD_TYPE]);
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

	private void addCardItemsKeyValuestoMap(BufferedReader br) {

		line = null;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\t", -1);

					cardItemMap.put(parts[CARD_ITEM_TERR_CD] + SEPARATOR_CHARACTER + parts[CARD_ITEM_MENU_ITEM], parts[CARD_ITEM_MENU_ITEM]);
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

	private void addCardTendersKeyValuestoMap(BufferedReader br) {

		line = null;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\t", -1);

					cardTenderMap.put(parts[CARD_TENDER_TERR_CD] + SEPARATOR_CHARACTER + parts[CARD_TENDER_DESC], parts[CARD_TENDER_DESC]);
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

	private void addCtryKeyValuestoMap(BufferedReader br) {

		line = null;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\t", -1);

					ctryMap.put(parts[CTRY_CTRY_ISO_NU], parts[CTRY_CTRY_ISO2_ABBR_CD]);
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

	private void addValueMealComponentsKeyValuestoMap(BufferedReader br) {

		line = null;
		
		try {
			while ((line = br.readLine()) != null) {
				if (line != null && !line.isEmpty()) {
					parts = line.split("\\t", -1);

					valueMealComponentMap.put(parts[VM_COMPONENT_DATE] + "|" + parts[VM_COMPONENT_CTRY_ISO_NU] + "|" + parts[VM_COMPONENT_MENU_ITEM_NU], parts[VM_COMPONENT_COMPONENTS]);
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
}

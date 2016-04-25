package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.io.BytesWritable;
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
import org.xml.sax.XMLReader;

import com.mcd.gdw.daas.util.TimeUtil;

//public class POS35TDAExtractMapper extends Mapper<Text, BytesWritable, NullWritable, Text> {
	public class POS35TDAExtractMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		
		NumberFormat nbrfmt = NumberFormat.getInstance();


	StringBuffer xmlBuffer = null;
	StringBuffer newtext = new StringBuffer();
	StringBuffer currentKeyBuf = new StringBuffer();
	String currentKeyBufStr =null;
	byte[] valuebytes;
	String valuebytesstr;
	InputSource xmlSource = null;
	Document doc = null;
	DOMSource source = null;
	StringWriter stringWriter = null;
	StreamResult result = null;
	SAXParserFactory factory = SAXParserFactory.newInstance();
	private String OUTPUT_FIELD_SEPERATOR = "|";
	 
	Text outputText = new Text();
	private MultipleOutputs<NullWritable, Text> mos;
	
	 ArrayList<ItemDetails> itemDetailsforTransaction;
	 BigDecimal totaldiscAmtforTransaction;
	 BigDecimal totalPromoAmtforTransaction;
	 BigDecimal totalAmtforTransaction;
	 String gdwLgcyLclRfrDefCd = "missinggdwLgcyLclRfrDefCd";
	 double maxTenderAmount = 0.0;
	 

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
		
		nbrfmt.setMinimumFractionDigits(2);
		nbrfmt.setMaximumFractionDigits(2);
		nbrfmt.setRoundingMode(RoundingMode.HALF_EVEN);
	}

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		
		 DocumentBuilderFactory docFactory;
		    DocumentBuilder docBuilder;
		    InputSource xmlSource;
		    Document xmlDoc;
		    Element eleTLD;

		    try {
		      String[] recArr = value.toString().split("\t");    
		      docFactory = DocumentBuilderFactory.newInstance();
		      docBuilder = docFactory.newDocumentBuilder();
		      xmlSource = new InputSource(new StringReader(recArr[recArr.length -1]));
		      xmlDoc = docBuilder.parse(xmlSource);

		      eleTLD = (Element)xmlDoc.getFirstChild();

		      if ( eleTLD.getNodeName().equals("TLDLOG") ) {
		    	  gdwLgcyLclRfrDefCd = eleTLD.getAttribute("gdwLgcyLclRfrDefCd");
		        npParseXml(eleTLD, context);
		      }
		    } catch (Exception ex) {
		      Logger.getLogger(NpStldXmlMapper.class.getName()).log(Level.SEVERE, null, ex);
		    }
		
	}
	
//	@Override
//	protected void map(Text key, BytesWritable value,
//			org.apache.hadoop.mapreduce.Mapper.Context context)
//			throws IOException, InterruptedException {
//		
//		  DocumentBuilderFactory docFactory;
//		  DocumentBuilder docBuilder;
//		  InputSource xmlSource;
//		  Document xmlDoc;
//		
//		valuebytes =value.getBytes();
//		if(valuebytes != null && valuebytes.length > 0){
//		
//			xmlBuffer = new StringBuffer(valuebytes.length);
//			valuebytesstr = new String(valuebytes,"UTF-8");
//			
//			xmlBuffer.append(valuebytesstr);
//			
////			System.out.println(" xmlBuffer org " + xmlBuffer.toString() );
//		
//			
//			//dereference unnecessary objects
//			valuebytes = null;
//			valuebytesstr = null;
//					
//		
//			int outStart = 0;
//			int outSize = xmlBuffer.length();
//	
//			boolean foundStartEndTag = false;
//	
//			while (foundStartEndTag == false && outStart < outSize) {
//				if (xmlBuffer.charAt(outStart) == '<') {
//					foundStartEndTag = true;
//				} else {
//					outStart++;
//				}
//			}
//	
//			if (outStart > 0) {
//				xmlBuffer = xmlBuffer.delete(0, outStart);
//			}
//	
//			outStart = 0;
//			outSize = xmlBuffer.length();
//			int outSizeOrig = outSize;
//	
//			foundStartEndTag = false;
//	
//			while (foundStartEndTag == false && outSize > 0) {
//				if (xmlBuffer.charAt(outSize - 1) == '>') {
//					foundStartEndTag = true;
//				} else {
//					outSize--;
//				}
//			}
//	
//			if (outSize != outSizeOrig) {
//				xmlBuffer = xmlBuffer.delete(outSize, outSizeOrig);
//			}
//			if (outStart > 0) {
//				outSize -= (outStart - 1);
//			}
//			
////			System.out.println(" xmlBuffer " + xmlBuffer.toString());
//			try{
//				Element tldlog;
//				if(isXmlValid(xmlBuffer)){
//					 docFactory = DocumentBuilderFactory.newInstance();
//				     docBuilder = docFactory.newDocumentBuilder();
//				     xmlSource = new InputSource(new StringReader(xmlBuffer.toString()));
//				     xmlDoc = docBuilder.parse(xmlSource);
//				     
//				     tldlog = (Element)xmlDoc.getFirstChild();
//	
//				      if ( tldlog.getNodeName().equals("TLDLOG") ) {
//				        npParseXml(tldlog, context);
//				      }
//	
//				}
//			}catch(Exception ex){
//				ex.printStackTrace();
//			}finally{
//				
//				xmlBuffer.setLength(0);
//				xmlBuffer = null;
//				
//				xmlDoc = null;
//				xmlSource = null;
//				docBuilder = null;
//				docFactory = null;
//				
//			}
//		}
//	}
	
	 private void npParseXml(Element eleTLDLog,
             Context context) throws IOException, InterruptedException {
		 
		 NodeList nlTransactions;
		 Element transactionNode;
		 String terrCd;
		 nlTransactions = eleTLDLog.getChildNodes();
		 
		 terrCd = eleTLDLog.getAttribute("gdwTerrCd");
		 
		
		 
		 if (nlTransactions != null && nlTransactions.getLength() > 0 ) {
		        for (int indx=0; indx < nlTransactions.getLength(); indx++ ) {
		        	
		          maxTenderAmount = 0.0;
		        	
		          if ( nlTransactions.item(indx).getNodeType() == Node.ELEMENT_NODE) {
		            transactionNode = (Element)nlTransactions.item(indx);

		            if ( transactionNode.getNodeName().equalsIgnoreCase("transaction") ) {
		            	
		            	itemDetailsforTransaction = new ArrayList<ItemDetails>();
		            	totaldiscAmtforTransaction = new BigDecimal(0);
		            	totalPromoAmtforTransaction = new BigDecimal(0);
		            	totalAmtforTransaction = new BigDecimal(0);
		            	parseNode(transactionNode, terrCd,context);
		            }
		          }
		        }
		      }
	 }
	 
	  public class ItemDetails {
		    public int menuItemNo;
		    public BigDecimal price;
		    public int qty;
		    public int itemType;
		    public int qtyPromo;
		    public int qtyVoid;
		    public int basePlu;
//		    public BigDecimal totalDiscount;
//		    public BigDecimal totalPromo;
//		    public String tenderName;

		    public ItemDetails() {
		      
		      qty = 0;
		      itemType = 0;
		      qtyPromo = 0;
		     
		    }
		    
		    public String toString(){
		    	
		    	return "menuItemNo - "+ menuItemNo + " price - "+price + " qty - " + qty + " itemType - "+itemType +
		    			" qtyPromo - "+ qtyPromo + " qtyVoid - "+qtyVoid + " basePlu - "+ basePlu ;
		    }
		  }
	  
	 private void parseNode(Element transaction,String terrCd,Context context) throws IOException, InterruptedException {
		 
		 try{
			 
			 String storeId = "";
//			 String gdwLgcyLclRfrDefCd = "missinggdwLgcyLclRfrDefCd";
			 String regId= "";
			 String orderNbr= "";
			 String businessDt= "";
			 String orderDt= "";
			 String orderTimeStr= "";
			 long orderTime = 0;
			 long endTime = 0;
			 String posTypeCd= "";
			 BigDecimal taxAmt = new BigDecimal(0);
			 String takeId = "";
			 int splTrnId = 0;
			 BigDecimal totalDiscount = new BigDecimal(0);
			 BigDecimal totalpromoQtyPrice = new BigDecimal(0);
			 BigDecimal totalAmount = new BigDecimal(0) ;
			 String  menuPriceBasis = "N";//read dist cache
			 String paymentMethod= "0";
//			 String terrCd;
			 
			 
			 BigDecimal grandTotalAmount = new BigDecimal(0);
			 String taxRounding = "0";
			 
			 NodeList transChldNodes = transaction.getChildNodes();
			 Node childNode;
			 String chldNodeValue;
			 boolean tenderdetail = false;
			 for(int indx=0;indx<transChldNodes.getLength();indx++){
//				 System.out.println( "  transChldNodes.item(indx) " + transChldNodes.item(indx).getNodeType() + "  : " +transChldNodes.item(indx).getNodeName());
				 if(transChldNodes.item(indx).getNodeType() == Node.ELEMENT_NODE){
					 childNode = (Node)transChldNodes.item(indx);
					 chldNodeValue = childNode.getTextContent();
					 
					
					 
					 if(childNode.getNodeName().equalsIgnoreCase("storeid"))
						 storeId = chldNodeValue;
					 else if(childNode.getNodeName().equalsIgnoreCase("registerNo"))
						 regId = chldNodeValue;
					 else if(childNode.getNodeName().equalsIgnoreCase("receiptNo"))
						 orderNbr = chldNodeValue;
					 else  if(childNode.getNodeName().equalsIgnoreCase("date")){
						 
						 businessDt = chldNodeValue;
						 orderDt = chldNodeValue;//same as businessdate??
					 
					 }else  if(childNode.getNodeName().equalsIgnoreCase("time")){
						 if(chldNodeValue != null && !chldNodeValue.isEmpty()){
							 
							 orderTimeStr = chldNodeValue.substring(0,4);
							 orderTime = getTimeinMills(orderDt,chldNodeValue,false);
						 }
					 }else  if(childNode.getNodeName().equalsIgnoreCase("endtime")){
						 if(chldNodeValue != null && !chldNodeValue.isEmpty()){
							 endTime = getTimeinMills(businessDt,chldNodeValue,true);
						 }
					 }
					 else  if(childNode.getNodeName().equalsIgnoreCase("postype"))
						 posTypeCd = chldNodeValue;
					 else  if(childNode.getNodeName().equalsIgnoreCase("taxamt")){
						
//						 String taxAmtStr = chldNodeValue.substring(0,chldNodeValue.length()-2)+"."+chldNodeValue.substring(chldNodeValue.length()-2);
						 double taxAmtDbl = Double.parseDouble(chldNodeValue)/100;
						 taxAmt = new BigDecimal(taxAmtDbl);
					 }
					 else  if(childNode.getNodeName().equalsIgnoreCase("type"))
						 takeId = chldNodeValue;
					 else  if(childNode.getNodeName().equalsIgnoreCase("saletype")){
						 if(chldNodeValue != null && !chldNodeValue.isEmpty()){
							 int saleTypeint = Integer.parseInt(chldNodeValue);
							 int divideby = 100;
							 if(saleTypeint > 999) divideby =1000;
							 
							 int recalledInd = saleTypeint / divideby;
							 splTrnId = saleTypeint% divideby;
						 }
					 }
					 else  if(childNode.getNodeName().equalsIgnoreCase("totalamt")){
						 String taxAmtStr = chldNodeValue.substring(0,chldNodeValue.length()-2)+"."+chldNodeValue.substring(chldNodeValue.length()-2);
						 totalAmount = new BigDecimal(taxAmtStr);
					 }
					 else  if(childNode.getNodeName().equalsIgnoreCase("tenderdetail")){
						 paymentMethod = getPaymentMethod(childNode,paymentMethod);
						 tenderdetail = true;
						
					 }else  if(childNode.getNodeName().equalsIgnoreCase("item")){
						 prepareItemList(childNode);
					 }
					 
				 }
			 }
			 if(!tenderdetail){
				 context.getCounter("Count","MissingTenderDetail").increment(1);
			 }
			 
			 BigDecimal totalqtyprice = new BigDecimal(0);
			 
			 BigDecimal totalvoidQtyPrice = new BigDecimal(0);
			 BigDecimal taxPercent = new BigDecimal(0);
			 BigDecimal curqtyPrice = new BigDecimal(0);
			 BigDecimal curvoidPrice = new BigDecimal(0);
			
			 for(ItemDetails itemDetail:itemDetailsforTransaction){
				
				 curqtyPrice = new BigDecimal(itemDetail.qty).multiply(itemDetail.price);
				 totalqtyprice = totalqtyprice.add( curqtyPrice);
				 
				 totalpromoQtyPrice = totalpromoQtyPrice.add(new BigDecimal(itemDetail.qtyPromo).multiply(itemDetail.price));
				 
				 curvoidPrice = new BigDecimal(itemDetail.qtyVoid).multiply(itemDetail.price);
				 totalvoidQtyPrice  = totalvoidQtyPrice.add(curvoidPrice);
				 
				if(isSaleTypevalidforDiscount(splTrnId)){
					totalDiscount = totalDiscount.add(curqtyPrice).subtract(curvoidPrice);
					
				}
				
				
				 
			 }
			 
			 if(isSaleTypevalidforDiscount(splTrnId)){
				 totalDiscount = totalDiscount.subtract(totalAmount);
			 }
			 
			 //TODO verify if we need this
//			 if(menuPriceBasis.equalsIgnoreCase("N")){
//				 totalDiscount = totalDiscount.add(taxAmt);
//			 }
			 
			
			 
			 grandTotalAmount = totalqtyprice.subtract(totalpromoQtyPrice).subtract(totalvoidQtyPrice);
			 
			 StringBuilder pmixOutput = new StringBuilder();
			 
			 int actualQty = 0;
			 for(ItemDetails itemDetail:itemDetailsforTransaction){
				 
				 actualQty = itemDetail.qty-itemDetail.qtyVoid;
				 if(actualQty >= 0){//skip output record if quantity is less than zero; not sure of qty 0
					 
					
					 if(menuPriceBasis.equalsIgnoreCase("N")){
						 if(taxAmt.doubleValue() > 0 && grandTotalAmount.doubleValue() > 0){
							 taxPercent = taxAmt.divide(grandTotalAmount,6,RoundingMode.HALF_EVEN).multiply(new BigDecimal(100));
						 }else{
							 if(taxAmt.doubleValue() <= 0)
								 context.getCounter("Count","TaxPercentCalcIssueTaxAmt").increment(1);
							 if(grandTotalAmount.doubleValue() <=0 )
								 context.getCounter("Count","TaxPercentCalcIssueTotAmt").increment(1);
						 }
					 }else{
						 if(grandTotalAmount.subtract(taxAmt).compareTo(new BigDecimal(0)) > 0){
							 taxPercent = taxAmt.divide( (grandTotalAmount.subtract(taxAmt)),6,RoundingMode.HALF_EVEN).multiply(new BigDecimal(100));
							 
						 }
					 }
					 
					
					 taxPercent = taxPercent.setScale(4, RoundingMode.HALF_EVEN);
					 
					 pmixOutput.setLength(0);
					 pmixOutput.append(Integer.parseInt(storeId)).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(Integer.parseInt(regId)).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(businessDt).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(Integer.parseInt(orderNbr)).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(itemDetail.menuItemNo).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(itemDetail.price).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(actualQty).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(itemDetail.qtyPromo).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(taxPercent).append(OUTPUT_FIELD_SEPERATOR);
					 pmixOutput.append(taxRounding);
					 
					 mos.write("TDAPMIX", NullWritable.get(), new Text(pmixOutput.toString()));
				}
			 }
			 
			 totalDiscount = totalDiscount.setScale(2,RoundingMode.HALF_EVEN);
			 totalpromoQtyPrice = totalpromoQtyPrice.setScale(2,RoundingMode.HALF_EVEN);
			 grandTotalAmount = grandTotalAmount.setScale(2,RoundingMode.HALF_EVEN);
			 
			 StringBuilder sb = new StringBuilder();
			 sb.append(Integer.parseInt(storeId)).append(OUTPUT_FIELD_SEPERATOR);
//			 sb.append(gdwLgcyLclRfrDefCd).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(Integer.parseInt(regId)).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(Integer.parseInt(orderNbr)).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(businessDt).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(orderDt).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(orderTimeStr).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(posTypeCd).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(takeId).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(splTrnId).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(nbrfmt.format(totalDiscount)).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(nbrfmt.format(totalpromoQtyPrice)).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(nbrfmt.format(grandTotalAmount)).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append("0").append(OUTPUT_FIELD_SEPERATOR);//hold time
			 sb.append(TimeUtil.getTimeinMMSS((endTime-orderTime))).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(menuPriceBasis).append(OUTPUT_FIELD_SEPERATOR);
			
			 sb.append(Integer.parseInt(paymentMethod)).append(OUTPUT_FIELD_SEPERATOR);
			 sb.append(terrCd);
			 
			 outputText.clear();
			 outputText.set(sb.toString());
//			 context.write(NullWritable.get(), outputText);
			 
			 mos.write("TDASALES", NullWritable.get(),outputText);
			 
		 }catch(Exception ex){
			 ex.printStackTrace();
		 }
		 
	 }
	 
	 private boolean isSaleTypevalidforDiscount(int saleType){
		 if(saleType == 16  || saleType == 17 || saleType == 18 || saleType == 19|| saleType == 20
				 || saleType == 22 || saleType == 24 || saleType == 32 || saleType == 33)
			 return true;
		 
		 return false;
	 }
	 
	 private long getTimeinMills(String dateinyyyymmdd,String timeinhhmmss,boolean adjustHour){
		 
//		 System.out.println(" dateinyyyymmdd "+ dateinyyyymmdd + " timeinhhmmss " + timeinhhmmss);
		 String year = dateinyyyymmdd.substring(0,4);
		 String month = dateinyyyymmdd.substring(4,6);
		 String day = dateinyyyymmdd.substring(6);
		 String hours = timeinhhmmss.substring(0,2);
		 String minutes = timeinhhmmss.substring(2,4);
		 String seconds = timeinhhmmss.substring(4);
		 
		 Calendar cal = Calendar.getInstance();
		 
		 cal.set(Calendar.YEAR, Integer.parseInt(year));
		 cal.set(Calendar.MONTH, Integer.parseInt(month)+1);
		 cal.set(Calendar.DATE, Integer.parseInt(day));
		 
		 
		 if(adjustHour && Integer.parseInt(hours) == 0)
			 hours = "24";
		 cal.set(Calendar.HOUR, Integer.parseInt(hours));
		 cal.set(Calendar.MINUTE, Integer.parseInt(minutes));
		 cal.set(Calendar.SECOND,Integer.parseInt(seconds));
		 
		 return cal.getTimeInMillis();
	 }
	 
	 private void prepareItemList(Node itemNode){
		 try{
		 
		 NodeList itemChldNodes = itemNode.getChildNodes();
		 
		 Node itemChldNode;
		 String itemCldNodeValue;
		 if(itemChldNodes != null && itemChldNodes.getLength() > 0){
			 ItemDetails itemDetails = new ItemDetails();
			 
			 
			 int promoQty = 0;
			 int qty = 0;
			 
			
			 for(int indx=0;indx<itemChldNodes.getLength();indx++){
				
				 itemChldNode = (Node)itemChldNodes.item(indx);
				 itemCldNodeValue = itemChldNode.getTextContent();
				 
				
				 
				 if(itemChldNode.getNodeName().equalsIgnoreCase("menuitemno")){
					 itemDetails.menuItemNo = Integer.parseInt(itemCldNodeValue);
				 }else  if(itemChldNode.getNodeName().equalsIgnoreCase("price")){
					 String priceStr = itemCldNodeValue.substring(0,itemCldNodeValue.length()-2)+"."+itemCldNodeValue.substring(itemCldNodeValue.length()-2);
					 itemDetails.price = new BigDecimal(priceStr);
				 }else  if(itemChldNode.getNodeName().equalsIgnoreCase("qty")){
					 itemDetails.qty = Integer.parseInt(itemCldNodeValue);
					 qty = itemDetails.qty;
				 }else  if(itemChldNode.getNodeName().equalsIgnoreCase("itemtype")){
					 itemDetails.itemType = Integer.parseInt(itemCldNodeValue);
				 }else  if(itemChldNode.getNodeName().equalsIgnoreCase("promqty")){
					 itemDetails.qtyPromo = Integer.parseInt(itemCldNodeValue);
					 promoQty =  itemDetails.qtyPromo;
				 }else  if(itemChldNode.getNodeName().equalsIgnoreCase("voidqty")){
					 itemDetails.qtyVoid = Integer.parseInt(itemCldNodeValue);
				 }else  if(itemChldNode.getNodeName().equalsIgnoreCase("baseplu")){
					 itemDetails.basePlu = Integer.parseInt(itemCldNodeValue);
				 }
				
				
				
			} 
//			 if(promoQty > 0){
//				 itemDetails.qty = itemDetails.qty - promoQty;
//			 }
//			 System.out.println( " itemDetails : " + itemDetails.toString());
			
			 itemDetailsforTransaction.add(itemDetails);
		 }
		 }catch(Exception ex){
			 ex.printStackTrace();
		 }
	 }
	 
	 private String getPaymentMethod(Node tenderdetail,String curPaymentType){
		 String paymentMethod = "";
		 double tenderAmt = 0.0;
		 
//		 System.out.println( " called with " + maxTenderAmount + " : " + curPaymentType);
		 try{
		  NodeList nlTenders = tenderdetail.getChildNodes();
		  Node tenderCldNode;
		  
		 boolean foundTenderAmt = false;
		  
	      if ( nlTenders !=null && nlTenders.getLength() > 0 ) {
	        for (int idxTenders=0; idxTenders < nlTenders.getLength(); idxTenders++ ) {
	          if ( nlTenders.item(idxTenders).getNodeType() == Node.ELEMENT_NODE ) {
	        	  tenderCldNode = (Node)nlTenders.item(idxTenders);
	        	  
	        	  if(tenderCldNode.getNodeName().equalsIgnoreCase("tendertype")){
	        		  paymentMethod = tenderCldNode.getTextContent();
	        	  }else  if(tenderCldNode.getNodeName().equalsIgnoreCase("tenderamt")){
	        		  if(tenderCldNode.getTextContent() != null && !tenderCldNode.getTextContent().isEmpty())
	        			  tenderAmt = Double.parseDouble(tenderCldNode.getTextContent())/100;
	        		  
	        		  foundTenderAmt = true;
	        		 
	        	  }
	          
	        	  if(foundTenderAmt){
	//	        	  System.out.println( " tenderAmt   " +  tenderAmt + " maxTenderAmount " + maxTenderAmount + " paymentMethod " + paymentMethod);
				      if(tenderAmt >= maxTenderAmount){
				    	  curPaymentType = paymentMethod ;
						  maxTenderAmount = tenderAmt;
					  }else{
						  paymentMethod = curPaymentType;
					  }
	        	  }
	          }
	          
	          
	          
	        }
	        
	        
	      }
	      
	      
		 }catch(Exception ex){
			 ex.printStackTrace();
		 }
		 return paymentMethod;
	 }
	
public boolean isXmlValid(StringBuffer filebuf) {
		
		if(1 == 1) return true;
		
		SAXParser parser  = null;
		XMLReader reader = null;
		InputSource xmlSource = null;
		String tempxml = null;
		StringReader sr = null;
		try {
			factory.setValidating(true);
			factory.setNamespaceAware(true);

			parser = factory.newSAXParser();

			reader = parser.getXMLReader();
			tempxml = filebuf.toString();
			sr = new StringReader(tempxml);
			xmlSource = new InputSource(sr);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		} finally {
			
			sr.close();
			tempxml   = null;
			xmlSource = null;
			reader    = null;
			parser    = null;
			sr = null;
		}

		return true;
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		mos.close();
	}
	
	
}

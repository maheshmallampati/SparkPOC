import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.mapreduce.POS35TDAExtractMapper.ItemDetails;


public class TestGetPaymentMethod {
	
	ArrayList<ItemDetails> itemDetailsforTransaction  = new ArrayList<ItemDetails>();
	
	 double maxTenderAmount = 0.0;
	 BigDecimal maxTenderAmountBg = BigDecimal.ZERO;
	public static void main(String[] args){
		TestGetPaymentMethod testGetPaymentMethod = new TestGetPaymentMethod();
		String fileName = "C:/Users/mc32445/Desktop/"+args[0];
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		Document document =null;
		InputSource xmlSource = null;
		try{
			
			  DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
	          DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
	          Document doc = docBuilder.parse (new File(fileName));
	          
	          NodeList tranChilds = doc.getChildNodes().item(0).getChildNodes();
	          Node tenderDetail = null;
	          String paymentMethod ="";
	          for (int idxTenders=0; idxTenders < tranChilds.getLength(); idxTenders++ ) {
//	        	  System.out.println( " tranChilds.item(idxTenders) " + tranChilds.item(idxTenders).getNodeName());
//	        	  if(tranChilds.item(idxTenders).getNodeType() == Node.ELEMENT_NODE && 
//	        			  tranChilds.item(idxTenders).getNodeName().equalsIgnoreCase("tenderdetail")){
//	        		  tenderDetail = tranChilds.item(idxTenders);
//	        		  
////	        		  System.out.println(" tenderDetail " + tenderDetail.getNodeName());
//	     	         
//	     	          paymentMethod = testGetPaymentMethod.getPaymentMethod(tenderDetail,paymentMethod);
//	     	          
//	     	          System.out.println("paymentMethod adadads a - " +paymentMethod);
//	     	          
////	        		  break;
//	        	  }
	      
	        	  if(tranChilds.item(idxTenders).getNodeType() == Node.ELEMENT_NODE && 
	        			  tranChilds.item(idxTenders).getNodeName().equalsIgnoreCase("item")){
	        		  
	        		  testGetPaymentMethod.prepareItemList(tranChilds.item(idxTenders));
	        		  
	        	  }
	          }
	          
	          testGetPaymentMethod.getQtys();
	            
		}catch(Exception ex){
			ex.printStackTrace();
			System.out.println(" DOM validation returning false ");
		
		}finally{
			document = null;
			builder = null;
			factory = null;
			
					
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
	  
	 private void prepareItemList(Node itemNode){
		 try{
		 
			 System.out.println(" prepareItemList  called " +itemNode.getNodeName());
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
		
			 if(promoQty > 0){
				 itemDetails.qty = itemDetails.qty - promoQty;
			 }
			 
			 System.out.println( " itemDetails : " + itemDetails.toString());
			 itemDetailsforTransaction.add(itemDetails);
		 }
		 }catch(Exception ex){
			 ex.printStackTrace();
		 }
	 }
	 
	 BigDecimal totalpromoQtyPrice = new BigDecimal(0);
	 
	private void getQtys(){
		
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
			 
			
		 }
		 
	}
		 private String getPaymentMethod(Node tenderdetail,String curPaymentType){
			 String paymentMethod = "";
			 double tenderAmt = 0.0;
			 BigDecimal tenderAmtbg = BigDecimal.ZERO;
			 
//			 System.out.println( " called with " + maxTenderAmount + " : " + curPaymentType);
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
		        		  
		        		  if(tenderCldNode.getTextContent() != null && !tenderCldNode.getTextContent().isEmpty()){
//		        			  tenderAmt = Double.parseDouble(tenderCldNode.getTextContent());
		        		  
		        		      tenderAmtbg = new BigDecimal(Double.parseDouble(tenderCldNode.getTextContent())/100);
		        		      tenderAmtbg.setScale(2, RoundingMode.HALF_EVEN);
		        		  }
		        		  	
		        		  foundTenderAmt = true;
		        	  }
		          
		        	  if(foundTenderAmt){
//			        	  System.out.println( " tenderAmt   " +  tenderAmtbg + " maxTenderAmount " + maxTenderAmountBg + " paymentMethod " + paymentMethod + " curPaymentType "+curPaymentType );
//					      System.out.println (" comp val " + tenderAmtbg.compareTo(maxTenderAmountBg));
			        	  if(tenderAmtbg.compareTo(maxTenderAmountBg) > 0){
					    	  curPaymentType = paymentMethod ;
					    	  maxTenderAmountBg = tenderAmtbg;
						  }
			        	  else{
							  System.out.println("setting paymentMethod to  " +curPaymentType);
							 
							  paymentMethod = curPaymentType;
						  }
		        	  }
		          }
		          
		          
		          
		        }
		        
		        
		      }
		      
		      
			 }catch(Exception ex){
				 ex.printStackTrace();
			 }
			 System.out.println("returning "+paymentMethod);
			 return paymentMethod;
		 }
	
}

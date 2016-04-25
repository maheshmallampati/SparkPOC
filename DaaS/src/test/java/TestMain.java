import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.MessageDigest;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;



public class TestMain {
	public static void main(String[] args){
		try{
			
		
//		SimpleEncryptAndDecrypt decrypt = new SimpleEncryptAndDecrypt();
//		
//		System.out.println( " asdasdsa dasd    " + decrypt.encryptAsHexString("H@ppyMe@l"));
//		
//		
//		System.out.println( " decrypted " + decrypt.decryptFromHexString(decrypt.encryptAsHexString("bigm@c11")));
//		StringBuffer sbf = new StringBuffer();
//		
//		sbf.append("Sateesh").append(" ");
//		
//		System.out.println(" sbf length " + sbf.length());
//		
//		System.out.println(" sbd string length " + sbf.toString().length());
//	
//		String key = args[0];
//		
//		System.out.println("key.hashCode() " + key.hashCode());
//		System.out.println("key.hashCode() & Integer.MAX_VALUE " + (key.hashCode() & Integer.MAX_VALUE));
//		System.out.println((key.hashCode() & Integer.MAX_VALUE) % 40);
		
//		String temp = "SCHPGDP0_156_3570031_400_20140323110002.csv";
//		String temp = "/daastest/testtdaanddaypartextracts/stldextract/stld/PMIX*";
//		String[] inputpathstrs = temp.split(",");
//		
//		for(String cachepathstr:inputpathstrs){
//			System.out.println( " cachepathstr " +cachepathstr);
//		}
//		int lastindx = temp.lastIndexOf("_");
//		System.out.println(temp.substring(lastindx+1,lastindx+9));
//		System.out.println(temp.substring(lastindx+4,lastindx+12));
//		System.out.println(temp.substring(4,11));
		
//		System.out.println(" " + 129%100 + " --- " + 129/100);
		
//		BigDecimal grandTotalAmount = new BigDecimal("7.90");
//		BigDecimal taxAmt= new BigDecimal("0.45");
//		
//		BigDecimal taxpct = taxAmt.divide( (grandTotalAmount.subtract(taxAmt)),6,RoundingMode.HALF_EVEN).multiply(new BigDecimal(100));
//		
//		System.out.println( " taxpct " + taxpct.floatValue());
//		
//	
//		
//		NumberFormat df = NumberFormat.getInstance();
//		df.setMinimumFractionDigits(2);
//		df.setMaximumFractionDigits(2);
//		df.setRoundingMode(RoundingMode.CEILING);
//			String temp ="0000,  1004,20140317,1,0x00000001,2,700,151.51,10,0.00,0,0.00,0,0.00,0,0.00,0,0.00,0,0.00,151.51,0.00,0.00,0";
//			
//			FileReader fr = new FileReader(new File("C:/Users/mc32445/Desktop/DaaS/APMEA/R2D2/SampleChinaData/CSHHRGP0/CSHHRGP0/CSHHRGP0_156_1400004_400_20140318022651.csv"));
//			
//			BufferedReader br = new BufferedReader(fr);
//			
//			StringBuilder sb = new StringBuilder();
//			String line;
//			while( (line = br.readLine()) != null){
//				line = line.replace("\"","");
//				System.out.println(line);
//				String[] values = line.split("0x2C");
//				for(int i=0;i<values.length;i++){
//					
//					System.out.println(values[i]);
//					
//				}
//				break;
//			}
			
//		
//			
//			String regex = "\"(\\([^)]*,\\)|[^\"])*,\"";
//			Pattern p = Pattern.compile(regex);
//			Matcher m = p.matcher(temp);
//			while(m.find()) {
//			    System.out.println(args[0].substring(m.start(),m.end()));
//			}
		
			
			
			
			
//			try{
//				
//			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
//				DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
//				
//				InputSource xmlSource = null;
//				
//				try{
//					xmlSource = new InputSource("/config.xml");
//				   
//					int size = xmlSource.getByteStream().read();
//					if(size == 0){
//						System.out.println(" config file not found asdadasd ");
//					}
//				}catch(Exception ex){
//					System.out.println(" config file not found ");
//					ex.printStackTrace();
//				}
//				
////				InputSource xmlSource = null;
//				InputStream iStream = TestMain.class.getResourceAsStream("/config.xml");
//				try{
////					xmlSource = new InputSource("\\/config.xml");
//				   
//					
//				}catch(Exception ex){
//					System.out.println(" config file not found ");
//					ex.printStackTrace();
//				}
//				
//				
//				Document doc = docBuilder.parse(iStream);
//				
//
//				Element eleRoot = (Element)doc.getFirstChild();
//				
//				System.out.println(" eleRoot " + eleRoot.getNodeName());
//			}catch(Exception ex){
//				
//				ex.printStackTrace();
//			}
//			
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//			SimpleDateFormat sdf2= new SimpleDateFormat("yyyy-MM-dd");
//			System.out.println( sdf2.format(sdf.parse("20150102")));
			
			String[] temp = new String[5];
			temp[0] = "sateesh";
			temp[1] = "\t";
			temp[2] = "p";
			temp[3] = "\t";
			temp[4] = "K";	
		
			StringBuffer sbf = new StringBuffer();
			sbf.append(Arrays.asList(temp));
			
			System.out.println(sbf.toString());
			
//			System.out.println(" hashkey " +  "05165".hashCode()) ;
			if(1 == 1 ) return;
//			
//			Calendar cal = Calendar.getInstance();
//			Date dt = sdf.parse("20140322");
//			cal.setTime(dt);
//			
//			System.out.println ( " cal " + cal.getTime().toString() + " adsad "+cal.get(Calendar.DAY_OF_WEEK));
//			System.out.println(StringUtils.leftPad("7777777",5,'0'));
			
//			 SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
//			 SimpleDateFormat sdfa = new SimpleDateFormat("yyyyMMddHHmmss");
//			 SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
//			 SimpleDateFormat sdfnoms = new SimpleDateFormat("yyyyMMddHHmmss");
//			 
//			 String orderTimestamp = "20140321173257"; 
//			 
//			 System.out.println(sdfa.parse("2014032053257"));
//			 
//			 System.out.println(sdf2.format(sdfa.parse("20140321183257")));
//			 
//			  SimpleDateFormat sdf3 = new SimpleDateFormat("yyyyMMdd");
//			  
//			  SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd");
//			  
//			  System.out.println(sdf3.parse("20140507"));
//			  
//			  System.out.println(sdf4.format(sdf3.parse("20140507")));
//			 
//			  
//			
//			  Date datetemp;
//			  
//			  try{
//				  datetemp = sdf.parse(orderTimestamp);
//			  }catch(ParseException ex){
//				  datetemp = sdfnoms.parse(orderTimestamp);
//			  }
//	    	 
//			  Calendar cal = Calendar.getInstance();
//			  cal.setTime(datetemp);
//			  int orderHour = cal.get(Calendar.HOUR_OF_DAY);
//			  
//			  System.out.println( " orderHour " + orderHour);
			  
//			   String valueStr = "Single Window DT 6 Stack,Split Function,MDS in Front,Afternoon,14:00 - 17:00,1.391,10,,Crew Pour No Drink Tower,Beverage Cell,Grill Wall,Single-sided MFY,\"Breakfast Inline Q-ing, Regular Inline Q-ing\",No Arch Dispenser,Integrated McCafe Crew Pour,Integrated Beverage Cell,McCafe Transactions,0,10,,,,,,,1,,,McCafe,1,MCRG,,,";
//			   String vvv ="1018,407900_??????,1018,0,GRILL,QINGOVENCONFIGS,\"BREAKFAST INLINE Q-ING, REGULAR INLINE Q-ING\",,15:41.2";
//			   String[] valuesArray = vvv.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);
////			  
//			  
//			 
//			   String carddata = "CASHLESS:Amex|***********1061|08/14|593731|0|0|279452|18693802||0|8.94|0|For gift card balance call\n1-877-458-2200\n|279452|";
//			  System.out.println(formateCashLessData(carddata));
//			  System.out.println(" aasdasd ");
			  
//			  String path = "DetailedSOSRxD12636RxD12620140802-r-00037.gz";
//			  int indx = path.lastIndexOf(".gz");
//			  
//			  System.out.println(path.substring(indx -5,indx));
			
//			int temp = (672 << 20);
//			System.out.println(" temp " + temp);
//			String temp = "SALES-201409011";
//			
//			System.out.println(temp.substring(6,14));
			
			Set itemValues = new HashSet();
			
		 JSONObject itemJSObj = new JSONObject();  
         JSONObject itemobj = new JSONObject();

         
         JSONArray itemValsJSArr = new JSONArray();
	     JSONObject joj = new JSONObject();
	    
		  joj.put("out_parent_node","Item");
	  	  joj.put("storeId","gdwLgcyLclRfrDefCd");
	  	  joj.put("businessDate","gdwBusinessDate");
	  	  joj.put("EventRegisterId","eventRegisterId");
	  	  joj.put("EventTime","eventTimestamp");
	  	  joj.put("status","trxSaleStatus");
  	  
  	  	itemValsJSArr.add(joj);
         
  	  	
  	  joj = new JSONObject();
	    
	  joj.put("out_parent_node","Item2");
  	  joj.put("storeId","gdwLgcyLclRfrDefCd2");
  	  joj.put("businessDate","gdwBusinessDate2");
  	  joj.put("EventRegisterId","eventRegisterId2");
  	  joj.put("EventTime","eventTimestamp2");
  	  joj.put("status","trxSaleStatus2");
	  
	  	itemValsJSArr.add(joj);
	  	
//  	  	itemValues.add(itemValsJSArr);
  	  
  	  	itemobj.put("Item",itemValsJSArr);
         
//         itemJSObj.put("Data", (Object)jarr);
         itemJSObj.put("Data", itemobj);
         
         System.out.println(itemJSObj.toString());
		        
		}catch(Exception ex){
			ex.printStackTrace();
		}
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
	 
//	 public static void getMD5(String[] args)throws Exception
//	    {
//	        MessageDigest md = MessageDigest.getInstance("MD5");
//	        
//	        
//	        FileInputStream fis = new FileInputStream("c:\\loging.log");
//	 
//	        byte[] dataBytes = new byte[1024];
//	 
//	        int nread = 0; 
//	        while ((nread = fis.read(dataBytes)) != -1) {
//	          md.update(dataBytes, 0, nread);
//	        };
//	        byte[] mdbytes = md.digest();
//	 
//	        //convert the byte to hex format method 1
//	        StringBuffer sb = new StringBuffer();
//	        for (int i = 0; i < mdbytes.length; i++) {
//	          sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
//	        }
//	 
//	        System.out.println("Digest(in hex format):: " + sb.toString());
//	 
//	        //convert the byte to hex format method 2
//	        StringBuffer hexString = new StringBuffer();
//	    	for (int i=0;i<mdbytes.length;i++) {
//	    		String hex=Integer.toHexString(0xff & mdbytes[i]);
//	   	     	if(hex.length()==1) hexString.append('0');
//	   	     	hexString.append(hex);
//	    	}
//	    	System.out.println("Digest(in hex format):: " + hexString.toString());
//	    }
}

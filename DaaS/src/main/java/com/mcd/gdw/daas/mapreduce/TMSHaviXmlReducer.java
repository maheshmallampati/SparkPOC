package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.derby.impl.sql.compile.NextSequenceNode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.daas.DaaSConstants;

/**
 * 
 * @author mc41946 
 * Modified for Offer Redemption Extract: MultipleOutput is implemented to write the different output files
 * for territory codes.
 *
 */
public class TMSHaviXmlReducer extends Reducer<Text,Text,NullWritable,Text> {

	
	HashMap<String,TreeMap<String,HashMap<String,String>>> ordpaItemCodeMap = new HashMap<String,TreeMap<String,HashMap<String,String>>>();
	Text value;
	String[] valArr;
	HashMap<String,String> itemCodeSet = new HashMap<String,String>();
	TreeMap<String,HashMap<String,String>> paPromotionIdItemCodeMap = new TreeMap<String,HashMap<String,String>>();
	  private MultipleOutputs<NullWritable, Text> mos;
	  private final static String HAVI_OUTPUT_FILE_NAME_PREFIX = "DGCID"+DaaSConstants.SPLCHARTILDE_DELIMITER+"HGS"+DaaSConstants.SPLCHARTILDE_DELIMITER+"Redemption";
	  private String fileNameSeperator = "_";
	  String loopOrd;
	  String orderKey 		= "";
	  public class ItemDetails {
			
			private String redeemed="0";
			private String purchased="0";
			
			public ItemDetails(String redeemed
					             ,String purchased) {
				
				this.redeemed = redeemed;
				this.purchased = purchased;
				
			}	
			
			/**
			 * @return the redeemed
			 */
			public String getRedeemed() {
				return redeemed;
			}

			/**
			 * @param redeemed the redeemed to set
			 */
			public void setRedeemed(String redeemed) {
				this.redeemed = redeemed;
			}

			/**
			 * @return the purchased
			 */
			public String getPurchased() {
				return purchased;
			}

			/**
			 * @param purchased the purchased to set
			 */
			public void setPurchased(String purchased) {
				this.purchased = purchased;
			}
			public String toString() {
				
				return(this.redeemed + "|" + this.purchased);
				
			}
		}
	
	  /**
	   * M-1939 Hadoop- Offer Redemption Extract changes for US Mobile.
	   * Initilaizing Multiple Outputs.
	   */
	 @Override
	  public void setup(Context context) throws IOException, InterruptedException{
		 try {
		 mos = new MultipleOutputs<NullWritable, Text>(context);
		 }
		 catch (Exception ex) {
		      ex.printStackTrace();
	 }
	 }
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		
		Iterator<Text> valIt = values.iterator();
		
		
		itemCodeSet.clear();
		paPromotionIdItemCodeMap.clear();
		ordpaItemCodeMap.clear();
		
		while(valIt.hasNext()){
			value = valIt.next();
			valArr = value.toString().split("\\|",-1);
			
			
			orderKey 		= valArr[0];
			String[] keyParts = key.toString().split("\\|");
			
			String key_part1 = keyParts[0] + DaaSConstants.PIPE_DELIMITER+keyParts[1]+DaaSConstants.PIPE_DELIMITER+keyParts[2];
			String buss_dt   = keyParts[2];
			
		
//			if(!"POS0003:395153600".equalsIgnoreCase(orderKey))
//				return;
			
			String paPromotionId 	= valArr[1];
			String itemCode 		= valArr[2];
		    String redeemedFlg= valArr[5];
			String purchasedFlg= valArr[6];
			String discountType=valArr[7];
//			String mapkey = orderKey+"_"+paPromotionId;
			
			if(ordpaItemCodeMap.containsKey(orderKey)){
				
				paPromotionIdItemCodeMap = ordpaItemCodeMap.get(orderKey);
				
				if(paPromotionIdItemCodeMap.containsKey(paPromotionId)){
					paPromotionIdItemCodeMap.get(paPromotionId).put(itemCode+ "|"+discountType,new ItemDetails(redeemedFlg,purchasedFlg).toString()); // Change Here
				}else{
					
					
					itemCodeSet = new HashMap<String,String>();
					
					itemCodeSet.put(itemCode+ "|"+discountType,new ItemDetails(redeemedFlg,purchasedFlg).toString()); //Change Here
					//new IncludeListKey(terrCd,lgcyLclRfrDefCd).toString()
					paPromotionIdItemCodeMap.put(paPromotionId, itemCodeSet);
				}
				
			}else{
				
				itemCodeSet = new HashMap<String,String>();
				itemCodeSet.put(itemCode+ "|"+discountType,new ItemDetails(redeemedFlg,purchasedFlg).toString());//change here
				paPromotionIdItemCodeMap.put(paPromotionId, itemCodeSet);
				
				ordpaItemCodeMap.put(orderKey, paPromotionIdItemCodeMap);
				
			}
			
		}
		
//		Iterator<String> tmpit = ordpaItemCodeMap.keySet().iterator();
//		String tmpKy;
//		TreeMap<String,HashSet<String>> tmpMap;
//		Iterator<String> tmpit2;
//		while(tmpit.hasNext()){
//			tmpKy = tmpit.next();
//			System.out.println( " Order Key : " + tmpKy);
//			tmpMap = ordpaItemCodeMap.get(tmpKy);
//
//			tmpit2 = tmpMap.keySet().iterator();
//			String paky;
//			while(tmpit2.hasNext()){
//				paky = tmpit2.next();
//				System.out.println( " PA PromotionID : " + paky + " size " + tmpMap.get(paky).size());
//			}
//			
//		}
		
		
		
		HashMap<String,Integer> paIdItemCodeCntMap = new HashMap<String,Integer>();
		String ordpaItemCodeMapKey;
		
		TreeMap<String,HashMap<String,String>> paPromotionIdItemCodeMap2;
		
		Text newValue = new Text();
		
		
		if(ordpaItemCodeMap != null && !ordpaItemCodeMap.isEmpty()){
			
			Iterator<String> keyIt = ordpaItemCodeMap.keySet().iterator();
			Iterator<String> paIt;
			
			while(keyIt.hasNext()){
				loopOrd = keyIt.next();
//				System.out.println ( "loop ing thru ord " + loopOrd);
				paPromotionIdItemCodeMap2 = ordpaItemCodeMap.get(loopOrd);
//				System.out.println( " paPromotionIdItemCodeMap2 size for ord " + loopOrd + " : " + paPromotionIdItemCodeMap2.size());
				
				paIt = paPromotionIdItemCodeMap2.keySet().iterator();
				String paId;
				String purchaseditemcode = "" ;
				String redeemeditemcode = "";
				boolean found = false;
				while(paIt.hasNext()){
					
					paId = paIt.next();
//					System.out.println ( " loop thru pa id " + paId);
					
					if(paId.isEmpty()){
//						System.out.println(" PA ID is empty " + paPromotionIdItemCodeMap2.get(paId).size());
						if(paPromotionIdItemCodeMap2.get(paId).size() > 1){
							purchaseditemcode = "";
						}else{
							//purchaseditemcode = (paPromotionIdItemCodeMap2.get(paId)).iterator().next();
							purchaseditemcode = (paPromotionIdItemCodeMap2.get(paId)).keySet().iterator().next();
							
						}
						found = true;
					}else{
//					System.out.println(" PA ID is not empty " +paId+ " - " + paPromotionIdItemCodeMap2.get(paId).size());
//					System.out.println("PA map "+ paPromotionIdItemCodeMap2.get(paId));
						  //redeemeditemcode = (paPromotionIdItemCodeMap2.get(paId)).keySet().iterator().next();
						 // Display elements -- Change Here
						for (Map.Entry<String, String> itemCodeMap : paPromotionIdItemCodeMap2.get(paId).entrySet()) {
	//						System.out.println("Key : " + itemCodeMap.getKey() + " Value : "+ itemCodeMap.getValue());
							if(itemCodeMap.getValue().split("\\|",-1)[0].equalsIgnoreCase("TRUE"))
							{
								redeemeditemcode=itemCodeMap.getKey().split("\\|",-1)[0];
								found=true;
//								System.out.println("redeemeditemcode-->"+redeemeditemcode);
							}
							if(itemCodeMap.getValue().split("\\|",-1)[1].equalsIgnoreCase("TRUE"))
							{
								purchaseditemcode=itemCodeMap.getKey().split("\\|",-1)[0];
	//							System.out.println("purchaseditemcode-->"+purchaseditemcode);
							}
						}
						
					      
						
//						System.out.println("purchased code  "+purchaseditemcode);
//						System.out.println("reedemed code  "+redeemeditemcode);
						/*if(!found ){
							purchaseditemcode = redeemeditemcode;
						}*/
						String[] keyParts = key.toString().split("\\|");
						
						String key_part1 = keyParts[0] + DaaSConstants.PIPE_DELIMITER+keyParts[1]+DaaSConstants.PIPE_DELIMITER+keyParts[2];
						String buss_dt   = keyParts[2];
						String fnlKey = purchaseditemcode+"_"+redeemeditemcode+"_"+loopOrd+"_"+buss_dt;
						//String fnlKey = purchaseditemcode+"_"+redeemeditemcode;
//						System.out.println( "*****************8fnlKey " + fnlKey);
						
						if(paIdItemCodeCntMap.containsKey(fnlKey)){
							paIdItemCodeCntMap.put(fnlKey, paIdItemCodeCntMap.get(fnlKey) +paPromotionIdItemCodeMap2.get(paId).size());
						}else{
							paIdItemCodeCntMap.put(fnlKey, paPromotionIdItemCodeMap2.get(paId).size());
						}
					}
					
				}
				
				
			}
			 
			
		}
		
		if(paIdItemCodeCntMap != null && paIdItemCodeCntMap.size() > 0){
			Iterator<String> tmpIt  = paIdItemCodeCntMap.keySet().iterator();
			String tmpFnlKey;
			String redeemedItem;
			String purchasedItem;
			
			String[] keyParts = key.toString().split("\\|");
		
			String key_part1 = keyParts[0] + DaaSConstants.PIPE_DELIMITER+keyParts[1]+DaaSConstants.PIPE_DELIMITER+keyParts[2];
			String terr_cd   = keyParts[3];
			
			//
			Date current_timeStamp = new Date();
			SimpleDateFormat extractedDateFormat = new SimpleDateFormat(
					"yyyyMMdd");			
			String extractedDate = extractedDateFormat
					.format(current_timeStamp);
			
			while(tmpIt.hasNext()){
				tmpFnlKey = tmpIt.next();
				
				purchasedItem = tmpFnlKey.split("_",-1)[0];
				redeemedItem  = tmpFnlKey.split("_",-1)[1];
				String order=tmpFnlKey.split("_",-1)[2];
				newValue.clear();
		
				
				//newValue.set(key_part1+DaaSConstants.PIPE_DELIMITER+purchasedItem+DaaSConstants.PIPE_DELIMITER+redeemedItem+DaaSConstants.PIPE_DELIMITER+paIdItemCodeCntMap.get(tmpFnlKey)+DaaSConstants.PIPE_DELIMITER+terr_cd+DaaSConstants.PIPE_DELIMITER+order);
				newValue.set(key_part1+DaaSConstants.PIPE_DELIMITER+purchasedItem+DaaSConstants.PIPE_DELIMITER+redeemedItem+DaaSConstants.PIPE_DELIMITER+paIdItemCodeCntMap.get(tmpFnlKey)+DaaSConstants.PIPE_DELIMITER+terr_cd);
				/**
				   * M-1939 Hadoop- Offer Redemption Extract changes for US Mobile.
				   * Creating Multiple output files based on the territory code.
				   */
				mos.write(HAVI_OUTPUT_FILE_NAME_PREFIX+DaaSConstants.SPLCHARTILDE_DELIMITER+terr_cd+DaaSConstants.SPLCHARTILDE_DELIMITER+extractedDate,NullWritable.get(),newValue);
				// context.write(NullWritable.get(),newValue);
			}
		}
		
		
		
	}
	/**
	   * M-1939 Hadoop- Offer Redemption Extract changes for US Mobile.
	   * Closing Multiple Output connection.
	   */
	 
	@Override 
	  protected void cleanup(Context contect) throws IOException, InterruptedException {
	    mos.close();
	  } 
	
	
	

}

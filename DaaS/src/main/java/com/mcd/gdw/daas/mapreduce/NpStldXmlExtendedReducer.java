package com.mcd.gdw.daas.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NpStldXmlExtendedReducer  extends Reducer<Text,Text,NullWritable,Text>{
	

	private MultipleOutputs<NullWritable, Text> mos;
	String generateExtraFields="";
	String outputFormat = "";
	String generateHeaders = "";
	  


	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<NullWritable, Text>(context);
		generateExtraFields=context.getConfiguration().get("GENERATE_EXTRA_FIELDS");
		outputFormat = context.getConfiguration().get("OUTPUT_FILE_FORMAT");
		generateHeaders = context.getConfiguration().get("GENERATE_HEADERS"); 
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String USHeader  = "Out_parent_node|StoreId|BusinessDate|EventRegisterId|EventTime|Status|POD|OrderTimestamp|OrderuniqueID|Orderkind|Orderkey|OrdersaleType|OrderTotalAmount|OrderTotalTax|ItemCode|ItemQty|ItemQtyPromo|ItemBPPrice|ItemBPTax|ItemBDPrice|ItemBDTax|temTotalPrice|ItemTotalTax|ItemDescription|ItemUnitPrice|ItemUnitTax|ItemPromoAppliedPromoID|ItemPromoAppliedPromotionCounter|Eligible|OriginalPrice|ItemPromoAppliedDiscountAmount|ItemPromotionAppliedDiscountType|OriginalPromoQty|OriginalProductCode|ItemPromoAppliedOfferID|OrderPromotionPromoID|OrderPromotionPromotionCounter|OrderPromotionDiscountType|OrderPromotionDiscountAmount|OrderPromotionOfferID|Exlusive|TagId|OrderOfferOfferID|Override|Applied|ClearAfterOverride|OrderOfferPromoID|CustomerId|OrderId|IsPaidMobileOrder|TenderQty|TenderValue|TenderCashlessCardProv|TenderCashlessAmount|TotalDiscount|TotalBD|DiscountId|DiscountType|TenderID|TenderKind|TenderName|TenderQuantity|TenderFacevalue|TenderAmount";
		String AUHeader = "Out_parent_node|StoreId|BusinessDate|EventRegisterId|EventTime|Status|POD|OrderTimestamp|OrderuniqueID|Orderkind|Orderkey|OrdersaleType|OrderTotalAmount|OrderTotalTax|ItemCode|ItemQty|ItemQtyPromo|ItemBPPrice|ItemBPTax|ItemBDPrice|ItemBDTax|temTotalPrice|ItemTotalTax|ItemDescription|ItemUnitPrice|ItemUnitTax|ItemPromoAppliedPromoID|ItemPromoAppliedPromotionCounter|Eligible|OriginalPrice|ItemPromoAppliedDiscountAmount|ItemPromotionAppliedDiscountType|OriginalPromoQty|OriginalProductCode|ItemPromoAppliedOfferID|OrderPromotionPromoID|OrderPromotionPromotionCounter|OrderPromotionDiscountType|OrderPromotionDiscountAmount|OrderPromotionOfferID|Exlusive|TagId|OrderOfferOfferID|Override|Applied|ClearAfterOverride|OrderOfferPromoID|CustomerId|OrderId|IsPaidMobileOrder|TenderQty|TenderValue|TenderCashlessCardProv|TenderCashlessAmount|TotalDiscount|TotalBD|DiscountId|DiscountType|TenderID|TenderKind|TenderName|TenderQuantity|TenderFacevalue|TenderAmount";
		String tsvHeader = "TERR_CD\tMCD_GBAL_LCAT_ID_NU\tPOS_BUSN_DT\tPOS_ORD_KEY_ID\tpos_itm_line_seq_nu\tLGCY_LCL_RFR_DEF_CD\tPOS_EVNT_RGIS_ID\tPOS_EVNT_TS\tPOS_TRN_STUS_CD\tPOS_AREA_TYP_SHRT_DS\tPOS_TRN_STRT_TS\tPOS_ORD_UNIQ_ID\tPOS_TRN_KIND_CD\tPOS_PRD_DLVR_METH_CD\tPOS_TOT_NET_TRN_AM\tPOS_TOT_TAX_AM\tSLD_MENU_ITM_ID\tSLD_MENU_ITM_ID\tPOS_ITM_TOT_QT\tPOS_ITM_PRMO_QT\tPOS_ITM_BP_PRC_AM\tPOS_ITM_BP_TAX_AM\tPOS_ITM_BD_TAX_AM\tPOS_ITM_BD_PRC_AM\tPOS_ITM_NET_TOT_AM\tPOS_ITM_TOT_TAX_AM\tPOS_ITM_UNT_PRC_AM\tPOS_ITM_UNT_TAX_AM\tITM_PRMO_APPD_DIGL_OFFR_ID\tITM_PRMO_APPD_OFFR_CTER_QT\tITM_PRMO_APPD_OFFR_ELIG_FL\tITM_PRMO_APPD_ORGN_PRC_AM\tITM_PRMO_APPD_DISC_AM\tITM_PRMO_APPD_DISC_TYP_CD\tITM_PRMO_APPD_ORGN_OFFR_QT\tITM_PRMO_APPD_ORGN_PRD_CD\tITM_PRMO_APPD_CUST_OFFR_ID\tPOS_PRMO_DIGL_OFFR_ID\tPOS_PRMO_CTER_QT\tPOS_PRMO_DISC_TYP_CD\tPOS_PRMO_DISC_AM\tPOS_PRMO_CUST_OFFR_ID\tPOS_PRMO_XCLU_FL\tCUST_OFFR_TAG_ID\tCUST_OFFR_ID\tCUST_OFFR_OVRD_FL\tCUST_OFFR_APPD_FL\tCUST_OFFR_CLER_AFT_OVRD_FL\tCUST_OFFR_DIGL_OFFR_ID\tCMIN_CUST_ID\tCMIN_ORD_ID\tCMIN_PAID_MOBL_ORD_FL\tCMIN_TEND_CNT_QT\tCMIN_TEND_VAL_AM\tCMIN_CSHL_CARD_PVDR_TYP_CD\tCMIN_CSHL_AM\tCMIN_TOT_DISC_AM\tCMIN_TOT_B4_DISC_AM\tCMIN_DIGL_OFFR_ID\tCMIN_DISC_TYP_CD\tPOS_TEND_TYP_CD\tPOS_PYMT_METH_TYP_CD\tPOS_PYMT_METH_DS_TX\tPOS_TEND_QT\tPOS_TEND_FACE_VAL_AM\tPOS_TEND_AM";
		
		if(generateExtraFields.equalsIgnoreCase("TRUE"))
		{
			USHeader  = "Out_parent_node|StoreId|BusinessDate|EventRegisterId|EventTime|Status|POD|OrderTimestamp|OrderuniqueID|Orderkind|Orderkey|OrdersaleType|OrderTotalAmount|OrderTotalTax|ItemCode|ItemQty|ItemQtyPromo|ItemBPPrice|ItemBPTax|ItemBDPrice|ItemBDTax|temTotalPrice|ItemTotalTax|ItemDescription|ItemUnitPrice|ItemUnitTax|ItemPromoAppliedPromoID|ItemPromoAppliedPromotionCounter|Eligible|OriginalPrice|ItemPromoAppliedDiscountAmount|ItemPromotionAppliedDiscountType|OriginalPromoQty|OriginalProductCode|ItemPromoAppliedOfferID|OrderPromotionPromoID|OrderPromotionPromotionCounter|OrderPromotionDiscountType|OrderPromotionDiscountAmount|OrderPromotionOfferID|Exlusive|TagId|OrderOfferOfferID|Override|Applied|ClearAfterOverride|OrderOfferPromoID|CustomerId|OrderId|IsPaidMobileOrder|TenderQty|TenderValue|TenderCashlessCardProv|TenderCashlessAmount|TotalDiscount|TotalBD|DiscountId|DiscountType|TenderID|TenderKind|TenderName|TenderQuantity|TenderFacevalue|TenderAmount|CardType|CardNumber|CardExpirationDate";
			AUHeader = "Out_parent_node|StoreId|BusinessDate|EventRegisterId|EventTime|Status|POD|OrderTimestamp|OrderuniqueID|Orderkind|Orderkey|OrdersaleType|OrderTotalAmount|OrderTotalTax|ItemCode|ItemQty|ItemQtyPromo|ItemBPPrice|ItemBPTax|ItemBDPrice|ItemBDTax|temTotalPrice|ItemTotalTax|ItemDescription|ItemUnitPrice|ItemUnitTax|ItemPromoAppliedPromoID|ItemPromoAppliedPromotionCounter|Eligible|OriginalPrice|ItemPromoAppliedDiscountAmount|ItemPromotionAppliedDiscountType|OriginalPromoQty|OriginalProductCode|ItemPromoAppliedOfferID|OrderPromotionPromoID|OrderPromotionPromotionCounter|OrderPromotionDiscountType|OrderPromotionDiscountAmount|OrderPromotionOfferID|Exlusive|TagId|OrderOfferOfferID|Override|Applied|ClearAfterOverride|OrderOfferPromoID|CustomerId|OrderId|IsPaidMobileOrder|TenderQty|TenderValue|TenderCashlessCardProv|TenderCashlessAmount|TotalDiscount|TotalBD|DiscountId|DiscountType|TenderID|TenderKind|TenderName|TenderQuantity|TenderFacevalue|TenderAmount|CardType|CardNumber";
		}
	    int i = 0;
		
		for(Text value:values){
			if(i == 0){
				if ( outputFormat.equalsIgnoreCase("tsv") ) {
					if ( generateHeaders.equalsIgnoreCase("TRUE") ) {
						mos.write(key.toString(), NullWritable.get(), new Text(tsvHeader));
					}
				} else {
					if(key.toString().startsWith("US")){
						mos.write(key.toString(), NullWritable.get(), new Text(USHeader));
					}else{
						mos.write(key.toString(), NullWritable.get(), new Text(AUHeader));
					}
				}
			}
			mos.write(key.toString(), NullWritable.get(), value);
			
			i++;
		}
		
	}
	
	

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		  mos.close();
	}


}

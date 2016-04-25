package com.mcd.gdw.daas.util;




import java.util.ArrayList;  
import java.util.Collections;  
import java.util.Comparator;  
import java.util.List;  
import java.math.BigDecimal;

import org.apache.commons.lang.StringUtils;
   
public class PaymentMethod  {  

  private List<PaymentMethodItem> paymentMethods = new ArrayList<PaymentMethodItem>();  

  private static Comparator<PaymentMethodItem> COMPARATOR = new Comparator<PaymentMethodItem>()  {  

    public int compare(PaymentMethodItem meth1,
                       PaymentMethodItem meth2) {

      return(meth2.getPaymentAmount().compareTo(meth1.getPaymentAmount()));
    }  
  };  

  public void addUpdatePayment(String paymentMethodName,
                               String paymentAmount,
                               boolean isNegative) {

    int idx=0;
    boolean found = false;
    PaymentMethodItem item;


    while ( found == false && idx < paymentMethods.size() ) {
    
      item = paymentMethods.get(idx);

      if ( item.getPaymentMethodName().equals(paymentMethodName) ) {
        found = true; 
        item.updatePaymentAmount(paymentAmount,isNegative);
      }

      idx++;
    } 

    if ( ! found ) {
      paymentMethods.add(new PaymentMethodItem(paymentMethodName,paymentAmount));
    }
  }
  
  public void addUpdatePayment(String tenderId,String tenderKind,String paymentMethodName,String tenderQuantity,String tenderFaceValue,String paymentAmount,boolean isNegative) {

	int idx=0;
	boolean found = false;
	PaymentMethodItem item;


	while ( found == false && idx < paymentMethods.size() ) {
	
	item = paymentMethods.get(idx);
	
	if ( item.getPaymentMethodName().equals(paymentMethodName) ) {
	found = true; 
	item.updatePaymentAmount(paymentAmount,isNegative);
	}
	
	idx++;
	} 

	if ( ! found ) {
	paymentMethods.add(new PaymentMethodItem(tenderId,tenderKind,paymentMethodName,tenderQuantity,tenderFaceValue,paymentAmount));
	}	
	
}
  //mc41946- CashlessData
  public void addUpdatePayment(String tenderId,String tenderKind,String paymentMethodName,String tenderQuantity,String tenderFaceValue,String paymentAmount,String cashlessData,boolean isNegative) {

		int idx=0;
		boolean found = false;
		PaymentMethodItem item;


		while ( found == false && idx < paymentMethods.size() ) {
		
		item = paymentMethods.get(idx);
		
		if ( item.getPaymentMethodName().equals(paymentMethodName) ) {
		found = true; 
		item.updatePaymentAmount(paymentAmount,isNegative);
		}
		
		idx++;
		} 

		if ( ! found ) {
		paymentMethods.add(new PaymentMethodItem(tenderId,tenderKind,paymentMethodName,tenderQuantity,tenderFaceValue,paymentAmount,cashlessData));
		}	
		
	}

  public PaymentMethodItem maxTender() {
	  if ( paymentMethods.size() > 0 ) { 
	      Collections.sort(paymentMethods, COMPARATOR);
	      return paymentMethods.get(0);
	     
	    }

	    return null;

	  }
  
  public String maxPaymentMethod() {

    String retMethodName = "**NONE**";

    if ( paymentMethods.size() > 0 ) { 
      Collections.sort(paymentMethods, COMPARATOR);
      PaymentMethodItem itm = paymentMethods.get(0);
      retMethodName = itm.getPaymentMethodName();
    }

    return(retMethodName);

  }

  public class PaymentMethodItem {

	Integer tenderId;
	Integer tenderKind;
    String paymentMethodName;
    Integer tenderQuantity;
    BigDecimal tenderFaceValue;
    BigDecimal paymentAmount;
    //@mc41946 - Cashless Fields
    String cashlessData;

    public String getCashlessData() {
		return this.cashlessData;
	}
	
  //@mc41946 - Cashless Fields
	public PaymentMethodItem(String tenderId,String tenderKind,String tenderName,String tenderQuantity,String tenderFaceValue,String tenderAmount) {    
    	
    	if(!StringUtils.isBlank(tenderId))
    		this.tenderId = new Integer(tenderId);
    	if(!StringUtils.isBlank(tenderKind))
    		this.tenderKind = new Integer(tenderKind);
    	if(!StringUtils.isBlank(tenderName))
    		this.paymentMethodName = tenderName;
    	if(!StringUtils.isBlank(tenderQuantity))
    		this.tenderQuantity = new Integer(tenderQuantity);
    	if(!StringUtils.isBlank(tenderFaceValue))
    		this.tenderFaceValue = new BigDecimal(tenderFaceValue);
    	if(!StringUtils.isBlank(tenderAmount))
    		this.paymentAmount = new BigDecimal(tenderAmount);
    }
	
   //@mc41946 - Cashless Fields
   public PaymentMethodItem(String tenderId,String tenderKind,String tenderName,String tenderQuantity,String tenderFaceValue,String tenderAmount,String cashlessData) {    
    	
    	if(!StringUtils.isBlank(tenderId))
    		this.tenderId = new Integer(tenderId);
    	if(!StringUtils.isBlank(tenderKind))
    		this.tenderKind = new Integer(tenderKind);
    	if(!StringUtils.isBlank(tenderName))
    		this.paymentMethodName = tenderName;
    	if(!StringUtils.isBlank(tenderQuantity))
    		this.tenderQuantity = new Integer(tenderQuantity);
    	if(!StringUtils.isBlank(tenderFaceValue))
    		this.tenderFaceValue = new BigDecimal(tenderFaceValue);
    	if(!StringUtils.isBlank(tenderAmount))
    		this.paymentAmount = new BigDecimal(tenderAmount);
    	if(!StringUtils.isBlank(cashlessData))
    		this.cashlessData = cashlessData;
    }

    public PaymentMethodItem(String paymentMethodName,
                             String paymentAmount) {    

      this.paymentMethodName = paymentMethodName;
      this.paymentAmount = new BigDecimal(paymentAmount);

    }  
 
    public Integer getTendeId(){
    	return this.tenderId;
    }
    
    public Integer getTenderKind(){
    	return this.tenderKind;
    }
   
    
    public String getPaymentMethodName() {

      return(this.paymentMethodName);
    }  

    public Integer getTenderQuantity(){
    	return this.tenderQuantity;
    }
    public BigDecimal getTenderFaceValue(){
    	return this.tenderFaceValue;
    }
    public BigDecimal getPaymentAmount() { 

      return(this.paymentAmount);
    }  

    public void updatePaymentAmount(String paymentMethodAmountAdjustment,
                                    boolean subtractAmount) {
      
      if ( subtractAmount ) {
        this.paymentAmount = this.paymentAmount.subtract(new BigDecimal(paymentMethodAmountAdjustment));
      } else {
        this.paymentAmount = this.paymentAmount.add(new BigDecimal(paymentMethodAmountAdjustment));
      }
    } 
    
   
  }
  
 
 } 


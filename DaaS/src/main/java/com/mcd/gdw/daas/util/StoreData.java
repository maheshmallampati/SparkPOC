package com.mcd.gdw.daas.util;

import java.math.BigDecimal;

import com.mcd.gdw.daas.driver.NextGenEmixDriver;

public class StoreData {
	private BigDecimal totalAmount;
	private BigDecimal driveThruAmount;
	private BigDecimal frontCounterAmount;
	private BigDecimal productAmount;
	private BigDecimal nonProductAmount;
	private BigDecimal couponRdmAmount;
	private BigDecimal giftCertRdmAmount;
	private BigDecimal giftCouponAmount;
	
	private int totalCount;
	private int driveThruCount;
	private int frontCounterCount;
	private int couponRdmQty;
	private int giftCertRdmQty;
	private int giftCouponQty;
	
	public BigDecimal getTotalAmount() {
		totalAmount = (totalAmount == null)? NextGenEmixDriver.DECIMAL_ZERO : totalAmount;
		return totalAmount;
	}
	public void setTotalAmount(BigDecimal totalAmount) {
		this.totalAmount = totalAmount;
	}
	
	public BigDecimal getDriveThruAmount() {
		driveThruAmount = (driveThruAmount == null)? NextGenEmixDriver.DECIMAL_ZERO : driveThruAmount;
		return driveThruAmount;
	}
	public void setDriveThruAmount(BigDecimal driveThruAmount) {
		this.driveThruAmount = driveThruAmount;
	}
	
	public BigDecimal getFrontCounterAmount() {
		frontCounterAmount = (frontCounterAmount == null)? NextGenEmixDriver.DECIMAL_ZERO : frontCounterAmount;
		return frontCounterAmount;
	}
	public void setFrontCounterAmount(BigDecimal frontCounterAmount) {
		this.frontCounterAmount = frontCounterAmount;
	}
	
	public BigDecimal getProductAmount() {
		productAmount = (productAmount == null)? NextGenEmixDriver.DECIMAL_ZERO : productAmount;
		return productAmount;
	}
	public void setProductAmount(BigDecimal productAmount) {
		this.productAmount = productAmount;
	}
	
	
	public BigDecimal getNonProductAmount() {
		nonProductAmount = (nonProductAmount == null)? NextGenEmixDriver.DECIMAL_ZERO : nonProductAmount;
		return nonProductAmount;
	}
	public void setNonProductAmount(BigDecimal nonProductAmount) {
		this.nonProductAmount = nonProductAmount;
	}
	
	public BigDecimal getCouponRdmAmount() {
		couponRdmAmount = (couponRdmAmount == null)? NextGenEmixDriver.DECIMAL_ZERO : couponRdmAmount;
		return couponRdmAmount;
	}
	public void setCouponRdmAmount(BigDecimal couponRdmAmount) {
		this.couponRdmAmount = couponRdmAmount;
	}
	
	public BigDecimal getGiftCertRdmAmount() {
		giftCertRdmAmount = (giftCertRdmAmount == null)? NextGenEmixDriver.DECIMAL_ZERO : giftCertRdmAmount;
		return giftCertRdmAmount;
	}
	public void setGiftCertRdmAmount(BigDecimal giftCertRdmAmount) {
		this.giftCertRdmAmount = giftCertRdmAmount;
	}
	
	public BigDecimal getGiftCouponAmount() {
		giftCouponAmount = (giftCouponAmount == null)? NextGenEmixDriver.DECIMAL_ZERO : giftCouponAmount;
		return giftCouponAmount;
	}
	public void setGiftCouponAmount(BigDecimal giftCouponAmount) {
		this.giftCouponAmount = giftCouponAmount;
	}
	
	public int getTotalCount() {
		return totalCount;
	}
	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}
	
	public int getDriveThruCount() {
		return driveThruCount;
	}
	public void setDriveThruCount(int driveThruCount) {
		this.driveThruCount = driveThruCount;
	}
	
	public int getFrontCounterCount() {
		return frontCounterCount;
	}
	public void setFrontCounterCount(int frontCounterCount) {
		this.frontCounterCount = frontCounterCount;
	}
	
	public int getCouponRdmQty() {
		return couponRdmQty;
	}
	public void setCouponRdmQty(int couponRdmQty) {
		this.couponRdmQty = couponRdmQty;
	}
	
	public int getGiftCertRdmQty() {
		return giftCertRdmQty;
	}
	public void setGiftCertRdmQty(int giftCertRdmQty) {
		this.giftCertRdmQty = giftCertRdmQty;
	}
	
	public int getGiftCouponQty() {
		return giftCouponQty;
	}
	public void setGiftCouponQty(int giftCouponQty) {
		this.giftCouponQty = giftCouponQty;
	}
}

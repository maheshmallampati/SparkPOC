package com.mcd.gdw.daas.util;

import java.math.BigDecimal;

import com.mcd.gdw.daas.driver.NextGenEmixDriver;

public class PmixData {
	
	private int mitmKey;
	private String terrCd;
	private String storeId;
	private String itemCode;
	private BigDecimal dlyPmixPrice;
	private int unitsSold;
	private int comboQty;
	private int totalQty;
	private int promoQty;
	private int promoComboQty;
	private int promoTotalQty;
	private BigDecimal comboUpDownAmt;
	private BigDecimal consPrice;
	private String gdwBusinessDate;
	private String haviBusinessDate;
	private String haviTimeKey;
	private String storeType;
	private String orderKey;
	private String lvl0PmixFlag;
	private int lvl0PmixQty;
	private String allComboComponents;
	private BigDecimal comboFullPrice;
	private int menuItemCount;
	private String lvl0InCmbCmpFlag;
	private BigDecimal totalDlyPmixPrice;
	private String gdwMcdGbalLcatIdNu;
	private String posEvntTypId;
	private String itemLevel;
	private String stldItemCode;
	private String itemTaxPercent;
	private String itemTaxAmt;
	private String comboBrkDwnPrice;
	private int itemLineSeqNum;
	private String menuItemComboFlag;
	
	public int getMitmKey() {
		return mitmKey;
	}
	public void setMitmKey(int mitmKey) {
		this.mitmKey = mitmKey;
	}
	public String getTerrCd() {
		return terrCd;
	}
	public void setTerrCd(String terrCd) {
		this.terrCd = terrCd;
	}
	public String getStoreId() {
		return storeId;
	}
	public void setStoreId(String storeId) {
		this.storeId = storeId;
	}
	public String getItemCode() {
		return itemCode;
	}
	public void setItemCode(String itemCode) {
		this.itemCode = itemCode;
	}
	public BigDecimal getDlyPmixPrice() {
		dlyPmixPrice = (dlyPmixPrice == null)? NextGenEmixDriver.DECIMAL_ZERO : dlyPmixPrice;
		return dlyPmixPrice;
	}
	public void setDlyPmixPrice(BigDecimal dlyPmixPrice) {
		this.dlyPmixPrice = dlyPmixPrice;
	}
	public int getUnitsSold() {
		return unitsSold;
	}
	public void setUnitsSold(int unitsSold) {
		this.unitsSold = unitsSold;
	}
	public int getComboQty() {
		return comboQty;
	}
	public void setComboQty(int comboQty) {
		this.comboQty = comboQty;
	}
	public int getTotalQty() {
		return totalQty;
	}
	public void setTotalQty(int totalQty) {
		this.totalQty = totalQty;
	}
	public int getPromoQty() {
		return promoQty;
	}
	public void setPromoQty(int promoQty) {
		this.promoQty = promoQty;
	}
	public int getPromoComboQty() {
		return promoComboQty;
	}
	public void setPromoComboQty(int promoComboQty) {
		this.promoComboQty = promoComboQty;
	}

	public int getPromoTotalQty() {
		return promoTotalQty;
	}
	public void setPromoTotalQty(int promoTotalQty) {
		this.promoTotalQty = promoTotalQty;
	}
	public BigDecimal getComboUpDownAmt() {
		comboUpDownAmt = (comboUpDownAmt == null)? NextGenEmixDriver.DECIMAL_ZERO : comboUpDownAmt;
		return comboUpDownAmt;
	}
	public void setComboUpDownAmt(BigDecimal comboUpDownAmt) {
		this.comboUpDownAmt = comboUpDownAmt;
	}
	public BigDecimal getConsPrice() {
		return consPrice;
	}
	public void setConsPrice(BigDecimal consPrice) {
		this.consPrice = consPrice;
	}
	public String getGdwBusinessDate() {
		return gdwBusinessDate;
	}
	public void setGdwBusinessDate(String gdwBusinessDate) {
		this.gdwBusinessDate = gdwBusinessDate;
	}
	public String getHaviBusinessDate() {
		return haviBusinessDate;
	}
	public void setHaviBusinessDate(String haviBusinessDate) {
		this.haviBusinessDate = haviBusinessDate;
	}
	public String getHaviTimeKey() {
		return haviTimeKey;
	}
	public void setHaviTimeKey(String haviTimeKey) {
		this.haviTimeKey = haviTimeKey;
	}
	public String getStoreType() {
		return storeType;
	}
	public void setStoreType(String storeType) {
		this.storeType = storeType;
	}
	public String getOrderKey() {
		return orderKey;
	}
	public void setOrderKey(String orderKey) {
		this.orderKey = orderKey;
	}
	public String getLvl0PmixFlag() {
		lvl0PmixFlag = (lvl0PmixFlag == null) ? "N" : lvl0PmixFlag;
		return lvl0PmixFlag;
	}
	public void setLvl0PmixFlag(String lvl0PmixFlag) {
		this.lvl0PmixFlag = lvl0PmixFlag;
	}
	public int getLvl0PmixQty() {
		return lvl0PmixQty;
	}
	public void setLvl0PmixQty(int lvl0PmixQty) {
		this.lvl0PmixQty = lvl0PmixQty;
	}
	public String getAllComboComponents() {
		allComboComponents = (allComboComponents == null) ? "0" : allComboComponents;
		return allComboComponents;
	}
	public void setAllComboComponents(String allComboComponents) {
		this.allComboComponents = allComboComponents;
	}
	public BigDecimal getComboFullPrice() {
		comboFullPrice = (comboFullPrice == null) ? NextGenEmixDriver.DECIMAL_ZERO : comboFullPrice;
		return comboFullPrice;
	}
	public void setComboFullPrice(BigDecimal comboFullPrice) {
		this.comboFullPrice = comboFullPrice;
	}
	public int getMenuItemCount() {
		return menuItemCount;
	}
	public void setMenuItemCount(int menuItemCount) {
		this.menuItemCount = menuItemCount;
	}
	public String getLvl0InCmbCmpFlag() {
		lvl0InCmbCmpFlag = (lvl0InCmbCmpFlag == null) ? "" : lvl0InCmbCmpFlag;
		return lvl0InCmbCmpFlag;
	}
	public void setLvl0InCmbCmpFlag(String lvl0InCmbCmpFlag) {
		this.lvl0InCmbCmpFlag = lvl0InCmbCmpFlag;
	}
	public String getItemLevel() {
		return itemLevel;
	}
	public void setItemLevel(String itemLevel) {
		this.itemLevel = itemLevel;
	}
	public String getGdwMcdGbalLcatIdNu() {
		return gdwMcdGbalLcatIdNu;
	}
	public void setGdwMcdGbalLcatIdNu(String gdwMcdGbalLcatIdNu) {
		this.gdwMcdGbalLcatIdNu = gdwMcdGbalLcatIdNu;
	}
	public String getComboBrkDwnPrice() {
		return comboBrkDwnPrice;
	}
	public void setComboBrkDwnPrice(String comboBrkDwnPrice) {
		this.comboBrkDwnPrice = comboBrkDwnPrice;
	}
	public BigDecimal getTotalDlyPmixPrice() {
		totalDlyPmixPrice = (totalDlyPmixPrice == null) ? NextGenEmixDriver.DECIMAL_ZERO : totalDlyPmixPrice;
		return totalDlyPmixPrice;
	}
	public void setTotalDlyPmixPrice(BigDecimal totalDlyPmixPrice) {
		this.totalDlyPmixPrice = totalDlyPmixPrice;
	}
	public String getPosEvntTypId() {
		return posEvntTypId;
	}
	public void setPosEvntTypId(String posEvntTypId) {
		this.posEvntTypId = posEvntTypId;
	}
	public String getStldItemCode() {
		return stldItemCode;
	}
	public void setStldItemCode(String stldItemCode) {
		this.stldItemCode = stldItemCode;
	}
	public String getItemTaxPercent() {
		return itemTaxPercent;
	}
	public void setItemTaxPercent(String itemTaxPercent) {
		this.itemTaxPercent = itemTaxPercent;
	}
	public String getItemTaxAmt() {
		return itemTaxAmt;
	}
	public void setItemTaxAmt(String itemTaxAmt) {
		this.itemTaxAmt = itemTaxAmt;
	}
	public int getItemLineSeqNum() {
		return itemLineSeqNum;
	}
	public void setItemLineSeqNum(int itemLineSeqNum) {
		this.itemLineSeqNum = itemLineSeqNum;
	}
	public String getMenuItemComboFlag() {
		menuItemComboFlag = (menuItemComboFlag == null) ? "0" : menuItemComboFlag;
		return menuItemComboFlag;
	}
	public void setMenuItemComboFlag(String menuItemComboFlag) {
		this.menuItemComboFlag = menuItemComboFlag;
	}
}
package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.mcd.gdw.daas.DaaSConstants;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.daas.util.SimpleEncryptAndDecrypt;

public class PIIFieldsMapper extends Mapper<LongWritable, Text, Text, Text> {

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private InputSource xmlSource = null;
	private Document doc = null;
	private StringReader strReader = null;

	private String[] parts = null;
	private String[] cashlessParts;

	// private String terrCd;
	private String lgcyLclRfrDefCd;
	private String terrCd;
	private String posBusnDt;
	private String orderTimestamp;
	private String orderDate;
	private String orderTime;
	private String kind;
	private BigDecimal posTotNetTrnAm;
	private String cashlessData;

	private boolean skip;

	private BigDecimal totCashAm;
	private BigDecimal totCashlessAm;
	private int trnCashQty;
	private int trnCashlessQty;

	private Element eleRoot;
	private Element eleNode;
	private Element eleEvent;
	private Element eleTrx;
	private Element eleOrder;
	private Element eleTenders;
	Element eleOrderItems;
	private Element eleTender;
	private Element eleTenderItem;

	private Node nodeText;

	private Text keyOut = new Text();
	private Text valueOut = new Text();
	// @mc41946
	SimpleEncryptAndDecrypt encryptString = null;
	private StringBuffer customerId = new StringBuffer();
	private String storeId;
	private MultipleOutputs<Text, Text> mos;
	Date curDate = new Date();
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	String gdwExtractedDate;

	@Override
	public void setup(Context context) {

		try {
			mos = new MultipleOutputs<Text, Text>(context);
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			encryptString = new SimpleEncryptAndDecrypt();
			gdwExtractedDate = format.format(curDate);

		} catch (Exception ex) {
			System.err.println("Error in initializing CashlessDataMapper:");
			System.err.println(ex.toString());
			System.exit(8);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			parts = value.toString().split("\t");

			if (parts.length >= 8) {
				// terrCd = parts[DaaSConstants.XML_REC_TERR_CD_POS];
				lgcyLclRfrDefCd = parts[DaaSConstants.XML_REC_LGCY_LCL_RFR_DEF_CD_POS];
				posBusnDt = parts[DaaSConstants.XML_REC_POS_BUSN_DT_POS];
				terrCd = parts[DaaSConstants.XML_REC_TERR_CD_POS];

				crewIdList.clear();
				crewNameList.clear();
				managerIdList.clear();
				managerNameList.clear();

				getData(parts[DaaSConstants.XML_REC_XML_TEXT_POS], context);

				context.getCounter("StoreCount", "File Count").increment(1);

				// Writing out data after calling all methods.
				// System.out.println(managerIdList.size());
				String crewIdListString = StringUtils.join(crewIdList, ":");
				String crewNameListString = StringUtils.join(crewNameList, ":");
				String managerIdListString = StringUtils.join(managerIdList,
						":");
				String managerNameListString = StringUtils.join(
						managerNameList, ":");
				String customerIdListString = StringUtils.join(
						customerIdList, ":");

				mosKey.setLength(0);
				mosKey.append(storeId);
				mosKey.append(DaaSConstants.PIPE_DELIMITER);
				mosKey.append(posBusnDt);
				mosKey.append(DaaSConstants.PIPE_DELIMITER);
				mosKey.append(terrCd);

				mosValue.setLength(0);
				mosValue.append(storeId);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(posBusnDt);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(terrCd);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(crewIdListString);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(crewNameListString);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(managerIdListString);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(managerNameListString);
				mosValue.append(DaaSConstants.PIPE_DELIMITER);
				mosValue.append(customerIdListString);

				outputkey.clear();
				outputvalue.clear();

				outputkey.set(HDFSUtil.replaceMultiOutSpecialChars(mosKey
						.toString()));
				outputvalue.set(mosValue.toString());
				context.write(outputkey, outputvalue);
				// mos.write( outputkey.toString(),NullWritable.get(),
				// outputvalue);

			}

		} catch (Exception ex) {
			System.err.println("Error occured in CashlessDataMapper.Map:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	StringBuffer mosKey = new StringBuffer();
	StringBuffer mosValue = new StringBuffer();
	Text outputkey = new Text();
	Text outputvalue = new Text();

	private void getData(String xmlText, Context context) {

		try {
			try {
				strReader = new StringReader(xmlText);
				xmlSource = new InputSource(strReader);
				doc = docBuilder.parse(xmlSource);

			} catch (Exception ex1) {
				return;
				
			}

			eleRoot = (Element) doc.getFirstChild();

			if (eleRoot.getNodeName().equals("TLD")) {
				storeId = eleRoot.getAttribute("storeId");
				/*
				 * if(!storeId.equalsIgnoreCase("28636")) return;
				 */

				processNode(eleRoot.getChildNodes(), context);

			}

		} catch (Exception ex) {
			System.err.println("Error occured in CashlessDataMapper.getData:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}
	}

	private void processNode(NodeList nlNode, Context context) throws Exception {

		if (nlNode != null && nlNode.getLength() > 0) {
			for (int idxNode = 0; idxNode < nlNode.getLength(); idxNode++) {
				if (nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE) {
					eleNode = (Element) nlNode.item(idxNode);
					if (eleNode.getNodeName().equals("Node")) {
						processEvent(eleNode.getChildNodes(), context);
					}
				}
			}
		}

	}

	String eventType = "";
	String eventTimeStamp = "";
	String regId = "";

	private void processEvent(NodeList nlEvent, Context context)
			throws Exception {

		if (nlEvent != null && nlEvent.getLength() > 0) {
			for (int idxEvent = 0; idxEvent < nlEvent.getLength(); idxEvent++) {
				if (nlEvent.item(idxEvent).getNodeType() == Node.ELEMENT_NODE) {
					eleEvent = (Element) nlEvent.item(idxEvent);
					if (eleEvent.getNodeName().equals("Event")) {
						eventType = eleEvent.getAttribute("Type");
						eventTimeStamp = eleEvent.getAttribute("Time");
						regId = eleEvent.getAttribute("RegId");

						if (eleEvent.getAttribute("Type").equalsIgnoreCase(
								"TRX_Sale")
								|| eleEvent.getAttribute("Type")
										.equalsIgnoreCase("TRX_Refund")
								|| eleEvent.getAttribute("Type")
										.equalsIgnoreCase("TRX_Overring")
								|| eleEvent.getAttribute("Type")
										.equalsIgnoreCase("TRX_Waste")) {
							eventType = eleEvent.getAttribute("Type");
							processTrxSale(eleEvent, eleEvent.getChildNodes(),
									context);
						} else {
							processTrx(eleEvent.getChildNodes(), eventType,
									regId, context);
						}
					}
				}
			}
		}

	}

	private void processTrx(NodeList nlTrxSale, String eventType, String redId,
			Context context) {

		if (nlTrxSale != null && nlTrxSale.getLength() > 0) {
			for (int idxTrxSale = 0; idxTrxSale < nlTrxSale.getLength(); idxTrxSale++) {
				if (nlTrxSale.item(idxTrxSale).getNodeType() == Node.ELEMENT_NODE) {
					eleTrx = (Element) nlTrxSale.item(idxTrxSale);
					// System.out.println("-------"+eleTrx.getChildNodes());
					processDetails(eleTrx.getChildNodes(), eventType, regId,
							context);

				}
			}
		}

	}

	private String posAreaTypShrtDs;

	private void processTrxSale(Element eleEvent, NodeList nlTrxSale,
			Context context) throws Exception {

		eleTrx = null;

		if (nlTrxSale != null && nlTrxSale.getLength() > 0) {
			for (int idxTrxSale = 0; idxTrxSale < nlTrxSale.getLength(); idxTrxSale++) {
				if (nlTrxSale.item(idxTrxSale).getNodeType() == Node.ELEMENT_NODE) {
					eleTrx = (Element) nlTrxSale.item(idxTrxSale);

					// if (
					// eleEvent.getAttribute("Type").equalsIgnoreCase("TRX_Sale")){
					// if(eleTrx.getAttribute("status").equals("Paid") ) {
					posAreaTypShrtDs = eleTrx.getAttribute("POD");

					if (eleEvent.getAttribute("Type").equalsIgnoreCase(
							"TRX_Sale")) {
						if (!eleTrx.getAttribute("status").equals("Paid")) {
							return;// if TRX_Sale, consider only the elements
									// with status Paid.
						}
					}
					processOrder(eleTrx.getChildNodes(), context);
					// }
					// }
				}
			}
		}

	}

	private void processOrder(NodeList nlOrder, Context context)
			throws Exception {

		eleOrder = null;

		try {
			if (nlOrder != null && nlOrder.getLength() > 0) {
				for (int idxOrder = 0; idxOrder < nlOrder.getLength(); idxOrder++) {
					if (nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE) {

						eleOrder = (Element) nlOrder.item(idxOrder);
						processOrderItems(eleOrder.getChildNodes(), context);
					}
				}
			}
		} catch (Exception ex) {
			System.err.println("Error occured in ProcessOrder");
			ex.printStackTrace(System.err);
			throw ex;
			// System.exit(8);
		}
	}

	private String crewName = "";
	private String crewId = "";
	private String managerName = "";
	private String managerId = "";

	HashSet<String> crewIdList = new HashSet<String>();
	HashSet<String> crewNameList = new HashSet<String>();
	HashSet<String> managerNameList = new HashSet<String>();
	HashSet<String> managerIdList = new HashSet<String>();
	HashSet<String> customerIdList = new HashSet<String>();

	private void processDetails(NodeList nlOrder, String eventType,
			String redId, Context context) {
		// System.out.println("Inside TRX ");

		try {
			if (nlOrder != null && nlOrder.getLength() > 0) {
				for (int idxOrder = 0; idxOrder < nlOrder.getLength(); idxOrder++) {
					if (nlOrder.item(idxOrder).getNodeType() == Node.ELEMENT_NODE) {
						eleOrder = (Element) nlOrder.item(idxOrder);
						// System.out.println("eventType----->"+eventType+" --->"+redId
						// +" ------->"+eleOrder.getNodeName());
						if (eleOrder.getNodeName().equalsIgnoreCase("CrewId")) {
							crewId = eleOrder.getTextContent();
							/*
							 * nodeText = eleOrder.getFirstChild();
							 * 
							 * if ( nodeText != null && nodeText.getNodeType()
							 * == Node.TEXT_NODE ) { crewId =
							 * nodeText.getTextContent();
							 */

							if (crewId != null)
								crewIdList.add(crewId.trim());
							// System.out.println("CrewId --> " + crewId);

						}
						if (eleOrder.getNodeName().equalsIgnoreCase("CrewName")) {

							crewName = eleOrder.getTextContent();

							/*
							 * if ( nodeText != null && nodeText.getNodeType()
							 * == Node.TEXT_NODE ) { crewName =
							 * nodeText.getTextContent();
							 */

							if (crewName != null && !crewName.trim().equals(""))
								crewNameList.add(crewName.trim());
							// System.out.println("crewName --> " + crewName);

						}
						if (eleOrder.getNodeName()
								.equalsIgnoreCase("ManagerID")) {
							/*
							 * if ( nodeText != null && nodeText.getNodeType()
							 * == Node.TEXT_NODE ) { managerId =
							 * nodeText.getTextContent();
							 */
							managerId = eleOrder.getTextContent();
							if (managerId != null)
								managerIdList.add(managerId.trim());
							System.out.println("managerId --> " + managerId);
						}

						if (eleOrder.getNodeName().equalsIgnoreCase(
								"ManagerName")) {
							/*
							 * if ( nodeText != null && nodeText.getNodeType()
							 * == Node.TEXT_NODE ) { managerName =
							 * nodeText.getTextContent();
							 */
							managerId = eleOrder.getTextContent();
							if (managerName != null
									&& !managerName.trim().equals(""))
								managerNameList.add(managerName.trim());
							// System.out.println("managerName --> " +
							// managerName);

						}
					}
				}
			}
		} catch (Exception ex) {
			System.err
					.println("Error occured in CashlessDataMapper.processOrder:");
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}

	private String offrCustId;

	private boolean processOrderItems(NodeList nlOrderItems, Context context)
			throws Exception {

		boolean isCashless = false;

		if (nlOrderItems != null && nlOrderItems.getLength() > 0) {
			for (int idxOrderItems = 0; idxOrderItems < nlOrderItems
					.getLength(); idxOrderItems++) {
				if (nlOrderItems.item(idxOrderItems).getNodeType() == Node.ELEMENT_NODE) {
					eleOrderItems = (Element) nlOrderItems.item(idxOrderItems);

					if (eleOrderItems.getNodeName().equals("Customer")) {

						offrCustId = getValue(eleOrderItems, "id");
						customerIdList.add(offrCustId);
					}

				}
			}
		}

		return (isCashless);

	}

	private boolean processTenders(NodeList nlTender, Context context)
			throws Exception {

		boolean isCashless = false;

		if (nlTender != null && nlTender.getLength() > 0) {
			for (int idxTender = 0; idxTender < nlTender.getLength(); idxTender++) {
				if (nlTender.item(idxTender).getNodeType() == Node.ELEMENT_NODE) {
					eleTender = (Element) nlTender.item(idxTender);

					isCashless = processTender(eleTender.getChildNodes(),
							context);
				}
			}
		}

		return (isCashless);

	}

	private boolean processTender(NodeList nlTenderItem, Context context)
			throws Exception {

		boolean isCashless = false;

		if (nlTenderItem != null && nlTenderItem.getLength() > 0) {
			for (int idxTenderItem = 0; idxTenderItem < nlTenderItem
					.getLength() && !isCashless; idxTenderItem++) {
				if (nlTenderItem.item(idxTenderItem).getNodeType() == Node.ELEMENT_NODE) {
					eleTenderItem = (Element) nlTenderItem.item(idxTenderItem);

					if (eleTenderItem.getNodeName().equals("CashlessData")) {
						nodeText = eleTenderItem.getFirstChild();

						if (nodeText != null
								&& nodeText.getNodeType() == Node.TEXT_NODE) {
							cashlessData = nodeText.getNodeValue();

							cashlessParts = cashlessData.split("\\|");

							if (cashlessParts[0].startsWith("CASHLESS")) {
								isCashless = true;
								/*
								 * } //if ( cashlessParts[0].endsWith("Visa") &&
								 * cashlessParts[1].endsWith("0301") &&
								 * cashlessParts[2].equals("03/17") ) { if (
								 * cashlessParts[0].endsWith("Visa") ) {
								 */
								/*
								 * customerId.setLength(0);
								 * customerId.append(cashlessParts[0].substring(
								 * cashlessParts[0].length()-4));
								 * customerId.append(cashlessParts[1].substring(
								 * cashlessParts[1].length()-4));
								 * customerId.append(cashlessParts[2].replace(
								 * "/", "")); customerId.append(storeId);
								 */
								keyOut.clear();
								keyOut.set("B");
								valueOut.clear();
								// valueOut.set(orderDate + "\t" + orderTime +
								// "\t" + lgcyLclRfrDefCd + "\t" +
								// posTotNetTrnAm.toString()+"\t"+encryptString.encryptAsHexString(customerId.toString())+"\t"+customerId.toString());
								valueOut.set(orderDate + "\t" + orderTime
										+ "\t" + lgcyLclRfrDefCd + "\t"
										+ posTotNetTrnAm.toString());
								context.write(keyOut, valueOut);
							}
						}

					}
				}
			}
		}

		return (isCashless);
	}

	private String convertToDelimitedText(ArrayList<String> list) {
		StringBuilder rString = new StringBuilder();
		String sep = ",";
		for (String each : list) {
			rString.append(sep).append(each);
		}

		return rString.toString();
	}

	private String getValue(Element ele, String attribute) {

		String retValue = "";

		try {
			retValue = ele.getAttribute(attribute);

		} catch (Exception ex) {
		}

		return (retValue.trim());
	}
}

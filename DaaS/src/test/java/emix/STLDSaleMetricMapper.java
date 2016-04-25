package emix;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;


@SuppressWarnings("deprecation")
public class STLDSaleMetricMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private static final BigDecimal DECIMAL_ZERO = new BigDecimal("0.00");


	private Text mapKey = new Text();
	private Text mapValue = new Text();
	private Text mapKeyhourly = new Text();
	private Text mapValuehourly = new Text();
	private Text mapKeyPmix = new Text();
	private Text mapValuePmix = new Text();

	private DocumentBuilderFactory docFactory = null;
	private DocumentBuilder docBuilder = null;
	private Document doc = null;

	private Element eleRoot;
	private NodeList nlNode;
	private Element eleNode;
	private NodeList nlEvent;
	private Element eleEvent;
	private String eventType;
	private Element eleTrx;
	private Element eleOrder;
	private NodeList nlTRX;
	private NodeList nlOrder;

	private String terrCd = "";
	private String lgcyLclRfrDefCd = "";
	private String businessDate = "";
	private String status = "";
	private String kind = "";
	private String currencyCode = "";
	private String itemCode = "";
	private String tenderName = "";
	private String trxPOD = "";
	private String orderTimeStamp = "";

	private BigDecimal tranTotalAmount = DECIMAL_ZERO;
	private BigDecimal totalAmount = DECIMAL_ZERO;
	private BigDecimal nonProductValue = DECIMAL_ZERO;
	private BigDecimal totalPrice = DECIMAL_ZERO;
	private BigDecimal tenderAmount = DECIMAL_ZERO;
	private BigDecimal counterAmount = DECIMAL_ZERO;
	private BigDecimal driveThruAmount = DECIMAL_ZERO;
	private BigDecimal couponAmount = DECIMAL_ZERO;

	private BigDecimal subItemTotalPrice = DECIMAL_ZERO;
	private BigDecimal unitPrice = DECIMAL_ZERO;

	private int tranCount = 0;
	private int itemCount = 0;
	private int itemQty = 0;
	private int minutes = 0;
	private int tenderKind = 0;
	private int seg_code = 100;
	private int hourValue = 0;
	private int counterCount = 0;
	private int tenderQuantity = 0;
	private int driveThruCount = 0;
	private int couponQuantity = 0;
	private int subItemQty = 0;

	//private String itemQtyPromo = "";


	private String menuItem = "";
	private String hourFormat = "";
	private String hour = "";
	String subItemCode = "";

	String timeSegment[] = { "", "00:00:00", "00:30:00", "01:00:00",
			"01:30:00", "02:00:00", "02:30:00", "03:00:00", "03:30:00",
			"04:00:00", "04:30:00", "05:00:00", "05:30:00", "06:00:00",
			"06:30:00", "07:00:00", "07:30:00", "08:00:00", "08:30:00",
			"09:00:00", "09:30:00", "10:00:00", "10:30:00", "11:00:00",
			"11:30:00", "12:00:00", "12:30:00", "13:00:00", "13:30:00",
			"14:00:00", "14:30:00", "15:00:00", "15:30:00", "16:00:00",
			"16:30:00", "17:00:00", "17:30:00", "18:00:00", "18:30:00",
			"19:00:00", "19:30:00", "20:00:00", "20:30:00", "21:00:00",
			"21:30:00", "22:00:00", "22:30:00", "23:00:00", "23:30:00" };
	String[] TrxKind = { "TRX_Sale", "TRX_Refund", "TRX_Overring" };

	Map<String, String> itemCodeLkp = null;
	Map<String, String> currencyCodeLkp = null;
	Map<String, String> primaryMenuLkp = null;
	Map<String, String> itemPriceLkp = null;
	Map<String, String> comboItemLkp = null;
	Map<String, String> genericDrinkLkp = null;

	public void setup(Context context) throws IOException, InterruptedException {

		BufferedReader br = null;
		String input_data_lkp = "";
		Configuration conf = context.getConfiguration();

		URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		itemCodeLkp = new HashMap<String, String>();
		currencyCodeLkp = new HashMap<String, String>();
		primaryMenuLkp = new HashMap<String, String>();
		itemPriceLkp = new HashMap<String, String>();

		comboItemLkp = new HashMap<String, String>();
		genericDrinkLkp = new HashMap<String, String>();

		// Build hashmap lookup for primary menu, currency code and item code
		for (int fileCounter = 0; fileCounter < cacheFiles.length; fileCounter++) {

			br = new BufferedReader(new FileReader(new Path(
					cacheFiles[fileCounter].getPath()).toString()));
			while ((input_data_lkp = br.readLine()) != null) {

				String[] lookUpData = input_data_lkp.toString().split("\\|");
				if (lookUpData.length > 1) {
					if (fileCounter == 0)
						currencyCodeLkp.put(lookUpData[0], lookUpData[1]);
					else if (fileCounter == 1)
						itemCodeLkp.put(lookUpData[1], lookUpData[2]);
					else if (fileCounter == 2)
						primaryMenuLkp.put(lookUpData[3], lookUpData[0]);
					else if (fileCounter == 3) {
						String temp = "";
						if (comboItemLkp.containsKey(lookUpData[0])) {
							temp = comboItemLkp.get(lookUpData[0]);
							comboItemLkp.put(lookUpData[0], temp
									+ STLDConstants.PIPE_DELIMETER
									+ lookUpData[2]);
						} else
							comboItemLkp.put(lookUpData[0], lookUpData[2]);
					} else if (fileCounter == 4) {
						itemPriceLkp.put(lookUpData[0] + lookUpData[3],
								lookUpData[2]);
					} else if (fileCounter == 5) {
						genericDrinkLkp.put(lookUpData[0], lookUpData[1]);
					}

				}
			}
			br.close();
		}
	}

	@SuppressWarnings("null")
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String xmlText = value.toString().substring(
				value.toString().indexOf("<"));

		BigDecimal[] hourlyDriveThruAmount = new BigDecimal[50];
		BigDecimal[] hourlyCounterAmount = new BigDecimal[50];
		BigDecimal[] hourlyTotalAmount = new BigDecimal[50];
		int[] hourlyCounterCount = new int[50];
		int[] hourlyDriveThruCount = new int[50];
		int[] hourlyTotalCount = new int[50];
		tranTotalAmount = DECIMAL_ZERO;
		tranCount = 0;
		driveThruCount = 0;
		counterCount = 0;
		driveThruAmount = DECIMAL_ZERO;
		counterAmount = DECIMAL_ZERO;
		nonProductValue = DECIMAL_ZERO;
		tenderAmount = DECIMAL_ZERO;
		unitPrice = DECIMAL_ZERO;
		totalPrice = DECIMAL_ZERO;
		itemCount = 0;
		tenderQuantity = 0;
		couponAmount = DECIMAL_ZERO;
		couponQuantity = 0;
		tenderKind = 0;
		subItemTotalPrice = DECIMAL_ZERO;
		String subItemCode = "";
		String subItemCode1 = "";
		//BigDecimal subItemPrice = DECIMAL_ZERO;

		String[] comboMenuItem1 = null;

		int pos_key_qt1 = 0;
		int total_key_qty = 0;
		int total_combo_count = 0;
		BigDecimal DLY_PMIX_PRC_AM1 = DECIMAL_ZERO; // Sum Of all Item
													// Prices // without
													// quantity
		BigDecimal POS_KEY_PRC1 = DECIMAL_ZERO; // Actual Price

		try {
			docFactory = DocumentBuilderFactory.newInstance();

			docBuilder = docFactory.newDocumentBuilder();
			doc = docBuilder.parse(new InputSource(new StringReader(xmlText)));

			eleRoot = (Element) doc.getFirstChild();

			if (eleRoot.getNodeName().equals("TLD")) {
				terrCd = eleRoot.getAttribute("gdwTerrCd");
				if (currencyCodeLkp.containsKey(terrCd)) {
					currencyCode = (String) currencyCodeLkp.get(terrCd);

				}
				/* Remove Leading zeros for store id */
				lgcyLclRfrDefCd = String.valueOf(Integer.parseInt(eleRoot
						.getAttribute("gdwLgcyLclRfrDefCd")));

				businessDate = eleRoot.getAttribute("gdwBusinessDate");

				nlNode = eleRoot.getChildNodes();
				if (nlNode != null && nlNode.getLength() > 0) {
					for (int idxNode = 0; idxNode < nlNode.getLength(); idxNode++) {
						if (nlNode.item(idxNode).getNodeType() == Node.ELEMENT_NODE) {
							eleNode = (Element) nlNode.item(idxNode);
							if (eleNode.getNodeName().equals("Node")) {

								nlEvent = eleNode.getChildNodes();
								if (nlEvent != null && nlEvent.getLength() > 0) {
									for (int idxEvent = 0; idxEvent < nlEvent
											.getLength(); idxEvent++) {
										if (nlEvent.item(idxEvent)
												.getNodeType() == Node.ELEMENT_NODE) {
											eleEvent = (Element) nlEvent
													.item(idxEvent);

											if (eleEvent.getNodeName().equals(
													"Event")) {
												eventType = eleEvent
														.getAttribute("Type");

												if (eventType
														.equals("TRX_Sale")
														|| eventType
																.equals("TRX_Refund")
														|| eventType
																.equals("TRX_Overring")) {
													eleTrx = null;
													nlTRX = eleEvent
															.getChildNodes();
													int idxTRX = 0;

													while (eleTrx == null
															&& idxTRX < nlTRX
																	.getLength()) {
														if (nlEvent
																.item(idxTRX)
																.getNodeType() == Node.ELEMENT_NODE) {
															eleTrx = (Element) nlTRX
																	.item(idxTRX);
															status = eleTrx
																	.getAttribute("status")
																	+ "";
															trxPOD = eleTrx
																	.getAttribute("POD");
														}
														idxTRX++;
													}

													if ((eventType
															.equals("TRX_Sale") && status
															.equals("Paid"))
															|| (eventType
																	.equals("TRX_Refund") || eventType
																	.equals("TRX_Overring"))) {
														eleOrder = null;
														nlOrder = eleTrx
																.getChildNodes();
														int idxOrder = 0;

														while (eleOrder == null
																&& idxOrder < nlOrder
																		.getLength()) {
															if (nlOrder
																	.item(idxOrder)
																	.getNodeType() == Node.ELEMENT_NODE) {
																eleOrder = (Element) nlOrder
																		.item(idxOrder);
															}
															idxOrder++;
														}

														totalAmount = new BigDecimal(
																eleOrder.getAttribute("totalAmount"));

														totalAmount = totalAmount
																.subtract(new BigDecimal(
																		eleOrder.getAttribute("nonProductAmount")));

														kind = eleOrder
																.getAttribute("kind");
														orderTimeStamp = eleOrder
																.getAttribute("Timestamp");

														if (!orderTimeStamp
																.equals("")
																&& !orderTimeStamp
																		.equals(null)) {
															hour = orderTimeStamp
																	.substring(
																			8,
																			10);
															minutes = Integer
																	.parseInt(orderTimeStamp
																			.substring(
																					10,
																					12));
															hourValue = Integer
																	.valueOf(hour);
															if (minutes < 30) {
																seg_code = (hourValue * 2) + 1;

															} else if (minutes >= 30) {

																seg_code = (hourValue * 2) + 2;

															}

															if (hourlyDriveThruAmount[seg_code] == null) {
																hourlyDriveThruAmount[seg_code] = (DECIMAL_ZERO);
															}
															if (hourlyTotalAmount[seg_code] == null) {
																hourlyTotalAmount[seg_code] = (DECIMAL_ZERO);
															}
															if (hourlyCounterAmount[seg_code] == null) {
																hourlyCounterAmount[seg_code] = (DECIMAL_ZERO);
															}
														}

														if (eventType
																.equals("TRX_Sale")) {

															if (kind.contains("Manager")
																	|| kind.contains("Crew")) {
																if (totalAmount
																		.compareTo(DECIMAL_ZERO) != 0) {
																	tranCount++;
																	hourlyTotalCount[seg_code]++;

																	if (trxPOD
																			.equals("Drive Thru")) {
																		driveThruCount++;
																		hourlyDriveThruCount[seg_code]++;

																	} else {
																		counterCount++;
																		hourlyCounterCount[seg_code]++;

																	}

																}
															} else {
																tranCount++;
																hourlyTotalCount[seg_code]++;

																if (trxPOD
																		.equals("Drive Thru")) {
																	driveThruCount++;
																	hourlyDriveThruCount[seg_code]++;

																} else {
																	counterCount++;
																	hourlyCounterCount[seg_code]++;

																}

															}

															tranTotalAmount = tranTotalAmount
																	.add(totalAmount);
															hourlyTotalAmount[seg_code] = hourlyTotalAmount[seg_code]
																	.add(totalAmount);

															nonProductValue = nonProductValue
																	.add(new BigDecimal(
																			eleOrder.getAttribute("nonProductAmount")));
															if (trxPOD
																	.equals("Drive Thru")) {
																driveThruAmount = driveThruAmount
																		.add(totalAmount);
																hourlyDriveThruAmount[seg_code] = hourlyDriveThruAmount[seg_code]
																		.add(totalAmount);

															} else {
																counterAmount = counterAmount
																		.add(totalAmount);
																hourlyCounterAmount[seg_code] = hourlyCounterAmount[seg_code]
																		.add(totalAmount);

															}
														}

														if (eventType
																.equals("TRX_Overring")) {
															tranCount--;
															hourlyTotalCount[seg_code]--;

															if (trxPOD
																	.equals("Drive Thru")) {
																driveThruCount--;
																hourlyDriveThruCount[seg_code]--;
															} else {
																counterCount--;
																hourlyCounterCount[seg_code]--;
															}

														}

														if (eventType
																.equals("TRX_Refund")
																|| eventType
																		.equals("TRX_Overring")) {
															tranTotalAmount = tranTotalAmount
																	.subtract(totalAmount);
															hourlyTotalAmount[seg_code] = hourlyTotalAmount[seg_code]
																	.subtract(totalAmount);
															if (trxPOD
																	.equals("Drive Thru")) {
																driveThruAmount = driveThruAmount
																		.subtract(totalAmount);
																hourlyDriveThruAmount[seg_code] = hourlyDriveThruAmount[seg_code]
																		.subtract(totalAmount);

															}

															else {
																counterAmount = counterAmount
																		.subtract(totalAmount);

																hourlyCounterAmount[seg_code] = hourlyCounterAmount[seg_code]
																		.subtract(totalAmount);

															}
														}

														NodeList ItemList = (eleOrder == null ? null
																: eleOrder
																		.getElementsByTagName("Item"));

														if (ItemList != null) {

															for (int s3 = 0; s3 < ItemList
																	.getLength(); s3++) {

																Element ItemElm = (Element) ItemList
																		.item(s3);

																if (ItemElm
																		.getAttribute(
																				"level")
																		.equals("0")) {
																	if (ItemList
																			.getLength() > 1) {

																		Element subItemElm = (Element) ItemList
																				.item(s3);
																		NodeList subItemList = (subItemElm == null ? null
																				: subItemElm
																						.getElementsByTagName("Item"));
																		subItemCode = "0";
																		subItemCode1 = "";

																		if (ItemElm
																				.getAttribute(
																						"level")
																				.equals("0")
																				&& subItemList
																						.getLength() >= 1) {

																			if (primaryMenuLkp
																					.containsKey(ItemElm
																							.getAttribute("code"))) {
																				menuItem = primaryMenuLkp
																						.get(ItemElm
																								.getAttribute("code"));
																			} else {
																				menuItem = ItemElm
																						.getAttribute("code");
																			}
																			subItemTotalPrice = new BigDecimal(
																					ItemElm.getAttribute("totalPrice"));

																			subItemQty = Integer
																					.parseInt(ItemElm
																							.getAttribute("qty"));
																			if (subItemQty > 0) {

																				unitPrice = subItemTotalPrice
																						.divide(new BigDecimal(
																								subItemQty),
																								2,
																								RoundingMode.HALF_UP);

																			}

																			for (int subItem = 0; subItem < subItemList
																					.getLength(); subItem++) {
																				Element subItemElement = (Element) subItemList
																						.item(subItem);

																				if (ItemElm
																						.getAttribute(
																								"level")
																						.equals("0")
																						&& subItemElement
																								.getAttribute(
																										"level")
																								.equals("1")) {

																					subItemCode = subItemElement
																							.getAttribute("code");
																					/*subItemPrice = new BigDecimal(
																							subItemElement
																									.getAttribute("totalPrice"));*/
																				}

																				if (subItemCode1
																						.equals("")) {
																					subItemCode1 = subItemElement
																							.getAttribute("code");
																					subItemTotalPrice = subItemTotalPrice
																							.add(new BigDecimal(
																									subItemElement
																											.getAttribute("totalPrice")));

																				} else {
																					subItemCode1 = subItemCode1
																							+ STLDConstants.PIPE_DELIMETER
																							+ subItemElement
																									.getAttribute("code");
																					subItemTotalPrice = subItemTotalPrice
																							.add(new BigDecimal(
																									subItemElement
																											.getAttribute("totalPrice")));

																				}

																			}

																		}

																	}

																	else if (ItemElm
																			.getAttribute(
																					"level")
																			.equals("0")
																			&& ItemList
																					.getLength() == 1) {
																		if (primaryMenuLkp
																				.containsKey(ItemElm
																						.getAttribute("code"))) {
																			menuItem = primaryMenuLkp
																					.get(ItemElm
																							.getAttribute("code"));
																		} else {
																			menuItem = ItemElm
																					.getAttribute("code");
																		}
																		subItemTotalPrice = new BigDecimal(
																				ItemElm.getAttribute("totalPrice"));
																		subItemQty = Integer
																				.parseInt(ItemElm
																						.getAttribute("qty"));
																		if (subItemQty > 0) {

																			unitPrice = subItemTotalPrice
																					.divide(new BigDecimal(
																							subItemQty),
																							2,
																							RoundingMode.HALF_UP);
																			System.out
																					.println("LLL"
																							+ unitPrice);

																		}
																	}

																	// Logic
																	// Ammended
																	// for
																	// Generic
																	// Drink

																	if (ItemElm
																			.hasAttributes()) {

																		NamedNodeMap nodeMapItem = ItemElm
																				.getAttributes();

																		for (int k = 0; k < nodeMapItem
																				.getLength(); k++) {

																			Node nodeItem = nodeMapItem
																					.item(k);
																			String NodeNameItem = nodeItem
																					.getNodeName()
																					.toString();

																			if (eventType
																					.equals("TRX_Sale")) {

																				if (NodeNameItem
																						.equals("qty")) {
																					itemQty = Integer
																							.parseInt(nodeItem
																									.getNodeValue());
																				}


																				/*if (NodeNameItem
																						.equals("qtyPromo")) {
																					itemQtyPromo = nodeItem
																							.getNodeValue();
																				}*/

																				if (NodeNameItem
																						.equals("totalPrice")
																						&& itemCodeLkp
																								.containsKey(itemCode)) {
																					// Modified by: Khaja

																					totalPrice = totalPrice
																							.add(new BigDecimal(
																									nodeItem.getNodeValue()));
																					itemCount = itemCount
																							+ itemQty;
																				}

																			}

																		}

																	}

																	if (comboItemLkp
																			.containsKey(menuItem))

																	{

																		if (subItemQty > 0) {

																			pos_key_qt1 = subItemQty;
																			total_key_qty = subItemQty;
																			DLY_PMIX_PRC_AM1 = subItemTotalPrice
																					.divide(new BigDecimal(
																							subItemQty),
																							2,
																							RoundingMode.HALF_UP);
																																						POS_KEY_PRC1 = unitPrice;
																			total_combo_count = 0;

																			mapKeyPmix
																					.set((new StringBuffer(
																							lgcyLclRfrDefCd)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(businessDate)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(menuItem
																									.trim())
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(Integer
																									.valueOf((DLY_PMIX_PRC_AM1
																											.multiply(new BigDecimal(
																													100)))
																											.intValue())))
																							.toString());
																			mapValuePmix
																					.set((new StringBuffer(
																							businessDate)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(terrCd)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(currencyCode)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(lgcyLclRfrDefCd)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(menuItem
																									.trim())
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append("alacarte")
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(pos_key_qt1)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(total_key_qty)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(total_combo_count)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(DLY_PMIX_PRC_AM1)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(POS_KEY_PRC1)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(STLDConstants.PMIX))
																							.toString());

																			context.write(
																					mapKeyPmix,
																					mapValuePmix);

																		}
																		comboMenuItem1 = comboItemLkp
																				.get(menuItem)
																				.split("\\|");

																		for (int i = 0; i < comboMenuItem1.length; i++)

																		{

																			if (genericDrinkLkp
																					.containsKey(comboMenuItem1[i])) {

																				// DLY_PMIX_PRC_AM1=subItemPrice;
																				// POS_KEY_PRC1=
																				// subItemPrice;
																				if (itemPriceLkp
																						.containsKey(menuItem
																								+ subItemCode)) {

																					DLY_PMIX_PRC_AM1 = new BigDecimal(
																							itemPriceLkp
																									.get(menuItem
																											+ subItemCode));
																					POS_KEY_PRC1 = new BigDecimal(
																							itemPriceLkp
																									.get(menuItem
																											+ subItemCode));
																				} else {

																					mapKeyPmix
																							.clear();
																					mapKeyPmix
																							.set((new StringBuffer(
																									lgcyLclRfrDefCd)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(businessDate)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(subItemCode
																											.trim())
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(STLDConstants.Errors))
																									.toString());

																					mapValuePmix
																							.clear();
																					mapValuePmix
																							.set((new StringBuffer(
																									businessDate)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(terrCd)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(currencyCode)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(lgcyLclRfrDefCd)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(subItemCode
																											.trim())
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(STLDConstants.Errors))
																									.toString());
																					context.write(
																							mapKeyPmix,
																							mapValuePmix);

																				}

																				total_combo_count = subItemQty;
																				total_key_qty = subItemQty;
																				pos_key_qt1 = 0;

																				mapKeyPmix
																						.clear();
																				mapKeyPmix
																						.set((new StringBuffer(
																								lgcyLclRfrDefCd)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(businessDate)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(subItemCode
																										.trim())
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(Integer
																										.valueOf((DLY_PMIX_PRC_AM1
																												.multiply(new BigDecimal(
																														100)))
																												.intValue())))
																								.toString());
																				mapValuePmix
																						.clear();
																				mapValuePmix
																						.set((new StringBuffer(
																								businessDate)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(terrCd)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(currencyCode)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(lgcyLclRfrDefCd)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(subItemCode
																										.trim())
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append("combo")
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(pos_key_qt1)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(total_key_qty)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(total_combo_count)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(DLY_PMIX_PRC_AM1)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(POS_KEY_PRC1)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(STLDConstants.PMIX))
																								.toString());
																				context.write(
																						mapKeyPmix,
																						mapValuePmix);

																			} else {

																				// DLY_PMIX_PRC_AM1=new
																				// BigDecimal(itemPrice1.get(menuItem+comboMenuItem1[i]));
																				// POS_KEY_PRC1=
																				// new
																				// BigDecimal(itemPrice1.get(menuItem+comboMenuItem1[i]));

																				if (itemPriceLkp
																						.containsKey(menuItem
																								+ comboMenuItem1[i])) {

																					DLY_PMIX_PRC_AM1 = new BigDecimal(
																							itemPriceLkp
																									.get(menuItem
																											+ comboMenuItem1[i]));
																					POS_KEY_PRC1 = new BigDecimal(
																							itemPriceLkp
																									.get(menuItem
																											+ comboMenuItem1[i]));
																				} else {

																					mapKeyPmix
																							.clear();
																					mapKeyPmix
																							.set((new StringBuffer(
																									lgcyLclRfrDefCd)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(businessDate)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(comboMenuItem1[i]
																											.trim())
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(STLDConstants.Errors))
																									.toString());

																					mapValuePmix
																							.clear();
																					mapValuePmix
																							.set((new StringBuffer(
																									businessDate)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(terrCd)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(currencyCode)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(lgcyLclRfrDefCd)
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(comboMenuItem1[i]
																											.trim())
																									.append(STLDConstants.PIPE_DELIMETER)
																									.append(STLDConstants.Errors))
																									.toString());
																					context.write(
																							mapKeyPmix,
																							mapValuePmix);
																				}

																				total_combo_count = subItemQty;
																				total_key_qty = subItemQty;
																				pos_key_qt1 = 0;

																				mapKeyPmix
																						.clear();
																				mapKeyPmix
																						.set((new StringBuffer(
																								lgcyLclRfrDefCd)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(businessDate)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(comboMenuItem1[i]
																										.trim())
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(Integer
																										.valueOf((DLY_PMIX_PRC_AM1
																												.multiply(new BigDecimal(
																														100)))
																												.intValue())))
																								.toString());
																				mapValuePmix
																						.clear();
																				mapValuePmix
																						.set((new StringBuffer(
																								businessDate)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(terrCd)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(currencyCode)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(lgcyLclRfrDefCd)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(comboMenuItem1[i]
																										.trim())
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append("combo")
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(pos_key_qt1)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(total_key_qty)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(total_combo_count)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(DLY_PMIX_PRC_AM1)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(POS_KEY_PRC1)
																								.append(STLDConstants.PIPE_DELIMETER)
																								.append(STLDConstants.PMIX))
																								.toString());
																				context.write(
																						mapKeyPmix,
																						mapValuePmix);
																			}

																		}
																	} else {

																		if ((subItemQty) > 0) {

																			pos_key_qt1 = subItemQty;
																			total_key_qty = subItemQty;

																			POS_KEY_PRC1 = unitPrice;

																			DLY_PMIX_PRC_AM1 = unitPrice;
																			total_combo_count = 0;

																			mapKeyPmix
																					.clear();
																			mapKeyPmix
																					.set((new StringBuffer(
																							lgcyLclRfrDefCd)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(businessDate)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(menuItem
																									.trim())
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(Integer
																									.valueOf((DLY_PMIX_PRC_AM1
																											.multiply(new BigDecimal(
																													100)))
																											.intValue())))
																							.toString());
																			mapValuePmix
																					.clear();
																			mapValuePmix
																					.set((new StringBuffer(
																							businessDate)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(terrCd)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(currencyCode)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(lgcyLclRfrDefCd)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(menuItem
																									.trim())
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append("alacarte")
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(pos_key_qt1)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(total_key_qty)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(total_combo_count)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(DLY_PMIX_PRC_AM1)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(POS_KEY_PRC1)
																							.append(STLDConstants.PIPE_DELIMETER)
																							.append(STLDConstants.PMIX))
																							.toString());
																			context.write(
																					mapKeyPmix,
																					mapValuePmix);

																		}
																	}
																}

															}

														}

														NodeList TendersList = (eleOrder == null ? null
																: eleOrder
																		.getElementsByTagName("Tenders"));

														if (TendersList != null) {

															for (int tList = 0; tList < TendersList
																	.getLength(); tList++) {
																Element TendersElm = (Element) TendersList
																		.item(tList);

																NodeList TenderList = TendersElm
																		.getElementsByTagName("Tender");
																if (TenderList != null
																		|| TenderList
																				.getLength() > 0) {
																	for (int tenderList = 0; tenderList < TenderList
																			.getLength(); tenderList++) {

																		Element TenderElm = (Element) TenderList
																				.item(tenderList);
																		NodeList TenderTagList = TenderElm
																				.getChildNodes();

																		if (TenderTagList != null
																				|| TenderTagList
																						.getLength() > 0) {
																			for (int tendertagList = 0; tendertagList < TenderTagList
																					.getLength(); tendertagList++) {
																				Node Tenderchild = (Node) TenderTagList
																						.item(tendertagList);
																				if (eventType
																						.equals("TRX_Sale")) {

																					if (Tenderchild
																							.getNodeName()
																							.equals("TenderName")) {

																						tenderName = Tenderchild
																								.getTextContent();

																					}

																					if (Tenderchild
																							.getNodeName()
																							.equals("TenderKind")) {

																						tenderKind = Integer
																								.parseInt(Tenderchild
																										.getTextContent());

																					}

																					if (Tenderchild
																							.getNodeName()
																							.equals("TenderQuantity")
																							&& tenderName
																									.equals("Gift Card")) {

																						tenderQuantity = tenderQuantity
																								+ (Integer
																										.parseInt(Tenderchild
																												.getTextContent()));

																					}
																					if (Tenderchild
																							.getNodeName()
																							.equals("TenderAmount")
																							&& tenderName
																									.equals("Gift Card")) {

																						tenderAmount = tenderAmount
																								.add(new BigDecimal(
																										Tenderchild
																												.getTextContent()));

																					}
																					// coupon
																					// Logic
																					if (Tenderchild
																							.getNodeName()
																							.equals("TenderQuantity")
																							&& tenderKind == 8) {

																						couponQuantity = couponQuantity
																								+ (Integer
																										.parseInt(Tenderchild
																												.getTextContent()));

																					}
																					if (Tenderchild
																							.getNodeName()
																							.equals("FaceValue")
																							&& tenderKind == 8) {

																						couponAmount = couponAmount
																								.add(new BigDecimal(
																										Tenderchild
																												.getTextContent()));

																					}

																				}
																			}
																		}
																	}

																}
															}
														}
													}

												}
											}
										}
									}
								}
							}
						}
					}
				}
				// Daily Sales Mapper Output
				mapKey.clear();

				mapKey.set((new StringBuffer(lgcyLclRfrDefCd)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(businessDate)).toString());
				mapValue.clear();
				mapValue.set((new StringBuffer(businessDate)
						.append(STLDConstants.PIPE_DELIMETER).append(terrCd)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(currencyCode)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(lgcyLclRfrDefCd)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(tranTotalAmount.toString())
						.append(STLDConstants.PIPE_DELIMETER).append(tranCount)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(tenderAmount.toString())
						.append(STLDConstants.PIPE_DELIMETER)
						.append(tenderQuantity)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(nonProductValue.toString())
						.append(STLDConstants.PIPE_DELIMETER)
						.append(totalPrice.toString())
						.append(STLDConstants.PIPE_DELIMETER).append(itemCount)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(counterAmount.toString())
						.append(STLDConstants.PIPE_DELIMETER)
						.append(counterCount)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(driveThruAmount.toString())
						.append(STLDConstants.PIPE_DELIMETER)
						.append(driveThruCount)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(couponAmount)
						.append(STLDConstants.PIPE_DELIMETER)
						.append(couponQuantity)
						.append(STLDConstants.PIPE_DELIMETER).append(STLDConstants.DAILY))
						.toString());
				context.write(mapKey, mapValue);

				// Half-Hourly Mapper Output
				for (int seg_id = 1; seg_id <= 48; seg_id++) {

					if (seg_id / 2 != 0) {

						hourFormat = timeSegment[seg_id];

					} else {

						hourFormat = timeSegment[seg_id];

					}
					if (hourlyTotalAmount[seg_id] != null) {
						mapKeyhourly.clear();
						// Modified by Khaja . Added Business Date to Key
						mapKeyhourly.set((new StringBuffer(lgcyLclRfrDefCd)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(businessDate)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourFormat)).toString());
						mapValuehourly.clear();
						mapValuehourly.set((new StringBuffer(businessDate)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(terrCd)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(currencyCode)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(lgcyLclRfrDefCd)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourFormat)
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyTotalAmount[seg_id])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyTotalCount[seg_id])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyCounterAmount[seg_id])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyCounterCount[seg_id])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyDriveThruAmount[seg_id])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(hourlyDriveThruCount[seg_id])
								.append(STLDConstants.PIPE_DELIMETER)
								.append(STLDConstants.HOURLY)).toString());
						context.write(mapKeyhourly, mapValuehourly);
					}

				}

			}

		} catch (Exception ex) {
			System.err.println("Error in EmixSalesMapper.getSalesMetric:");
			ex.printStackTrace(System.err);
			System.exit(8);
		} finally {
			doc = null;
		}
	}

}